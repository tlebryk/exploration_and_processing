"""
If you're working on different machines, overwrite the ROOTPATH to whatever path you have on your machine.
.\nSupport HKFP | Code of Ethics | Error/typo? | Contact Us | Newsletter | Transparency & Annual Report'}

"""
import re

import boto3
from botocore.exceptions import ClientError
import logging
import os
import pandas as pd
import time
from io import StringIO


logger = logging.getLogger(__name__)

ROOTPATH = r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data"


class Publication:
    """Standardizes retreival of disparately named columns."""

    def __init__(self, name, textcol, uidcol, hdcol) -> None:
        self.name = name
        self.textcol = textcol
        self.uidcol = uidcol
        self.headline = hdcol
        # self.authorcol = authcol

    def __repr__(self) -> str:
        return self.name


publications = {
    "scmp": Publication("scmp", "Body", "Index", "Headline"),
    "nyt": Publication("nyt", "text", "sourceurl", "title"),
    "globaltimes": Publication("globaltimes", "Body", "Art_id", "Title"),
    "chinadaily": Publication("chinadaily", "plainText", "id", "title"),
    "hkfp": Publication("hkfp", "Body", "Art_id", "Headline"),
}

s3 = boto3.client("s3")

LOOKUP = dict(
    Index="Art_id",
    id="Art_id",
    x_id="Art_id",
    sourceurl="Art_id",
    plainText="Body",
    text="Body",
    title="Headline",  # nyt and gt?
)


def upload_s3(filepath, bucket="newyorktime", key=None):
    """Uploads a file to s3"""
    if not key:
        key = filepath
    try:
        logger.info("uploading from %s to %s : %s", filepath, bucket, key)
        response = s3.upload_file(filepath, bucket, key)
        return response
    except ClientError as e:
        logger.exception(e)


def df_to_s3(df, key, bucket="newyorktime"):
    """Directly saves a dataframe to a csv on s3 without saving locally."""
    logger.info("uploading to s3://%s/%s", bucket, key)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())


def get_perc(a, b):
    """Return percent a out of a+b to 3 decs"""
    return round(a / (a + b), 3) * 100


def report(a, b, preprint="", postprint=""):
    """prints: a/a+b as percent and full fraction & returns
    :param a: numerator
    :param b: remaining value added to a for demonenator
    :param preprint: str to print before fraction report
    :param postpring: str to print acter fraction report
    """
    perc = get_perc(a, b)
    if preprint:
        print(preprint)
    print(perc, "%")
    print(a, "/", a + b)
    if postprint:
        print(postprint)
    return perc

def tts_match(df, pub: Publication, baba=False):
    """Gets the train 
    :param df: a standardized dataframe.
    """
    try:
        # gotta navigate baba here..
        if baba: 
            basepath = os.path.join(ROOTPATH, "baba", pub.name, "tts_mask")
        else:
            basepath = os.path.join(ROOTPATH, pub.name, "tts_mask")

        train = standardize(pd.read_csv(os.path.join(basepath, "train_main1.csv")), pub)
        test = standardize(pd.read_csv(os.path.join(basepath, "test_main1.csv")), pub)
    except FileNotFoundError as fe:
        logger.warning("couldn't find tts mask locally; reading from s3")
        if baba:
            bucket = "aliba"
        else:
            bucket = "newyorktime"
        
        train = standardize(read_df_s3(f"{pub.name}/tts_mask/train_main1.csv", bucket), pub)
        test = standardize(read_df_s3(f"{pub.name}/tts_mask/test_main1.csv", bucket), pub)
    mask = df.Art_id.isin(train.Art_id)
    df.loc[mask, 'tts'] = "train"
    mask = df.Art_id.isin(test.Art_id)
    df.loc[mask, 'tts'] = "test"
    df.tts.value_counts(dropna=False)
    df.tts = df.tts.fillna("drops")
    df.tts = df.tts.astype("category")
    return df

def get_df(publication, *args):
    """
    Gets dataframe from publication.name/args*
    :param: args are strings which are joined to be the final path
        e.g. "polimask", "pmask_.csv" becomes rootpath/publication/polimask/pmask_.csv
        If no args are given, default to full csv.
    """
    if not args:
        args = [f"{publication.name}_full.csv"]
    fullpath = os.path.join(ROOTPATH, publication.name, *args)
    df = pd.read_csv(fullpath)
    # get training and test information
    # drop the drops for scmp
    # if publication.name == "scmp" and drops:
    #     drop_df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\tts_mask\drops_main1.csv")
    #     mask = ~df.Index.isin(drop_df.Index)
    #     df = drop_report(df, mask)
    return df


def justletters(s):
    return re.sub(r"[^A-Za-z]+", "", s)


def timeit(fn, *args, **kwargs):
    """Takes a function and optional args
    and returns output but prints time
    """
    s = time.perf_counter()
    ret = fn(*args, **kwargs)
    e = time.perf_counter()
    try:
        print(f"{fn.__name__} took {e-s} secs")
    except AttributeError as ae:
         print(f"{fn} took {e-s} secs")
    return ret


def read_df_s3(object_key, bucket="newyorktime", pubdefault=None):
    """Reads a csv from s3 and loads into pandas;
    Means do not have to store large files locally anymore.
    :param pubdefault: pass a publication and overrides object_key
        fn to just read the main df from that publication.
        lazy and hacky but deal with it.
    """
    if pubdefault:
        object_key = f"{pubdefault.name}/{pubdefault.name}_full.csv"
    logger.info("reading from s3://%s/%s", bucket, object_key)
    csv_obj = s3.get_object(Bucket=bucket, Key=object_key)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_string))
    return df


def cleanbody(df, pub: Publication, drop_dups=True):
    """Some common cleaning for the maindf (i.e. one with a Body col).
        returns a df with bodylower (body just lowercase) and bodyalphabet
        (just alphabetical chars, no spaces, punctuation or numbers) cols.
    :param df: standard df with common column names already looked up.
    """
    # generic cleaning
    if "Body" in df.columns:
        df.Body = df.Body.astype(str)
        shortmask = df.Body.str.len().gt(100)
        df = drop_report(df, shortmask, "body < 100 chars")
        # in tts_mask, maybe we need a pure duplication mask
        # based on Art_id & Body
        # or we could redo our tts entirely...
        # and a sections mask for scmp and global times
        if drop_dups:
            dupmask = ~df["Body"].duplicated()
            df = drop_report(df, dupmask, "Body duplicates")
        df["bodylower"] = df.Body.str.lower()
        df["bodyalphabet"] = df["bodylower"].str.replace('[^a-zA-Z]', '')
    else:
        logger.warning("Didn't find body col - did you mean to call cleanbody?")
    return df


def standardize(df: pd.DataFrame, pub: Publication, drop_dups=True):
    """Standardizes column values for following columns:
        - Art_id
        - Body
        - Headline
    not yet standardize topics
    """
    logger.info("working on %s", pub.name)
    df = (
        df.rename(columns=LOOKUP).assign(
            Publication=pub.name,
        )
        # .drop("Unnamed: 0", axis=1, errors="ignore")
    )
    if drop_dups:
        df = df.drop_duplicates("Art_id")
    return df


def drop_report(df, mask, preprint=""):
    """Prints number of rows dropped by a mask and returns filtered df
    Mask is True for keeps, False for drops
    """
    preprint += "Dropping rows:"
    try:
        report(mask.value_counts().loc[False], mask.value_counts().loc[True], preprint)
    except KeyError as ke:
        print("nothing to drop")
    return df[mask]
    # print(f"Dropping {mask.value_counts().loc[False]} / {len(maindf)} rows ()")


def main_date_load(pub, baba=False, *args):
    """Merges the main df with the datedf,

    :param baba: if true and nyt, cd, or gt, correct date filepath
    :param args: optional args to read from different location
        for main df [e.g. nyt, cd, gt need to read from baba/pub_full.csv]
        meaning args should be "baba", f"{pub.name}_full.csv.
        remember get_df defaults to just pub.name_full.csv pub data dir.
    """
    if baba: # and pub.name in ("nyt", "chinadaily", "globaltimes")
        bucket = "aliba"
    else:
        bucket = "newyorktime"
    datedf = read_df_s3(f"{pub.name}/date/date.csv", bucket)
    datedf = standardize(datedf, pub)
    df = read_df_s3(None, bucket, pubdefault=pub)
    df = standardize(df, pub)
    # df = standardize(get_df(pub, *args), pub)
    df = df.drop("Date", axis=1, errors="ignore")
    df["Textcol"] = df.Headline + "; " + df.Body
    mask = df.Textcol.notna()
    print("keeping non-na text cols:")
    print(mask.value_counts())
    df = df[mask]
    # get other masks 
    hkmask = standardize(read_df_s3(f"{pub.name}/hk_mask/hkmask.csv") , pub)
    polimask = standardize(read_df_s3(f"{pub.name}/polimask/pmask_.csv"), pub)
    df = df.merge(polimask, on="Art_id", how="left")
    df['hkmask'] = df.Art_id.isin(hkmask.Art_id)
    # avoid dup cols
    datedf = datedf.drop("Publication", axis=1, errors="ignore")
    df = df.merge(datedf, on="Art_id", how="left")
    df.Date = pd.to_datetime(df.Date, infer_datetime_format=True)
    # str preprocessing
    df = cleanbody(df, pub)
    # get tts mask, adds to tts col
    df = tts_match(df, pub, baba)
    return df

    # get datedf
    # dt = (
    #     utils.get_df(pub, "date", "date.csv")
    #     .rename(columns=LOOKUP)
    #     .drop_duplicates("Art_id")
    #     # .drop("Unnamed: 0", axis=1, errors="ignore")
    # )
    # df2 = df.merge(dt, on="Art_id", how="inner")
    # # get polimask
    # poli = (
    #     utils.get_df(pub, "polimask", "pmask_.csv")
    #     .rename(columns=LOOKUP)
    #     .drop("Unnamed: 0", axis=1, errors="ignore")
    #     .drop_duplicates("Art_id")
    # )
    # df = df.merge(poli, on="Art_id")

    # # get hkmask
    # hk = (
    #     utils.get_df(pub, "hk_mask", "hkmask.csv")
    #     .rename(columns=LOOKUP)
    #     .drop_duplicates("Art_id")
    # )
    # df["hk"] = df.Art_id.isin(hk.Art_id)
    # # get ttsmask
    # tts = (
    #     utils.get_df(pub, "tts_mask", "train_main1.csv")
    #     .rename(columns=LOOKUP)
    #     .drop_duplicates("Art_id")
    # )
    # df["train"] = df.Art_id.isin(tts.Art_id)
    # keeps = [
    #     "Art_id",
    #     "Headline",
    #     "Textcol",
    #     "Date",
    #     "Year",
    #     "Publication",
    #     "post_baba",
    #     "baba_ownership",
    #     "poliestimation",
    #     "train",
    #     "hk",
    # ]
    # df = df.filter(keeps, axis=1)
    # dfls.append(df)
