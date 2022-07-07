"""
If you're working on different machines, overwrite the ROOTPATH to whatever path you have on your machine.
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
        response = s3.upload_file(filepath, bucket, key)
        return response
    except ClientError as e:
        logger.exception(e)


def df_to_s3(df, key, bucket="newyorktime"):
    """Directly saves a dataframe to a csv on s3 without saving locally."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())


def get_perc(a, b):
    """Return percent a out of a+b to 3 decs"""
    return round(a / (a + b), 3) * 100


def report(a, b):
    """prints: a/a+b as percent and full fraction & returns"""
    perc = get_perc(a, b)
    print(perc, "%")
    print(a, "/", a + b)
    return perc


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
    print(f"{fn.__name__} took {e-s} secs")
    return ret


def read_df_s3(object_key, bucket="newyorktime"):
    """Reads a csv from s3 and loads into pandas;
    Means do not have to store large files locally anymore.
    """
    csv_obj = s3.get_object(Bucket=bucket, Key=object_key)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_string))
    return df


def standardize(df: pd.DataFrame, pub: Publication, drop_dups=True):
    """Standardizes column values for following columns:
        - Art_id
        - Body
        - Headline
    not yet standardize topics
    """

    df = (
        df.rename(columns=LOOKUP)
        .assign(
            Publication=pub.name,
        )
        .drop("Unnamed: 0", axis=1, errors="ignore")
    )
    if drop_dups:
        df = df.drop_duplicates("Art_id")
    return df


def main_date_load(pub, baba=False, *args):
    """Merges the main df with the datedf,

    :param baba: if true and nyt, cd, or gt, correct date filepath
    :param args: optional args to read from different location
        for main df [e.g. nyt, cd, gt need to read from baba/pub_full.csv]
        meaning args should be "baba", f"{pub.name}_full.csv.
        remember get_df defaults to just pub.name_full.csv pub data dir.
    """
    df = standardize(get_df(pub, *args), pub)
    df = df.drop("Date", axis=1, errors="ignore")
    df["Textcol"] = df.Headline + "; " + df.Body
    mask = df.Textcol.notna()
    print("keeping non-na text cols:")
    print(mask.value_counts())
    df = df[mask]
    if baba and pub.name in ("nyt", "chinadaily", "globaltimes"):
        datedf = standardize(get_df(pub, "baba", "date", "date.csv"), pub)
    else:
        datedf = standardize(get_df(pub, "date", "date.csv"), pub)
    # avoid dup cols
    datedf = datedf.drop("Publication", axis=1, errors="ignore")
    df = df.merge(datedf, on="Art_id")
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
