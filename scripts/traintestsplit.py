"""Splits the data into train_main1, test_main1, and drop_main1.csv. 
Run on both the HK corpus (.1:.9 train:test) and the baba corpus (.5:.5). 
Drops include duplicates and wrong section
"""
from thesisutils import utils
import os
from sklearn.model_selection import train_test_split
import pandas as pd

# %%
# TRAINTESTSPLITtrain_test_split
def tts(publication, df=None, splitname="", path=None, *args, **kwargs):
    """Train test splits data for a publication
    :param args: arguments to be passed to train_test_split
    """
    if df is None:
        df = utils.get_df(
            publication, f"{publication.name}_full.csv"
        )  # "polimask", "pmask_.csv")
    train, test = train_test_split(df[publication.uidcol], *args, **kwargs)
    if not path:
        path = os.path.join(utils.ROOTPATH, publication.name, "tts_mask")
        if not os.path.exists(path):
            os.makedirs(path)
    print("saving to ", path)
    train.to_csv(os.path.join(path, f"train_{splitname}.csv"))
    test.to_csv(os.path.join(path, f"test_{splitname}.csv"))
    return train, test


def traintestdrops3(pub, bucket, train, test, drops=None):
    """saves train, test, and potentially drops dataframes to s3."""
    utils.df_to_s3(train, key=f"{pub.name}/tts_mask/train_main1.csv", bucket=bucket)
    utils.df_to_s3(test, key=f"{pub.name}/tts_mask/test_main1.csv", bucket=bucket)
    if drops is None:
        return None
    utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/tts_mask/drops_main1.csv", bucket=bucket)


# %% cleaning for each publication
# HK CORPUS RUN ######################################
# scmp
def go():
    kwargs = dict(random_state=1, test_size=0.9)
    bucket="newyorktime"
    # %% 
    # scmp
    pub = utils.publications["scmp"]
    df = utils.get_df(pub)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    # result: no duplicates
    maskna = df[pub.textcol].isna()
    sections = utils.get_df(pub, *["sections", "sections.csv"])
    strsects = (
        sections.filter(regex="section", axis=1)
        .fillna("")
        .astype(str)
        .agg(", ".join, axis=1)
    )
    # remove non-news sections
    masksec = strsects.str.contains(
        "comment|opinion|cooking|letters|food-drink|style|sport|books|movies|hk-magazine|yp"
    ) 
    drops = utils.drop_report(df, maskna | masksec)
    keeps = utils.drop_report(df, ~maskna & ~masksec)
    train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
    drops[pub.uidcol].to_csv(
        os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    )
    traintestdrops3(pub, bucket, train, test, drops)

    # %%
    # chinadaily
    pub = utils.publications["chinadaily"]
    df = utils.get_df(pub)
    # 1080 masked value counts. 
    maskst = df.storyType.astype(str).str.contains("VIDEO|AUDIO|HREF|INNERLINK")
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    sectwords = "Op-Ed|Opinion|Comment|Editorial|Video|Life|Sports|Art|Advertorial|Food|Movies"
    opmask = (
        df.assign(
            columnchannel = lambda x: x.channelName.str.cat(x.columnName.astype(str))
    )
    # .str.lower()
    # .str.replace("-", ""))
    .columnchannel.str.contains(sectwords)
    )
    opmask.value_counts()
    # 315 duplicates
    drops = df[maskst | dupmask | opmask]
    keeps = df[~maskst & ~dupmask & ~opmask]
    keeps.channelName.str.contains("Comment").value_counts()
    train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
    drops[pub.uidcol].to_csv(
        os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    )
    traintestdrops3(pub, bucket, train, test, drops)
    # %%
    # nyt
    pub = utils.publications["nyt"]
    df = utils.get_df(pub)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    # 418 duplicates
    drops = df[dupmask]
    keeps = df[~dupmask]
    train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
    drops[pub.uidcol].to_csv(
        os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    )
    traintestdrops3(pub, bucket, train, test, drops)
    # %%
    # hkfp
    pub = utils.publications["hkfp"]
    df = utils.get_df(pub)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    df.Section.str.split(",").explode().value_counts(dropna=False)
    sectwords = "Opinion|Sport|Animals|Lifestyle|Arts|Video|Humour|Editorials|Travel"
    masksec = df.Section.astype(str).str.contains(sectwords)
    drops = df[dupmask | masksec]
    keeps = df[~dupmask & ~masksec]
    train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
    drops[pub.uidcol].to_csv(
        os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    )
    traintestdrops3(pub, bucket, train, test, drops)

    # %%
    # globaltimes
    pub = utils.publications["globaltimes"]
    df = utils.read_df_s3(None, pubdefault=pub)
    df.Section = df.Section.astype(str)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    sectwords = "OPINION|SPORT|Comment|Viewpoint|ARTS|Film|LIFE|Soccer|Art|Books|Golf|Culture & Leisure|Food|TV|VIDEO|Tennis|Fashion|Basketball|Letters|Dance|GT Voice|Opinion"
    masksec = df.Section.str.contains(sectwords)
    yrmask = df.assign(
        year = pd.to_datetime(df.Date).dt.year,
        yrmask = lambda d: d.year.lt(2011) | d.year.gt(2021), axis=1
    ).yrmask
    drops = df[dupmask | masksec | yrmask]
    keeps = df[~dupmask & ~masksec & ~yrmask]
    # no duplciates :)
    train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
    # drops[pub.uidcol].to_csv(
    #     os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    # )
    traintestdrops3(pub, bucket, train, test, drops)

    # utils.df_to_s3(train, key=f"{pub.name}/tts_mask/train_main1.csv", bucket="aliba")
    # utils.df_to_s3(test, key=f"{pub.name}/tts_mask/test_main1.csv", bucket="aliba")
    # utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/tts_mask/drops_main1.csv", bucket="aliba")


# go()


# %%
# ALIBABA RUN ####################
def babarun(tts):
    kwargs = {"random_state": 1, "test_size": 0.5}
    bucket="aliba"
    # %%
    # SCMP
    pub = utils.publications["scmp"]
    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket=bucket)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    # result: no duplicates
    maskna = df[pub.textcol].isna()
    maskna.value_counts()
    # no nas :)
    # KEEP ALL SECTIONS FOR NOW.
    sections = utils.get_df(pub, *["sections", "sections.csv"])
    sections = sections.merge(df[["Index", "Url"]], on="Index")
    # sections = sections[sections.Index.isin(df.Index)]
    # sections.set_index("Index")
    strsects = (
        sections.filter(regex="section", axis=1)
        .fillna("")
        .astype(str)
        .agg(", ".join, axis=1)
    )

    masksec = strsects.str.contains(
        "comment|opinion|cooking|letters|food-drink|style|sport"
    )  # |books|movies|hk-magazine|yp,")
    masksec.value_counts()
    maskna.value_counts()

    drops = df[maskna | masksec]
    keeps = df[~maskna & ~masksec]
    path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
    drops[pub.uidcol].to_csv(os.path.join(path, "drops_main1.csv"))
    traintestdrops3(pub, bucket, train, test, drops)
    # %%
    # chinadaily
    pub = utils.publications["chinadaily"]
    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket=bucket)
    maskst = df.storyType.astype(str).str.contains("VIDEO|AUDIO|HREF|INNERLINK")
    dupmask = df[pub.uidcol].duplicated()
    sectwords = "Op-Ed|Opinion|Comment|Editorial|Video|Life|Sports|Art|Advertorial|Food|Movies"
    opmask = (
        df.assign(
            columnchannel = lambda x: x.channelName.str.cat(x.columnName.astype(str))
    )
    # .str.lower()
    # .str.replace("-", ""))
    .columnchannel.str.contains(sectwords)
    )
    opmask.value_counts()
    dupmask.value_counts()
    maskst.value_counts()
    # 152 duplicates, 151 wrong story type
    drops = df[maskst | dupmask | opmask]
    keeps = df[~maskst & ~dupmask & ~opmask]
    path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
    drops[pub.uidcol].to_csv(os.path.join(path, "drops_main1.csv"))
    traintestdrops3(pub, bucket, train, test, drops)


    # %%
    # nyt
    pub = utils.publications["nyt"]
    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket=bucket)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    # 6 duplicates
    drops = df[dupmask]
    keeps = df[~dupmask]
    path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
    drops[pub.uidcol].to_csv(os.path.join(path, "drops_main1.csv"))
    traintestdrops3(pub, bucket, train, test, drops)

    # %%
    # hkfp
    pub = utils.publications["hkfp"]
    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket=bucket)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    sectwords = "Opinion|Sport|Animals|Lifestyle|Arts|Video|Humour|Editorials|Travel"
    masksec = df.Section.astype(str).str.contains(sectwords)
    masksec.value_counts(dropna=False)
    drops = df[dupmask | masksec]
    keeps = df[~dupmask & ~masksec]
    # EMPTY no duplicates :)
    path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    drops[pub.uidcol].to_csv(os.path.join(path, "drops_main1.csv"))

    # keeps = df
    train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
    traintestdrops3(pub, bucket, train, test, drops)

    # %%
    # globaltimes
    pub = utils.publications["globaltimes"]

    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket=bucket)
    df.Section = df.Section.astype(str)
    dupmask = df[pub.uidcol].duplicated()
    dupmask.value_counts()
    sectwords = "OPINION|SPORT|Comments|Viewpoint|ARTS|Film|LIFE|Soccer|Art|Books|Golf|Culture & Leisure|Food|TV|VIDEO|Tennis|Fashion|Basketball|Letters|Dance|GT Voice|Opinion"
    masksec = df.Section.str.contains(sectwords)
    yrmask = df.assign(
        year = pd.to_datetime(df.Date).dt.year,
        yrmask = lambda d: d.year.lt(2011) | d.year.gt(2021), axis=1
    ).yrmask
    drops = df[dupmask | masksec | yrmask]
    keeps = df[~dupmask & ~masksec & ~yrmask]
     # no duplciates :)
    path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    # no duplciates :)
    train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
    # drops[pub.uidcol].to_csv(
    #     os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
    # )
    traintestdrops3(pub, bucket, train, test, drops)
    drops[pub.uidcol].to_csv(os.path.join(path, "drops_main1.csv"))

babarun(tts)
