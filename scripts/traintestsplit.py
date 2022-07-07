from thesisutils import utils
import os
from sklearn.model_selection import train_test_split



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


kwargs = dict(random_state=1, test_size=0.9)

# %% cleaning for each publication
# HK CORPUS RUN ######################################
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

masksec = strsects.str.contains(
    "comment|opinion|cooking|letters|food-drink|style|sport"
)  # |books|movies|hk-magazine|yp,")
drops = df[maskna | masksec]
keeps = df[~maskna & ~masksec]
tts(pub, df=keeps, splitname="main1", **kwargs)
drops[pub.uidcol].to_csv(
    os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
)
# %%
# chinadaily
pub = utils.publications["chinadaily"]
df = utils.get_df(pub)
maskst = df.storyType.astype(str).str.contains("VIDEO|AUDIO|HREF|INNERLINK")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# 315 duplicates
drops = df[maskst | dupmask]
keeps = df[~maskst & ~dupmask]
train, test = tts(pub, df=keeps, splitname="main1", **kwargs)
drops[pub.uidcol].to_csv(
    os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
)
# %%
# nyt
pub = utils.publications["nyt"]
df = utils.get_df(pub)
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# 418 duplicates
drops = df[dupmask]
keeps = df[~dupmask]
tts(pub, df=keeps, splitname="main1", **kwargs)
drops[pub.uidcol].to_csv(
    os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv")
)
# %%
# hkfp
pub = utils.publications["hkfp"]
df = utils.get_df(pub)
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# no duplicates :)
tts(pub, splitname="main1", **kwargs)
# %%
# globaltimes
pub = utils.publications["globaltimes"]

df = utils.get_df(pub)
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# no duplciates :)
tts(pub, splitname="main1", **kwargs)


# %% 
# ALIBABA RUN ####################
kwargs = {"random_state":1, "test_size": 0.5}


# %% 
# SCMP
pub = utils.publications["scmp"]
df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# result: no duplicates
maskna = df[pub.textcol].isna()
maskna.value_counts()
# no nas :)
# KEEP ALL SECTIONS FOR NOW.
# sections = utils.get_df(pub, *["sections", "sections.csv"])
# strsects = (
#     sections.filter(regex="section", axis=1)
#     .fillna("")
#     .astype(str)
#     .agg(", ".join, axis=1)
# )

# masksec = strsects.str.contains(
#     "comment|opinion|cooking|letters|food-drink|style|sport"
# )  # |books|movies|hk-magazine|yp,")
# maskna.value_counts()

# drops = df[maskna | masksec]
keeps = df[~maskna & ~masksec]
# no drops to worry about
path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
if not os.path.exists(path):
    os.makedirs(path)
train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
utils.df_to_s3(train, key=f"{pub.name}/train_main1.csv", bucket="aliba")
utils.df_to_s3(test, key=f"{pub.name}/test_main1.csv", bucket="aliba")
# utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/drops_main1.csv", bucket="aliba")

# %%
# chinadaily
pub = utils.publications["chinadaily"]
df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
maskst = df.storyType.astype(str).str.contains("VIDEO|AUDIO|HREF|INNERLINK")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
maskst.value_counts()
# 152 duplicates, 151 wrong story type
drops = df[maskst | dupmask]
keeps = df[~maskst & ~dupmask]
path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
if not os.path.exists(path):
    os.makedirs(path)
train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
utils.df_to_s3(train, key=f"{pub.name}/train_main1.csv", bucket="aliba")
utils.df_to_s3(test, key=f"{pub.name}/test_main1.csv", bucket="aliba")
utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/drops_main1.csv", bucket="aliba")

# %%
# nyt
pub = utils.publications["nyt"]
df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# 6 duplicates
drops = df[dupmask]
keeps = df[~dupmask]
path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
if not os.path.exists(path):
    os.makedirs(path)
train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
drops[pub.uidcol].to_csv(
    os.path.join(path, "drops_main1.csv")
)
utils.df_to_s3(train, key=f"{pub.name}/train_main1.csv", bucket="aliba")
utils.df_to_s3(test, key=f"{pub.name}/test_main1.csv", bucket="aliba")
utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/drops_main1.csv", bucket="aliba")

# %%
# hkfp
pub = utils.publications["hkfp"]
df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# no duplicates :)
path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
if not os.path.exists(path):
    os.makedirs(path)
keeps = df
train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
# drops[pub.uidcol].to_csv(
#     os.path.join(path, "drops_main1.csv")
# )
utils.df_to_s3(train, key=f"{pub.name}/train_main1.csv", bucket="aliba")
utils.df_to_s3(test, key=f"{pub.name}/test_main1.csv", bucket="aliba")
# utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/drops_main1.csv", bucket="aliba")

# %%
# globaltimes
pub = utils.publications["globaltimes"]

df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
dupmask = df[pub.uidcol].duplicated()
dupmask.value_counts()
# no duplciates :)
path = os.path.join(utils.ROOTPATH, "baba", pub.name, "tts_mask")
if not os.path.exists(path):
    os.makedirs(path)
keeps = df
train, test = tts(pub, df=keeps, splitname="main1", path=path, **kwargs)
# drops[pub.uidcol].to_csv(
#     os.path.join(path, "drops_main1.csv")
# )
utils.df_to_s3(train, key=f"{pub.name}/train_main1.csv", bucket="aliba")
utils.df_to_s3(test, key=f"{pub.name}/test_main1.csv", bucket="aliba")
# utils.df_to_s3(drops[pub.uidcol], key=f"{pub.name}/drops_main1.csv", bucket="aliba")

