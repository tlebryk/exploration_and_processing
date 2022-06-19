from thesisutils import utils
import os
from sklearn.model_selection import train_test_split



# %%
# TRAINTESTSPLITtrain_test_split
def tts(publication, df=None, splitname="", *args, **kwargs):
    """Train test splits data for a publication
    :param args: arguments to be passed to train_test_split
    """
    if df is None:
        df = utils.get_df(
            publication, f"{publication.name}_full.csv"
        )  # "polimask", "pmask_.csv")
    train, test = train_test_split(df[publication.uidcol], *args, **kwargs)
    path = os.path.join(utils.ROOTPATH, publication.name, "tts_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    train.to_csv(os.path.join(path, f"train_{splitname}.csv"))
    test.to_csv(os.path.join(path, f"test_{splitname}.csv"))
    return train, test


kwargs = dict(random_state=1, test_size=0.9)

# %% cleaning for each publication
# scmp
publication = utils.publications["scmp"]
df = utils.get_df(publication)
maskna = df[publication.textcol].isna()
sections = utils.get_df(publication, *["sections", "sections.csv"])
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
tts(publication, df=keeps, splitname="main1", **kwargs)
drops[publication.uidcol].to_csv(
    os.path.join(utils.ROOTPATH, publication.name, "tts_mask", "drops_main1.csv")
)
# %%
# chinadaily
publication = utils.publications["chinadaily"]
df = utils.get_df(publication)
maskst = df.storyType.astype(str).str.contains("VIDEO|AUDIO|HREF|INNERLINK")
drops = df[maskst]
keeps = df[~maskst]
tts(publication, df=keeps, splitname="main1", **kwargs)
drops[publication.uidcol].to_csv(
    os.path.join(utils.ROOTPATH, publication.name, "tts_mask", "drops_main1.csv")
)
# %%
# nyt
# NOT READY YET
publication = utils.publications["nyt"]
tts(publication, splitname="main1", **kwargs)

# %%
# hkfp
publication = utils.publications["hkfp"]
tts(publication, splitname="main1", **kwargs)
# %%
# globaltimes
publication = utils.publications["globaltimes"]
tts(publication, splitname="main1", **kwargs)


