""" Has script to do column renaming mirroring stm R code. 
Useful for any future python analysis on the full dataset. 
"""
# %%
import pandas as pd
from thesisutils import utils
# %%
lookup = dict(
    Index = "Art_id",
    id = "Art_id",
    x_id = "Art_id",
    sourceurl = "Art_id",
    plainText = "Body",
    text = "Body",
    title = "Headline" # nyt and gt?
)
# %%
dfls = []
for pub in utils.publications.values():
    # get full df
    df = (
        utils.get_df(pub)
        .rename(columns=lookup)
        .assign(
            Publication = pub.name,
            Textcol = lambda d: d.Headline + "; " + d.Body,
            )
        .drop("Date", axis=1, errors="ignore")
        .drop("Unnamed: 0", axis=1, errors="ignore")
        .drop_duplicates("Art_id")
    )
    # get datedf
    dt = (
        utils.get_df(pub, "date", "date.csv")
        .rename(columns=lookup)
        .drop_duplicates("Art_id")

        # .drop("Unnamed: 0", axis=1, errors="ignore")
    )
    df2 = df.merge(dt, on="Art_id", how="inner")
    # get polimask
    poli = (
        utils.get_df(pub, "polimask", "pmask_.csv")
        .rename(columns=lookup)
        .drop("Unnamed: 0", axis=1, errors="ignore")
        .drop_duplicates("Art_id")

    )
    df = df.merge(poli, on="Art_id")

    # get hkmask
    hk = (
        utils.get_df(pub, "hk_mask", "hkmask.csv")
        .rename(columns=lookup)
        .drop_duplicates("Art_id")
    )
    df["hk"] = df.Art_id.isin(hk.Art_id)
    # get ttsmask
    tts = (
        utils.get_df(pub, "tts_mask", "train_main1.csv")
        .rename(columns=lookup)
        .drop_duplicates("Art_id")
    )
    df['train'] = df.Art_id.isin(tts.Art_id)
    keeps = [
        "Art_id", "Headline",
        "Textcol", "Date", "Year", "Publication",
        "post_baba", "baba_ownership", "poliestimation",
        "train", "hk"    
    ]
    df = df.filter(keeps, axis=1)
    dfls.append(df)
# %% 
df = pd.concat(dfls)
df.groupby("Publication").size()
df[(df.poliestimation > .4) & df.hk].groupby("Publication").size()
df[df.hk].groupby("Publication").size()


df.head()

df.rename(lookup)
df.rename(columns={"Index": "Art_id"})
df.Index