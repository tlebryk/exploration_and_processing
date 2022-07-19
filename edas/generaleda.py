""" Has script to do column renaming mirroring stm R code. 
Useful for any future python analysis on the full dataset. 
"""
# %%
import pandas as pd
from thesisutils import utils
from IPython.display import display
# %%
lookup = dict(
    Index="Art_id",
    id="Art_id",
    x_id="Art_id",
    sourceurl="Art_id",
    plainText="Body",
    text="Body",
    title="Headline",  # nyt and gt?
)
# %%
dfls = []
for pub in utils.publications.values():
    # get full df
    df = (
        utils.get_df(pub)
        .rename(columns=lookup)
        .assign(
            Publication=pub.name,
            Textcol=lambda d: d.Headline + "; " + d.Body,
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
    df["train"] = df.Art_id.isin(tts.Art_id)
    keeps = [
        "Art_id",
        "Headline",
        "Textcol",
        "Date",
        "Year",
        "Publication",
        "post_baba",
        "baba_ownership",
        "poliestimation",
        "train",
        "hk",
    ]
    df = df.filter(keeps, axis=1)
    dfls.append(df)
# %%
df = pd.concat(dfls)
df.groupby("Publication").size()
len(df)
df.train.value_counts()
train = df[(df.poliestimation > 0.5) & df.hk & df.train]

d = train
# %%
def lookupword(word, train=train, mask=None):
    """Returns df with articles containing a substring

    :param word: substring to look up (lowercase)
    :param train: df with articles
    :param mask: boolean mask length of train.
    :returns: df filtered by for only article containing string
    """
    if mask is None:
        mask = train.Art_id.notna()
    filtered = train[mask].loc[lambda d: (d["Textcol"].str.lower().str.contains(word))]
    # print first n windows with word
    filtered["index1"] = filtered.Textcol.str.lower().str.find(word)
    filtered["window1"] = filtered.apply(
        lambda d: d.Textcol[max(d.index1 - 25, 0) : min(d.index1 + 25, len(d.Textcol))],
        axis=1,
    )
    oldwidth = pd.options.display.max_colwidth
    pd.options.display.max_colwidth = 80
    display(filtered["window1"])
    pd.options.display.width = oldwidth
    return filtered

mask = train.Publication.eq("chinadaily")
x = lookupword(word="shadow", mask=mask)

# %%
df[df.hk].groupby("Publication").size()


df.head()

df.rename(lookup)
df.rename(columns={"Index": "Art_id"})
df.Index
# %%
# GLOBAL TIMES CD DUPLCIATION ANALYSIS ####################
cd = utils.get_df(utils.publications['chinadaily'])
gt = utils.get_df(utils.publications['globaltimes'])

gtcd = pd.concat(
    [utils.standardize(cd, utils.publications['chinadaily']),
    utils.standardize(gt, utils.publications['globaltimes'])]
    )

# get duplicates 
# keep first instance i.e. china daily instance. 
dupmask = gtcd.assign(bodylow = lambda d: d.Body.str.lower().str.strip()).bodylow.duplicated()
gtcd[dupmask].Body.str.contains("Alibaba").value_counts(dropna=False)
# 386 dropped non-unique stories [184 had nan bodies so only 386 were real dups]
# code to filter if you wanted to: 
# note that we want non-duplicated values ~
# gtcd[~dupmask]