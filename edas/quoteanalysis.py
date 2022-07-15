"""

TODO: change 21 references to full dataset. 
For now, we want to keep things fast so only running on year 21. 

NOTE: something weird is happening with capitalization but I'm moving on for now
"""
# %%
import re
from thesisutils import utils
import pandas as pd
from tqdm import tqdm

tqdm.pandas()
# %%
publication = utils.publications["scmp"]

maindf = utils.get_df(publication)
df = utils.get_df(publication, "quotes", "q_2021.csv")
# did a bunch of cleaning later in the script; pick up where you left off
# by uncommenting this instead.
# df21 = pd.read_csv("../tmp_quote21.csv")

# df = df.merge(
#     maindf[[publication.uidcol, "Url", publication.textcol]],
#     left_on="source",
#     right_on=publication.uidcol,
#     how="left",
# )

# ner = utils.get_df(publication, "ner", "ner_2021.csv")

# drop na speakers/entities
df = df[df.speaker.notna()]
ner = ner[ner.entity.notna()]


# %%
# FUNCTIONS #############################################
def removepunct(s):
    return re.sub(r"[^\w\s]", "", str(s))


def lookupname(quoterow):
    """Looks up a single word speaker and tries to find a full name entity.

    :param quoterow: row in dataframe with quotes in it.
    """

    index = quoterow["source"]  # .squeeze()
    s = quoterow["prepro_speaker"]
    y = ner[ner.Index.eq(index)]
    # NOTE: could do just PERSON entities?
    candidates = y[y.prepro_entity.str.contains(s)]
    if len(candidates) == 0:
        return s
    longest = candidates.prepro_entity.str.len().idxmax()
    return y.loc[longest].prepro_entity


# %%
def longspeakerpipeline(df, ner):
    """df contains quotes/speakers, ner contains entities. 
    Preprocesses speakers & entities; matches single word speakers to longer
     speakers, stores in "longer speakers" column Takes 2.5 minutes to run
    """
    df["prepro_speaker"] = (
        df.speaker.str.lower().
        str.replace("’s|'s", "").
        progress_apply(removepunct)
    )
    ner["prepro_entity"] = (
        ner.entity.str.lower()
        .str.replace("’s|'s", "")
        .progress_apply(removepunct)
    )
    drops = [
        "he",
        "she",
        "it",
        "they",
        "you",
    ]  # "a source", "the who", "the post"],
    # dropmask just filters for speakers who are NOT pronouns
    # dropmask = ~df.prepro_speaker.isin(drops)
    df["single_speaker"] = (
        df.prepro_speaker
        .str.split()
        .str.len()
        .eq(1)
    )
    # run on non-direct quotes and filter later. 
    mask2 = ~df.prepro_speaker.isin(drops) & df.single_speaker# & ~df.direct
    # match single word, non-pronoun quotes
    df.loc[mask2, "long_speaker"] = df[mask2].progress_apply(lookupname, axis=1)
    # add multi word speakers to long speaker column ()
    # takes ~ 2.5 minutes
    df.loc[~df.single_speaker, "long_speaker"] = df.loc[
        ~df.single_speaker
    ].prepro_speaker
    return df
# %%
dfls = (
    pd.read_csv(fr"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\q{year}_edits.csv") for year in range(2011, 2022)
)
pd.concat(dfls).to_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\quotes_full_edits.csv")
utils.upload_s3(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\quotes_full_edits.csv",
    key="scmp\quotes\quotes_full_edits.csv",
)

# %%
df = utils.get_df(publication, "quotes", f"quotes_full.csv")
ner = utils.get_df(publication, "ner", f"ner_full.csv")
df2 = longspeakerpipeline(df, ner)
df2[[
    # "speaker_index", 
    "long_speaker",
    "prepro_speaker",
    "source",
    "single_speaker",
    # "quote_index"
    ]].to_csv(fr"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\quotes_full_edits.csv")

# %%


# %%
# df2.quote_type.value_counts()
df2[df2.prepro_speaker.str.contains("biden")]
df2 = longspeakerpipeline(df, ner)
df2.long_speaker.value_counts().head()
# %%
bidenner = ner.loc[lambda d: d.entity.str.lower().str.contains("biden")]

bidenner.entity.str.replace("’s|'s", "")

biden = df.loc[lambda d: d.speaker.str.lower().str.contains("biden")]
biden.speaker.apply(lambda s: re)

# %%
# get most common single speakers we were able to find longer matches for
(
    df[df.long_speaker.str.split().str.len().gt(1)]
    .long_speaker.value_counts()  # .index.str.lower().str
    .loc[lambda s: s.index.str.lower().str.contains("biden")]
)

# TODO:
# assign political affiliation to all
# get sophisticated metric for quote position

# Joe Biden               610
# Carrie Lam              530
# Wang Yi                 330
# Xi Jinping              325
# Antony Blinken          294
# Cheng Yuet-ngor         251
# David Hui Shu-cheong    145
# Boris Johnson           145
# Patrick Nip Tak-kuen    143
# Paul Chan               138
# Scott Morrison          131
# Vladimir Putin          131
# Zhao Lijian             130
# Donald Trump            123
# Chuang Shuk-kwan        117
# Sophia Chan Siu-chee    116


# %%
# save after lookup
# df21[df21.speaker.eq("Chan")].long_speaker.value_counts()

# df2.drop(["Body", "Url", "Unnamed: 0"], axis=1).to_csv("../tmp_quote21.csv")

# %% 
# save longspeaker 
year = "2021"
df2[[
    # "speaker_index", 
    "long_speaker",
    "prepro_speaker",
    "source",
    "single_speaker",
    # "quote_index"
    ]].to_csv(f"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\q{year}_edits.csv")



# %%
# Inspect QSCQ patters

# df21.loc[lambda d: d.quote_type.str.contains("QSCQ")]
# # inspect QCQ pattern
# QCQ = df21.loc[lambda d: d.quote_type.str.contains("QCQ")]
df[df.long_speaker.eq("joe biden")]
df.loc[mask2, "long_speaker"].value_counts().head(10)

# Instructions: 
# generate proper long speakers for every year of scmp
# This takes 1-2 hours per publication. 
#       can I run this on Sagemaker?
# get speaker index relative to entire index of article
# subset to only HK protest coverage, legco election coverage, carrie lam coverage? Using tags? 
# look at who gets quoted in these (how big is hk protest?)
# metric one: earliest mention of quote by:
#   Carrie Lam
#   police chief
#   opposition ppl? Joshua Wong, Martin Lee, Benny Tai, Agnes Chau, Nathan Law, 
# metric two: unweighted words given by
#   Carrie Lam
#   police chief

# other stream of info: how long does poli scores for sentiment take for all Alibaba references? 


# Analysis part 2 ###################
#%% 
cdailyq = utils.read_df_s3("chinadaily/quotes/quotes_full_edits.csv")
cdailyq.head()#.long_speaker.value_counts().head(20)
cdailyner = utils.read_df_s3("chinadaily/ner/ner_full.csv")
cdailyq.iloc[1].head()
cdailyner.loc[lambda d: d.id.eq(14361751)]

cdailymain = utils.get_df(utils.publications['chinadaily'])
cdailymain.loc[lambda d: d.id.eq("14361751")].plainText.squeeze()[800:900]

# Problem: quote ner is by character index
# normal ner is by word index 
# solution: get character index of the ners 
# how long will this take? 
# perhaps sagemaker can help on this one. 

import spacy
import os
nlp = spacy.load("en_core_web_lg")
nlp = spacy.load("en_core_web_md")

# %%
txt = nlp(cdailymain.loc[lambda d: d.id.eq("14361751")].plainText.squeeze(), disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer", "ner"])
# seems to work on with medium to... 
txt[155:157].start_char
pub = utils.publications['chinadaily']
# for every publication
# for pub in utils.publication.values():
maindf = utils.get_df(pub)
nerdf = utils.read_df_s3(f"{pub.name}/ner/ner_full.csv")
quotedf = utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits.csv")
nerdf[pub.uidcol] = nerdf[pub.uidcol].astype(str)
maindf[pub.uidcol] = maindf[pub.uidcol].astype(str)

# algo: for every article...
maindf.apply(get_doc_nerchars, axis=1)
nerdf.start_char = nerdf.start_char.astype(pd.Int64Dtype())
nerdf.end_char = nerdf.end_char.astype(pd.Int64Dtype())
nerdf = nerdf.rename({"Unnamed: 0": "ner_index"}, axis=1)
path = f"../../data/{pub.name}/ner/ner_full2.csv"
os.makedirs(os.path.dirname(path), exist_ok=True) 
nerdf.to_csv(path)
utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full2.csv")
import numpy as np
np.unique(nerdf.index == nerdf["Unnamed: 0"], return_counts=True)
nerdf[nerdf.index != nerdf["Unnamed: 0"]]
nerdf[21243-5:21243+5]
nerdf[""].value_counts().sort_index()
def get_doc_nerchars(row):
    """Creates start and end_char columns for ner df based on 
    token indexes of doc of current row of maindf. 
    """
    doc = nlp(row[pub.textcol], disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer", "ner"])
    mask = nerdf[pub.uidcol].eq(row[pub.uidcol])
    nerdf.loc[mask, "start_char"], nerdf.loc[mask, "end_char"] = zip(*nerdf[mask].apply(nerchar, doc=doc, axis=1))
    
def nerchar(nrow, doc):
    """Gets the start/end_chars of tokens in ner."""
    ent = doc[nrow["start"]:nrow["end"]]
    return int(ent.start_char), int(ent.end_char)
# get all ners 
# get start and end token
# save to new file we will merge on 
# we're going to have to use ner df's index as the truthful index
# right now it's duplicated by unnamed col 0 fwiw. 