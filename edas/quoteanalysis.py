""" EDA on quote csvs (train and test).
Generates the old csvs 

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

ner = utils.get_df(publication, "ner", "ner_2021.csv")

# drop na speakers/entities
df = df[df.speaker.notna()]
ner = ner[ner.entity.notna()]


# %%
# FUNCTIONS #############################################
def removepunct(s):
    return re.sub(r"[^\w\s]", "", str(s))


def lookupname(qrow):
    """Looks up a single word speaker and tries to find a full name entity.

    :param qrow: row in dataframe with quotes in it.
    """

    index = qrow["source"]  # .squeeze()
    s = qrow["prepro_speaker"]
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
     speakers, stores in "long_speakers" column Takes 2.5 minutes to run
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
# scmp long_speaker generation
# publication = utils.publications["scmp"]
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
    ]].to_csv(fr"{utils.ROOTPATH}\scmp\quotes\quotes_full_edits.csv")


# %%
# quick biden analysis
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


# %%
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


# ner speaker matching ###################


# %%
pub = utils.publications['nyt']
quotedf = utils.standardize(utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits.csv"), pub, drop_dups=False)
nerdf = utils.standardize(utils.read_df_s3(f"{pub.name}/ner/ner_full2.csv"), pub, drop_dups=False)
datedf = utils.standardize(utils.read_df_s3(f"{pub.name}/date/date.csv"), pub, drop_dups=False)
quotedf = (
    quotedf.merge(datedf[["Art_id", "Year"]], on="Art_id")
    .drop_duplicates(subset=nerdf.columns.difference(['Unnamed: 0.1']))
)
nerdf = (
    nerdf.merge(datedf[["Art_id", "Year"]], on="Art_id")
    .drop_duplicates(subset=nerdf.columns.difference(['ner_index']))
)
ppl = nerdf[nerdf.label_.eq("PERSON")]
# %%
def find_entity(qrow, ner):
    """Looks up an entity within the speakers span."""
    # neridx = ner rows from article of quote in qrow
    neridx = ner[ner.Art_id.eq(qrow.Art_id)]
    # convert string tuple to py tuple
    tup = eval(qrow.speaker_index)
    overlaps = neridx[(neridx.start_char.ge(tup[0])) & (neridx.start_char.lt(tup[1]))]
    return overlaps[['entity', "ner_index"]].values.tolist()
# %%
# random note: we prolly shouldn't just limit to ppl?
# we might want "ORG" quotes too...
# worth a good ole investigation later... 
# only 1-2% have more than 1 entity
# I think it's fine to not do resolution here and just assume the first one for our initial analysis
# later we can query key entities and if any contain we can return 

valcnts = quotedf.entity_1.isna().value_counts()
# viz(
    quotedf[quotedf.entity_1.isna()].speaker.head(100).apply(print)
    # )
# how many had an entity? 
utils.get_perc(valcnts[True], valcnts[False])
 # %% 
 # Chan analysis
# Looks like it generally is the same Paul Chan - some
# member of HK administration. 
chandf = quotedf[quotedf.entity_1.eq("Chan")]
channer = nerdf[nerdf.Art_id.isin(chandf.Art_id)]
channer[channer.entity.str.contains("Chan")].entity.value_counts().head(10)
channer[channer.entity.str.contains("Paul Chan")].merge(df, on="Art_id", how="inner").Body.str.contains("ecretary").value_counts()
len(channer.Art_id.unique())
len(chandf.Art_id.unique())
# %%
# first entity main one we care about... 
quotedf.entity_1.value_counts().head(20)
# %%
# single speaker matching algo...
# split entities on spaces 
# look for entities that are ppl with 2-5 elements ie words

# single entity speakers
# singleentityspeaker is when entity_1 is a single word

# let's try matching those. 
artids = quotedf[quotedf.singleentityspeaker].Art_id
singleentners = nerdf[nerdf.Art_id.isin(artids)]
singleentners = singleentners[singleentners.label_.eq("PERSON")]
singleentners['splitent'] = singleentners.entity.str.split()

singleentners[singleentners['splitent'].str.len().ge(2) & singleentners['splitent'].str.len().lt(5)]
quotedf[quotedf['singleentityspeaker']].iloc[0].entity_1
# for every Xi speaker, check if Xi jinping was mentioned in that article...
# let's just sanity check to see if it's 100%
# xi df is anytime we see Xi

artid = quotedf[quotedf.entity_1.eq("Xi")].Art_id.iloc[0]
quotedf[quotedf.entity_1.eq("Xi Jinping")]
ents=nerdf[nerdf.Art_id.eq(artid)]
ents.entity.str.lower().str.replace('[^a-zA-Z]', '').str.contains("xijinping").value_counts()

# resolving conflicts...
# let's just choose first entity for now and see how many nas we have

# let's add a column to the quote dataframe with 
# matched entities... maybe arbitrary number of matched entities? 
#       ok so do I match only for long cols or also more or less exact matches? 
#       I think it's fine for our NER to be our source of truth to weed out hard 
#       to parse quotes? 
# don't think it'll be more than 3-5 max
# our best speaker will be the shortened entity
# matched with a longer name potentially
# and later coreference resolution????
# %%


# Problem: quote ner is by character index
# normal ner is by word index 
# solution: get character index of the ners 
# how long will this take? 
# perhaps sagemaker can help on this one. 
# %%
# character index problem #####################
import spacy
import os
# nlp = spacy.load("en_core_web_lg")
nlp = spacy.load("en_core_web_md")

# %%
def get_doc_nerchars(row):
    """Creates start and end_char columns for ner df based on 
    token indexes of doc of current row of maindf. 
    """
    # print(row[pub.uidcol])
    try:
        doc = nlp(row[pub.textcol], disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer", "ner"])
    except ValueError as ve:
        # print(row[pub.uidcol])
        # print(ve)
        return None

    mask = nerdf[pub.uidcol].eq(row[pub.uidcol])
    try:
        # newner = nerdf[mask]
        # newner["start_char"], newner["end_char"] = zip(*newner.apply(nerchar, doc=doc, axis=1))
        # return newner
        nerdf.loc[mask, "start_char"], nerdf.loc[mask, "end_char"] = zip(*nerdf[mask].apply(nerchar, doc=doc, axis=1))
    except ValueError as ve:
        # print(row[pub.uidcol])
        # print(ve)
        return None
   
    
def nerchar(nrow, doc):
    """Gets the start/end_chars of tokens in ner."""
    # print(nrow.ner_index)
    ent = doc[nrow["start"]:nrow["end"]]
    return ent.start_char, ent.end_char
# %% 
# for every publication
# for pub in utils.publication.values():
# pub = utils.publications['chinadaily']
# maindf = utils.get_df(pub)
# nerdf = utils.read_df_s3(f"{pub.name}/ner/ner_full.csv")
# quotedf = utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits.csv")
# nerdf[pub.uidcol] = nerdf[pub.uidcol].astype(str)
# maindf[pub.uidcol] = maindf[pub.uidcol].astype(str)
# nerdf = nerdf.rename({"Unnamed: 0": "ner_index"}, axis=1)
# path = f"../../data/{pub.name}/ner/ner_full2.csv"
# os.makedirs(os.path.dirname(path), exist_ok=True) 
# # run in blocks of 200
# start =0
# blocksize = 200
# for i in tqdm(range(start, len(maindf), blocksize)):
#     idx = maindf.iloc[i:i+blocksize][pub.uidcol]
#     maindf.iloc[i:i+blocksize].apply(get_doc_nerchars, axis=1)
#     nerdf[nerdf[pub.uidcol].isin(idx)].to_csv(f"../../data/{pub.name}/ner/ner_full{i}.csv")
# nerdf.to_csv(f"../../data/{pub.name}/ner/ner_full2.csv")
# utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full2.csv")
ppl = nerdf[nerdf.label_.eq("PERSON")]
ppl

# %%
# need to run
year = 2020
nerdf = utils.get_df(pub, f"ner/ner_{year}.csv")
maindf = utils.get_df(pub, f"{year}.csv")
nerdf[pub.uidcol] = nerdf[pub.uidcol].astype(str)
maindf[pub.uidcol] = maindf[pub.uidcol].astype(str)
nerdf = nerdf.rename({"Unnamed: 0": "ner_index"}, axis=1)
maindf.progress_apply(get_doc_nerchars, axis=1)
# newners = pd.concat(newners.tolist())
path = f"../../data/{pub.name}/ner/ner_full_{year}.csv"
os.makedirs(os.path.dirname(path), exist_ok=True) 
nerdf.to_csv(path)
utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full_{year}.csv")

# %%
# NOT RUN: 
# This is play around code for the quotes notebook code which ran on 
#  sagemaker to create the ner_full_year dataframes for scmp.
def scmprun(year):
    nerdf = utils.get_df(pub, f"ner/ner_{year}.csv")
    maindf = utils.get_df(pub, f"{year}.csv")
    nerdf[pub.uidcol] = nerdf[pub.uidcol].astype(str)
    maindf[pub.uidcol] = maindf[pub.uidcol].astype(str)
    nerdf = nerdf.rename({"Unnamed: 0": "ner_index"}, axis=1)
    maindf.progress_apply(get_doc_nerchars, axis=1)
    path = f"../../data/{pub.name}/ner/ner_full_{year}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True) 
    nerdf.to_csv(path)
    utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full_{year}.csv")
# %%
scmprun(2020)
# %%

pub = utils.publications['scmp']
maindf = utils.get_df(pub)
nerdf = utils.read_df_s3(f"{pub.name}/ner/ner_full.csv")
quotedf = utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits.csv")
nerdf[pub.uidcol] = nerdf[pub.uidcol].astype(str)
maindf[pub.uidcol] = maindf[pub.uidcol].astype(str)
nerdf = nerdf.rename({"Unnamed: 0": "ner_index"}, axis=1)
path = f"../../data/{pub.name}/ner/ner_full2020.csv"
os.makedirs(os.path.dirname(path), exist_ok=True) 
# %% 
# NOT RUN: makes the ner_full2.csv for scmp and other cols
maindf.progress_apply(get_doc_nerchars, axis=1)
nerdf.start_char = nerdf.start_char.astype(pd.Int64Dtype())
nerdf.end_char = nerdf.end_char.astype(pd.Int64Dtype())
path = f"../../data/{pub.name}/ner/ner_full2.csv"
os.makedirs(os.path.dirname(path), exist_ok=True) 
nerdf.to_csv(path)
utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full2.csv")

# %%
# run in blocks of 200
start =0
blocksize = 200
for i in tqdm(range(start, len(maindf), blocksize)):
    idx = maindf.iloc[i:i+blocksize][pub.uidcol]
    maindf.iloc[i:i+blocksize].apply(get_doc_nerchars, axis=1)
    nerdf[nerdf[pub.uidcol].isin(idx)].to_csv(f"../../data/{pub.name}/ner/ner_full{i}.csv")
nerdf.to_csv(f"../../data/{pub.name}/ner/ner_full2.csv")
utils.df_to_s3(nerdf, f"{pub.name}/ner/ner_full2.csv")
# %%
# dfls = []
# ndf = pd.concat(pd.read_csv(f"../../data/{pub.name}/ner/ner_full{i}.csv") for i in range(0, 400, blocksize))
# path = f"../../data/{pub.name}/ner/ner_full2.csv"
# nerdf.to_csv(path)
# utils.df_to_s3(ndf, f"{pub.name}/ner/ner_full2.csv")
#%%