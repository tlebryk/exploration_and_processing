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

df21 = utils.get_df(publication, "quotes", "q_2021.csv")
# did a bunch of cleaning later in the script; pick up where you left off
# by uncommenting this instead.
# df21 = pd.read_csv("../tmp_quote21.csv")

df21 = df21.merge(
    maindf[[publication.uidcol, "Url", publication.textcol]],
    left_on="source",
    right_on=publication.uidcol,
    how="left",
)

ner21 = utils.get_df(publication, "ner", "ner_2021.csv")

# drop na speakers/entities
df21 = df21[df21.speaker.notna()]
ner21 = ner21[ner21.entity.notna()]


# %%
# FUNCTIONS #############################################
def removepunct(s):
    return re.sub(r"[^\w\s]", "", s)


def lookupname(quoterow):
    """Looks up a single word speaker and tries to find a full name entity.
    
    :param quoterow:
    """

    index = quoterow["source"]  # .squeeze()
    s = quoterow["prepro_speaker"]
    y = ner21[ner21.Index.eq(index)]
    # NOTE: could do just PERSON entities?
    candidates = y[y.prepro_entity.str.contains(s)]
    if len(candidates) == 0:
        return s
    longest = candidates.prepro_entity.str.len().idxmax()
    return y.loc[longest].prepro_entity


# %%
# remove punctuation
df21["prepro_speaker"] = df21.speaker.str.lower().progress_apply(removepunct)
ner21["prepro_entity"] = ner21.entity.str.lower().progress_apply(removepunct)


ner21[ner21.prepro_entity.str.contains("biden")]
# %%
# get direct quotes only

# quote type scheme:
# V: Verb
# S: Subject
# C: Content
# Q: Quotation mark

df21["direct"] = df21.quote_type.str.contains("QCQ")
df21["quote_type"].value_counts()

# %%
# drop ambiguous pronouns for now.
drops = ["he", "she", "it", "they", "you",] # "a source", "the who", "the post"], 
dropmask = ~df21.prepro_speaker.isin(drops)

# when speaker is only a single word.
df21["single_speaker"] = df21.prepro_speaker.str.split().str.len().eq(1)

# mask = (dropmask & df21.single_speaker) #  & df21.direct
mask2 = dropmask & df21.single_speaker & ~df21.direct

# assign long speaker to rows which fit mask;
# 54310 quote records takes about 3 minutes (1 minute~18000 records)
df21.loc[mask2, "long_speaker"] = df21[mask2].progress_apply(lookupname, axis=1)
# for now, all the masked values are nan
# %%
df21.loc[~df21.single_speaker, "long_speaker"] = (
    df21.loc[~df21.single_speaker]
    .prepro_speaker
)
# df21.assign(
#     long_speaker = lambda d: d.mask(
#         (~d.single_speaker & d.direct),
#         d.prepro_speaker,
#     )
# )
# %%
# get most common single speakers we were able to find longer matches for
(
    df21[df21.long_speaker.str.split().str.len().gt(1)]
    .long_speaker.value_counts()
    .head(30)
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

df21.drop(["Body", "Url", "Unnamed: 0"], axis=1).to_csv("../tmp_quote21.csv")


# %%
# Inspect QSCQ patters

# df21.loc[lambda d: d.quote_type.str.contains("QSCQ")]
# # inspect QCQ pattern
# QCQ = df21.loc[lambda d: d.quote_type.str.contains("QCQ")]
df21[df21.long_speaker.eq("joe biden")]