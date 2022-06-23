# %%
import re
import string
from thesisutils import utils

# %%
publication = utils.publications["scmp"]

maindf = utils.get_df(publication)

df21 = utils.get_df(publication, "quotes", "q_2021.csv")
df21 = df21.merge(maindf[[publication.uidcol, "Url", publication.textcol]], left_on="source", right_on=publication.uidcol, how="left" )
ner21 = utils.get_df(publication, "ner", "ner_2021.csv")

# %%
def removepunct(s):
    return re.sub(r'[^\w\s]','',s)

# df21 = 
df21 = df21[df21.speaker.notna()]
ner21 = ner21[ner21.entity.notna()]

df21["prepro_speaker"]  = df21.speaker.str.lower().apply(removepunct)
ner21['prepro_entity'] = ner21.entity.str.lower().apply(removepunct)

# we could include "according to" as well? 
df21['direct'] = df21.quote_type.str.contains("QCQ")
# df21['quote_type'].value_counts()

drops = ["he", "she", "it", "they", "who", "you"]

dropmask = df21.prepro_speaker.isin(drops)

mask = (~dropmask & df21.single_speaker) #  & df21.direct



df21['single_speaker'] = df21.speaker.str.split().str.len().eq(1)
# when to try look up.
# not in drops; direct word and single speaker. 

def lookupname(quoterow):
    """Looks up a single word speaker and tries to find a full name entity."""
    
    index = quoterow['source']#.squeeze()
    s = quoterow['prepro_speaker']

    y = ner21[ner21.Index.eq(index)]
    # just look up people for now
    candidates = y[y.prepro_entity.str.contains(s)]
    if len(candidates) == 0:
        return s
    longest = candidates.entity.str.len().idxmax()
    return y.loc[longest].entity
mask2 =  (~dropmask & df21.single_speaker  & ~df21.direct)

df21.loc[mask2, "long_speaker"] = df21[mask2].apply(lookupname, axis=1)

df21.speaker.value_counts().head(20)

df21[df21.speaker.eq("Chan")]


df21.long_speaker.fillna(df21.prepro_speaker)[~dropmask].value_counts().head()
mask(
    lambda s: s.isna(),
    1
).value_counts()

df21.assign(
    long_speaker = lambda d: d.mask
)

# %% 
# Inspect QSCQ patters

df21.loc[lambda d: d.quote_type.str.contains("QSCQ")]


# inspect QCQ pattern
QCQ = df21.loc[lambda d: d.quote_type.str.contains("QCQ")]