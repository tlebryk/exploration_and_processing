# %%
from thesisutils import utils
import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer as CV
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import SGDClassifier, LogisticRegression
# %%
pub = utils.publications['scmp']
df = utils.main_date_load(pub)
df = df[df.tts.ne("drop")]
df.Headline = df.Headline.astype(str)

# %%
# random analysis on article update ignore
df['archivemon'] = pd.to_datetime(df.Archive, format="sitemap_archive_%Y_%b.xml")
df['dtdiff'] = ((df.Date - df.archivemon.dt.tz_localize('UTC'))/np.timedelta64(1, 'M'))
df['dtdiff'] = df['dtdiff'].astype(int)

df[df.dtdiff.gt(12)].Topics.str.split(",").explode().value_counts().head(10)
len(df[df.dtdiff.gt(12)])/len(df)
df['dtdiff'].astype(int).value_counts().head(10)
.dtdiff.plot(kind="hist", bins=25)
df.dtdiff.plot(kind="hist")
df.archivemon.dt.month

df.Date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def archive_extract(archive):
    archive = archive.str.replace("sitemap_archive_", "").str.replace(".xml", "")
    year = archive.str[:4]
    mon = archive.str[-3:]

    archive = "sitemap_archive_2018_Jul.xml"
    archive.replace("sitemap_archive_")
# filter opinion 
# extradition bill introduction starts Mar 15 2019
# set end as December 2019 (2020 covid shut down protests)
# %%
# protest subset
protests = df[df.archivemon.ge("2019-03-01") & df.archivemon.lt("2020-01-01")]
protests.Topics = protests.Topics.astype(str)
protests['hk_protest_tag'] = protests.Topics.str.contains("Hong Kong protests")
protests['extradition_tag'] = protests.Topics.str.contains("Hong Kong extradition bill")
protests['Hong_Kong_police'] = protests.Topics.str.contains("Hong Kong police")
protests['Hong_Kong_Basic_Law'] = protests.Topics.str.contains("Hong Kong Basic Law")

protests['antimainland'] = protests.Topics.str.contains("Anti-mainland China sentiments")
protests.Headline.str.contains("Hong Kong protest").value_counts()
# %%
protests['hk_protest_tag'].value_counts()
protests['extradition_tag'].value_counts()
protests['Hong_Kong_police'] .value_counts()
protests[protests['Hong_Kong_police']].Headline.apply(print)
protests[protests.Hong_Kong_Basic_Law].Url.apply(print)
protests[protests['antimainland']].iloc[0]


print(len(protests[protests['Hong_Kong_police'] & (~protests['hk_protest_tag'] & ~protests['extradition_tag'])]))#.Headline.apply(print)
protests[protests['hk_protest_tag'].ne(protests['extradition_tag'])]
x=protests.Topics.str.split(",").explode().value_counts()

protests.Url.str.contains("comment").value_counts()

datedf = utils.standardize(utils.get_df(pub, "date/date.csv"), pub)
df = df.merge(datedf, on="Art_id")
df = df[df.Body.notna()]
protests =

post19 = df[df.Year.gt(2018)]
pd.D
topics = post19.Topics.str.split(",").explode().value_counts()

post19['bodyprepro'] = post19.Body.str.lower()#.str.replace('[^a-zA-Z]', '')
post19.Body.isna().value_counts()
pteststrcont = post19[post19['bodyprepro'].str.contains("protest")]
pteststrcont["ptestind"] = pteststrcont.bodyprepro.str.find("protest")
pteststrcont["window"] = pteststrcont.bodyprepro.str[max(pteststrcont["ptestind"]-100, 0):pteststrcont["ptestind"]+100]
# %%
pteststrcont.assign(
    start = lambda d: d["ptestind"].subtract(100).clip(0),
    end = lambda d: d["ptestind"]+100,
    window = lambda d: d.apply(lambda d2: d2.bodyprepro[d2.start:d2.end], axis=1)
).sort_values("Date_y").apply(lambda d: print(d.window, d.Date_y), axis=1)
# %% 
# HKFP ANALYSIS ##################################
pub = utils.publications['hkfp']
hkfp = utils.main_date_load(pub)
protests = hkfp[hkfp.Date.ge("2019-03-01") & hkfp.Date.lt("2020-01-01")]
x=protests.Topics.str.split(",").explode().value_counts()
# main keyword is China Extradition
# China Extradition                   1319
# Carrie Lam                           291
# Police                               224
# Xi Jinping                           101
# OccupyHK                             100
# Taiwan                                98
# Xinjiang                              91
# USA                                   79
# 1989 Tiananmen Massacre               77

# %%
pub = utils.publications['nyt']
ny = utils.get_df(pub)
ny.columns
nyt = utils.main_date_load(pub)
nytkws = utils.get_df(pub, "kws_main.csv")
protests = nyt[nyt.Date.ge("2019-03-01") & nyt.Date.lt("2020-01-01")]
nytkws19 = nytkws[nytkws._id.isin(nyt._id)]
x=nytkws19.value.value_counts()
# main keyword is "Hong Kong Protests (2019)"
# happens about 419 times...
# %%
pub = utils.publications['globaltimes']
gt = utils.get_df(pub)
gt[gt.Topics.notna()].Topics
# Topics is not a helpful column; will need to use prediction
# for this one... 

# %%

pub = utils.publications['chinadaily']
cd = utils.main_date_load(pub)
protests = cd[cd.Date.ge("2019-03-01") & cd.Date.lt("2020-01-01")]
x=protests.keywords.fillna("[]").apply(eval).explode().value_counts()
protests[protests.bodylower.str.contains("protest")].Headline.apply(print)
opinionwords = "oped|opinion|comment|editorial"
opmask = (
    cd.assign(
        columnchannel = lambda x: x.channelName.str.cat(x.columnName.astype(str))
    )
    # .str.lower()
    # .str.replace("-", ""))
    .columnchannel.str.contains(opinionwords)
)
opmask.value_counts()
opinionwords = "Op-Ed|Opinion|Comment|Editorial"

cd.channelName.str.contains(opinionwords).value_counts()
# CD news filtration process: 
# columnName contains "Op-Ed," "Opinion", or "Editorials" then opinion 
# channelName contains "Comment" "Op-ed" or "Opinion" then opinion.

# other irrelevant 
    # columnName: "Video", ""
    # channelName: "Life", "Sports", "Art", "Advertorial", "Food", "Movies"

y=cd.channelName.value_counts()
y=cd.columnName.value_counts()
# there are violence, police, law, protesters, protests, violent, and rioters,
# but note all these are basically less than 100 individually
# still seems comparable to nyt, but might be missing?
cd.keywords
gt[gt.Topics.notna()].Topics

protests.merge(nytkws, on="_id")
# merge on id; df already exploded
nytkws.merge(nyt, on="_id")

value.value_counts()
nytkws = utils.standardize(nytkws, pub, drop_dups=False)
nytkws
protests = hkfp[hkfp.Date.ge("2019-03-01") & hkfp.Date.lt("2020-01-01")]
protests.Topics.str.split(",").explode().value_counts().head(20)



# %% 
# 2 options: use tagging system...
# figure out keywords and make hand-coded features.
# let's do some eda first on other sources

# %%
pub = utils.publications['hkfp']
df = utils.get_df(pub)
df = utils.standardize(df, pub)
datedf = utils.standardize(utils.get_df(pub, "date/date.csv"), pub)
df = df.merge(datedf, on="Art_id")
df = df[df.Body.notna()]
post19 = df[df.Year.gt(2018)]
# %%

df.Date = df.Date_x
post19.head()
masklambda = lambda d: d.Date.ge
df.Topics.str.split(",").explode().value_counts().head(10)

# %%
pteststrcont.bodyprepro.str[2:5]
protests = post19.Topics.str.contains("Hong Kong Protests")

