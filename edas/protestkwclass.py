"""Eda for splitting the data into occupy and 2019 extradition bill articles."""
# %%
from thesisutils import utils
import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer as CV
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import SGDClassifier, LogisticRegression
# %%
# scmp analysis
# summary: topics is premeir classification
pub = utils.publications['scmp']
df = utils.main_date_load(pub)
df = df[df.tts.ne("drop")]
df.Headline = df.Headline.astype(str)

# %%
#  ignore
# random analysis on article update time;
df['archivemon'] = pd.to_datetime(df.Archive, format="sitemap_archive_%Y_%b.xml")
df['dtdiff'] = ((df.Date - df.archivemon.dt.tz_localize('UTC'))/np.timedelta64(1, 'M'))
df['dtdiff'] = df['dtdiff'].astype(int)

df[df.dtdiff.gt(12)].Topics.str.split(",").explode().value_counts().head(10)
len(df[df.dtdiff.gt(12)])/len(df)
df['dtdiff'].astype(int).value_counts().head(10)
df.dtdiff.plot(kind="hist")
df.archivemon.dt.month
# %%
# scmp protest
 
# extradition bill introduction starts Mar 15 2019
# ends as December 2019 (2020 covid shut down protests)

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


# %%
# scmp umbrella coverage ###################
pub = utils.publications['scmp']
scmp = utils.main_date_load(pub)
scmp['archivemon'] = pd.to_datetime(scmp.Archive, format="sitemap_archive_%Y_%b.xml")
scmpdropped = scmp[~scmp.tts.eq("drop")]

protests = scmpdropped[scmpdropped.archivemon.ge("2019-03-01") & scmpdropped.archivemon.lt("2020-01-01")]
protests.columns
x=protests.Topics.str.split(",").explode().value_counts()#.head(10)
# get topic count
# 1305 using Topics

# get fulltext kw search AND must contain hong kong in text
# 1899 w/ protest or riot
# %%
# riot catches riot games...
keyword = "protest"
window=50
(
    protests[
        protests.bodylower.str.contains(keyword) &
        protests.bodylower.str.contains("hong kong")
    ]
    .assign(
        found = lambda d: d.bodylower.str.find(keyword),
        start = lambda d: d.found.subtract(window).clip(0),
        end = lambda d: d.found.add(window)
    )
    # .loc[lambda d: d.bodylower.str.contains("central|movement|protest|march")]
    .apply(lambda d: print(d.bodylower[d.start:d.end]), axis=1)
)
# %%
# when do they disagree? 
keyword="protest"
(
    protests[
        ~protests.Topics.astype(str).str.contains("Hong Kong protests|Hong Kong extradition bill") &
        (protests.bodylower.str.contains(keyword) & protests.bodylower.str.contains("hong kong"))

        ]
        .Headline.apply(print)
)
# analysis: lots of false positives when just looking for 

# ANALYSIS: 
# tag approach is best quality for scmp
# let's see if something similar is true for nyt & occupy

# %%
# SCMP OCCUPY ANALYSIS #############################
occupy = scmpdropped[scmpdropped.archivemon.ge("2014-01-01") & scmpdropped.archivemon.lt("2015-01-01")]
z=occupy.Topics.str.split(",").explode().value_counts()
occupy.Topics = occupy.Topics.astype(str)
# 510 articles using topic filtering
topic = occupy[occupy.Topics.str.contains("Universal suffrage in Hong Kong") | occupy.Topics.str.contains("Occupy Central")]
topic
# %%
#  using full body search

keyword="occupy"
window=50
# 909 occupy using keyword search
fulltxt = (
    occupy[occupy.bodylower.str.contains(keyword)]
    .assign(
        found = lambda d: d.bodylower.str.find(keyword),
        start = lambda d: d.found.subtract(window).clip(0),
        end = lambda d: d.found.add(window)
    )
    .loc[lambda d: d.bodylower.str.contains("central|movement|protest|march")]
    # .apply(lambda d: print(d.bodylower[d.start:d.end]), axis=1)
    # .apply(lambda d: print(d.Headline), axis=1)
    # .keywords.fillna("[]").apply(eval).explode().value_counts().head(10)
)

# %%
# disagreement:
fulltxt[~fulltxt.index.isin(topic.index)].apply(lambda d: print(d.Headline), axis=1)
# again a good deal of false positives...
# topic is a tighter way to do it?
#

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
# NYT ANALYSIS ################################
pub = utils.publications['nyt']
ny = utils.get_df(pub)
ny.columns
nyt = utils.main_date_load(pub)

# %% 
# nyt protest ######################
nytkws = utils.get_df(pub, "kws_main.csv")
protests = nyt[nyt.Date.ge("2019-03-01") & nyt.Date.lt("2020-01-01")]
nytkws19 = nytkws[nytkws._id.isin(nyt._id)]
x=nytkws19.value.value_counts()
# main keyword is "Hong Kong Protests (2019)"
# happens about 419 times...

hkkws = nytkws19[nytkws19.value.astype(str).str.contains("Hong Kong Protests")]
topics = protests[protests._id.isin(hkkws._id)]


#  %%
# 307 using full text keyword search protest or riot
# topics definitely the way to go for this one...
keyword = "protest|riot"
window=50
fulltxt = (
    protests[
        protests.bodylower.str.contains(keyword) #&
        # protests.bodylower.str.contains("hong kong")
    ]
    .assign(
        found = lambda d: d.bodylower.str.find(keyword),
        start = lambda d: d.found.subtract(window).clip(0),
        end = lambda d: d.found.add(window)
    )
    # .loc[lambda d: d.bodylower.str.contains("central|movement|protest|march")]
    # .apply(lambda d: print(d.bodylower[d.start:d.end]), axis=1)
)
# %%
# nyt disagreement
topics[~topics._id.isin(fulltxt._id)].Headline.apply(print)
fulltxt[~fulltxt._id.isin(topics._id)].Headline.apply(print)
# topics is slightly better, but not super clear. 

# 256 "Hong Kong Protests (2019)" articles
# 
protests.merge(nytkws, on="_id")
# merge on id; df already exploded

#  %% 
# nyt umbrella coverage ###################
pub = utils.publications['nyt']
nyt = utils.main_date_load(pub)
nytdropped = nyt[~nyt.tts.eq("drop")]

occupy = nytdropped[nytdropped.Date.ge("2014-01-01") & nytdropped.Date.lt("2015-01-01")]
nytkwsoccupy = nytkws[nytkws._id.isin(occupy._id)]
x=nytkwsoccupy.value.value_counts()
x.head()


occukws = nytkwsoccupy[nytkwsoccupy.value.astype(str).str.contains("Demonstrations, Protests and Riots")]
topics = occupy[occupy._id.isin(occukws._id)]
topics.Headline.apply(print)

# %%
topics2 = (
    occupy[
        occupy._id.isin(
            nytkwsoccupy[
                nytkwsoccupy.value
                .astype(str)
                .str.contains("Occupy Central")
                ]._id)
        ]
)
# %%
topics2[~topics2._id.isin(topics._id)].Headline.apply(print)



# %%
# GLOBAL TIMES ANALYSIS ##########################
# WORTHLESS NO TOPICS
pub = utils.publications['globaltimes']
gt = utils.get_df(pub)
gt[gt.Topics.notna()].Topics

# Topics is not a helpful column; will need to use prediction
# for this one... 

# %%
# CHINA DAILY ANALYSIS ##########################
pub = utils.publications['chinadaily']
cd = utils.main_date_load(pub)
protests = cd[cd.Date.ge("2019-03-01") & cd.Date.lt("2020-01-01")]
x=protests.keywords.fillna("[]").apply(eval).explode().value_counts()
protests[protests.bodylower.str.contains("protest")].Headline.apply(print)

y=cd.channelName.value_counts()
y=cd.columnName.value_counts()
# there are violence, police, law, protesters, protests, violent, and rioters,
# but note all these are basically less than 100 individually
# still seems comparable to nyt, but might be missing?

# %%
# FILTERING FOR CD TOPICS
# cd keyword function only good in last two years :(
# For earlier years, lets first try hand crafted features.
pub = utils.publications["chinadaily"]
cd = utils.main_date_load(pub)
cddropped = cd[~cd.tts.eq("drop")]


# %% 
# extradition analysis
protests = cddropped[cddropped.Date.ge("2019-03-01") & cddropped.Date.lt("2020-01-01")]
x=protests.keywords.fillna("[]").apply(eval).explode().value_counts()
# 1245 protest or riot coverage using fullbody kw search
protests[protests.bodylower.str.contains("protest|riot")].Headline.apply(print)
# 255 riot only stories which appear to be useful additions.
protests[protests.bodylower.str.contains("riot") & ~protests.bodylower.str.contains("protest")].Headline.apply(print)

# %%
keyword="protest"
(
    protests[protests.bodylower.str.contains(keyword)]
    .assign(
        found = lambda d: d.bodylower.str.find(keyword),
        start = lambda d: d.found.subtract(window).clip(0),
        end = lambda d: d.found.add(window)
    )
    # .loc[lambda d: d.bodylower.str.contains("central|movement|protest|march")]
    .apply(lambda d: print(d.bodylower[d.start:d.end]), axis=1)
    # .apply(lambda d: print(d.Headline), axis=1)
    # .keywords.fillna("[]").apply(eval).explode().value_counts().head(10)
)
# %%
# 358 articles using just keywords 
kws = "police|protest|violen|riot"
protests[protests.keywords.astype(str).str.lower().str.contains(kws)]

# extradition keyword
# 327 using fulltext kw search
protests[protests.bodylower.str.contains("extradit")].Headline.apply(print)

# %%
# occupy analysis
df2014 = cddropped[cddropped.Date.ge("2014-01-01") & cddropped.Date.lt("2015-01-01")]
x=df2014.keywords.fillna("[]").apply(eval).explode().value_counts()
df2014[df2014.bodylower.str.contains("occupy")].Headline.apply(print)
keyword="occupy"
window=50
# 235 occupy articles
(
    df2014[df2014.bodylower.str.contains("occupy")]
    .assign(
        found = lambda d: d.bodylower.str.find(keyword),
        start = lambda d: d.found.subtract(window).clip(0),
        end = lambda d: d.found.add(window)
    )
    .loc[lambda d: d.bodylower.str.contains("central|movement|protest|march")]
    # .apply(lambda d: print(d.bodylower[d.start:d.end]), axis=1)
    .apply(lambda d: print(d.Headline), axis=1)
    # .keywords.fillna("[]").apply(eval).explode().value_counts().head(10)
)

# %% 
# legco coverage
df2016 = cddropped[cddropped.Date.ge("2016-01-01") & cddropped.Date.lt("2017-01-01")]
x=df2016.keywords.fillna("[]").apply(eval).explode().value_counts()
# 81 legco articles
df2016[df2016.bodylower.str.contains("legco")].Headline.apply(print)
# 475 election articles
df2016[df2016.bodylower.str.contains("elect")].Headline.apply(print)

# %%

df2012 = cddropped[cddropped.Date.ge("2012-01-01") & cddropped.Date.lt("2013-01-01")]
x=df2012.keywords.fillna("[]").apply(eval).explode().value_counts()
# 88 legco articles
df2012[df2012.bodylower.str.contains("legco")].Headline.apply(print)
# 813 election articles
df2012[df2012.bodylower.str.contains("elect")].Headline.apply(print)

# %%
(
    df2016[
        df2016.bodylower.str.contains("elect") &
        df2016.bodylower.str.contains("legco")
        ]
    .Headline.apply(print)
)
