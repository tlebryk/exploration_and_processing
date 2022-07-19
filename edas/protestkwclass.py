# %%
from thesisutils import utils
import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer as CV
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import SGDClassifier, LogisticRegression
# %%
pub = utils.publications['scmp']
df = utils.main_date_load(pub)
# filter opinion 
# extradition bill introduction starts Mar 15 2019
# set end as December 2019 (2020 covid shut down protests)
protests = df[df.Date.ge("2019-03-15") & df.Date.lt("2020-01-01")]
protests.Topics = protests.Topics.astype(str)
protests['hk_protest_tag'] = protests.Topics.str.contains("Hong Kong protests")
protests['extradition_tag'] = protests.Topics.str.contains("Hong Kong extradition bill")
protests['Hong_Kong_police'] = protests.Topics.str.contains("Hong Kong police")
protests['Hong_Kong_Basic_Law'] = protests.Topics.str.contains("Hong Kong Basic Law")
protests[protests.Hong_Kong_Basic_Law].Headline.apply(print)

protests[protests['Hong_Kong_police'] & (~protests['hk_protest_tag'] & ~protests['extradition_tag'])]
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

