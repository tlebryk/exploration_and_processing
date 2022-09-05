"""
Explores the effect of subsetting the data on political articles
 and dropping columns based on keywords
"""
# %%
import pandas as pd
import os
import re
from thesisutils import utils

ROOTPATH = utils.ROOTPATH

# %%
# minieda 
class Publication:
    def __init__(self, name, textcol, uidcol) -> None:
        self.name = name
        self.textcol = textcol
        self.uidcol = uidcol
    def __repr__(self) -> str:
        return self.name


publications = [
    Publication("scmp", "Body", "Index"),
    # Publication("nyt", "text", "sourceurl"),
    Publication("globaltimes", "Body", "Art_id"),
    Publication("chinadaily", "plainText", "id"),
    Publication("hkfp", "Body", "Art_id"),
]

def get_perc(a, b):
    return round(a/ (a+b),3) *100

def report(a, b):
    perc = get_perc(a,b)
    print(perc, "%")
    print(a, "/", a+b)

def get_df(publication, *args):
    """
    
    :param: args are strings which are joined to be the final path
        e.g. "polimask", "pmask_.csv" becomes rootpath/publication/polimask/pmask_.csv
    """
    fullpath = os.path.join(ROOTPATH, publication.name, *args)
    df = pd.read_csv(fullpath)
    return df


def analysis(publication : Publication):
    """read polimask and report perc over >.5
    
    :param Publication object see above with name, textcol, uidcol. 
    """
    print(publication.name.upper())
    df = get_df(publication)
    poli =  df.loc[lambda d: d.poliestimation >= 0.5]
    nonpoli =  df.loc[lambda d: d.poliestimation < 0.5]
    print("political percentage:")
    report(len(poli), len(nonpoli))



for publication in publications:
    analysis(publication)

# political filtering: 
# SCMP
# 59.699999999999996 %
# 160482 / 268629
# GLOBALTIMES
# 69.89999999999999 %
# 18386 / 26293
# CHINADAILY
# 87.2 %
# 93105 / 106770
# HKFP
# 84.7 %
# 17280 / 20396

# %% hong kong filter
publication = "scmp"
df = pd.read_csv(rf"{utils.ROOTPATH}\scmp\scmp_full.csv")
df['textconcat'] = df.Headline + df.Body


def justletters(s):
    return re.sub(r"[^A-Za-z]+", '', s)
df.textconcat = df.textconcat.astype(str).apply(justletters)

mask = df.textconcat.str.lower().str.contains("hongkong")
mask.value_counts(dropna=False)


report(len(df[mask]), len(df[~mask]))
# hong kong is in 44.2 % of articles
# 118733 / 268629
df.loc[mask.notna() & mask.eq(False)].Topics.explode().value_counts().head(10)
# hong kong filter seems effective - topics are irrelevant to hk or magazine anyway. 

# %%
# political filter mask: 
# let's see when we do the 0.75 level. 
from thesisutils import utils
# %%
def loaddata(pub):
    df = utils.main_date_load(pub) 
    pmask = utils.get_df(pub, "polimask/pmask_.csv")
    pmask = utils.standardize(pmask, pub)
    #df =
    df=df.merge(pmask, how="left", on="Art_id")
    return df

# %%
pub = utils.publications['chinadaily']
chinadaily = loaddata(pub)
# %%
pub = utils.publications['nyt']
nyt = loaddata(pub)
# %%
pub = utils.publications['hkfp']
hkfp = loaddata(pub)
# %%
pub = utils.publications['scmp']
scmp = loaddata(pub)
# %%
pub = utils.publications['globaltimes']
globaltimes = loaddata(pub)
# %%
def get_intermediary_heads(df):
    utils.drop_report(df, df.poliestimation.ge(.5) & df.poliestimation.le(0.75))
    df[df.poliestimation.ge(.5) & df.poliestimation.le(0.75)].Headline.apply(print)
# %%
dfls = [chinadaily, nyt, hkfp, scmp, globaltimes]
# %%
get_intermediary_heads(scmp)
# %%
get_intermediary_heads(hkfp)
# %%
get_intermediary_heads(chinadaily)
# %%
get_intermediary_heads(hkfp)
# %%
get_intermediary_heads(nyt)
# %%
get_intermediary_heads(globaltimes)

# %%
def gettrainingsize(df):
    # print(df.rename(columns={"Publication": "Publication_x"}).Publication.iloc[0])
    print(len(df[df.poliestimation.ge(0.75)])*.1)
[gettrainingsize(d) for d in dfls]


# NOTE: need to drop more from China daily
x=df[df.Headline.str.contains("China Daily editorial")]
# NOTE: need to drop more from Global Times editorial
x