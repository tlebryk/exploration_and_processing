"""Point here is to create a utility function to create the baba dataframe."""
# %%
from thesisutils import utils
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

import logging
# %%
def dfprep(pub: utils.Publication, filter=False):
    """gets df-date merged and filters for scmp and hkfp for 
    alibaba containing articles.
    
    :param filter: True for HKFP and SCMP where no specific 
        baba dataset. 
    """
    print(pub.name)
    if pub.name in ("nyt", "chinadaily", "globaltimes"):
        args = ["baba", f"{pub.name}_full.csv"]
    else:
        args = [f"{pub.name}_full.csv"]
    df = utils.main_date_load(pub, True, *args)
    if filter:
        mask = df.Textcol.str.lower().str.contains("alibaba")
        print("fitering...")
        print(mask.value_counts(dropna=False))
        df = df[mask]
    return df



# %%
cd = dfprep(utils.publications["chinadaily"], True)
cd.Year.value_counts().sort_index()
# %%
nyt = dfprep(utils.publications["nyt"], True)
nyt.Year.value_counts().sort_index()
# %%
scmp = dfprep(utils.publications["scmp"], True)
scmp.Year.value_counts().sort_index()
# %% 
globaltimes = dfprep(utils.publications["globaltimes"], True)
globaltimes.Year.value_counts().sort_index()
# %%
hkfp = dfprep(utils.publications["hkfp"], True)
hkfp.Year.value_counts().sort_index()
val_cnts = scmp.Year.value_counts()#.plot()
val_cnts.sort_index().plot()

# months starting with 1/2011 as first month and 12/2021 as 132. 
# 61 is our rough starting point of 1/2016
# scmp["monthid"] = (scmp.Year - 2011)*12 + scmp.Month
# max(scmp.monthid)

# monthgrouped = scmp.groupby("monthid").size()
# for i in range(0, 133):
#     if i not in monthgrouped.index:
#         monthgrouped.loc[i] = 0
# monthgrouped = monthgrouped.sort_index()

maindf = pd.concat([nyt, cd, scmp, globaltimes, hkfp] )
maindf = maindf[maindf.Year.ge(2011)]
utils.just_letters()
maindf['headlow'] = maindf.Headline.str.lower().apply(utils.justletters)
# drop duplicate headlines from same year. 
maindf = maindf.drop_duplicates(subset=['headlow', 'Year'])
maindf.head()
maindf.Year.value_counts(dropna=False)
maindf['cnt']  = 1
maindfgrouped = maindf.groupby(["Publication", "Year"]).size().rename("Alibaba mentions")
maindfgrouped
# gotta clean up this code
# %%
# plotting yearly alibaba mentions by each publication
ax = sns.lineplot(x="Year", y="Alibaba mentions", hue="Publication", data=maindfgrouped.reset_index()) 
ax.set_title("Figure _._: Mentions of Alibaba per Year")
ax

# %%
def meanb4after(df):
    """Gets average mentions before and after 2016"""
    cutoff = 2015
    val_cnts = df.Year.value_counts()
    # if missing year, fill with zero. 
    for i in range(2011, 2022):
        if i not in val_cnts.index:
            val_cnts.loc[i] = 0
    val_cnts = val_cnts.sort_index()
    pre16 = val_cnts.loc[2011:cutoff]
    post16 = val_cnts.loc[cutoff+1:2022]
    before = pre16.mean()
    after = post16.mean()
    print("percent change = ", 100*(after-before)/before)
    return before, after
# %%
ny = meanb4after(nyt)
# %% 
sc = meanb4after(scmp)
# %
gt = meanb4after(globaltimes)
# %%
c = meanb4after(cd)
# %%
hk = meanb4after(hkfp)

# %%
# Run for table with before and after

tabledf = pd.DataFrame(np.array([ny, hk, gt, c, sc]),
 index=["NYT", "HKFP", "Global Times", "China Daily", "SCMP"],
 columns = ["pre-2016", "post-2016"]
 )
tabledf["% increase"] = 100*(tabledf["post-2016"] - tabledf["pre-2016"])/ tabledf["pre-2016"]
pd.options.display.precision = 0
display(tabledf)
pd.options.display.precision = 5


# COMPETITOR ANALYSIS #########################
# %%
pub = utils.publications["scmp"]
args = [f"{pub.name}_full.csv"]

scmpfull = utils.main_date_load(pub, True, *args)
scmpfull['textcollower'] = scmpfull.Textcol.str.lower()
 # %% 
 
def searchterm(term,df = scmpfull):
    """Looks up a term in df.textcollower"""
    term = term.lower()
    mask = df.textcollower.str.contains(term)
    print("fitering...")
    print(mask.value_counts(dropna=False))
    return df[mask]

# %%
tencent = searchterm("tencent")
ten = meanb4after(tencent)
baidu = searchterm("baidu")
bai = meanb4after(baidu)
jd = searchterm("jd.com")
j = meanb4after(jd)
taobao = searchterm("taobao")
tb = meanb4after(taobao)
wechat = searchterm("wechat")
wc = meanb4after(wechat)
pingduoduo = searchterm("pingduoduo")
pdd = meanb4after(pingduoduo)
didi = searchterm("didi")
dd = meanb4after(didi)
bytedance = searchterm("bytedance")
bd = meanb4after(bytedance)

# %%
tabledf = pd.DataFrame(np.array([sc, ten, bai, j, tb, wc]),
 index=["Alibaba", "Tencent", "Baidu", "JD.com", "Taobao", "WeChat"],
 columns = ["pre-2016", "post-2016"]
 )
tabledf["% increase"] = 100*(tabledf["post-2016"] - tabledf["pre-2016"])/ tabledf["pre-2016"]
pd.options.display.precision = 0
display(tabledf)
pd.options.display.precision = 5


# %%
# old analysis ignore.
pub = utils.publications['scmp']
scmp = utils.get_df(pub)
sections = utils.get_df(pub, "sections", "sections.csv")
datedf = utils.get_df(pub, "date", "date.csv")
scmp = scmp.merge(sections, on="Index")
scmp = scmp.merge(datedf, on="Index")
# 
scmp['textcol'] = scmp.Headline + scmp.Body
mask = scmp.Body.isna()


nas = scmp[mask]
nas.section0.value_counts()
len(nas)
# 12267 nans bodys; but 11,000 are sport or yp so not a big deal'=;
# %%
scmp.textcol.str.contains("Alibaba").value_counts()
# 5000 baba stories
# %%
scmp.textcol.str.lower().str.contains("alibaba").value_counts()
# 5008
# %%
pub = utils.publications['hkfp']
hkfp = utils.get_df(pub)
hkfp['textcol'] = hkfp[pub.headline] + hkfp[pub.textcol]
hkfp.textcol.str.contains("Alibaba").value_counts()
hkfp.textcol.str.lower().str.contains("alibaba").value_counts()
# 178 hkfp
# %%
pub =  utils.publications['nyt']
nyt = utils.get_df(pub, "baba", "nyt_full.csv")
nyt
# 1048 articles
# %%
pub =  utils.publications['globaltimes']
gt = utils.get_df(pub, "baba", f"{pub.name}_full.csv")

# 4688 articles
# %%
pub =  utils.publications['chinadaily']
cd = utils.get_df(pub, "baba", f"{pub.name}_full.csv")
# 7547 articles
# %% 
