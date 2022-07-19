"""
Because of difficulty of exporting political-news-filter
(https://github.com/lukasgebhard/Political-News-Filter)
as a package to be run anywhere, I do exploration using that script
in C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\polinews\polinews\section_poc.py.
See that script for information about scmp section eda, where we verify that
we're getting mostly news articles on recent years. 
. 

Random note: sections are robust by 2013, but 2011 & 2012 data is sketchy. 
"""
# %%
import xml.etree.ElementTree as ET
import os
from pathlib import Path
import pandas as pd

# %% 
# looking at just the archives from the sitemap. 
path = r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\scmp\sscmp\archives\months"
months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec"
]
df_ls = []
for yr in range(2011, 2021):
    for mon in months:
        pass
        filename = f"sitemap_archive_{yr}_{mon}.xml"
        tree = ET.parse(os.path.join(path, filename))
        root = tree.getroot()
        urls = [item[0].text for item in root]
        df = pd.DataFrame(urls, columns=["url"])
        df["Year"] = yr
        df_ls.append(df)
maindf = pd.concat(df_ls)
# %% 
# percent of articles which were editorial
valcnts = maindf.url.str.contains('comment').value_counts()

from thesisutils import utils
tru = valcnts.loc[True]
fals = valcnts.loc[False]
utils.report(tru, fals)
# %%