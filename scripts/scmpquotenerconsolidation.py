"""SCMP was originally written by year. 
We're just doing consolidation here to fit formatting
of other ner and quotes csvs. 

This will be a very large file unfortunately :(
"""
from thesisutils import utils
import pandas as pd


publication = utils.publications['scmp']
# %% 
# ner
ls = (utils.get_df(publication, "ner", f"ner_{year}.csv") for year in range(2011, 2022))
df = pd.concat(ls) 
utils.df_to_s3(df, f"{publication.name}/ner/ner_full.csv")
df.to_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\ner\ner_full.csv")

# %%
# quotes
ls = ( utils.get_df(publication, "quotes", f"q_{year}.csv") for year in range(2011, 2022))
df = pd.concat(ls) 
utils.df_to_s3(df, f"{publication.name}/quotes/quotes_full.csv")
df.to_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\quotes\quotes_full.csv")
