"""Proof of concept for direct quote extraction and attribution using
textacy.
Read files from Thesis/data/scmp/2021.csv, get text, and extract quotes 

"""

#%%
# import pandas as pd
from quotes import quote_extractor
import spacy
import time 


def timeit(fn, *args, **kwargs):
    s = time.perf_counter()
    ret = fn(*args, **kwargs)
    e = time.perf_counter()
    print(f"{fn.__name__} took {e-s} secs")
    return ret

#%%
# first col has quotes
nlp = spacy.load("en_core_web_lg")
df = pd.read_csv(fr"{utils.ROOTPATH}\scmp\2021.csv")
df.Body = df.Body.astype(str)

# %%
def extract_quotes(row, text_col, uid_col):
    """Row is a row in a dataframe with a text column and unique idenifier column."""
    doc = nlp(row[text_col], disable=[
        # "tok2vec",
        "tagger",
        # "parser",
        "attribute_ruler",
        "lemmatizer"
    ])
    # list of dictionaries
    final_quotes = quote_extractor.extract_quotes(row[uid_col], doc)
    for item in final_quotes:
        item["source"] = row[uid_col]
    return final_quotes
row = df.iloc[2]
print(len(extract_quotes(row, "Body", "Index")))
# %%
def run(input_df, output_name):
    quote_dcts = input_df.apply(
        lambda row: extract_quotes(row, "Body", "Index"), axis=1
    )
    quotedf = pd.json_normalize(quote_dcts.explode()).dropna().convert_dtypes()
    quotedf.to_csv(output_name)
    return quotedf
# %%
resdf = timeit(run, df.head(100), "test2.csv")
print(len(resdf))
# timeit(run, df.head(), "test.csv")
# df.Body = df.Body.astype(str)
# 2021 had some Nan columns which will not have named entities
# TODO: create cleaning script which has columns to drop


# %%
"""RESULTS: 
On 30,000 rows: 
- time: 52 minutes
- space: 55 mb

"""
# quotedf.speaker

