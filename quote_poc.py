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
doc = nlp(txt)
qs = quote_extractor.quote_extractor("1", doc)
print(qs)
# df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\2021.csv")

# %%
def extract_quotes(row, text_col, uid_col):
    """Row is a row in a dataframe with a text column and unique idenifier column."""
    doc = nlp(row[text_col])
    # list of dictionaries
    final_quotes = quote_extractor.extract_quotes(row[uid_col], doc)
    for item in final_quotes:
        item["source"] = row[uid_col]
    return final_quotes


# %%
def run(input_df, output_name):
    quote_dcts = input_df.apply(
        lambda row: extract_quotes(row, "Body", "Index"), axis=1
    )
    quotedf = pd.json_normalize(quote_dcts.explode()).dropna().convert_dtypes()
    quotedf.to_csv(output_name)
    return quotedf
# %%
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

# row = df.iloc[1]
# doc = nlp(row.Body)

# doc.set_ents
# ents = doc.ents
# dict(ent)
# for ent in ents:
#     print("BREAKBREAK")
#     print(f"{ent=}")
#     print(f"{ent.label_=}")
#     print(f"{ent.label=}")
#     # print(f"{ent.ent_id=}")
#     # print(f"{ent.ent_id_=}")
#     # print(f"{ent.id=}")
#     # print(f"{ent.kb_id_=}")
#     # print(f"{ent.kb_id=}")
#     # print(f"{ent.sentiment=}")
#     # print(f"{ent.start=}")
#     # print(f"{ent.end=}")
#     print(f"{ent.vector=}")
#     print(f"{ent.text_with_ws=}")

#     print(f"{ent.start_char=}")
#     print(f"{ent.end_char=}")
    

# nlp.get_pipe("ner").labels
# ents[0].similarity(ents[1])
# %%
