from thesisutils import utils
import pandas as pd
import spacy
import logging

import logging, logging.config
from pathlib import Path
import os

import pandas as pd

import logconfig

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)


# logger = logging.getLogger("simpleExample")




lgconf = logconfig.logconfig(Path(__file__).stem)


#%%
# first col has quotes
nlp = spacy.load("en_core_web_lg")
# df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\2021.csv")
# %%
ner_filter = [
    "DATE",
    "WORK_OF_ART",
    "PERCENT",
    "QUANTITY",
    "TIME",
    "MONEY",
    # "LAW",
    "LANGUAGE"
    "ORDINAL",
    "CARDINAL",

]
# %%
# row = df.iloc[2]
# doc = nlp(row["Body"], disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
# doc.ents
# %%

def ner(row, text_col, uid_col, publication, year="2012"):
    dct_ls = []
    doc = nlp(row[text_col], disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
    ents = doc.ents
    for ent in ents:
        if ent.label_ not in ner_filter:
            dct = {

                "entity" : ent.text,
                "label_" : ent.label_,
                # "label" : ent.label,
                "start" : ent.start,
                "end" : ent.end,
                publication.uidcol: row[uid_col],
                "publication": publication.name,
                "year": year,
            }
            dct_ls.append(dct)
    return dct_ls

def run(input_df, output_name, text_col, **kwargs):
    input_df[text_col] = input_df[text_col].astype(str)
    ner_dcts = input_df.apply(
        lambda row: ner(row, text_col, **kwargs), axis=1
    )
    nerdf = pd.json_normalize(ner_dcts.explode()).dropna().convert_dtypes()
    nerdf.to_csv(output_name)
    return nerdf
    # print(f"{list(ent.sents)=}") # for debugging

publication = utils.publications["scmp"]
for year in range(2011, 2020):
    
    df = utils.get_df(publication, f"{year}.csv")
    kwargs = {
        "year": year,
        "publication": publication,
        "text_col": publication.textcol,
        "uid_col": publication.uidcol
    }
    # %%

    nerdf = utils.timeit(run, df, f"ner_{year}.csv", **kwargs)


# %%
# paper = "scmp"
# nerdf = timeit(run, df.head(1000), "ner.csv", paper, year)
# en = nerdf.iloc[0]
# body = df.loc[df.Index.eq(en.Index)].Body.squeeze()
# body
# doc = nlp(body)
# doc[en.start].sent
# en
# %%
    # print(f"{list(ent.sents)=}")

    # print(f"{ent.ent_id=}")
    # print(f"{ent.ent_id_=}")
    # print(f"{ent.id=}")
    # print(f"{ent.kb_id_=}")
    # print(f"{ent.kb_id=}")
    # print(f"{ent.sentiment=}")
    # print(f"{ent.start=}")
    # print(f"{ent.end=}")
    # print(f"{ent.vector=}")
    # print(f"{ent.text_with_ws=}")


    

# %%
