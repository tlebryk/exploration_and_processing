"""Searches for people, orgs, nations, places, laws using spacy NER."""
import os
from thesisutils import utils
import pandas as pd
import spacy
from tqdm import tqdm

import pandas as pd

import logging, logging.config
from pathlib import Path
import logconfig

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)

tqdm.pandas()

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

def ner(row, text_col, uid_col, publication):
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
                # "year": year,
            }
            dct_ls.append(dct)
    return dct_ls

def run(input_df, output_name, text_col, **kwargs):
    input_df[text_col] = input_df[text_col].astype(str)
    ner_dcts = input_df.progress_apply(
        lambda row: ner(row, text_col, **kwargs), axis=1
    )
    nerdf = pd.json_normalize(ner_dcts.explode()).dropna().convert_dtypes()
    nerdf.to_csv(output_name)
    return nerdf
    # print(f"{list(ent.sents)=}") # for debugging

# for publication in utils.publications.values():
#     if publication.name == "scmp":
#         continue
#     else:

# publication = utils.publications["scmp"]
# for year in range(2020, 2021):#2020
    # logger.info("working on %s", year)

# %%
# publication = utils.publications["nyt"]
# logger.info("working on %s", publication.name)
# df = utils.get_df(publication)
# kwargs = {
#     # "year": year,
#     "publication": publication,
#     "text_col": publication.textcol,
#     "uid_col": publication.uidcol
# }
# key = f"{publication.name}/ner/ner_full.csv"
# output_name = os.path.join(utils.ROOTPATH, key)
# nerdf = utils.timeit(run, df, output_name, **kwargs)
# utils.df_to_s3(nerdf, key)
# %%
def ner_run(publication):
    # publication = utils.publications["nyt"]
    logger.info("working on %s", publication.name)
    df = utils.get_df(publication)
    kwargs = {
        # "year": year,
        "publication": publication,
        "text_col": publication.textcol,
        "uid_col": publication.uidcol
    }
    key = f"{publication.name}/ner/ner_full.csv"
    output_name = os.path.join(utils.ROOTPATH, key)
    if not os.path.exists(output_name):
        os.makedirs(os.path.dirname(output_name))
    nerdf = utils.timeit(run, df, output_name, **kwargs)
    utils.df_to_s3(nerdf, key)
# %%
ner_run(utils.publications['chinadaily'])
# %%
ner_run(utils.publications['nyt'])
# %%
# 3 run starting here
ner_run(utils.publications['hkfp'])
# %%
ner_run(utils.publications['globaltimes'])
# %%
ner_run(utils.publications['scmp'])


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
