"""Searches for people, orgs, nations, places, laws using spacy NER."""

# %%
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
# %%
# first col has quotes
nlp = spacy.load("en_core_web_lg")
# df = pd.read_csv(rf"{utils.ROOTPATH}\scmp\2021.csv")
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
    """Get all nes from a a row in the main df where row has a text_col"""
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

# HKMAIN RUN #####################################
 # %%
def run(input_df, output_name, text_col, **kwargs):
    """takes df and creates ner df out of it."""
    input_df[text_col] = input_df[text_col].astype(str)
    ner_dcts = input_df.progress_apply(
        lambda row: ner(row, text_col, **kwargs), axis=1
    )
    nerdf = pd.json_normalize(ner_dcts.explode()).dropna().convert_dtypes()
    nerdf.to_csv(output_name)
    return nerdf

# %%
def ner_run(publication, key=None):
    """Wraps run by getting df and other vars for a spec publication."""
    # publication = utils.publications["nyt"]
    logger.info("working on %s", publication.name)
    df = utils.get_df(publication)
    kwargs = {
        # "year": year,
        "publication": publication,
        "text_col": publication.textcol,
        "uid_col": publication.uidcol
    }
    if not key:
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


    
# ALIBABA RUN #####################################
# %%
def ner_run_baba(pub, local=True, outputkey=None):
    """Copy of ner_run but with new functionality to 
        fit baba data.
    """
    # publication = utils.publications["nyt"]
    logger.info("working on %s", pub.name)
    if local:
        df = utils.get_df(pub)
    else:
        df = utils.read_df_s3(object_key=f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
    kwargs = {
        # "year": year,
        "publication": pub,
        "text_col": pub.textcol,
        "uid_col": pub.uidcol
    }
    # s3 key
    outputkey = f"{pub.name}/ner/ner_full.csv"
    output_name = os.path.join(utils.ROOTPATH, "baba", outputkey)
    if not os.path.exists(os.path.dirname(output_name)):
        os.makedirs(os.path.dirname(output_name))
    nerdf = utils.timeit(run, df, output_name, **kwargs)
    utils.df_to_s3(nerdf, outputkey)
# %%
pub = utils.publications['globaltimes']
ner_run_baba(pub, False)


