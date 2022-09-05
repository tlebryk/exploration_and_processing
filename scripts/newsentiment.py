"""
Uses NewsMTSC to get pos, neg, neutral sentiment towards any entity. 
Current pipeline is to read from NER, link to full body
and targetting entities containing certain substrings
Right now runs on "alibaba" containing entities but nothing else 
and no coref resolution. 

runs on newsentiment environ NOT thesis.
Also see pocs/newsentiment_poc.py
"""
# %%
# takes 2 minutes to load target sentiment classifier
import logging
import os
import pandas as pd
import spacy
from NewsSentiment import TargetSentimentClassifier
from NewsSentiment.customexceptions import TooLongTextException, TargetNotFoundException
from thesisutils import utils
from tqdm import tqdm

import logging, logging.config
from pathlib import Path
import logconfig

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)
# %%
tsc = TargetSentimentClassifier()
tqdm.pandas()
# %%
# DATA PREP ##################################
# %%
nlp = spacy.load("en_core_web_md")
nlp.add_pipe("sentencizer")

# %%
# FUNCTIONS ###########################################
def span_clean(span):
    """
    span is a spacy span e.g. doc[1:14];
    default is to return span.text,
    but if we see a "Post" reference,
    remove that clause before text conversion.
    """
    delimiters = [",", "(", "-", ")"]
    delim_idx = []
    for tok in span:
        if tok.text in delimiters:
            delim_idx.append(tok.i - span.start)
    if delim_idx:
        ls = []
        subspan = span[: delim_idx[0]]
        if not "Post" in subspan.text:
            ls.append(subspan)
        for i, el in enumerate(delim_idx):
            if i + 1 == len(delim_idx):
                subspan = span[el:]
                if not "Post" in subspan.text:
                    ls.append(subspan)
            else:
                subspan = span[el : delim_idx[i + 1]]
                if "Post" in subspan.text:
                    continue
                ls.append(span[el : delim_idx[i + 1]])
        return "".join(e.text for e in ls)
    else:
        if "Post" in span.text:
            return ""
        else:
            return span.text


# %%
# NOTE: these functions take a standardized df;
# STANDARDIZE IN FIRST LOADING STEP BEFORE passing to fns.
def get_sentiment(row, doc, publication):
    """
    returns dictionary with positive, negative, and neutral
        probabilities for a given NER object.
    Note: result has nan values when input is too long.
    Be sure to do exploration / reporting later.
    :param row: row of NER datafarme
    :param doc: spacy nlped (w/ sentencizer) doc
    """
    start = row.start
    end = row.end
    sent = doc[start].sent
    targ = doc[start:end].text
    if start == sent.start:
        # empty left, target, full right pattern
        tup = ("", targ, span_clean(doc[end : sent.end]))
    elif end == sent.end:
        tup = (span_clean(doc[sent.start : start]), targ, "")
        # full left target empty right
    else:
        tup = (
            span_clean(doc[sent.start : start]),
            targ,
            span_clean(doc[end : sent.end]),
        )
    result = {
        "ner_index": row._name,
        "publication": publication.name,
        "sentence": sent,
        "Art_id": row["Art_id"],
    }
    # return result
    result["Art_id"] = row["Art_id"]
    result["debug"] = " ".join(tup)
    labels = ["negative", "neutral", "positive"]
    try:
        sentiment = tsc.infer_from_text(*tup)
        # might need to add could not find target too.
    except TooLongTextException as e:
        logging.warning("TOO LONG?", e)
        result.update({label: None for label in labels})
        return result
    except TargetNotFoundException as e2:
        logging.warning("TOO TARGET NOT FOUND?", e2)
        result.update({label: None for label in labels})
        return result
    result.update(
        {
            label: dct["class_prob"]
            for label in labels
            for dct in sentiment
            if dct["class_label"] == label
        }
    )

    return result


def getsent2(row, df_target, publication):
    """Basically wrapper around get_sentiment to set doc variable.
    :param row: entity row.
    """
    try:
        doc = nlp(
            df_target.loc[row.Art_id]["Body"],
            disable=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner"],
        )
        return get_sentiment(row, doc, publication)
    except Exception as e:
        logger.warning(e)
        logging.warning(
            "exception encountered on article %s index %s", row.Art_id, row._name
        )
        result = {
            "ner_index": row._name,
            "publication": publication.name,
            "sentence": "",
            "Art_id": row["Art_id"],
        }
        labels = ["negative", "neutral", "positive"]
        result.update({label: None for label in labels})
        return result


def save(key, maindf, bucket="newyorktime"):
    # key = f"{publication.name}/sentiment/{target}_test.csv"
    path = os.path.join(utils.ROOTPATH, "baba", key)  # NOTE: baba in path.
    logger.info("saving %s", key)
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    maindf.to_csv(path)
    logger.info("uploading to %s/%s", bucket, key)
    utils.upload_s3(path, bucket, key)


# %%
def run(pub, target, tts="full", bucket="newyorktime"):
    """Performs sentiment analysis on publications ner
    :param sample: subsets data to just 10% train split.
    :param tts: train test split: 'full', 'train', or 'test'.
        train or test will filter for just that mask.
    """
    # todo: get df from s3
    nerdf = utils.standardize(
        utils.read_df_s3(f"{pub.name}/ner/ner_full.csv", bucket),
        # utils.get_df(pub, "ner", "ner_full.csv"),
        pub,
        drop_dups=False,
    )
    df = utils.standardize(
        utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket), pub
    )
    df = df.set_index("Art_id")
    if tts != "full":
        split = utils.standardize(
            utils.read_df_s3(f"{pub.name}/tts_mask/{tts}_main1.csv", bucket), pub
        )
        df = df[df.index.isin(split.Art_id)]
        nerdf = nerdf[nerdf.Art_id.isin(split.Art_id)]

    # filter maindf for only cases where baba shows up;
    # takes 1.5 minutes to filter
    mask = nerdf.entity.astype(str).str.lower().str.contains(target)
    ner_target = nerdf[mask]
    # scmp has 12627 entities to look up;
    # ~1000 training examples
    logger.info(f"working on {len(ner_target)} entities")
    # masking main df takes 23 s
    mask2 = df["Body"].astype(str).str.lower().str.contains(target)
    df_target = df[mask2]
    logger.info(f"working on {len(df_target)} documents")
    # 5007 documents with alibaba total; 432 in training set.
    # 1.5-2s per iteration = 5 hours to run on full;30m-1 hr on subset
    rows = ner_target.progress_apply(lambda row: getsent2(row, df_target, pub), axis=1)
    maindf = pd.json_normalize(rows)
    key = f"{pub.name}/sentiment/{target}_{tts}.csv"
    save(key, maindf, bucket)
    return maindf


# %%
target = "alibaba"
bucket = "aliba"

# %%
pub = utils.publications["hkfp"]
maindf = utils.timeit(run, pub, target, "train", bucket)
print(maindf.head())
# %%
pub = utils.publications["nyt"]
maindf = utils.timeit(run, pub, target, "train", bucket)
print(maindf.head())
# %%
pub = utils.publications["globaltimes"]
maindf = utils.timeit(run, pub, target, "test", bucket)
maindf = utils.timeit(run, pub, target, "train", bucket)

print(maindf.head())
# %%
pub = utils.publications["scmp"]
maindf = utils.timeit(run, pub, target, "test", bucket)
maindf = utils.timeit(run, pub, target, "train", bucket)
print(maindf.head())
# %%
pub = utils.publications["scmp"]
maindf = utils.timeit(run, pub, target, "train", bucket)
print(maindf.head())
# %%
pub = utils.publications["chinadaily"]
maindf = utils.timeit(run, pub, target, "train", bucket)
print(maindf.head())
# sanity check the post removal works.
# y = maindf.sentence.str.contains("Post")
# y.apply(lambda d: print(d.debug, "\n", d.sentence, "\n new \n"), axis=1)

# %%
# GROUP APPROACH SLIGHTLY SLOWER? ########################################
# groups = ner_target.groupby("Art_id")
# dfls = []
# groups = ner_target.groupby("Art_id")
# for name, group in tqdm(list(groups)):
#     # if name == 876073:
#         print(name)
#         doc = nlp(
#             df_target.loc[name]["Body"],
#             disable=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner"],
#         )
# mask = group.entity.str.lower().str.contains("alibaba")
# filtered = group[mask]
# if len(filtered) > 0:
# res = group.progress_apply(lambda row: get_sentiment(row, doc, pub), axis=1)
# resdf = pd.json_normalize(res)
# dfls.append(resdf)
# maindf = pd.concat(dfls)
# len(list(groups))
# %%
# name
# group
# dfls = []

# ner_baba
# ner_target.Art_id.eq(876073).value_counts()
# for name, group in list(groups)[:5]:
#     doc = nlp(
#         df_target.loc[name]["Body"],
#         disable=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner"],
#     )
#     # mask = group.entity.str.lower().str.contains("alibaba")
#     # filtered = group[mask]
#     # if len(filtered) > 0:
#     res = group.apply(lambda row: get_sentiment(row, doc), axis=1)
#     resdf = pd.json_normalize(res)
#     dfls.append(resdf)
# # takes 30 seconds to run on 5 groups which is 18 entities

# very slow for now...
# but does get sentiment for each relevant entity;
# seems better to filter for baba before grouping bc group operations suck.
# maindf = pd.concat(dfls)
# maindf
# upload to s3 here.

# %%

# import logging


maindf.tail()[["negative", "positive", "neutral"]]
maindf.tail().debug.apply(print)
