"""
Uses NewsMTSC to get pos, neg, neutral sentiment towards any entity. 
Current pipeline is to read from NER, link to full body
and targetting entities containing certain substrings
 (thinking alibaba and maybe some poli entities). 
 Run using newssentiment environment (or coref for bottom half) NOT thesis.


Current issues: requires splitting up the string on the entity. 
Solution: 
- let's use basic NER first
- then do mini coreference resolution: attribute last names to first names using code from quoteanalysis.py
- Do a full coref pipelint as started at the bottom of this fule
    - Coref res would affect: quote attribution, NER (sorta), keyword analysis (contextr), and target-based sentiment. 
"""
# %%
import logging
import os
import pandas as pd
import spacy
from NewsSentiment import TargetSentimentClassifier
from NewsSentiment.customexceptions import TooLongTextException
# , TargetNotFoundException
from thesisutils import utils
from tqdm import tqdm

tsc = TargetSentimentClassifier()

# sample sentences: 
# sentiment = tsc.infer_from_text("I don't like", "Robert.", "")
# print(sentiment[0])
# sentiment = tsc.infer_from_text("" ,"Mark Meadows", "'s coverup of Trump’s coup attempt is falling apart.")
# print(sentiment[0])
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
            delim_idx.append(tok.i-span.start)
    if delim_idx:
        ls = []
        subspan = span[:delim_idx[0]]
        if not "Post" in subspan.text:
            ls.append(subspan)
        for i, el in enumerate(delim_idx):
            if i+1 == len(delim_idx):
                subspan = span[el:]
                if not "Post" in subspan.text:
                    ls.append(subspan)
            else:
                subspan = span[el:delim_idx[i+1]]
                if "Post" in subspan.text:
                    continue
                ls.append(span[el:delim_idx[i+1]])
        return "".join(e.text for e in ls)
    else:
        if "Post" in span.text:
            return ""
        else:
            return span.text



# %%
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
        publication.uidcol: row[publication.uidcol],
    }
    # return result
    result[publication.uidcol] = row[publication.uidcol]
    result['debug'] = " ".join(tup)
    labels = ["negative", "neutral", "positive"]
    try:
        sentiment = tsc.infer_from_text(*tup)
        # might need to add could not find target too. 
    except TooLongTextException as e:
        logging.warning("TOO LONG?", e)
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
    """Basically wrapper around get_sentiment to set doc variable."""
    doc = nlp(
        df_target.loc[row.Index][publication.textcol],
        disable=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner"],
    )
    return get_sentiment(row, doc, publication)
    
def save(publication, target, df):
    key = f"{publication.name}/sentiment/{target}.csv"
    path = os.path.join(utils.ROOTPATH, key)
    logging.info("saving %s", key)
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
        df.to_csv(path)
        utils.upload_s3(path, key=key)

# %%
def run(publication, target):
    nerdf = utils.get_df(publication, "ner", "ner_full.csv")
    df = utils.get_df(publication)
    df = df.set_index("Index")
    # filter maindf for only cases where baba shows up;
    mask = nerdf.entity.astype(str).str.lower().str.contains(target)
    ner_target = nerdf[mask]
    # filter nerdf for only cases entity has baba
    mask2 = df[publication.textcol].astype(str).str.lower().str.contains(target)
    df_target = df[mask2]
    rows = ner_target.progress_apply(lambda row: getsent2(row, df_target, publication), axis=1)
    maindf = pd.json_normalize(rows)
    save(publication, target, maindf)
    return maindf

# %%
target = "alibaba"
publication = utils.publications["scmp"]
maindf = utils.timeit(run, publication, target)
# sanity check the post removal works. 
# y = maindf.sentence.str.contains("Post")
# y.apply(lambda d: print(d.debug, "\n", d.sentence, "\n new \n"), axis=1)




# %%
# GROUP APPROACH SLIGHTLY SLOWER? ########################################
# groups = ner_baba.groupby(publication.uidcol)

# len(list(groups))
# %%
# name
# group
# dfls = []

# ner_baba

# for name, group in list(groups)[:5]:
#     doc = nlp(
#         df_baba.loc[name][publication.textcol],
#         disable=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner"],
#     )
#     # mask = group.entity.str.lower().str.contains("alibaba")
#     # filtered = group[mask]
#     # if len(filtered) > 0:
#     res = group.apply(lambda row: get_sentiment(row, doc), axis=1)
#     resdf = pd.json_normalize(res)
#     dfls.append(resdf)
# takes 30 seconds to run on 5 groups which is 18 entities

# very slow for now...
# but does get sentiment for each relevant entity;
# seems better to filter for baba before grouping bc group operations suck.
# maindf = pd.concat(dfls)
# maindf
# upload to s3 here.

# %%

# import logging

# import pandas as pd
# import neuralcoref
# %%
# COREFF APPORACH OLD
# import spacy

# logging.basicConfig(level=logging.INFO)
# nlp = spacy.load("en_core_web_md")
# neuralcoref.add_to_pipe(nlp)
# #%%
# doc = nlp("My sister has a dog. She loves him.")
# nlp.remove_pipe(name="neuralcoref")
# coref = neuralcoref.NeuralCoref(
#     nlp.vocab, conv_dict={"Lam": ["woman", "Carrie", "executive"]}
# )
# nlp.add_pipe(coref, name="neuralcoref")
# doc = nlp(
#     "Carrie Lam passed the extradition bill, which Ted Hui said will ruin Hong Kong. Lam disagrees with him."
# )
# doc._.has_coref
# doc._.coref_clusters[1].main.text  # .mentions[0].text
# doc._.coref_scores
# doc._.coref_resolved

# #%%
# df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\2021.csv")
# row = df.iloc[3]
# r
# doc = nlp(row.Body)

# ppl = [ent for ent in doc.ents if ent.label_ == "PERSON"]
# ppl[-1].start
# ppl[-1].end
# sent_start = ppl[-1].sent.start
# off_start = ppl[-1].start - sent_start
# off_end = ppl[-1].end
# ppl[-1].sent
# [p.sent for p in ppl][-1].start
# [0]
# # .start
# # [1]

# doc.ents[0].label_ == "PERSON"
# print(x.Body)
