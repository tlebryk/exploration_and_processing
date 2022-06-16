import pandas as pd
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
df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\scmp\2021.csv")
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
row = df.iloc[2]
doc = nlp(row["Body"], disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
doc.ents
# %%
ent.__dir__()
def ner(row, text_col, uid_col, publication="scmp", year="2012"):
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
                "Index": row[uid_col],
                "publication": publication,
                "year": year,
            }
            dct_ls.append(dct)
    return dct_ls

class RunArgs:
    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)

# runarg = RunArgs(**kwargs)
# just use ner kwarg dictionary

def run(input_df, output_name, text_col, **kwargs):
    input_df[text_col] = input_df[text_col].astype(str)
    ner_dcts = input_df.apply(
        lambda row: ner(row, text_col, **kwargs), axis=1
    )
    nerdf = pd.json_normalize(ner_dcts.explode()).dropna().convert_dtypes()
    nerdf.to_csv(output_name)
    return nerdf
    # print(f"{list(ent.sents)=}") # for debugging
for year in range(2011, 2021):
    year = 2021
    publication = "scmp"
    kwargs = {
        "year": year,
        "publication": publication,
        "text_col": "Body",
        "uid_col": "Index"
    }
    # %%
    nerdf = timeit(run, df, f"ner_{year}.csv", **kwargs)


# %%
# paper = "scmp"
# nerdf = timeit(run, df.head(1000), "ner.csv", paper, year)
en = nerdf.iloc[0]
body = df.loc[df.Index.eq(en.Index)].Body.squeeze()
body
doc = nlp(body)
doc[en.start].sent
en
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
