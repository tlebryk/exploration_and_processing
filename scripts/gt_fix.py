"""scrper somehow picked up pre 2011 articles. I manually dropped 
these articles from the main globaltimes_full.csv. I store the drops in tts_mask/drops_main1.csv
Each block removes the drops from subsequent files. I manually uploaded to s3 to replace old files.

"""
# %%
from thesisutils import utils
import os
import pandas as pd

# %%
pub = utils.publications["globaltimes"]
drops = utils.get_df(pub, "tts_mask", "drops_main1.csv")
test = utils.get_df(pub, "tts_mask", "test_main1.csv")
train = utils.get_df(pub, "tts_mask", "train_main1.csv")

train2 = train[~train.Art_id.isin(drops.Art_id)]
test2 = test[~test.Art_id.isin(drops.Art_id)]
train2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "train_main1.csv"))
test2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "test_main1.csv"))

# %% TODO:
# filter ner
# filter hk mask
# filter poli mask
# filter date
# quotes
# replace on S3.

ner = utils.get_df(pub, "ner", "ner_full.csv")
ner2 = ner[~ner.Art_id.isin(drops.Art_id)]
ner2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "ner", "ner_full2.csv"))
# %%
ner = utils.get_df(pub, "ner", "ner_full.csv")
ner2 = ner[~ner.Art_id.isin(drops.Art_id)]
ner2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "ner", "ner_full2.csv"))
# %%
quotes = utils.get_df(pub, "quotes", "quotes_full.csv")
quotes2 = quotes[~quotes.Art_id.isin(drops.Art_id)]
quotes2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "quotes", "quotes_full2.csv"))
# %% 
hkmask = utils.get_df(pub, "hk_mask", "hkmask.csv")
hkmask2 = hkmask[~hkmask.Art_id.isin(drops.Art_id)]
hkmask2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "hk_mask", "hkmask2.csv"))
# %%
polimask = utils.get_df(pub, "polimask", "pmask_.csv")
polimask2 = polimask[~polimask.Art_id.isin(drops.Art_id)]
polimask2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "polimask", "pmask2_.csv"))
# %%
date = utils.get_df(pub, "date", "date.csv")
date2 = date[~date.Art_id.isin(drops.Art_id)]
date2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "date", "date2.csv"))
# %%
# PART II ##########################################################
# 8/2/2022: we tried to train test split on opinions, but 
# missed two categories. 
# here we change the original train, test, drop files on local and s3
# and change the ner and quotes; 
# run on both hk and alibaba data

pub = utils.publications['globaltimes']
# %%
df = utils.read_df_s3(None, pubdefault=pub)
train = utils.read_df_s3(f"{pub.name}/tts_mask/train_main1.csv")
test = utils.read_df_s3(f"{pub.name}/tts_mask/test_main1.csv")
drops = utils.read_df_s3(f"{pub.name}/tts_mask/drops_main1.csv")


opinionmask = df.Section.astype(str).str.contains("GT Voice|Opinion")
opinions = utils.drop_report(df, opinionmask)
traindrops = train[train.Art_id.isin(opinions.Art_id)]
testdrops =  test[test.Art_id.isin(opinions.Art_id)]

train2 = utils.drop_report(train, ~train.Art_id.isin(opinions.Art_id))
test2 = utils.drop_report(test, ~test.Art_id.isin(opinions.Art_id))
drops2 = pd.concat((drops, traindrops, testdrops))
# %%
train.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "train_main0.csv"))
train2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "train_main1.csv"))
utils.df_to_s3(train2, f"{pub.name}/tts_mask/train_main1.csv", bucket="newyorktime")
# %%
test.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "test_main0.csv"))
test2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "test_main1.csv"))
utils.df_to_s3(test2, f"{pub.name}/tts_mask/test_main1.csv", bucket="newyorktime")
# %%
drops.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main0.csv"))
drops2.to_csv(os.path.join(utils.ROOTPATH, pub.name, "tts_mask", "drops_main1.csv"))
utils.df_to_s3(drops2, f"{pub.name}/tts_mask/drops_main1.csv", bucket="newyorktime")
# %%
# yr = 2012
for yr in range(2011, 2022):
    print(yr)
    print("ner")
    # NER first 
    key = f"{pub.name}/ner/ner_test_{yr}(2).csv"
    nerdf = utils.read_df_s3(key)
    nerdf2 = utils.drop_report(nerdf, ~nerdf.Art_id.isin(drops2.Art_id))
    utils.df_to_s3(nerdf2, key)
    print("quote")
    key = f"{pub.name}/quotes/quotes_test_{yr}(2).csv"
    quotedf = utils.read_df_s3(key)
    quotedf2 = utils.drop_report(quotedf, ~quotedf.Art_id.isin(drops2.Art_id))
    utils.df_to_s3(quotedf2, key)


# %%
#  ALIBABA filter 

# %%
bucket = "aliba"
df = utils.read_df_s3(None, pubdefault=pub, bucket=bucket)
train = utils.read_df_s3(f"{pub.name}/tts_mask/train_main1.csv", bucket=bucket)
test = utils.read_df_s3(f"{pub.name}/tts_mask/test_main1.csv", bucket=bucket)
drops = utils.read_df_s3(f"{pub.name}/tts_mask/drops_main1.csv", bucket=bucket)
# %%
opinionmask = df.Section.astype(str).str.contains("GT Voice|Opinion")
opinions = utils.drop_report(df, opinionmask)
traindrops = train[train.Art_id.isin(opinions.Art_id)]
testdrops =  test[test.Art_id.isin(opinions.Art_id)]
# %%
train2 = utils.drop_report(train, ~train.Art_id.isin(opinions.Art_id))
test2 = utils.drop_report(test, ~test.Art_id.isin(opinions.Art_id))
drops2 = pd.concat((drops, traindrops, testdrops))
# %%
train.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "train_main0.csv"))
train2.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "train_main1.csv"))
utils.df_to_s3(train2, f"{pub.name}/tts_mask/train_main1.csv", bucket=bucket)
#%%
test.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "test_main0.csv"))
test2.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "test_main1.csv"))
utils.df_to_s3(test2, f"{pub.name}/tts_mask/test_main1.csv", bucket=bucket)
# %%
drops.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "drops_main0.csv"))
drops2.to_csv(os.path.join(utils.ROOTPATH, 'baba', pub.name, "tts_mask", "drops_main1.csv"))
utils.df_to_s3(drops2, f"{pub.name}/tts_mask/drops_main1.csv", bucket=bucket)
# %%
