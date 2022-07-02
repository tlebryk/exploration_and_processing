"""scrper somehow picked up pre 2011 articles. I manually dropped 
these articles from the main globaltimes_full.csv. I store the drops in tts_mask/drops_main1.csv
Each block removes the drops from subsequent files. I manually uploaded to s3 to replace old files.

"""
from thesisutils import utils
import os

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
