"""Run on 7/19 to split the two "long" formatted dfs
into train and test dfs. 
We can still operate of the 'full' df, but it's best to
do that by concatinating train and test because 
the full full actually contains rows we want to drop. 

This script also consoldiates the q_year_full_edits_new716.csv
dfs into a single one. 
"""
# %%
from thesisutils import utils
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
# %%
# convert scmp q_year_edits.csv into quotes_full_edits.csv
dfls = (
    pd.read_csv(fr"{utils.ROOTPATH}\scmp\quotes\q{year}_edits.csv") for year in range(2011, 2022)
)
pd.concat(dfls).to_csv(rf"{utils.ROOTPATH}\scmp\quotes\quotes_full_edits.csv")
utils.upload_s3(fr"{utils.ROOTPATH}\scmp\quotes\quotes_full_edits.csv",
    key="scmp\quotes\quotes_full_edits.csv",
)

# %%

# quote tts split 
def nerquotetts(pub, quote=True):
    if quote:
        readkey = f"{pub.name}/quotes/quotes_full_edits_new716.csv"
        trainkey = f"{pub.name}/quotes/quote_train.csv"
        testkey = f"{pub.name}/quotes/quote_test.csv" 
    else:
        readkey = f"{pub.name}/ner/ner_full2.csv"
        trainkey = f"{pub.name}/ner/ner_train.csv"
        testkey = f"{pub.name}/ner/ner_test.csv"
    df = utils.standardize(utils.read_df_s3(readkey), pub, drop_dups=False)
    # unnamedcols = df.filter(regex="Unnamed",axis=1).columns
    # df= df.drop(unnamedcols, axis=1)
    train = utils.get_df(pub, "tts_mask", f"train_main1.csv")
    train = utils.standardize(train, pub)
    trainmask = df.Art_id.isin(train.Art_id)
    traindf = utils.drop_report(df,trainmask)
    utils.df_to_s3(traindf, trainkey)
    test = utils.get_df(pub, "tts_mask", f"test_main1.csv")
    test = utils.standardize(test, pub)
    testmask = df.Art_id.isin(test.Art_id)
    testdf = utils.drop_report(df,testmask)
    utils.df_to_s3(testdf, testkey)


# 
l=len(drop)+len(train)+len(test)
len(drop)/l
len(train)/l
len(test)/l
# %% 
pub = utils.publications['hkfp']
nerquotetts(pub, True)
nerquotetts(pub, False)
# %%
pub = utils.publications['nyt']
nerquotetts(pub, True)
nerquotetts(pub, False)
# %%
pub = utils.publications['globaltimes']
nerquotetts(pub, True)
nerquotetts(pub, False)
# %%
pub = utils.publications['chinadaily']
nerquotetts(pub, True)
nerquotetts(pub, False)
# %%
# SCMP is special 
# run yearly consolidation first
pub = utils.publications['scmp']
df = pd.concat(utils.standardize(utils.read_df_s3(f"scmp/quotes/q_{yr}_full_edits_new716.csv"), pub) for yr in range(2011, 2022))
df.index = df.index.set_names(["quid"])
df = df.reset_index()
df = df.drop(['entities', "year", "publication"], axis=1)
utils.df_to_s3(df, "scmp/quotes/quotes_full_edits_new716.csv" )
nerquotetts(pub, True)
nerquotetts(pub, False)