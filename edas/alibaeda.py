# %% 
from thesisutils import utils
import pandas as pd
# %%
pub = utils.publications["hkfp"]
dfls = []
for pub in utils.publications.values():
    df = utils.read_df_s3(f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
    df["publication"] = pub.name
    dfls.append(df)
maindf = pd.concat(dfls)

# %% 
# EDA
len(maindf)
# 18454 entries
# for context, our main hk training sample was 13,000.
maindf.groupby("publication").size()
# breakdown: 
# publication
# chinadaily     7547
# globaltimes    4688
# hkfp            174
# nyt            1048
# scmp           5007

# %% TTS SPLIT
# see tts split; 50 50 so just divide above numbers in half and get rough estimate. 
