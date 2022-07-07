"""Runs the alibaba filter and creates a csv subsetting pub_full.csv
with only rows containing alibaba. 
Run on hkfp and scmp because data collection process is diff for those
    we have the full corpus rather than just hk articles in our main data
    repo. 
Uploads to aliba bucket in the same format as nyt, gt and cd. 
We will treat this as a separate dataset with its own unique 
train test filter because we need a larger training sample. 
"""
# %% 
from thesisutils import utils

# %% 
def go():
    keys = ("scmp", "hkfp")
    for key in keys:
        pub = utils.publications[key]
        # load data
        df = utils.get_df(pub)
        # filter data 
        mask = df["Body"].astype(str).str.lower().str.contains("alibaba")
        df = df[mask]
        utils.df_to_s3(df, f"{pub.name}/{pub.name}_full.csv", bucket="aliba")
# save data
go()