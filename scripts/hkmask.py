"""Searches for 'hong kong' in the headline or body of a text. """

import pandas as pd
from thesisutils import utils
import os
# %%
def hk_mask(publication):
    """Generates csv with only articles which mention hong kong. 
    Applies mainly to hkfp and scmp which had different data collection
    process. 
    """
    df = utils.get_df(publication)
    df.head()
    df['textcol'] = df[publication.headline] + "; " + df[publication.textcol]

    df = df[df.textcol.notna()]
    df['hk_mask'] = df.textcol.str.lower().str.contains("hong kong")
    hk = df[df["hk_mask"]][[publication.uidcol]].drop_duplicates()
    path = os.path.join(utils.ROOTPATH, publication.name, "hk_mask")
    if not os.path.exists(path):
        os.makedirs(path)
    hk.to_csv(os.path.join(path, "hkmask.csv"))
    return  df['hk_mask']
# %% 
# scmp mask
publication = utils.publications['scmp']
hk = hk_mask(publication)
hk.value_counts()
# False    138882
# True     117480
# %% 
# hkfp sanity check: this should fail bc we didn't filter
publication = utils.publications['hkfp']
hk = hk_mask(publication)
hk.value_counts()
# True     15231
# False     5165

# %%
# sanity check on nyt
publication = utils.publications['nyt']
hk = hk_mask(publication)
hk.value_counts()
# False    5728
# True     5175
# This fails... but turns out we only get the top of article in many cases
# not sure how to deal with this... 
# %%
# sanity check on global times
publication = utils.publications['globaltimes']
df = utils.get_df(publication)
hk = hk_mask(publication)
hk.value_counts()
# True     26265
# False       28


# %%
# china daily
publication = utils.publications['chinadaily']
hk = hk_mask(publication)
hk.value_counts()
# True     47172
# False       64

