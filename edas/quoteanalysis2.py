# %%
from thesisutils import utils
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
# %%
# ANALYSIS PART 2 ###################
pub = utils.publications['hkfp']
quotedf = utils.standardize(utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits_new716.csv"), pub, drop_dups=False)
nerdf = utils.standardize(utils.read_df_s3(f"{pub.name}/ner/ner_full2.csv"), pub, drop_dups=False)
df = utils.standardize(utils.get_df(pub), pub)
# %%
quotedf['lenentityspeaker'] = quotedf.entity_1.str.split().str.len()
quotedf['singleentityspeaker'] = quotedf['lenentityspeaker'].eq(1)
qrow = quotedf[quotedf['singleentityspeaker']].iloc[0]
nerdf['splitent'] = nerdf.entity.str.split()
nerdf['entlen'] = nerdf.splitent.str.len()
ppl = nerdf[nerdf.label_.eq("PERSON")]
# %%
def viz(quotedf, morecols=None):
    """Drops columns to make a less cumbersome df to view"""
    ls = quotedf.filter(regex='entity', axis=1).columns.tolist()
    ls.remove("entity_1")
    ls2 = quotedf.filter(regex='nerindex', axis=1).columns.tolist()
    ls2.remove("nerindex_1")
    ls+=ls2
    if morecols:
        ls+=morecols
    display(quotedf.drop(ls, axis=1, errors=True))


# %%
# @ least 1 entity in speaker block
valcnts = quotedf.entity_1.isna().value_counts()
utils.report(valcnts[False], valcnts[True], preprint='percentage with at least 1 entity in speaker block');
y=quotedf[quotedf.entity_1.isna()].speaker

viz(quotedf[quotedf['singleentityspeaker']])

quotedf.entity_1.isna().value_counts()
quotedf['singleentityspeaker'].value_counts(dropna=False)
valcnts = quotedf['singleentityspeaker'].value_counts()
utils.report(valcnts[True], quotedf.entity_1.isna().value_counts()[True]-valcnts[True], preprint="entity 1 with single word")
# %%
def lookupsingleent(qrow):
    nerart = ppl[ppl.Art_id.eq(qrow.Art_id)]
    # nerart = nerart[nerart.label_.eq("PERSON")]
    candidates = nerart[nerart['entlen'].ge(2) & nerart['entlen'].lt(5)]
    result = candidates[candidates.splitent.apply(lambda s: qrow.entity_1 in s)]

    # result = candidates[candidates.splitent.apply(lambda s: "Wong" in s)]
    
    
    if len(result) == 1:
        return result.entity.squeeze()
    elif len(result) == 0:
        return None # do we need a flag for whether we changed the entity?
    # multiple candidates... 
    else:
        # try to return a candidate that appears before speaker in question
        endspeaker = eval(qrow.speaker_index)[-1]
        endspeaker
        result2 = result[result.start_char.le(endspeaker)]
        if len(result2) == 1:
            return result2.entity.squeeze()
        # if result2 was zero don't replace otherwise use as filter
        if len(result2) > 1:
            result = result2.squeeze()
        # select closest result 
        result['distance'] = abs(result.start_char - endspeaker)
        return result.sort_values("distance").iloc[0].entity
x=quotedf[quotedf['singleentityspeaker']].progress_apply(lookupsingleent, axis=1)
# %%
valcnts = x.isna().value_counts()
len(quotedf[quotedf['singleentityspeaker']])
len(x)
utils.report(valcnts[False], valcnts[True])
x=quotedf[quotedf['singleentityspeaker']].progress_apply(lookupsingleent, axis=1)
x.value_counts().sum() / x.value_counts(dropna=False).sum()
x.columns


# %%
index = 8898
artid = quotedf.loc[index].Art_id
df[df.Art_id.eq(artid)]
nerdf.loc[lambda d: d.Art_id.eq(artid)]
# %%
def lookupent(qrow, entstr="xijinping"):
    """Looks at every entity in the nerdf for the article 
        in qrow. 
        Returns true if so if any entity contains entstr
        Useful when given a last name eg Xi or alias eg Yuet-ngor 
        and want to ensure that name is not shorthand for some rando 
        but rather the person of interest. 
    :param qrow: a row from a quotedf.
    :param entstr: a lowercase alphabetical only string (no spaces) which would be 
    the full entity. Usually some famous person of interest.
    """
    ents = nerdf[nerdf.Art_id.eq(qrow.Art_id)]
    valcnts = ents.entity.str.lower().str.replace('[^a-z]', '').str.contains("xijinping").value_counts()
    return True in valcnts.index
# %%
  
# %%
