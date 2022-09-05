"""
Old quote analysis before ner consolidation. 
"""
# %%
from thesisutils import utils
import pandas as pd
from tqdm import tqdm
import numpy as np
tqdm.pandas()
# %%
# ANALYSIS PART 2 ###################
def loaddata(pub):
    """
    :returns: ppl, quotedf, nerdf
    """
    hkmask = utils.standardize(utils.get_df(pub, "hk_mask", "hkmask.csv") , pub)
    quotedf = utils.standardize(utils.read_df_s3(f"{pub.name}/quotes/quotes_full_edits_new716.csv"), pub, drop_dups=False)
    quotedf = quotedf[quotedf.Art_id.isin(hkmask.Art_id)]
    nerdf = utils.standardize(utils.read_df_s3(f"{pub.name}/ner/ner_full2.csv"), pub, drop_dups=False)
    nerdf = nerdf[nerdf.Art_id.isin(hkmask.Art_id)]
    df = utils.standardize(utils.main_date_load(pub), pub)

    df = df[df.Art_id.isin(hkmask.Art_id)]
    quotedf.speaker = quotedf.speaker.astype(str)
    quotedf['lenspeaker'] = quotedf.speaker.str.split().str.len()
    # drop zero length speaker [usually \n\n lines]
    mask = ~quotedf.lenspeaker.eq(0)
    quotedf = utils.drop_report(quotedf, mask)
    quotedf['lenentityspeaker'] = quotedf.entity_1.str.split().str.len()
    quotedf['singleentityspeaker'] = quotedf['lenentityspeaker'].eq(1)
    # for debugging:
    quotedf = quotedf.merge(df[["Art_id",  'Year', 'Month', 'Day', 'post_baba',
       'baba_ownership', 'tts', "Body", "bodylower", "bodyalphabet", ]], on="Art_id")
    # qrow = quotedf[quotedf['singleentityspeaker']].iloc[0]
    nerdf['splitent'] = nerdf.entity.str.split()
    nerdf['entlen'] = nerdf.splitent.str.len()
    ppl = nerdf[nerdf.label_.eq("PERSON")]
    return ppl, quotedf, nerdf
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

def get_dates(pub, df):
    datedf = utils.standardize(utils.main_date_load(pub), pub)
    return df.merge(datedf[['Year', 'Month', 'Day', 'post_baba',
       'baba_ownership', 'tts', "Art_id"]], on="Art_id", how="left")

# scmp = get_dates(utils.publications['scmp'], scmp)
# hkfp = get_dates(utils.publications['hkfp'], hkfp)
# nyt = get_dates(utils.publications['nyt'], nyt)
# chinadaily = get_dates(utils.publications['chinadaily'], chinadaily)
# globaltimes = get_dates(utils.publications['globaltimes'], globaltimes)

# %%
# @ least 1 entity in speaker block
# can recomment later to repeat analysis 

# valcnts = quotedf.entity_1.isna().value_counts()
# utils.report(valcnts[False], valcnts[True], preprint='percentage with at least 1 entity in speaker block');
# y=quotedf[quotedf.entity_1.isna()].speaker

# viz(quotedf[quotedf['singleentityspeaker']])

# quotedf.entity_1.isna().value_counts()
# quotedf['singleentityspeaker'].value_counts(dropna=False)
# valcnts = quotedf['singleentityspeaker'].value_counts()
# utils.report(valcnts[True], quotedf.entity_1.isna().value_counts()[True]-valcnts[True], preprint="entity 1 with single word")
# %%
USECLOSEST = 0
def lookupsingleent(qrow, ppl):
    """When a matched entity within a speaker block is 1 word,
        we lookup multiword entities with that word.
        If we have multiple candidates, we select the closest result 
        Currently slow but not prohibitively...
    """
    global USECLOSEST
    nerart = ppl[ppl.Art_id.eq(qrow.Art_id)]
    # nerart = nerart[nerart.label_.eq("PERSON")]
    candidates = nerart[nerart['entlen'].ge(2) & nerart['entlen'].lt(5)]
    result = candidates[candidates.splitent.apply(lambda s: qrow.entity_1 in s)]

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
        USECLOSEST += 1
        return result.sort_values("distance").iloc[0].entity



# %%
# task 1: get list of relevant entities
# entities: Joshua Wong, Benny Tai, Xi Jinping, Carrie Lam
# task 2: generate aliases for each:
    # Joshua Wong: 
        # joshua    
        # josh
        # wong  
    # Benny Tai 
        # benny
        # tai
        # yiu-ting        
    # Carrie Lam
        # lam
        # chief executive carrie lam
        # cheng yuet-ngor
        # yuet-ngor
    # Xi Jinping
        # xi 
        # jinping 
        # by position? 

# task 3: describe algorithm: 
    # Look up all lookupspeaker2 which match any of the above aliases
    # try exact match vs contains string match.
    # Can think about it like we're assuming that any ent with the alias
    # is the real ent. 
# one at a time
# Carrie lam match

# %% 
def debug_quote(qrow):
    """prints a section of the body with the start and end"""
    quote = qrow.Body[int(qrow.earliest_start): int(qrow.latest_end)]
    print(quote)
    return quote
# %%
def lookupent(qrow, entstr="xijinping"):
    """Looks at every entity in the nerdf for the article 
        in qrow. 
        Returns true if so if any entity contains entstr
        Useful when given a last name eg Xi or alias eg Yuet-ngor 
        and want to ensure that name is not shorthand for some rando 
        but rather the person of interest. 
    :param qrow: a row from a quotedf. Usually be filtered to rows 
        with some derivative of entstr
    :param entstr: a lowercase alphabetical only string (no spaces) which would be 
    the full entity. Usually some famous person of interest.
    """
    ents = nerdf[nerdf.Art_id.eq(qrow.Art_id)]
    valcnts = ents.entity.str.lower().str.replace('[^a-z]', '').str.contains(entstr).value_counts()
    return True in valcnts.index

# %%
def reportvalcnts(mask):
    """prints true false ratio if they both exist else reports
        that only True or False is around. """
    valcnts = mask.value_counts(dropna=False)
    if True in valcnts.index and False in valcnts.index:
        utils.report(valcnts[True], valcnts[False])
        return None
    elif True in valcnts.index:
        print(f"No falses, only {valcnts[True]}")
    else: 
        print(f"No trues, only {valcnts[False]}")

def lamcheck(quotedf):
    """Looks for speakers who might be lam and checks if "carrie lam" was in the text.
        If so, replaces with Carrie Lam
    Developed on hkfp coverage"""
    print("Lam matching")
    qdf = quotedf.copy()
    mask1 = qdf.speaker2.eq("Lam") & qdf.bodyalphabet.str.contains("carrielam")
    mask2 = qdf.speaker2.str.lower().str.contains("yuetngor") & qdf.bodyalphabet.str.contains("carrielamyuetngor|carrielamchengyuetngor")
    mask3 = qdf.speaker2.eq("Ms Lam|Mrs Lam") & qdf.bodyalphabet.str.contains("carrielam")
    mask4 =  ~qdf.speaker2.eq("Carrie Lam") & qdf.speaker2.str.contains("Carrie Lam")
    mask5 = qdf.speaker2.eq("Hong Kong leader") & qdf.speaker2.str.contains("Carrie Lam")
    mask = mask1 | mask2 | mask3 | mask4 | mask5
    reportvalcnts(mask)
    qdf.loc[mask, "speaker2"] = "Carrie Lam"
    return qdf

def xijinpingcheck(quotedf):
    print("Xi Jinping matching")
    qdf = quotedf.copy()
    mask1 = qdf.speaker2.eq("Xi") & qdf.bodyalphabet.str.contains("xijinping")
    mask2 =  ~qdf.speaker2.eq("Xi Jinping") & qdf.speaker2.str.contains("Xi Jinping")
    mask = mask1 | mask2 
    reportvalcnts(mask)
    qdf.loc[mask, "speaker2"] = "Xi Jinping"
    return qdf


def wongcheck(quotedf):
    print("Joshua Wong matching")
    qdf = quotedf.copy()
    mask1 = qdf.speaker2.eq("Wong") & qdf.bodyalphabet.str.contains("joshuawong")
    mask2 =  ~qdf.speaker2.eq("Joshua Wong") & qdf.speaker2.str.contains("Joshua Wong")
    mask = mask1 | mask2
    reportvalcnts(mask)
    qdf.loc[mask, "speaker2"] = "Joshua Wong"
    return qdf

def bennytaicheck(quotedf):
    print("Benny Tai matching")
    qdf = quotedf.copy()
    mask1 = qdf.speaker2.eq("Tai") & qdf.bodyalphabet.str.contains("bennytai")
    mask2 = ~qdf.speaker2.eq("Benny Tai") & qdf.speaker2.str.contains("Benny Tai")
    mask = mask1 | mask2
    reportvalcnts(mask)
    qdf.loc[mask, "speaker2"] = "Benny Tai"
    return qdf

def policecheck(quotedf):
    print("Police matching")
    qdf = quotedf.copy()
    mask1 = qdf.speaker2.str.lower().str.contains("police")
    mask = mask1
    reportvalcnts(mask)
    qdf.loc[mask, "speaker2"] = "Police"
    return qdf


# %%
# Unconditional quote analysis: ########################
def run(pub):

    # load quotes 
    ppl, quotedf, nerdf  = loaddata(pub)
    # convert indexes to start and ends. 
    quotedf = (
        quotedf.assign(
            quotetup = lambda d: d.quote_index.fillna("None").apply(eval),
            quotestart = lambda d: d.quotetup.apply(lambda x: x[0] if isinstance(x, tuple) else None),
            quoteend= lambda d: d.quotetup.apply(lambda x: x[1] if isinstance(x, tuple) else None),
            quotelen = lambda d: d.quoteend - d.quotestart,
            verbtup = lambda d: d.verb_index.fillna("None").apply(eval),
            verbstart = lambda d: d.verbtup.apply(lambda x: x[0] if isinstance(x, tuple) else None),
            verbend= lambda d: d.verbtup.apply(lambda x: x[1] if isinstance(x, tuple) else None),
            speakertup = lambda d: d.speaker_index.fillna("None").apply(eval),
            speakerstart = lambda d: d.speakertup.apply(lambda x: x[0] if isinstance(x, tuple) else None),
            speakerend= lambda d: d.speakertup.apply(lambda x: x[1] if isinstance(x, tuple) else None),
            earliest_start = lambda d: d[["verbstart", "speakerstart", "quotestart"]].min(axis=1),
            latest_end = lambda d: d[["verbend", "speakerend", "quoteend"]].max(axis=1), 
            artlen = lambda d: d.Body.str.len(),
            relativeposition = lambda d: d.quotestart/d.artlen
        )
        .drop([
        "quotetup",
        "verbtup",
        "speakertup"], axis=1)
    ) 
    # lookup single entities
    quotedf.loc[quotedf['singleentityspeaker'], 'lookupsingleent'] = quotedf[quotedf['singleentityspeaker']].progress_apply(lookupsingleent, ppl=ppl, axis=1)

    # get top entities
    drops = [
        "he",
        "she",
        "it",
        "they",
        "you",
        "who",
        "that",
        "which"
        
    ] 

    quotedf = quotedf.assign(
        fillent1 = lambda d: d.lookupsingleent.isna() & d.entity_1.astype(str).str.split().str.len().between(2,5),
        fillspeaker = lambda d: d.lookupsingleent.isna() & ~d.entity_1.astype(str).str.split().str.len().between(2,5),
        filllookupsingleent = lambda d: ~d.lookupsingleent.isna(),
    )
    quotedf['speaker2'] = np.select([
            quotedf.fillent1,
            quotedf.fillspeaker,
            quotedf.filllookupsingleent
            ], 
            [quotedf.entity_1,quotedf.speaker,quotedf.lookupsingleent])
    # more speaker 2 edits: 
    quotedf.speaker2 = (
        quotedf.speaker2
        .astype(str)
        # .str.strip("\n")
        # is this a good idea to make lower?
        # .str.lower()
        .str.replace("'s|’s|\.|,|-", "")
    )

    quotedf = lamcheck(quotedf)
    quotedf = xijinpingcheck(quotedf)
    quotedf = wongcheck(quotedf)
    quotedf = bennytaicheck(quotedf)
    quotedf = policecheck(quotedf)
    # generate table counts
    entities = ["Carrie Lam", "Xi Jinping", "Benny Tai", "Joshua Wong", "Police"]
    print(quotedf.speaker2.value_counts(dropna=False).loc[entities])
    return quotedf
# %%
pub = utils.publications['nyt']
nyt = run(pub)
# NYT
# Carrie Lam      13
# Xi Jinping      33
# Benny Tai        4
# Joshua Wong     19
# Police         185

# %%
# HKFP
pub = utils.publications['hkfp']
hkfp = run(pub)
# Carrie Lam     1777
# Xi Jinping      105
# Benny Tai       122
# Joshua Wong     292
# Police         1176
# Name: speaker2, dtype: int64

# %%
pub = utils.publications['scmp']
scmp = run(pub)
# x=scmp.loc[lambda d: ~d.speaker2.str.lower().isin(drops)].speaker2.value_counts(dropna=False)

# Carrie Lam     3904
# Xi Jinping      855
# Benny Tai       449
# Joshua Wong     425
# Police         7156

# %% 
pub = utils.publications['chinadaily']
chinadaily = run(pub)
# Carrie Lam     2534
# Xi Jinping     1335
# Benny Tai        32
# Joshua Wong      17
# Police          824
# %%
pub = utils.publications['globaltimes']
globaltimes = run(pub)
# Carrie Lam     823
# Xi Jinping     635
# Benny Tai       14
# Joshua Wong     16
# Police         837


# %%
scmp.to_csv("tmpscmpquote.csv")
hkfp.to_csv("tmphkfpquote.csv")
nyt.to_csv("tmpnytquote.csv")
chinadaily.to_csv("tmpchinadailyquote.csv")
globaltimes.to_csv("tmpglobaltimesquote.csv")

# %% 
scmp = pd.read_csv("tmpscmpquote.csv")
hkfp = pd.read_csv("tmphkfpquote.csv")
nyt = pd.read_csv("tmpnytquote.csv")
chinadaily = pd.read_csv("tmpchinadailyquote.csv")
globaltimes = pd.read_csv("tmpglobaltimesquote.csv")


# %%
# %% 
# TODOS
# investigate each value count to see if we missed some aliases. quotedf = (
cols = ["Art_id", "Publication", 'quotelen', "quotestart", 
    'relativeposition', "speaker2", "artlen", "Year", 'Month', 
    'Day', 'post_baba', 'baba_ownership']
maindf = pd.concat([scmp[cols], nyt[cols], globaltimes[cols], chinadaily[cols], hkfp[cols]])
entities = ["Carrie Lam", "Xi Jinping", "Benny Tai", "Joshua Wong", "Police"]
# grouped = maindf[maindf.speaker2.isin(entities)].groupby(["speaker2", "Publication",
#  "post_baba"
#  ])
# %%
# ent = entities[0]
# offset is the starting table number
offset = 0
i=0
total_arts = maindf.groupby(["Publication", "post_baba"]).agg(total_arts=("Art_id", "nunique"))
for i, ent in enumerate(entities):
    grouped = maindf[maindf.speaker2.eq(ent)].groupby([
        # "speaker2",
        "Publication", "post_baba"])

    agged = ( 
        grouped.agg(count=("Art_id", "count"),
        nunique=("Art_id", "nunique"),
        quotelen = ("quotelen", "mean"), 
        relativeposition = ("relativeposition", "mean"))
        # ["count", "nunique"],
        # "quotelen": "mean", "relativeposition": "mean"})
        .unstack(fill_value=0)
        .stack()
        .join(total_arts)
        .assign(**{
            "Quotes / mentioning article": lambda d:  d["count"] / d["nunique"],
            "Quotes / article in corpus": lambda d: d['count'] / d['total_arts']
        })
    )
    # agged["Quotes / mentioning article"] =   agged["count"] / agged["nunique"]
    display(
        # agged.drop(columns = ["nunique"], axis=1)
        agged.drop(index=["nyt", 'globaltimes'])

        .rename(index={
            "chinadaily": "China Daily",
            "scmp": "SCMP",
            "hkfp": "HKFP",
            "nyt": "NYT",
            "globaltimes": "Global Times",
        })
        # .rename_axis(["Publication", "After 2016"]) 
        .reset_index()
        [[ "Publication", "post_baba","Quotes / article in corpus", "quotelen","relativeposition"]] # "Quotes / mentioning article", 
        .assign(
            post_baba = lambda d: d.post_baba.astype(str)
        )
        .rename(columns={
            "quotelen": "Mean tokens / quote",
            "relativeposition": "Relative position",
            "post_baba": "After 2016",
            "count": "Total quotes"
        })
        .fillna(0)
        # .set_index(["Publication", "After 2016"])
        .style
        .hide_index()
        .background_gradient(cmap='Blues')
        .set_caption(f"Table _:{i+offset} {ent} quotes before and after 2016")
        .set_properties(**{'text-align': 'center'})
        .set_table_styles([
            {'selector': 'th', 'props': [('text-align', 'center')]},
            {'selector': 'caption', 'props': [('font-weight', "bolder"), ('font-size', '16px')]}
        ])
    )
# %%
agged = ( 
    grouped.agg({"Art_id":
    ["count", "nunique"],
    "quotelen": "mean", "relativeposition": "mean"})
    .unstack(fill_value=0)
    .stack()
)
agged["Art_id", "quotes_per_unique_art"] =   agged["Art_id", "count"] / agged["Art_id", "nunique"]
agged = agged.sort_index(axis=1)
agged.style.background_gradient(cmap='Blues')
# %%
x=chinadaily.speaker2.value_counts(dropna=False)

# get number of times quoted [mostly done]
# get number of tokens total & per quote
# get average absolute placement in article 
# get proportional placement in article 

grouped = scmp[scmp.speaker2.isin(entities)].groupby("speaker2")
grouped[['quotelen', 'relativeposition']].describe()
# .reindex()
.style.background_gradient(cmap='Blues')
grouped.quotelen.mean()


def num_quoted(quotedf, entity):
    """Returns number of times an entity was quoted"""
    return quotedf.value_counts(dropna=False).loc[entity]
def num_tokens(quotedf, entity):
    qdf = quotedf[quotedf.speaker2.eq(entity)]
    quotelen = qdf.quoteend - qdf.quotestart 



# %%
# SCRATCH ###########

scmp.earliest_start.mean()
hkfp.earliest_start.mean()
globaltimes.earliest_start.mean()
chinadaily.earliest_start.mean()
nyt.earliest_start.mean()
