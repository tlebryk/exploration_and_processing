# %%
from statistics import LinearRegression
from thesisutils import utils
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf

# import statsmodels.api as sm
import seaborn as sns

# %%
# load data
# get train and test
pub = utils.publications["scmp"]


def get_df(pub, tts="train"):
    """
    :param tts: 'train', 'test' or 'full'
    """
    df = utils.standardize(
        utils.read_df_s3(f"{pub.name}/sentiment/alibaba_{tts}.csv", "aliba"),
        pub,
        drop_dups=False,
    ).assign(tts=tts)
    # TODO: do date generatioin
    datedf = utils.standardize(
        utils.read_df_s3(f"{pub.name}/date/date.csv", "aliba"), pub
    )
    return df.merge(datedf, on="Art_id", how="left")


# %%
maindf = pd.concat(
    get_df(pub, tts) for pub in utils.publications.values() for tts in ("train", "test")
)

# %%
# Duplicate sentence analysis
scmp = utils.get_df(utils.publications["scmp"])
maindf.loc[lambda d: d.publication.eq("scmp")].sentence.value_counts().head(20)
# "Learn about the AI ambitions of Alibaba..." keeps getting used over and over
# maybe we can remove?
scmp.set_index("Index").loc[
    maindf.loc[lambda d: d.publication.eq("scmp") & d.Year.eq(2020)]
    .loc[lambda d: d.sentence.str.contains("Learn about the AI ambitions of Alibaba")]
    .Art_id
].Body.str.count("Ali").value_counts()
# turns out many of these are false positives: they are not actually articles
# about Alibaba but rather a trailing plug. Removing in all baba analysis

# %%
# light cleaning
maindf = utils.drop_report(maindf, maindf.Year.ge(2011))

maindf = utils.drop_report(maindf, maindf.positive.notna())

maindf = utils.drop_report(maindf, maindf.Year.notna())
# drop some duplicate sentences which don't aid sentiment.
maskstr = "Abacus is a unit of the South China Morning Post, which is owned by Alibaba|\
Learn about the AI ambitions of Alibaba, Baidu & JD.com through our in-depth case studies, and explore new applications of AI across industries.|\
Taobao is owned by Alibaba, which also owns the South China Morning Post ."
mask = ~maindf.sentence.str.contains(maskstr)
maindf = utils.drop_report(maindf, mask)
maindf.Year = maindf.Year.astype(int)
maindf["monthid"] = (maindf.Year - 2011) * 12 + maindf.Month


# %%
# articles per year table#################
pubgrouped = maindf.groupby("publication")


def meanb4after(df):
    """Gets average mentions before and after 2016 and percent change"""
    cutoff = 2015
    val_cnts = df.Year.value_counts()
    # if missing year, fill with zero.
    for i in range(2011, 2022):
        if i not in val_cnts.index:
            val_cnts.loc[i] = 0
    val_cnts = val_cnts.sort_index()
    pre16 = val_cnts.loc[2011:cutoff]
    post16 = val_cnts.loc[cutoff + 1 : 2022]
    before = pre16.mean()
    after = post16.mean()
    print("percent change = ", 100 * (after - before) / before)
    return before, after


def b4afterarts(group):
    arts = group.drop_duplicates("Art_id")
    return meanb4after(arts)


tble = (
    pubgrouped.apply(b4afterarts)
    .apply(pd.Series)
    .applymap(lambda e: int(round(e, 0)))
    .rename(columns={0: "pre-2016", 1: "post-2016"})
)
tble["% change"] = 100 * (tble["post-2016"] - tble["pre-2016"]) / tble["pre-2016"]
tble.style.set_caption(
    "Table _._: Average articles per year mentioning Alibaba before and after 2016"
).format(precision=1)

# %%
# SCMP ali art per year count ###############
artsperyr = pd.DataFrame(maindf.groupby(["publication", "Year"]).Art_id.nunique())
# mentions per day
artsperyr.loc["scmp", 2017].squeeze() / 365

# %%
# mentions per year (mult ment per art counted) ############
ax = sns.lineplot(
    x="Year",
    y="ner_index",
    hue="publication",
    data=pd.DataFrame(maindf.groupby(["publication", "Year"]).ner_index.nunique()),
)
ax.set_title("Figure _._: Number of mentions of Alibaba per year")
fig = ax.get_figure()
fig.savefig("../reports/alimentionsperyear.png")

# articles mentioning ali / year (1 art regardless # mentions in art) #####
ax = sns.lineplot(
    x="Year",
    y="Art_id",
    hue="publication",
    data=pd.DataFrame(maindf.groupby(["publication", "Year"]).Art_id.nunique()),
)
ax.set_title("Figure _._: Number of articles mentioning Alibaba per year")
fig = ax.get_figure()
fig.savefig("../reports/aliartsperyear.png")

# %%
# Mentions of Alibaba across Publications################
maindf.publication.value_counts(dropna=False).style.set_caption(
    "Table _._ mentions of Alibaba across Publications"
)
# .to_clipboard()
# %%
# more cleaning
mask = maindf.debug.str.split().str.len().gt(3)
maindf = utils.drop_report(maindf, mask)

# %%
# SCMP histogram by year ##################
scmpgrouped = maindf.loc[lambda d: d.publication.eq("scmp")].groupby("Year")


def polaritydists(posneg):
    """Plot SCMP's distribution for a given polarity.
    Generates plots from 2011 to 2021, saves to "../reports/ali{posneg}scmp.png"
    :param posneg: "positive" "negative" or "neutral"
    """
    fig, axes = plt.subplots(11, 1, sharex=True, figsize=(9, 18))
    fig.tight_layout(h_pad=2)
    plt.subplots_adjust(top=0.95, left=0.07)
    # axes.sharex
    for i, (yr, d) in enumerate(scmpgrouped):
        sns.histplot(d[posneg], ax=axes[i], bins=25).set(title=yr)
        axes[i].set(ylabel="")
        # axes[i].margins(x=.5,y=.9)
    axes[i].set(xlabel="")
    # plt.tick_params(labelcolor='none', which='both', top=False, bottom=False, left=False, right=False)
    fig.supylabel("Count")
    # fig.supxlabel("Positivity Score")
    fig.suptitle(f"Figure _._: SCMP's distribution of {posneg} scores over time")
    fig.savefig(f"../reports/ali{posneg}scmp.png")


# %%
polaritydists("positive")
# %%
polaritydists("negative")
# %%
polaritydists("neutral")
# %%
# group nyt and hkfp into single publication
x = maindf.assign(
    publication2=lambda d: d.publication.mask(
        d.publication.isin(("hkfp", "nyt")), "hkfp/nyt"
    )
).assign(
    publication2=lambda d: d.publication2.mask(
        d.publication2.isin(("chinadaily", "globaltimes")), "gt/cd"
    )
)
pre = x.loc[lambda d: d.post_baba.eq(0)]
post = x.loc[lambda d: d.post_baba.eq(1)]
# %%
def pubscoredist(posneg):
    """Plot distribution across publications before and after 2016
    Comebines gt/cd and nyt/hkfp
    save to "../reports/ali{posneg}distprepostpubs.png"
    :param posneg: "positive" "negative" or "neutral"
    """
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    sns.histplot(
        y=posneg,
        hue="publication2",
        data=pre.reset_index(),
        ax=axes[0],
        stat="density",
        bins=30,
        common_norm=False,
    )
    axes[0].get_legend().set_title("pre-2016")
    z = sns.histplot(
        y=posneg,
        hue="publication2",
        data=post.reset_index(),
        ax=axes[1],
        stat="density",
        bins=30,
        common_norm=False,
    )
    axes[1].get_legend().set_title("post-2016")
    fig.suptitle(
        f"Figure _._: {posneg} score distribution across publications before and after 2016"
    )
    fig.savefig(f"../reports/ali{posneg}distprepostpubs.png")


# %%
pubscoredist("positive")
# %%
pubscoredist("negative")
# %%
pubscoredist("neutral")


# %%
# analysis:
# let's get post baba averages
predf = maindf.loc[lambda d: d.post_baba.eq(0)].groupby("publication")
postdf = maindf.loc[lambda d: d.post_baba.eq(1)].groupby("publication")
newdf = pd.DataFrame()
premean = predf.agg(
    {
        "positive": ["mean", "sem"],
        "negative": ["mean", "sem"],
        "neutral": ["mean", "sem"],
    }
)
postmean = postdf.agg(
    {
        "positive": ["mean", "sem"],
        "negative": ["mean", "sem"],
        "neutral": ["mean", "sem"],
    }
)


def getseminterval(meandf, posneg, stddev="sem"):
    """Generates min and max values within plus or minus standard error of mean.

    :param meandf: multiindex df with a posneg col at level 1 and mean and sem cols at level 2
    :param posneg: 'positive' 'negative' or 'neutral.
    :param stddev: 'sem' or 'std'
    """
    meandf[posneg, "min"] = meandf[posneg]["mean"] - meandf[posneg][stddev]
    meandf[posneg, "max"] = meandf[posneg]["mean"] + meandf[posneg][stddev]
    return meandf


premean = getseminterval(premean, "positive", "sem")
premean = getseminterval(premean, "negative", "sem")

postmean = getseminterval(postmean, "positive", "sem")
postmean = getseminterval(postmean, "negative", "sem")

posneg="positive"
def cioverlap(premean, postmean, posneg):
    """Returns dataframe with publications as rows and true is means are in 
    overlapping CIs, false if (significantly) different. 
    """
    # two conditions when there is overlap
    cond = (
        (premean[posneg, "max"] > postmean[posneg, "min"])
        & (premean[posneg, "min"] < postmean[posneg, "min"])
    ) | (
        (premean[posneg, "min"] < postmean[posneg, "max"])
        & (premean[posneg, "max"] > postmean[posneg, "max"])
    )
    return cond
posneg = "negative"
postmean[posneg].join(premean[posneg], lsuffix="-pre", rsuffix="-post").assign(
    **{"In CI" : lambda d: cioverlap(premean, postmean, posneg)}
).style.background_gradient(subset=["In CI"]).set_caption(f"Table _._: Mean {posneg} score from before and after purchase with standard errors")

postmean["negative"]

premean["negative", "min"] = premean["negative"]["mean"] - premean["negative"]["sem"]
premean["negative", "max"] = premean["negative"]["mean"] + premean["negative"]["sem"]
premean

postmean["positive", "min"] = postmean["positive"]["mean"] - postmean["positive"]["sem"]
postmean["positive", "max"] = postmean["positive"]["mean"] + postmean["positive"]["sem"]
postmean["negative", "min"] = postmean["negative"]["mean"] - postmean["negative"]["sem"]
postmean["negative", "max"] = postmean["negative"]["mean"] + postmean["negative"]["sem"]
postmean
# %%
# running did experiment
# positivity = month effect + publication effect + treatment effect
est2 = smf.ols(
    formula="neutral ~ C(publication) + C(Year) +  baba_ownership", data=maindf
).fit()
est2.summary()
print(est2.summary2(float_format="%.4f"))
# %%


print(est2.summary(float_format="%.4f"))

maindf = maindf.reset_index()

# %%
monthdummies = pd.get_dummies(maindf.Year)
pubdummies = pd.get_dummies(maindf.publication)
maindf["publication"] = maindf["publication"].astype("category")
x = maindf[["publication", "post_baba", "baba_ownership"]]
# x = pubdummies#monthdummies.join(pubdummies)
# x['postbaba'] = maindf.post_baba
# x['treatment'] = maindf.baba_ownership
y = maindf.positive


# .loc[lambda d: d.Year.astype(int).ge(2012)]

# summary:
# I've played around with using binary time, month, and year.
# and I've looked at positive, negative and neutral.
# no model is that great (R2 of 0.03).
# As expected GT and CD are most positive, least negative.
# nyt and hkfp are least positive most negative
# SCMP generally in the middle of pack.
# No statistically significant baba effect with month/year and positive coverage
# Small stat sig neg effect for baba ownership on negative news.
# as expected, that means a slight increase in neutral coverage with baba effect.


# %%
x2 = sm.add_constant(x)
est = sm.OLS(y, x2)
est2 = est.fit()


# %%

# get non-overlapping confidence intervals.
yearpub = maindf.groupby(["Year", "publication"])
means = yearpub.agg(
    {
        "positive": ["mean", "sem"],
        "negative": ["mean", "sem"],
        "neutral": ["mean", "sem"],
    }
)
# get min range
means["positive", "min"] = means["positive"]["mean"] - means["positive"]["sem"]
means["positive", "max"] = means["positive"]["mean"] + means["positive"]["sem"]
# just look at positive for now
# get cases max is less than scmp min or
# scmp max less than other's min.
def inCI(yrdf, means, yr, posneg="positive"):
    """
    adds a mask (posneg_outci) true if that pubs mean CI is outside
        of scmp's means CI.
    :param yrdf: element of means df groped @ level 0;
        iow reps single 'yr' w/ rows each pub & cols are
        positive, negative, or neutral mean and sem [standard errs of mean]
    :param means: aggregated df which yrdf is a single group.
    :param yr: name of group: 2011-2021
    :param posneg: string either: positive, negative, or neutral.
    """
    scmpmin = yrdf.loc[yr, "scmp"][posneg, "min"]
    scmpmax = yrdf.loc[yr, "scmp"][posneg, "max"]
    # print(scmpmin, scmpmax)
    means.loc[yr, f"{posneg}_belowci"] = yrdf.apply(
        lambda d: (d[posneg, "max"] < scmpmin), axis=1
    )
    means.loc[yr, f"{posneg}_aboveci"] = yrdf.apply(
        lambda d: (d["positive", "min"] > scmpmax), axis=1
    )
    # means.loc[yr, f"{posneg}_outci"] = means.loc[yr][f"{posneg}_aboveci"] | means.loc[yr][f"{posneg}_belowci"]


# find a way to iterate through years...
posneg = "positive"
for yr, yrdf in means.groupby(level=0):
    print(yr)
    inCI(yrdf, means, yr, "positive")
means[f"{posneg}_outci"] = means[f"{posneg}_aboveci"] | means[f"{posneg}_belowci"]

means[
    means.positive_outci | list(means.index.get_level_values("publication") == "scmp")
]
means.unstack(level=-1)

sns.barplot(x="Year", y="mean", hue="publication", data=means.unstack())

means.apply(lambda r: print(r._name), axis=1)

# .apply(lambda d: d.positive, axis=1)

postdf.agg(
    {
        "positive": ["mean", "sem"],
        "negative": ["mean", "sem"],
        "neutral": ["mean", "sem"],
    }
)


predf.positive.sem()


maindf.loc[lambda d: d.post_baba.eq(1)].groupby("publication").positive.mean()
maindf.loc[lambda d: d.post_baba.eq(1)].groupby("publication").positive.sem()
maindf.groupby("publication").positive.mean()
maindf.post_baba
# let's chart average positivity over time by publication
# first by year.

ax = sns.lineplot(x="Year", y="negative", hue="publication", data=maindf.reset_index())
ax.set_title("Figure _._: Mentions of Alibaba per Year")
# %%


maindf.groupby("publication")
ax = sns.lineplot(
    x="monthid", y="positive", hue="publication", data=maindf.reset_index()
)
ax = sns.lineplot(
    x="monthid", y="negative", hue="publication", data=maindf.reset_index()
)
# %%
ax = sns.barplot(x="Year", y="positive", hue="publication", data=maindf)
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)
plt.xticks(rotation=45)

# analysis: one year in 2019 with suprisingly positive coverage
# from scmp
# everything else suggest that SCMP has been middle of the line: more positive than
# nyt and hkfp in general but less positive than gt and cd.
# %%

# drop a bunch of garbage
# maindf.loc[lambda d: d.Publication.eq("hkfp")].loc[lambda d: d.negative.isna()]
# merge with date
# %%
# SCRAP ###########################
# pub= utils.publications['scmp']
# scmptrain = get_df(pub, "train") for pub in utils.publications.values() for tts in ("train", "test")

# %%
scmp
sentscmp = maindf[maindf.publication.eq("scmp")]
sentscmp[sentscmp.Art_id.eq(730844)]
sentscmp.set_index("Art_id")
scmp.Body.str.count("Alibaba").value_counts()
