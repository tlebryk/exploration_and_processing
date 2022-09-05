"""
Makes a standardized "Date" column for each 
publication and saves under "{pub}/date/date.csv"
Run on 6/19/2021;
Run on 8/2/2022 again for global times for baba and hk stories
"""
from thesisutils import utils
import pandas as pd
import os
import logging, logging.config
from pathlib import Path
import logconfig

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)
# publication.name
publication = utils.publications["globaltimes"]

# %%
def run(baba=False):
    """
    Currently reads local data; can change to s3 later.
    :param baba: boolean for whether it's on baba subset or main subset.

    Saves to (baba)/pubname/date/date.csv [if not baba then str is emtpy].
        if file already exists, we write to "tmp_{publication.name}.csv" then
        you manually inspect if you wanna overwrite.
    """
    for publication in utils.publications.values():
        if baba:
            # if publication.name in ["scmp", "hkfp"]:
            #     continue
            bucket = "aliba"
            path = os.path.join(utils.ROOTPATH, "baba", publication.name, "date")
            df = utils.read_df_s3(None, bucket, publication)
            # df = utils.get_df(publication, "baba",publication.name,  f"{publication.name}_full.csv")
        else:
            bucket = "newyorktime"
            path = os.path.join(utils.ROOTPATH, publication.name, "date")
            df = utils.get_df(publication, f"{publication.name}_full.csv")
        logger.info(publication.name)
        logger.info("bucket: %s", bucket)
        logger.info("save path: %s", path)

        # date extraction
        if publication.name == "scmp":
            df["Date"] = pd.to_datetime(
                df.Date,
                format="%Y-%m-%d %H:%M:%S",
                infer_datetime_format=True,
                utc=True,
            )
        if publication.name == "nyt":
            df["Date"] = pd.to_datetime(
                df.pub_date,
                format="%Y-%m-%dT%H:%M:%S%z",
                infer_datetime_format=True,
                utc=True,
            )
        if publication.name == "globaltimes":
            df["Date"] = pd.to_datetime(
                df.Date, format="%Y/%m/%d", infer_datetime_format=True, utc=True
            )
        if publication.name == "hkfp":
            df.Date = df.Date.str.replace(":", "")
            df["Date"] = pd.to_datetime(
                df.Date,
                format="%Y-%m-%dT%H%M%S%z",
                infer_datetime_format=True,
                utc=True,
            )
        if publication.name == "chinadaily":
            df["Date"] = pd.to_datetime(
                df.pubDateStr,
                format="%Y-%m-%d %H:%M",
                infer_datetime_format=True,
                utc=True,
            )
        df = df[df.Date.notna()]
        df["Year"] = df.Date.dt.year
        df = df[df.Year < 2022]
        df = df[df.Year > 2010]

        df["Month"] = df.Date.dt.month
        df["Day"] = df.Date.dt.day
        df["Hour"] = df.Date.dt.hour
        # official announcement was 12/15/2015, but unclear when purchase was completed;
        # follow report was that significant changes took  place in April 2016;
        # 2016 seems safe as delimiter?
        # https://techcrunch.com/2016/04/05/alibaba-completes-scmp-acquisition-and-removes-the-papers-online-paywall/
        df["post_baba"] = (df.Year >= 2016).astype(int)
        # months starting with 1/2011 as first month and 12/2021 as 132. 
        # 61 is our rough starting point of 1/2016
        df["monthid"] = (df.Year - 2011)*12 + df.Month
        df["publication"] = publication.name
        df["baba_ownership"] = (df.post_baba.eq(1) & df.publication.eq("scmp")).astype(
            int
        )

        os.makedirs(path, exist_ok=True)
        file = os.path.join(path, "date.csv")
        if not os.path.exists(file):
            """Don't overwrite files just in case"""
            logger.info("writing to %s", file)
            df[
                [
                    publication.uidcol,
                    "Date",
                    "Year",
                    "Month",
                    "Day",
                    # "Hour",
                    "post_baba",
                    "baba_ownership",
                ]
            ].to_csv(file)
        else:
            logger.warning(
                f"WARNING: file already exists, writing to tmp_{publication.name}.csv"
            )
            df[
                [
                    publication.uidcol,
                    "Date",
                    "Year",
                    "Month",
                    "Day",
                    # "Hour",
                    "post_baba",
                    "baba_ownership",
                ]
            ].to_csv(f"tmp_{publication.name}.csv")
        logger.info("saving s3 to %s %s", bucket, f"{publication.name}/date/date.csv")
        utils.df_to_s3(
            df[
                [
                    publication.uidcol,
                    "Date",
                    "Year",
                    "Month",
                    "Day",
                    # "Hour",
                    "post_baba",
                    "baba_ownership",
                ]
            ],
            f"{publication.name}/date/date.csv",
            bucket,
        )
    logger.info("EXITING SUCESSFULLY")


# HONG KONG SUBSET ################################
# %%
run(baba=False)
# %%
# ALIBABA SUBSET ######################################
run(baba=True)
# %%
