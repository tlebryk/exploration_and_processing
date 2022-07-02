"""
Makes a standardized "Date" column for each 
publication and saves under "{pub}/date/date.csv"
Run on 6/19/2021
"""
from thesisutils import utils
import pandas as pd
import os

# publication.name
# publication = utils.publications['scmp']
for publication in utils.publications.values():
    
    # on everything 
    # df = utils.get_df(
    #     publication, f"{publication.name}_full.csv"
    # ) 

    # on baba
    if publication.name in ["scmp", "hkfp"]:
        continue
    df = utils.get_df(publication, "baba", f"{publication.name}_full.csv")


    # date extraction
    if publication.name == "scmp":
        df["Date"] = pd.to_datetime(
            df.Date, format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True, utc=True
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
            df.Date, format="%Y-%m-%dT%H%M%S%z", infer_datetime_format=True, utc=True
        )
    if publication.name == "chinadaily":
        df["Date"] = pd.to_datetime(
            df.pubDateStr, format="%Y-%m-%d %H:%M", infer_datetime_format=True, utc=True
        )
    df = df[df.Date.notna()]
    df["Year"] = df.Date.dt.year
    df = df[df.Year < 2022]
    df["Month"] = df.Date.dt.month
    df["Day"] = df.Date.dt.day
    df["Hour"] = df.Date.dt.hour
    # official announcement was 12/15/2015, but unclear when purchase was completed;
    # follow report was that significant changes took  place in April 2016;
    # 2016 seems safe as delimiter?
    # https://techcrunch.com/2016/04/05/alibaba-completes-scmp-acquisition-and-removes-the-papers-online-paywall/
    df["post_baba"] = (df.Year > 2016).astype(int)
    df["publication"] = publication.name
    df["baba_ownership"] = (df.post_baba.eq(1) & df.publication.eq("scmp")).astype(int)

    # on everything
    # path = os.path.join(utils.ROOTPATH, publication.name, "date")
    
    # on baba
    path = os.path.join(utils.ROOTPATH, publication.name, "baba", "date")
    os.makedirs(path, exist_ok=True)
    file = os.path.join(path, "date.csv")
    if not os.path.exists(file):
        """Don't overwrite files just in case"""
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
        print(f"WARNING: file already exists, writing to tmp_{publication.name}.csv")
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


