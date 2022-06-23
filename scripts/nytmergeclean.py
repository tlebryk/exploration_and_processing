"""Ran once 6/18/2022 to get a date on the full csv. commented section clue for further merging"""

import pandas as pd

# merging strat:
(
    full.merge(
        clean[[
            "web_url",
            "_id",
            "pub_date",
            # "year"
        ]],
        left_on="sourceurl",
        how="left",
        right_on="web_url",
        suffixes=["", "_clean"],
    )
    .drop(["web_url"], axis=1)
    # .merge(kws, on="_id", how="left", suffixes=["", "_kws"])
    # .merge(authors, on="_id", how="left", suffixes=["", "_authors"])
    # .dropna(how="all", axis="columns")
).to_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data\nyt\nyt_full.csv")