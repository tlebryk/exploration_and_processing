"""
If you're working on different machines, overwrite the ROOTPATH to whatever path you have on your machine.
"""
import re

import boto3
from botocore.exceptions import ClientError
import logging
import os
import pandas as pd
import time
from io import StringIO # Python 3.x


logger = logging.getLogger(__name__)

ROOTPATH = r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data"

class Publication:
    """Standardizes retreival of disparately named columns. """
    def __init__(self, name, textcol, uidcol, hdcol) -> None:
        self.name = name
        self.textcol = textcol
        self.uidcol = uidcol
        self.headline = hdcol
        # self.authorcol = authcol
    def __repr__(self) -> str:
        return self.name


publications = {
    "scmp": Publication("scmp", "Body", "Index", "Headline"),
    "nyt": Publication("nyt", "text", "sourceurl", "title"),
    "globaltimes": Publication("globaltimes", "Body", "Art_id", "Title"),
    "chinadaily": Publication("chinadaily", "plainText", "id", "title"),
    "hkfp": Publication("hkfp", "Body", "Art_id", "Headline"),
}

s3 = boto3.client("s3")

def upload_s3(filepath, bucket, key=None):
    if not key:
        key = filepath
    try:
        response = s3.upload_file(filepath, bucket, key)
        return response
    except ClientError as e:
        logger.exception(e)



def get_perc(a, b):
    return round(a/ (a+b),3) *100

def report(a, b):
    perc = get_perc(a,b)
    print(perc, "%")
    print(a, "/", a+b)

def get_df(publication, *args):
    """
    
    :param: args are strings which are joined to be the final path
        e.g. "polimask", "pmask_.csv" becomes rootpath/publication/polimask/pmask_.csv
        If no args are given, default to full csv. 
    """
    if not args:
        args = [f"{publication.name}_full.csv"]
    fullpath = os.path.join(ROOTPATH, publication.name, *args)
    df = pd.read_csv(fullpath)
    return df

def justletters(s):
    return re.sub(r"[^A-Za-z]+", '', s)

def timeit(fn, *args, **kwargs):
    s = time.perf_counter()
    ret = fn(*args, **kwargs)
    e = time.perf_counter()
    print(f"{fn.__name__} took {e-s} secs")
    return ret


def read_df_s3(object_key, bucket="newyorktime"):
    """Reads a csv from s3 and loads into pandas;
    Means do not have to store large files locally anymore. 
    """
    csv_obj = s3.get_object(Bucket=bucket, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df