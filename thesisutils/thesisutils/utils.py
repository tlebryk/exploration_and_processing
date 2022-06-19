"""
If you're working on different machines, overwrite the ROOTPATH to whatever path you have on your machine.
"""
import boto3
from botocore.exceptions import ClientError
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

ROOTPATH = r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data"

class Publication:
    def __init__(self, name, textcol, uidcol) -> None:
        self.name = name
        self.textcol = textcol
        self.uidcol = uidcol
    def __repr__(self) -> str:
        return self.name


publications = {
    "scmp": Publication("scmp", "Body", "Index"),
    "nyt": Publication("nyt", "text", "sourceurl"),
    "globaltimes": Publication("globaltimes", "Body", "Art_id"),
    "chinadaily": Publication("chinadaily", "plainText", "id"),
    "hkfp": Publication("hkfp", "Body", "Art_id"),
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