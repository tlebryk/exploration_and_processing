"""Run once using newspaper to get nyt articles. 
Tries to scrape twice and has sleep so takes about 10 hours to run on 10,000 articles. 

 """
# IMPORTS ##################################
import newspaper
from time import sleep
from newspaper.article import ArticleException, ArticleDownloadState
import pandas as pd
import boto3 
from botocore.exceptions import ClientError
import logging
# LOAD DATA AND CONFIG ##########################
logging.basicConfig(filename="scrape.log", level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")
s3.download_file("newyorktime", "nyt/clean_main.csv", "clean_main.csv")

df  = pd.read_csv("clean_main.csv")

# FUNCTIONS ####################
def scrape(url):
    # Download article
    print("scarping ", url)
    slept = 0
    article = newspaper.Article(url=url)
    article.download()
    while article.download_state == ArticleDownloadState.NOT_STARTED:
        # Raise exception if article download state does not change after 10 seconds
        if slept > 9:
            raise ArticleException("Download never started")
        sleep(1)
        slept += 1
    article.parse()
    obj = {
        "title": article.title,
        "authors": article.authors,
        "text": article.text,
        "keywords": article.keywords,
        "tags": article.tags,
        # article.source_url,
        "newurl": article.url,
        "sourceurl": url,
    }
    return obj

def upload_s3(filepath, bucket, key=None):
    if not key:
        key = filepath
    try:
        response = s3.upload_file(filepath, bucket, key)
        return response
    except ClientError as e:
        logger.exception(e)

# FIRST RUN #############################
urls = df.web_url.tolist()

badurls = []
objs = []
for url in urls:
    try:
        obj = scrape(url)
        if obj:
            objs.append(obj)
    except Exception as e:
        logger.error(e)
        badurls.append(url)
    sleep(3)

maindf = pd.json_normalize(objs) 
maindf.to_csv("nyt_full1.csv")

with open("badfiles.txt", "a") as f:
    for u in badurls:
        f.write(u)

# RUN 2 #########################################
with open("badfiles.txt", "r") as f:
    urls = f.readlines()

badurls = []
objs = []
for url in urls:
    try:
        obj = scrape(url)
        if obj:
            objs.append(obj)
    except Exception as e:
        logger.error(e)
        badurls.append(url)
    sleep(3)

maindf2 = pd.json_normalize(objs) 
maindf2.to_csv("nyt_full2.csv")

with open("badfiles2.txt", "a") as f:
    for u in badurls:
        f.write(u)


# UPLOAD #####################################
pd.concat(
    [
        pd.read_csv("nyt_full1.csv", index_col=0),
        pd.read_csv("nyt_full2.csv", index_col=0)
    ]
).to_csv('nyt_full.csv')
upload_s3("nyt_full.csv", "newyorktime", "nyt_full.csv")

# df  = pd.read_csv("clean_main.csv")
# print(df.head())
# news = df.web_url.apply(NewsPlease.from_url)

# url = "https://www.nytimes.com/2011/12/22/world/asia/hong-kong-culls-chickens-after-bird-flu-is-found.html"

# new = NewsPlease.from_url(url)
# maindf = pd.json_normalize(news.apply(lambda s: s.__dict__))#.maintext.iloc[0]
# maindf.to_csv("nyt_full.csv")