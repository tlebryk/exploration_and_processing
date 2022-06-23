"""Script to upload locally created masks for each publication 
to s3;
major uploads: date, hk_mask, poli_mask, section (scmp only), and train
test split mask (including drops for some. )
"""
from thesisutils import utils
import os
import logging, logging.config
from pathlib import Path

import logconfig

import pandas as pd

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)

bucket = "newyorktime"
rootpath = r"C:\Users\tlebr\OneDrive - pku.edu.cn\Thesis\data"

# DATE UPLOAD ##############################
# for publication in utils.publications.values():
#     # if publication.name == "nyt":
#     #     continue
#     logger.info("WORKING ON DATE ------------")
#     logger.info("WORKING ON %s", publication.name)
#     relativepath = os.path.join(publication.name, "date")
#     files = os.listdir(os.path.join(rootpath, relativepath))
#     for file in files:
#         key = os.path.join(relativepath, file)
#         logger.info("working on %s", key)
#         datapath = os.path.join(rootpath,key)
#         key = key.replace("\\","/")
#         if not os.path.exists(datapath):
#             logger.warning("WARNING WHY ISN'T DATA HERE", publication.name)
#         else:
#             res = utils.upload_s3(datapath, bucket, key)
def upload(folder, publication):
    logger.info("WORKING ON %s %s ------------", publication.name, folder)
    relativepath = os.path.join(publication.name, folder)
    files = os.listdir(os.path.join(rootpath, relativepath))
    for file in files:
        key = os.path.join(relativepath, file)
        logger.info("key: %s", key)
        datapath = os.path.join(rootpath,key)
        if not os.path.exists(datapath):
            logger.warning("WARNING WHY ISN'T DATA HERE", publication.name)
        else:
            key = key.replace("\\","/")
            utils.upload_s3(datapath, bucket, key)


for publication in utils.publications.values():
    logger.info(publication)
    upload("date", publication)
    upload("hk_mask", publication)
    upload("tts_mask", publication)
    upload("polimask", publication)
    upload("sections", publication)