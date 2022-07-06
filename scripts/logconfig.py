"""Dictionary for logging configuration. 
sample usage as import: 
```
import logging, logging.config
from pathlib import Path
import logconfig

lgconf = logconfig.logconfig(Path(__file__).stem)
logging.config.dictConfig(lgconf.config_dct)
logger = logging.getLogger(__name__)
```

"""
from datetime import datetime
import os
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")

LOGPATH = "../logs/"


class logconfig:

    def __init__(self, foldername=None):
        if not foldername:
            log_file = os.path.join(LOGPATH, "tmp.log")
        else:
            log_file = os.path.join(LOGPATH, foldername, f'{DATETIMENOW}.log')
            os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self.config_dct = { 
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': { 
                'standard': { 
                    'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
                },
            },
            'handlers': { 
                'stream_handler': { 
                    'level': 'INFO',
                    'formatter': 'standard',
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout',  # stream_handler is stderr
                },
                'file_handler': {
                    'level': 'INFO',
                    'formatter': 'standard',
                    'class': 'logging.FileHandler',
                    'filename': log_file,
                    # 'stream': 'ext://sys.stdout',  # stream_handler is stderr
                }
            },
            'loggers': { 
                '': {  # root logger
                    'handlers': ['stream_handler'],
                    'level': 'WARNING',
                    'propagate': False
                },
                'my.packg': { 
                    'handlers': ['stream_handler'],
                    'level': 'DEBUG',
                    'propagate': False
                },
                '__main__': {  # if __name__ == '__main__'
                    'handlers': ['stream_handler', 'file_handler'],
                    'level': 'DEBUG',
                    'propagate': False
                },
            } 
        }

def log_clean():
    """Deletes all but most recent file in each logging folder"""
    pass


