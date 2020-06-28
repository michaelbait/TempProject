import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)

logging.config.fileConfig('config/logging.ini')


ENV = "test"
OPTIONS = {
    'logger': logging.getLogger('pdbextractor'),
    'data': os.path.join(BASE_PATH, 'data'),
    'url': 'https://www.peeringdb.com/api',
    "page_size": 10000,
    "ENV": ENV
}

if 'test' in ENV:
    OPTIONS["page_size"] = 5000


def getoptions(): return OPTIONS
