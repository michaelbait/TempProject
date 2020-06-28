# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)
LOGS = os.path.join(BASE_PATH, 'logs')

DEBUG = True

OPTIONS = {
    'image_dir': "/www/data/images/ascore/prod/",
    'rurl': 'http://as-rank-2.caida.org/as_core/',
    'max_asns_to_process': 1000,
    'influxdb': {
        'host': '127.0.0.1',
        'port': '8086',
        'dbname': 'asrankw',
        'user': 'root',
        'password': 'root'
    },
    'memcached': {
        'host': '127.0.0.1',
        'port': 11211
    }
}

if DEBUG:
    OPTIONS['image_dir'] = '/www/data/images/ascore/dev/'
    OPTIONS['rurl'] = 'http://as-rank-test.caida.org/as_core/'
    OPTIONS['dbname'] = 'asrankwt'
    OPTIONS['memcached']['port'] = 11212


# Loggging
logging.config.fileConfig('config/logging.ini')
logger = logging.getLogger('mc_util')

# Console Color
reset = '\033[0m'  # Reset
black = '\033[0;30m'  # Black
white = '\033[0;37m'  # White
red = '\033[0;91m'  # Red
green = '\033[0;92m'  # Green
yellow = '\033[0;93m'  # Yellow
blue = '\033[0;34m'  # Blue
purple = '\033[0;95m'  # Purple
cyan = '\033[0;96m'  # Cyan


def getoptions(): return OPTIONS
