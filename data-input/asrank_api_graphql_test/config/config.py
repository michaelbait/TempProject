# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)

ENV = "prod"

# Loggging
logging.config.fileConfig('config/logging.ini')

OPTIONS = {

    'input_folder': '/home/baitaluk/Projects/Hisorical_DATA_Small',
    'output_folder':'/home/baitaluk/Projects/asrank_api3_test/data',
    'api_url': 'http://192.172.226.57:8002/dev/graphql/',
    'print_count': 20,
    'max_top': 20,

    'logging': {
        'logger': logging.getLogger('root'),
        'logger2': logging.getLogger('api3test'),
        'color': {
            # Console Color
            'reset': '\033[0m',     # Reset
            'black': '\033[0;30m',  # Black
            'white': '\033[0;37m',  # White
            'red': '\033[0;91m',    # Red
            'green': '\033[0;92m',  # Green
            'yellow': '\033[0;93m', # Yellow
            'blue': '\033[0;34m',   # Blue
            'purple': '\033[0;95m', # Purple
            'cyan': '\033[0;96m',   # Cyan
        }
    }
}

if 'test' in ENV:
    OPTIONS['input_folder'] = '/media/efanchic/documents/asrank_data/test'
    OPTIONS['output_folder'] = os.path.abspath('data')
    OPTIONS['api_url'] = 'http://127.0.0.1:8000/dev/graphql/'


def getoptions(): return OPTIONS
