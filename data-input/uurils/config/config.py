# -*- coding: utf-8 -*-int(self.tmp)

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
LOGS = os.path.join(BASE_PATH, 'logs')

# Loggging
logging.config.fileConfig('config/logging.ini')
logger = logging.getLogger('asrankt')


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
