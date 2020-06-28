# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

import sys
import os
import re
import glob
import json
import getopt
from pathlib import Path
from datetime import datetime, date, time, timedelta
from influxdb import InfluxDBClient
from collections import defaultdict


class Main:
    dbclient,

    def __init__(self):
        path = '/media/efanchic/documents/asrank_data/'
        client = InfluxDBClient('127.0.0.1', 8086, 'asrankuser', 'rankas', 'asrank', ssl=False, verify_ssl=False)

    @staticmethod
    def read_json_from_file():
        pass