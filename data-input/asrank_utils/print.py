#! /bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import time
from datetime import datetime
from config.config import *
from influxdb import InfluxDBClient

options = getoptions()

def args_parser():
    result = {}
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", required=False, type=str, help="Path to folder with json data sources.")
    parser.add_argument("-dp", "--datapoint", required=True, type=str, 
                        help="Datapoint[s] to be processed: [datasets, asns, orgs, cones, links, relations, locations ].")
    args = parser.parse_args()
    if args.source:
        result['source'] = args.source
    if args.datapoint:
        ms = set(str(args.datapoint).split(','))
        unknown = []
        for m in ms:
            if m not in options["fnames"]:
                unknown.append(m)
        if len(unknown) > 0:
            print ("unknown",m,file=sys.stderr)
            sys.exit(1)

        result['datapoints'] = ms
    else:
        result['datapoints'] = ['all']
    return result

class InfluxExporter:
    influxdb = None
    host = options['influxdb']['host']
    port = options['influxdb']['port']
    username = options['influxdb']['user']
    password = options['influxdb']['password']
    database = options['influxdb']['dbname']

    def __init__(self):
        self.influxdb = InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            timeout=3600*12
        )
        try:
            lsv = []
            for row in self.influxdb.get_list_database():
                for k, v in row.items():
                    lsv.append(v)
            if self.database not in lsv:
                raise InfluxDBClientError("Cannot access database <{}>.".format(self.database))
        except InfluxDBClientError as e:
            logger.error(e)
            exit(1)

if __name__ == '__main__':
    args = args_parser()
    influx = InfluxExporter()
    for type in args["datapoints"]:
        print (type,file=sys.stderr)
        for rows in influx.influxdb.query("select * from "+type):
            for row in rows:
                print (row)
