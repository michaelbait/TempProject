#! /bin/env python3
# -*- coding: utf-8 -*-

import argparse
import time
from datetime import datetime
from config.config import *
from aii import InfluxImporter

def args_parser():
    result = {}
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", required=False, type=str, help="Path to folder with json data sources.")
    parser.add_argument("-dp", "--datapoint", required=False, type=str, default='all',
                        help="Datapoint[s] to be processed: [datasets, asns, orgs, cones, links, relations, locations or all]. By default: 'all'.")
    args = parser.parse_args()
    if args.source:
        result['source'] = args.source
    if args.datapoint:
        ms = set(str(args.datapoint).split(','))
        if len(ms) > 1 and 'all' in ms:
            ms.remove('all')
        result['datapoints'] = ms
    else:
        result['datapoints'] = ['all']
    return result


if __name__ == '__main__':
    idb = InfluxImporter(args_parser())
    logger.info("{}START PROCESSING...{}".format(cyan, reset))
    startf = time.time()
    startt = datetime.now()
    try:
        idb.process()
    except KeyboardInterrupt:
        pass
    finally:
        endf = time.time()
        endt = datetime.now()
        logger.info("{}Start/End:{}{} {} - {} {}".format(purple, reset, cyan, startt, endt, reset))
        logger.info("{}Total execution time:{} {}{:.3f} sec. {}".format(purple, reset, cyan, endf - startf, reset))
        logger.info("{}END.{}".format(cyan, reset))
