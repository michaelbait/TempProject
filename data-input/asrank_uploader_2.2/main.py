# -*- coding: utf-8 -*-
from dbutil import DBUtil

__author__ = 'baitaluk'

import argparse
import sys
import time
from datetime import datetime
from config.config import getoptions
from dataloader import DataLoader

options = getoptions()
logger = options['logging']['logger']
color = options['logging']['color']


def args_parser():
    result = {}
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", required=False, type=str, help="Path to folder with json data sources.")
    parser.add_argument("-m", "--mode", required=False, type=str, default='create', help="Upload mode [create, update or delete]. By default 'create'.")
    parser.add_argument("-dp", "--datapoint", required=False, type=str, default='all',
                        help="Datapoint[s] to be processed: [dataset, asns, orgs, links, locations or all]. By default: 'all'.")
    args = parser.parse_args()
    if args.source:
        result['source'] = args.source
    if args.mode:
        result['mode'] = args.mode
    if args.datapoint:
        ms = set(str(args.datapoint).split(','))
        if len(ms) > 1 and 'all' in ms:
            ms.remove('all')
        result['datapoints'] = ms
    else:
        result['datapoints'] = ['all']
    return result


if __name__ == '__main__':
    print("{}START PROCESSING...{}".format(color['cyan'], color['reset']), file=sys.stdout)
    logger.info("START PROCESSING...")
    startf = time.time()
    startt = datetime.now()
    try:
        idb = DataLoader(args_parser())
        idb.start()
    except (Exception, SystemExit) as e:
        print("{} {} {}".format(color['red'], e, color['reset']))
        logger.error(e)
        exit(1)
    finally:
        endf = time.time()
        endt = datetime.now()
        print("{}Total execution time:{} {}{:.3f} sec. {}".format(color['green'], color['reset'], color['blue'], endf - startf, color['reset']), file=sys.stdout)
        logger.info("Total execution time: {:.3f} sec.".format(endf - startf))
        print("{}END.{}".format(color['cyan'], color['reset']), file=sys.stdout)
        logger.info("END...")

