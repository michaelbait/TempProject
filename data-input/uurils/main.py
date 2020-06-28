import time
import argparse
from datetime import datetime
from aii_v2.influxiportv2 import V2InfluxImporter
from aii_v2.config.config import *


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
    """
        Scripts that load and parse data from json files 
        and insert that into influxdb as metrics for ASRANK-API-V2.
        Script process only one folder into root level folder,
        whose name is the date and seems to be latest.
        For recursive root folder processing, uncomment code in
        V2InfluxImporter.process() method from 51 to 77 lines and
        comment line 78.
        Script accept parameters for datasource path and limit datapoints processing
        Ex: main.py -s datasource_path -dp all
        -s - datasource path to root folder with data-metrics folders inside
            /data/ - root folder
                /20180101   - metric folder
                /20180201   - metric folder
        -dp - name of metric/ 
        Must be: "all" or one from list: "datasets", "asns", "orgs", "cones", "links", "locations", "relations"
        
        To change ENV mode from to "prod" set DEBUG var in aii_v2/config/config.py to False.
        
        Set variable "process_limit" = None in aii_v2/influxiportv2.py to some number if want to enable processing limit.
        
    """
    logger = logging.getLogger('asrv2')
    logger.info("{}START PROCESSING...{}".format(cyan, reset))
    startf = time.time()
    startt = datetime.now()
    idb = V2InfluxImporter(args_parser())
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


