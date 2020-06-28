import sys
import argparse
import traceback
from datetime import datetime
from config.config import getoptions
from workers.PDBMamager import PDBWorker

options = getoptions()
logger = options['logger']

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", dest="verbose", help="Prints out lots of messages", required=False, action="store_true")
parser.add_argument("-t", dest="type", help="Upload type [org, net, poc, all]. Default -  org", choices=['org', 'net', 'poc', 'all'],
                    type=str, required=False, default='all')
args = parser.parse_args()

if __name__ == "__main__":
    logger.info("START PROCESSING...")
    startf = datetime.now()
    try:
        PDBWorker.start(args)
    except Exception as e:
        if 'test' in options.get("ENV", None):
            logger.error(traceback.format_exc())
        else:
            logger.error(e)
        sys.exit(1)
    finally:
        endf = datetime.now()
        logger.info("END - {}".format(endf - startf))
