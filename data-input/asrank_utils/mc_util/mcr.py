# -*- coding: utf-8 -*-
__author__ = 'baitauk'

import time
from datetime import datetime
from config.config import *
from ASSvgRadarCacher import MSvgRadarCacher

if __name__ == '__main__':
    logger.info("{}START PROCESSING...{}".format(cyan, reset))
    startf = time.time()
    startt = datetime.now()
    idb = MSvgRadarCacher()
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
