import os
import requests
import operator
from collections import OrderedDict
from datetime import datetime
from utils.gu import save_as_yaml, remove_file, dstd
from workers.BaseWorker import BaseWorker
from config.config import getoptions

options = getoptions()
logger = options['logger']


class OrgWorker(BaseWorker):
    page_size = options.get("page_size", 10000)
    api_url = "https://www.peeringdb.com/api/org"
    api_type = "org"

    @classmethod
    def process(cls):
        fn = os.path.join(options.get("data"), cls.api_type + ".yaml")
        remove_file(fn)
        skip = 0
        while True:
            query = "{}?depth=1&limit={}&skip={}".format(cls.api_url, cls.page_size, skip)
            entries = cls._get(query)
            if not entries['data']:
                break

            elems = []
            for entry in entries['data']:
                d = cls._parse(entry)
                elems.append(d)
            try:
                fn = os.path.join(options.get("data"), cls.api_type + ".yaml")
                #elems = sorted(elems, key=operator.itemgetter("organization_id"))
                save_as_yaml(elems, fn, "a")
            except IOError as e:
                logger.error(e)

            skip += cls.page_size

    @staticmethod
    def _parse(content):
        result = None
        try:
            elem = OrderedDict()
            elem["organization_id"] = content["id"] if "id" in content else ""
            elem["name"] = content["name"] if "name" in content else ""
            elem["contributor"] = "baitaluk@caida.org"
            elem["web_id"] = "PeeringDB"
            elem["url"] = "www.peeringdb.com"
            elem["created"] = dstd(content["created"]) if "created" in content else ""
            elem["extracted"] = dstd(datetime.now())
            mr = ", ".join([str(x) for x in content["net_set"]]) if "net_set" in content else ''
            elem["members"] = {"asns": "["+mr+"]"}
            result = elem
        except IOError as e:
            pass
        return result

    @staticmethod
    def _get(url):
        result = None
        try:
            request = requests.get(url, timeout=120)
            if request.status_code == 200:
                result = request.json()
        except requests.RequestException as e:
            logger.error(e)
        return result
