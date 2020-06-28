# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

import requests
import cairosvg
from influxdb import InfluxDBClient
from pymemcache.client.base import Client
from config.config import *

options = getoptions()


class MSvgRadarCacher:
    """
        Retrieve SVG content for asns from asrank-v2 api
        and add them to memcached server.
    """

    influxdb = None
    datapoints = 'all'
    image_dir = options['image_dir']
    host = options['influxdb']['host']
    port = options['influxdb']['port']
    username = options['influxdb']['user']
    password = options['influxdb']['password']
    database = options['influxdb']['dbname']

    # Memcached options
    mc = None
    mc_host = options['memcached']['host']
    mc_port = options['memcached']['port']

    max_asns = options['max_asns_to_process']

    query_asns = "select asn_f, rank from asns where rank != '0';"
    rurl = options['rurl']

    def __init__(self):
        self.influxdb = InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            timeout=3600 * 12
        )

        self.mc = Client((self.mc_host, self.mc_port))

    def process(self):
        self.process_asns()
        self.mc.close()

    def process_asns(self):
        rows = self.retrieve_asns()
        asns = sorted(rows, key=lambda obj: int(obj['rank']) if str(obj['rank']).isdigit() else obj['rank'])
        count = 0
        for asn in asns:
            if count > self.max_asns: break
            logger.info("-" * 20)
            logger.info("{}Start processing asn: {}{}".format(yellow, asn['asn_f'], reset))
            asnf = asn['asn_f']
            content = self.http_get_svg(asnf)
            if content:
                self.save_to_cache(content, asnf)
            count += 1
            logger.info("{}Done processing asn: {}.{}".format(yellow, asn['asn_f'], reset))
            logger.info("")

        logger.info("{}At end processed {} asns.{}".format(purple, count, reset))

    def http_get_svg(self, aid):
        result = None
        try:
            svgurl = self.rurl + aid
            logger.info("{}tray to get svg from url: ({}) {}...".format(white, svgurl, reset))
            r = requests.get(svgurl)
            if r.status_code == 200:
                result = r.text
                if not result:
                    raise requests.exceptions.RequestException('No content recieved from url: {}'.format(svgurl))
                logger.info("{}SVG content for asn ({}) sucessfully recieved".format(green, aid, reset))
            else:
                raise requests.exceptions.RequestException('No content recieved from url: {}'.format(svgurl))
        except requests.exceptions.RequestException as e:
            logger.error("{}Error for asn ({}): {}{}".format(green, aid, e, reset))
        return result

    def save_to_cache(self, content, aid):
        result = True
        fn = "{}{}.svg".format(self.image_dir, aid)
        fkey = "ascore{}".format(aid)
        try:
            logger.info("{}Tray to save asn ({}) to cache...{}".format(white, fkey, reset))
            cairosvg.svg2svg(bytestring=content, write_to=fn)
            self.mc.delete(fkey)
            self.mc.set(fkey, content)
        except Exception as e:
            result = False
            logger.error("{}Error save asn ({}) to cache: {}{}".format(green, fkey, e, reset))
        return result

    def retrieve_asns(self):
        query = self.query_asns
        rs = self.influxdb.query(query)
        return list(rs.get_points())

    def get_filename(self, fn):
        return os.path.basename(fn)
