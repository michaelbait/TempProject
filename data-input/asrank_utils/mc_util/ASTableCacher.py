# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

from json import JSONEncoder, JSONEncoder
import requests
import cairosvg
from influxdb import InfluxDBClient
from pymemcache.client.base import Client
from config.config import *

options = getoptions()


class ASTableCacher:
    """
       Retrieve asns rows from influxdb
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

    q_asns = 'SELECT * FROM asns WHERE (rank != \'0\' AND customer_cone_asns != \'0\')'

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
        count = 0
        try:
            for r in self.retrieve_asns():
                if count > self.max_asns: break
                key = 'asn{}'.format(count)
                self.mc.set(key, JSONEncoder().encode(r))
                logger.info("{}Added to cache key: {}{}".format(yellow, key, reset))
                count += 1
            self.mc.set("asrank_asns_count", count)
        except Exception as e:
            logger.error("{}{}{}".format(yellow, e, reset))
        logger.info("{}At end processed {} asns.{}".format(purple, count, reset))

    def test_asns_cache(self):
        count = 0
        while True:
            key = 'asn{}'.format(count)
            d = self.mc.get(key)
            if not d:
                break
            print(d)
            count += 1

    def retrieve_asns(self):
        query = self.q_asns
        rs = self.influxdb.query(query)
        return rs.get_points()
