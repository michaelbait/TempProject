# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

import sys
import re
import os
import signal
import json
import uuid
from datetime import datetime
from influxdb import InfluxDBClient
from influxdb.exceptions import *
from .config.config import *

logger = logging.getLogger('asrv2')

# set to None for disable data processing limit
process_limit = None


class V2InfluxImporter:
    influxdb = None
    datapoints_all = ["datasets","asns","orgs","locations","links","relations"]
    datapoints = ["all"]

    source_path = OPTIONS['input_folder']
    host = OPTIONS['influxdb']['host']
    port = OPTIONS['influxdb']['port']
    username = OPTIONS['influxdb']['user']
    password = OPTIONS['influxdb']['password']
    database = OPTIONS['influxdb']['dbname']
    timestamp = datetime.now().timestamp()

    re_json = re.compile("([^\.]+)\.jsonl")
    json_ext = 'jsonl'

    def __init__(self, kwargs):
        if 'source' in kwargs:
            self.source_path = kwargs['source']
        else:
            self.source_path = self.extract_last_data_dir(self)
        self.data_files = self.extract_data_files()

        if 'datapoints' in kwargs:
            self.datapoints = kwargs['datapoints']
        if "all" in self.datapoints:
            self.datapoints = self.datapoints_all

        self.datapoint_func = {
            "datasets":self.process_datasets,
            "orgs":self.process_orgs,
            "asns":self.process_asns,
            "locations":self.process_locations,
            "links":self.process_links,
            "relations":self.process_relations
            }
        self.validate_datapoints()

        #########################################
        self.influxdb = InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            timeout=3600 * 12
        )
        self.validate_influxdb_connection(self)

    def process(self):
        logger.info("{}Command line args: {} {}".format(blue, self.datapoints, reset))
        for type in self.datapoints_all:
            if type in self.datapoints:
                self.datapoint_func[type](self.data_files[type])

    def process_datasets(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading dataset",file)

        msm = 'datasets'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = int(ds['dataset_id'])
            el = {
                # tags
                'dataset_id': str(ds['dataset_id']) if 'dataset_id' in ds else '',
                'ts': self.timestamp,

                # fields
                'ds_id_f': str(ds['dataset_id']) if 'dataset_id' in ds else '',
                'date': str(ds['date']) if 'date' in ds else '',
                'address_family': str(ds['address_family']) if 'address_family' in ds else '',
                'clique': json.dumps(ds['clique']) if 'clique' in ds else '[]',
                'sources': json.dumps(ds['sources']) if 'sources' in ds else '[]',
                'asn_ixes': json.dumps(ds['asn_ixes']) if 'asn_ixes' in ds else '[]',
                'asn_reserved_ranges': json.dumps(ds['asn_reserved_ranges']) if 'asn_reserved_ranges' in ds else '[]',
                'asn_assigned_ranges': json.dumps(ds['asn_assigned_ranges']) if 'asn_assigned_ranges' in ds else '[]',
                'number_asnes': int(ds['number_asnes']) if 'number_asnes' in ds else 0,
                'number_organizes': int(ds['number_organizes']) if 'number_organizes' in ds else 0,
                'number_prefixes': int(ds['number_prefixes']) if 'number_prefixes' in ds else 0,
                'number_addresses': int(ds['number_addresses']) if 'number_addresses' in ds else 0,
            }
            processed.add(oid)
            elems.append(el)
        rlt = elems
        if process_limit is not None:
            rlt = elems[0:process_limit]

        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, rlt)

    def process_asns(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading asns",file)

        msm = 'asns'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = int(ds['asn'])
            if oid in processed: continue
            el = {
                # fields
                'asn': str(ds['asn']) if 'asn' in ds else '',
                'ts': self.timestamp,

                # tags
                'asn_f': str(ds['asn']) if 'asn' in ds else '',
                'asn_name': str(ds['asn_name']) if 'asn_name' in ds else '',
                'rank': int(ds['rank']) if 'rank' in ds else 0,
                'source': str(ds['source']) if 'source' in ds else '',
                'org_id': str(ds['org_id']) if 'org_id' in ds else '',
                'org_name': str(ds['org_name']) if 'org_name' in ds else '',
                'country': str(ds['country']) if 'country' in ds else '',
                'country_name': str(ds['country_name']) if 'country_name' in ds else '',
                'latitude': str(ds['latitude']) if 'latitude' in ds else "",
                'longitude': str(ds['longitude']) if 'longitude' in ds else "",
                'customer_cone_asns': int(ds['customer_cone_asnes']) if 'customer_cone_asnes' in ds else 0,
                'customer_cone_prefixes': int(ds['customer_cone_prefixes']) if 'customer_cone_prefixes' in ds else 0,
                'customer_cone_addresses': int(ds['customer_cone_addresses']) if 'customer_cone_addresses' in ds else 0,
                'degree_peer': int(ds['degree_peer']) if 'degree_peer' in ds else 0,
                'degree_global': int(ds['degree_global']) if 'degree_global' in ds else 0,
                'degree_customer': int(ds['degree_customer']) if 'degree_customer' in ds else 0,
                'degree_sibling': int(ds['degree_sibling']) if 'degree_sibling' in ds else 0,
                'degree_transit': int(ds['degree_transit']) if 'degree_transit' in ds else 0,
                'degree_provider': int(ds['degree_provider']) if 'degree_provider' in ds else 0,
            }
            processed.add(oid)
            elems.append(el)
        rlt = elems
        rlt.sort(key=lambda e: e['rank'])
        if process_limit is not None:
            rlt = elems[0:process_limit]

        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, rlt)

    def process_orgs(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading orgs",file)

        msm = 'orgs'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = ds['org_id']
            if oid in processed: continue
            el = {
                # tags
                'org_id': str(ds['org_id']) if 'org_id' in ds else '',
                'ts': self.timestamp,

                # fields
                'org_id_f': str(ds['org_id']) if 'org_id' in ds else '',
                'org_name': str(ds['org_name']) if 'org_name' in ds else '',
                'rank': int(ds['rank']) if 'rank' in ds else 0,
                'country': str(ds['country']) if 'country' in ds else '',
                'country_name': str(ds['country_name']) if 'country_name' in ds else '',
                'org_transit_degree': int(ds['org_transit_degree']) if 'org_transit_degree' in ds else 0,
                'org_degree_global': int(ds['org_degree_global']) if 'org_degree_global' in ds else 0,
                'degree_transit': int(ds['asn_degree_transit']) if 'asn_degree_transit' in ds else 0,
                'degree_global': int(ds['asn_degree_global']) if 'asn_degree_global' in ds else 0,
                'customer_cone_asns': int(ds['customer_cone_asnes']) if 'customer_cone_asnes' in ds else 0,
                'customer_cone_orgs': int(ds['customer_cone_orgs']) if 'customer_cone_orgs' in ds else 0,
                'customer_cone_addresses': int(ds['customer_cone_addresses']) if 'customer_cone_addresses' in ds else 0,
                'customer_cone_prefixes': int(ds['customer_cone_prefixes']) if 'customer_cone_prefixes' in ds else 0,
                'number_members': int(ds['number_members']) if 'number_members' in ds else 0,
                'number_members_ranked': str(ds['number_members_ranked']) if 'number_members_ranked' in ds else '0',
                'members': json.dumps(ds['members']) if 'members' in ds else '[]'
            }
            processed.add(oid)
            elems.append(el)
        els = elems
        if process_limit is not None:
            els = elems[0:process_limit]
        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, els)

    def process_locations(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading locations",file)

        msm = 'locations'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = str(ds['lid'])
            if oid in processed: continue
            el = {
                'lid_f': str(ds['lid']) if 'lid' in ds else '',
                'ts': self.timestamp,

                "lid": str(ds['lid']) if 'lid' in ds else '',
                "city": str(ds['city']) if 'city' in ds else '',
                "country": str(ds['country']) if 'country' in ds else '',
                "continent": str(ds['continent']) if 'continent' in ds else '',
                "region": str(ds['region']) if 'region' in ds else '',
                "population": int(ds['population']) if 'population' in ds else 0,
                "latitude": float(ds['latitude']) if 'latitude' in ds else "",
                "longitude": float(ds['longitude']) if 'longitude' in ds else "",
            }
            processed.add(oid)
            elems.append(el)
        els = elems
        if process_limit is not None:
            els = elems[0:process_limit]
        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, els)

    def process_links(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading links",file)

        msm = 'links'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = str(ds['asn1'])
            if oid in processed: continue
            el = {
                'asn0_f': str(ds['asn0']) if 'asn0' in ds else '',
                'asn1_f': str(ds['asn1']) if 'asn1' in ds else '',
                'ts': self.timestamp,

                'asn0': str(ds['asn0']) if 'asn0' in ds else '',
                'asn1': str(ds['asn1']) if 'asn1' in ds else '',
                'relationship': str(ds['relationship']) if 'relationship' in ds else '',
                'number_paths': int(ds['number_paths']) if 'number_paths' in ds else 0,
            }
            processed.add(oid)
            elems.append(el)
        els = elems
        if process_limit is not None:
            els = elems[0:process_limit]
        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, els)

    def process_cones(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading links",file)

        msm = 'cones'
        processed = set()
        elems = []
        for ds in datapoints:
            oid = str(ds['asn'])
            if oid in processed: continue
            el = {
                'asn_f': str(ds['asn']) if 'asn' in ds else '',
                'ts': self.timestamp,

                'asn': str(ds['asn']) if 'asn' in ds else '',
                'cone': json.dumps(ds['cone']) if 'cone' in ds else '[]',
                'in_cone': oid
            }
            processed.add(oid)
            elems.append(el)
        els = elems
        if process_limit is not None:
            els = elems[0:process_limit]
        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, els)

    def process_relations(self, files):
        asns_file, links_file = files
        asnsl = self.extract_data_from_file_gen(asns_file)
        links = self.extract_data_from_file_gen(links_file)
        print ("loading relations",asns_file,links_file)


        msm = 'relations'
        asn_processed = set()
        asnsm = {}
        for asn in asnsl:
            asnid = int(asn['asn'])
            if asnid not in asnsm.keys():
                asnsm[asnid] = asn
        elems = []
        for link in links:
            asn1 = int(link['asn1'])
            al0 = None
            if asn0 in asnsm.keys():
                al0 = asnsm[asn0]
            if asn1 in asnsm.keys():
                al = asnsm[asn1]
                el = {
                    'asn0_f': str(link['asn0']) if 'asn0' in link else '',
                    'asn1_f': str(link['asn1']) if 'asn1' in link else '',
                    'ts': self.timestamp,

                    'asn0': str(link['asn0']) if 'asn0' in link else '',
                    'asn1': str(link['asn1']) if 'asn1' in link else '',
                    'paths': str(link['number_paths']) if 'number_paths' in link else '',
                    'locations': json.dumps(link['locations']) if 'locations' in link else '[]',
                    'relationship': str(link['relationship']) if 'relationship' in link else '',
                    'name': str(al['asn_name']) if 'asn_name' in al else '',
                    'rank': int(al['rank']) if 'rank' in al else 0,
                    'source': str(al['source']) if 'source' in al else '',
                    'country': str(al['country']) if 'country' in al else '',
                    'country_name': str(al['country_name']) if 'country_name' in al else '',
                    'latitude': str(al['latitude']) if 'latitude' in al else "",
                    'longitude': str(al['longitude']) if 'longitude' in al else "",
                    'org_id': str(al['org_id']) if 'org_id' in al else '',
                    'org_name': str(al['org_name']) if 'org_name' in al else '',
                    'cone_asns': int(al['customer_cone_asnes']) if 'customer_cone_asnes' in al else 0,
                    'cone_addresses': int(al['customer_cone_addresses']) if 'customer_cone_addresses' in al else 0,
                    'cone_prefixes': int(al['customer_cone_prefixes']) if 'customer_cone_prefixes' in al else 0,
                    'degree_siblings': int(al['degree_sibling']) if 'degree_sibling' in al else 0,
                    'degree_customers': int(al['degree_customer']) if 'degree_customer' in al else 0,
                    'degree_peers': int(al['degree_peer']) if 'degree_peer' in al else 0,
                    'degree_globals': int(al['degree_global']) if 'degree_global' in al else 0,
                    'degree_transits': int(al['degree_transit']) if 'degree_transit' in al else 0,

                    'org0_id': str(al0['org_id']) if 'org_id' in al0 else '',
                    'org0_name': str(al0['org_name']) if 'org_name' in al0 else '',
                    'rank0': int(al0['rank']) if 'rank' in al0 else 0,
                }
                elems.append(el)
            asn_processed.add(asn1)

        els = elems
        if process_limit is not None:
            els = elems[0:process_limit]
        self.influxdb.drop_measurement(msm)
        self.save_data_to_db(msm, els)

    def save_data_to_db(self, ms, elems):
        dp = OPTIONS['dp']
        logger.info('{}Process measurement: ({}) ...{}'.format(purple, ms, reset))
        count = 0
        errs = 0
        for el in elems:
            try:
                entry = self.create_dp(ms, el, dp[ms]['tags'], dp[ms]['fields'], self.timestamp)
                self.write(entry)
                if count > 0 and count % 500 == 0:
                    logger.info('{} Processed {} elements. {}'.format(yellow, count, reset))
            except (InfluxDBServerError, InfluxDBClientError) as e:
                errs += 1
                logger.error("{}Error({}) at pos {} {}".format(red, e, count, reset))
            count += 1
        logger.info('{} Total processed: ({}) elements. With error: {}.{}'.format(purple, count, errs, reset))

    def drop_all_measures(self):
        msms = OPTIONS['measurements']
        for ms in msms:
            self.influxdb.drop_measurement(ms)

    def batch_write(self, dpl):
        """
        Write datapoints in batch mode (no keep the insert order)

        :param dpl: List of datapoints
        """
        self.influxdb.write_points(dpl, batch_size=500)

    def write(self, dp):
        """
        Write datapoints one-by-one to keep the insert order

        :param dp: List of datapoints
        """
        self.influxdb.write_points([dp])

    def extract_data_from_file(self, subdir, fname):
        result = []
        fn = self.resolve_file(self, subdir, fname)
        if self.validate_source_path(fn):
            with open(fn) as f:
                for line in f:
                    data = json.loads(line.strip())
                    result.append(data)
            return result

    def extract_last_data_dir(self):
        result = None
        subdirs = [x for x in os.listdir(self.source_path) if os.path.isdir(os.path.join(self.source_path, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
        return result

    @staticmethod
    def sorting(entries, tag, direction):
        label = tag if tag else 'ts'
        direction = True if str(direction).lower() == 'desc' else False
        return sorted(entries, key=lambda obj: int(obj[label]) if str(obj[label]).isdigit() else obj[label], reverse=direction)

    @staticmethod
    def create_dp(dp_name, dp_obj, dp_tags, dp_fields, timestamp):
        entry = {
            "measurement": dp_name,
            "tags": {},
            "fields": {},
        }
        # populate tags
        for tag in dp_tags:
            if tag in dp_obj:
                entry['tags'][tag] = dp_obj[tag]
            else:
                entry['tags'][tag] = ""

        # populate fields
        for field in dp_fields:
            if field in dp_obj:
                entry['fields'][field] = dp_obj[field]
        return entry

    @staticmethod
    def resolve_file(self, subdir, name):
        result = None
        if subdir and name:
            fn = '{dir}.{name}.{ext}'.format(dir=subdir, name=name, ext=self.json_ext)
            file = os.path.join(self.source_path, subdir, fn)
            if self.validate_source_path(file):
                result = file
        return result

    @staticmethod
    def validate_influxdb_connection(self):
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

    @staticmethod
    def validate_source_path(source):
        result = False
        try:
            if source and os.path.exists(source) and os.access(source, os.R_OK):
                result = True
            if not result:
                raise IOError("Cannot access directory <{}>".format(source))
        except IOError as e:
            logger.error(e)
            exit(1)
        return result

    #######################################################################
    def extract_data_from_file_gen(self, fname):
        if os.path.exists(fname):
            with open(fname) as f:
                for line in f:
                    data = json.loads(line.strip())
                    yield data
        else:
            logger.error(Exception("file "+fname+" doesn't exit"))
            exit(1)

    def extract_last_data_dir(self):
        directory = self.source_path_parent_dir
        result = None
        subdirs = [x for x in os.listdir(directory) if os.path.isdir(os.path.join(directory, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
            result = os.path.join(directory,result)
        else:
            logger.error(Exception("directory "+directory+" empty"))
            exit(1)
        return result

    def extract_data_files(self):
        directory = self.source_path
        type_files = {}
        if not os.path.isdir(directory):
            logger.error(Exception("directory "+directory+" doesn't exist"))
            exit(1)
        for file in os.listdir(directory):
            m = self.re_json.search(file)
            if m:
                path = os.path.join(directory,file)
                type = m.groups(0)[0]
                type_files[type] = path
        for type,name in  OPTIONS['fnames'].items():
            if isinstance(name,list):
                files = []
                for n in name:
                    if n in type_files:
                        files.append(type_files[n])
                if len(files) == len(name):
                    type_files[type] = files
            elif name in type_files:
                type_files[type] = type_files[name]
        return type_files

    def validate_datapoints(self):
        unknown = set()
        missing = set()
        for type in self.datapoints:
            if type not in self.datapoint_func:
                unknown.add(type)
            elif type not in self.data_files:
                missing.add(type)
        if len(unknown) > 0:
            logger.error(Exception("unknown data type "+",".join(unknown)))
        if len(missing) > 0:
            logger.error(Exception("directory "+self.source_path+" doesn't have data for "+",".join(missing)))

        if len(unknown) > 0 or len(missing) > 0:
            exit(1)

