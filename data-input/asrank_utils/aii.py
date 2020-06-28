# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

import re
import signal
import json
import uuid
from datetime import datetime
from config.config import *
from influxdb import InfluxDBClient
from influxdb.exceptions import *
from concurrent.futures import ProcessPoolExecutor

options = getoptions()
dp = options.get('dp')
fnames = options.get('fnames')

process_limit = None

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class InfluxImporter:
    influxdb = None
    datapoints_all = ["datasets","asns","orgs","locations","links","relations"]
    datapoints = ["all"]
    source_path_parent_dir = options['input_folder']
    host = options['influxdb']['host']
    port = options['influxdb']['port']
    username = options['influxdb']['user']
    password = options['influxdb']['password']
    database = options['influxdb']['dbname']

    json_ext = 'jsonl'
    re_json = re.compile("([^\.]+)\.jsonl")
    thread_finish = False

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

        self.influxdb = InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            timeout=3600*12
        )
        self.validate_influxdb_connection(self)

    def test(self,file):
        print ("test!",file)

    def process(self):
        logger.info("{}Command line args: {} {}".format(blue, self.datapoints, reset))

        # I'm not sure if the order matters, but this will preserve the order 
        for type in self.datapoints_all:
            if type in self.datapoints:
                self.datapoint_func[type](self.data_files[type])

        self.process_counts()

    def terminate(self):
        self.thread_finish = True

    #def extract_data_from_file(self, subdir, fname):
        #result = []
        #fn = self.resolve_file(self, subdir, fname)
        #if self.validate_source_path(fn):
            #with open(fn) as f:
                #for line in f:
                    #data = json.loads(line.strip())
                    #result.append(data)
            #return result

    def extract_data_from_file_gen(self, fname):
        if os.path.exists(fname):
            with open(fname) as f:
                for line in f:
                    data = json.loads(line.strip())
                    yield data
        else:
            logger.error(Exception("file "+fname+" doesn't exit"))
            exit(1)

    def process_datasets(self,file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading dataset",file)

        count = 0
        entries = []
        for ds in datapoints:
            ds_id = int(ds['dataset_id'])
            el = {
                'ds_id_f': str(ds['dataset_id']) if 'dataset_id' in ds else '',
                'dataset_id': str(ds['dataset_id']) if 'dataset_id' in ds else '',
                'date': str(ds['date']) if 'date' in ds else '',
                'address_family': str(ds['address_family']) if 'address_family' in ds else '',
                'clique': json.dumps(ds['clique']) if 'clique' in ds else '[]',
                'sources': json.dumps(ds['sources']) if 'sources' in ds else '[]',
                'asn_ixes': json.dumps(ds['asn_ixes']) if 'asn_ixes' in ds else '[]',
                'asn_reserved_ranges': json.dumps(ds['asn_reserved_ranges']) if 'asn_reserved_ranges' in ds else '[]',
                'asn_assigned_ranges': json.dumps(ds['asn_assigned_ranges']) if 'asn_assigned_ranges' in ds else '[]',
                'number_asns': int(ds['number_asns']) if 'number_asns' in ds else 0,
                'number_organizes': int(ds['number_organizes']) if 'number_organizes' in ds else 0,
                'number_prefixes': int(ds['number_prefixes']) if 'number_prefixes' in ds else 0,
                'number_addresses': int(ds['number_addresses']) if 'number_addresses' in ds else 0,
            }
            entry = self.create_dp('datasets', el, dp['datasets']['tags'], dp['datasets']['fields'])
            entries.append(entry)
            count += 1
            logger.info('Processed ds: {}'.format(ds_id))

        self.influxdb.drop_measurement('datasets')
        self.batch_write(entries)
        logger.info('Total processed: {}'.format(count))

    def process_orgs(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading orgs",file)

        msms = options.get('orgs_measurements')
        processed = set()
        elems = []
        for ds in datapoints:
            oid = str(ds['org_id'])
            if oid in processed: continue
            el = {
                'org_id_f': str(ds['org_id']) if 'org_id' in ds else '',

                'ts': datetime.now().timestamp(),
                'org_id': str(ds['org_id']) if 'org_id' in ds else '',
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
        rlt = elems
        if process_limit is not None:
            rlt = elems[0:process_limit]
        with ProcessPoolExecutor() as pool:
            for ms in msms:
                self.influxdb.drop_measurement(ms)
                pool.submit(self.save_data_to_db, ms, rlt)

    def process_asns(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading asns",file)

        msms = options.get('asns_measurements')
        processed = set()
        elems = []
        for ds in datapoints:
            oid = int(ds['asn'])
            if oid in processed: continue
            asnes = int(ds['customer_cone_asnes']) if 'customer_cone_asnes' in ds else 0,
            if asnes == 0: continue
            el = {
                'asn_f': str(ds['asn']) if 'asn' in ds else '',

                'id': str(ds['asn']) if 'asn' in ds else '',
                'ts': datetime.now().timestamp(),
                'asn': str(ds['asn']) if 'asn' in ds else '',
                'asn_name': str(ds['asn_name']) if 'asn_name' in ds else '',
                'rank': int(ds['rank']) if 'rank' in ds else 0,
                'source': str(ds['source']) if 'source' in ds else '',
                'org_id': str(ds['org_id']) if 'org_id' in ds else '',
                'org_name': str(ds['org_name']) if 'org_name' in ds else '',
                'country': str(ds['country']) if 'country' in ds else '',
                'country_name': str(ds['country_name']) if 'country_name' in ds else '',
                'latitude': float(ds['latitude']) if 'latitude' in ds else 0.0,
                'longitude': float(ds['longitude']) if 'longitude' in ds else 0.0,
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
        if process_limit is not None:
            rlt = elems[0:process_limit]

        with ProcessPoolExecutor() as pool:
            for ms in msms:
                self.influxdb.drop_measurement(ms)
                pool.submit(self.save_data_to_db, ms, rlt)

        rlt.sort(key=lambda e: e['rank'])
        self.influxdb.drop_measurement('asns')
        self.save_data_to_db_a('asns', rlt)

    def process_relations(self, files):
        asns_file, links_file = files
        asnsl = self.extract_data_from_file_gen(asns_file)
        links = self.extract_data_from_file_gen(links_file)
        print ("loading relations",asns_file,links_file)

        asn_processed = set()
        asnsm = {}
        #asnsl = self.extract_data_from_file_gen(subdir, fnames['asns'])
        for asn in asnsl:
            asnid = int(asn['asn'])
            if asnid not in asnsm.keys():
                asnsm[asnid] = asn
        elems = []
        count = 0
        #links = self.extract_data_from_file_gen(subdir, fnames['links']):
        for link in links:
            asn0 = int(link['asn0'])
            asn1 = int(link['asn1'])
            if asn0 in asnsm.keys():
                al0 = asnsm[asn0]
            if asn1 in asnsm.keys():
                al1 = asnsm[asn1]
                el = {
                    'asn0_f': str(link['asn0']) if 'asn0' in link else '',
                    'asn1_f': str(link['asn1']) if 'asn1' in link else '',

                    'ts': datetime.now().timestamp(),
                    'asn0': str(link['asn0']) if 'asn0' in link else '',
                    'asn1': str(link['asn1']) if 'asn1' in link else '',
                    'paths': str(link['number_paths']) if 'number_paths' in link else '',
                    'locations': json.dumps(link['locations']) if 'locations' in link else '[]',
                    'relationship': str(link['relationship']) if 'relationship' in link else '',
                    'name': str(al0['asn_name']) if 'asn_name' in al0 else '',
                    'rank0': int(al0['rank']) if 'rank' in al0 else 0,
                    'rank1': int(al1['rank']) if 'rank' in al1 else 0,
                    'source': str(al0['source']) if 'source' in al0 else '',
                    'country0': str(al0['country']) if 'country' in al0 else '',
                    'country1': str(al1['country']) if 'country' in al1 else '',
                    'country_name': str(al0['country_name']) if 'country_name' in al0 else '',
                    'latitude': float(al0['latitude']) if 'latitude' in al0 else 0.0,
                    'longitude': float(al0['longitude']) if 'longitude' in al0 else 0.0,
                    'org0_id': str(al0['org_id']) if 'org_id' in al0 else '',
                    'org0_name': str(al0['org_name']) if 'org_name' in al0 else '',
                    'org1_id': str(al1['org_id']) if 'org_id' in al1 else '',
                    'org1_name': str(al1['org_name']) if 'org_name' in al1 else '',
                    'cone_asns0': int(al0['customer_cone_asnes']) if 'customer_cone_asnes' in al0 else 0,
                    'cone_asns1': int(al1['customer_cone_asnes']) if 'customer_cone_asnes' in al1 else 0,
                    'cone_addresses': int(al0['customer_cone_addresses']) if 'customer_cone_addresses' in al0 else 0,
                    'cone_prefixes': int(al0['customer_cone_prefixes']) if 'customer_cone_prefixes' in al0 else 0,
                    'degree_siblings': int(al0['degree_sibling']) if 'degree_sibling' in al0 else 0,
                    'degree_customers': int(al0['degree_customer']) if 'degree_customer' in al0 else 0,
                    'degree_peers': int(al0['degree_peer']) if 'degree_peer' in al0 else 0,
                    'degree_globals': int(al0['degree_global']) if 'degree_global' in al0 else 0,
                    'degree_transits': int(al0['degree_transit']) if 'degree_transit' in al0 else 0,
                    'degree_providers': int(al0['degree_provider']) if 'degree_provider' in al0 else 0
                }
                elems.append(el)
            count += 1
            asn_processed.add(asn1)
        rlt = elems
        if process_limit is not None:
            rlt = elems[0:process_limit]

        rlt.sort(key=lambda e: (e['relationship'], -e['rank0']), reverse=True)
        ms = "relations"
        self.influxdb.drop_measurement(ms)
        self.save_data_to_db_a(ms, rlt)

        # with ProcessPoolExecutor() as pool:
        #     for ms in msms:
        #         if 'relations.rank.asc' in ms:
        #             elems.sort(key=lambda e: (e['relationship'], -e['rank']), reverse=True)
        #         if 'relations.rank.desc' in ms:
        #             elems.sort(key=lambda e: (e['relationship'], -e['rank']))
        #         self.influxdb.drop_measurement(ms)
        #         pool.submit(self.save_data_to_db, ms, elems)

    # These are not called by anything
    #def process_orgs_n(self, subdir):
        #datasets = self.extract_data_from_file_gen(file)
        #print ("loading asns",file)
        #return
        #processed = set()
        #datapoints = self.extract_data_from_file(subdir, fnames['orgs'])
        #count = 0
        #entries = []
        #for ds in datapoints:
            #oid = str(ds['org_id'])
            #if oid in processed: continue
            #el = {
                #'org_id_f': str(ds['org_id']) if 'org_id' in ds else '',
#
                #'org_id': str(ds['org_id']) if 'org_id' in ds else '',
                #'org_name': str(ds['org_name']) if 'org_name' in ds else '',
                #'rank': int(ds['rank']) if 'rank' in ds else 0,
                #'country': str(ds['country']) if 'country' in ds else '',
                #'country_name': str(ds['country_name']) if 'country_name' in ds else '',
                #'org_transit_degree': int(ds['org_transit_degree']) if 'org_transit_degree' in ds else 0,
                #'org_degree_global': int(ds['org_degree_global']) if 'org_degree_global' in ds else 0,
                #'asn_degree_transit': int(ds['asn_degree_transit']) if 'asn_degree_transit' in ds else 0,
                #'asn_degree_global': int(ds['asn_degree_global']) if 'asn_degree_global' in ds else 0,
                #'customer_cone_asnes': int(ds['customer_cone_asnes']) if 'customer_cone_asnes' in ds else 0,
                #'customer_cone_orgs': int(ds['customer_cone_orgs']) if 'customer_cone_orgs' in ds else 0,
                #'customer_cone_addresses': int(ds['customer_cone_addresses']) if 'customer_cone_addresses' in ds else 0,
                #'customer_cone_prefixes': int(ds['customer_cone_prefixes']) if 'customer_cone_prefixes' in ds else 0,
                #'number_members': int(ds['number_members']) if 'number_members' in ds else 0,
                #'number_members_ranked': str(ds['number_members_ranked']) if 'number_members_ranked' in ds else '0',
                #'members': json.dumps(ds['members']) if 'members' in ds else '[]'
            #}
            #entry = self.create_dp('orgs', el, dp['orgs']['tags'], dp['orgs']['fields'])
            #entries.append(entry)
            #processed.add(id)
            #count += 1
            #logger.info('Processed org: {}'.format(oid))
        #self.batch_write(entries)
        #logger.info('Total processed: {}'.format(count))

    #def process_asns_n(self, subdir):
        #processed = set()
        #datapoints = self.extract_data_from_file(subdir, fnames['asns'])
        #count = 0
        #entries = []
        #for ds in datapoints:
            #oid = int(ds['asn'])
            #if oid in processed: continue
            #el = {
                #'asn_f': str(ds['asn']) if 'asn' in ds else '',
#
                #'asn': str(ds['asn']) if 'asn' in ds else '',
                #'asn_name': str(ds['asn_name']) if 'asn_name' in ds else '',
                #'rank': int(ds['rank']) if 'rank' in ds else 0,
                #'source': str(ds['source']) if 'source' in ds else '',
                #'org_id': str(ds['org_id']) if 'org_id' in ds else '',
                #'org_name': str(ds['org_name']) if 'org_name' in ds else '',
                #'country': str(ds['country']) if 'country' in ds else '',
                #'country_name': str(ds['country_name']) if 'country_name' in ds else '',
                #'latitude': float(ds['latitude']) if 'latitude' in ds else 0.0,
                #'longitude': float(ds['longitude']) if 'longitude' in ds else 0.0,
                #'customer_cone_asnes': int(ds['customer_cone_asnes']) if 'customer_cone_asnes' in ds else 0,
                #'customer_cone_prefixes': int(ds['customer_cone_prefixes']) if 'customer_cone_prefixes' in ds else 0,
                #'customer_cone_addresses': int(ds['customer_cone_addresses']) if 'customer_cone_addresses' in ds else 0,
                #'degree_peer': int(ds['degree_peer']) if 'degree_peer' in ds else 0,
                #'degree_global': int(ds['degree_global']) if 'degree_global' in ds else 0,
                #'degree_customer': int(ds['degree_customer']) if 'degree_customer' in ds else 0,
                #'degree_sibling': int(ds['degree_sibling']) if 'degree_sibling' in ds else 0,
                #'degree_transit': int(ds['degree_transit']) if 'degree_transit' in ds else 0,
                #'degree_provider': int(ds['degree_provider']) if 'degree_provider' in ds else 0,
            #}
            #entry = self.create_dp('asns', el, dp['asns']['tags'], dp['asns']['fields'])
            #entries.append(entry)
            #processed.add(oid)
            #count += 1
            #logger.info('Processed asn: {}'.format(oid))
        #self.batch_write(entries)
        #logger.info('Total processed: {}'.format(count))

    #def process_relations_n(self, subdir):
        #asn_processed = set()
        #asnsm = {}
        #asnsl = self.extract_data_from_file_gen(subdir, fnames['asns'])
        #for asn in asnsl:
            #asnid = int(asn['asn'])
            #if asnid not in asnsm.keys():
                #asnsm[asnid] = asn
        #entities = []
        #count = 0
        #for link in self.extract_data_from_file_gen(subdir, fnames['links']):
            #asn1 = int(link['asn1'])
            #if asn1 in asnsm.keys():
                #al = asnsm[asn1]
                #el = {
                    #'asn0_f': str(link['asn0']) if 'asn0' in link else '',
                    #'asn1_f': str(link['asn1']) if 'asn1' in link else '',
#
                    #'asn0': str(link['asn0']) if 'asn0' in link else '',
                    #'asn1': str(link['asn1']) if 'asn1' in link else '',
                    #'paths': str(link['number_paths']) if 'number_paths' in link else '',
                    #'locations': json.dumps(link['locations']) if 'locations' in link else '[]',
                    #'relationship': str(link['relationship']) if 'relationship' in link else '',
                    #'name': str(al['asn_name']) if 'asn_name' in al else '',
                    #'rank': int(al['rank']) if 'rank' in al else 0,
                    #'source': str(al['source']) if 'source' in al else '',
                    #'country': str(al['country']) if 'country' in al else '',
                    #'country_name': str(al['country_name']) if 'country_name' in al else '',
                    #'latitude': float(al['latitude']) if 'latitude' in al else 0.0,
                    #'longitude': float(al['longitude']) if 'longitude' in al else 0.0,
                    #'org_id': str(al['org_id']) if 'org_id' in al else '',
                    #'org_name': str(al['org_name']) if 'org_name' in al else '',
                    #'cone_asns': int(al['customer_cone_asnes']) if 'customer_cone_asnes' in al else 0,
                    #'cone_addresses': int(al['customer_cone_addresses']) if 'customer_cone_addresses' in al else 0,
                    #'cone_prefixes': int(al['customer_cone_prefixes']) if 'customer_cone_prefixes' in al else 0,
                    #'degree_siblings': int(al['degree_sibling']) if 'degree_sibling' in al else 0,
                    #'degree_customers': int(al['degree_customer']) if 'degree_customer' in al else 0,
                    #'degree_peers': int(al['degree_peer']) if 'degree_peer' in al else 0,
                    #'degree_globals': int(al['degree_global']) if 'degree_global' in al else 0,
                    #'degree_transits': int(al['degree_transit']) if 'degree_transit' in al else 0}
                #entry = self.create_dp('relations', el, dp['relations']['tags'], dp['relations']['fields'])
                #entities.append(entry)
            #count += 1
            #asn_processed.add(asn1)
            #logger.info('Processed relation: {}'.format(asn1))
        #self.batch_write(entities)
        #logger.info('Total processed: {}'.format(count))

    def process_locations(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading locations",file)

        processed = set()
        count = 0
        entries = []
        for ds in datapoints:
            lid = str(ds['lid'])
            if lid in processed: continue
            el = {
                'lid_f': str(ds['lid']) if 'lid' in ds else '',

                "lid": str(ds['lid']) if 'lid' in ds else '',
                "city": str(ds['city']) if 'city' in ds else '',
                "country": str(ds['country']) if 'country' in ds else '',
                "continent": str(ds['continent']) if 'continent' in ds else '',
                "region": str(ds['region']) if 'region' in ds else '',
                "population": int(ds['population']) if 'population' in ds else 0,
                "latitude": float(ds['latitude']) if 'latitude' in ds else 0.0,
                "longitude": float(ds['longitude']) if 'longitude' in ds else 0.0,
            }
            entry = self.create_dp('locations', el, dp['locations']['tags'], dp['locations']['fields'])
            entries.append(entry)
            processed.add(lid)
            count += 1
            logger.info('Processed location: {}'.format(lid))
        self.batch_write(entries)
        logger.info('Total processed: {}'.format(count))

    def process_links(self, file):
        datapoints = self.extract_data_from_file_gen(file)
        print ("loading links",file)

        count = 0
        entries = []
        for ds in datapoints:
            el = {
                'asn0_f': str(ds['asn0']) if 'asn0' in ds else '',
                'asn1_f': str(ds['asn1']) if 'asn1' in ds else '',

                'asn0': str(ds['asn0']) if 'asn0' in ds else '',
                'asn1': str(ds['asn1']) if 'asn1' in ds else '',
                'relationship': str(ds['relationship']) if 'relationship' in ds else '',
                'locations': json.dumps(ds['locations']) if 'locations' in ds else '[]',
                'number_paths': int(ds['number_paths']) if 'number_paths' in ds else 0,
            }
            entry = self.create_dp('links', el, dp['links']['tags'], dp['links']['fields'])
            entries.append(entry)
            count += 1
        self.batch_write(entries)
        logger.info('Total processed: {}'.format(count))


    def process_counts(self):
        """
        Save counts of records for all measurements into separate measurement.
        InfluxDB agreggate function Count is very slow.

        :param subdir: Path to json data files
        :return: Save counts.
        """
        el = {"cid": str(uuid.uuid1())[0:8]}
        for type in self.datapoints:
            ranked = 0
            nonranked = 0
            if type == "relations":
                data = self.extract_data_from_file_gen(self.data_files[type][1])
            else:
                data = self.extract_data_from_file_gen(self.data_files[type])
            #nonranked = len(data)
            nonranked = 0
            for d in data:
                rank = int(d['rank']) if 'rank' in d else 0
                customer_cone_asnes = int(d['customer_cone_asnes']) if 'customer_cone_asnes' in d else 0
                if rank > 0 and customer_cone_asnes > 0:
                    ranked += 1
                nonranked += 1

            el[type] = nonranked
            el[type+'_rank'] = ranked
        entry = self.create_dp('counts', el, dp['counts']['tags'], dp['counts']['fields'])
        self.influxdb.drop_measurement('counts')
        self.write(entry)
        logger.info('Processed counts.'.format())

    def save_data_to_db_sort(self, ms, elems):
        dpn, tag, sd = ms.split('.')
        count = 0
        logger.info('{}Process measurement: ({}){}'.format(yellow, ms, reset))
        elems.sort(key=lambda e: e['rank'])
        for el in elems:
            entry = self.create_dp(ms, el, dp[dpn]['tags'], dp[dpn]['fields'])
            try:
                self.write(entry)
                if count > 0 and (count % 1000) == 0:
                    logger.info('{}Processed elements: {}.{}'.format(green, count, reset))
            except (InfluxDBServerError, InfluxDBClientError) as e:
                logger.error("{}Error({}) at pos.{}{}".format(red, e, count, reset))
            count += 1
        logger.info('Processed: ({}{}{}) elements.'.format(purple, count, reset))

    def save_data_to_db(self, ms, els):
        dpn, tag, sd = ms.split('.')
        count = 0
        logger.info('{}Process measurement: ({}){}'.format(yellow, ms, reset))
        elems = self.sorting(els, tag, sd)
        for el in elems:
            entry = self.create_dp(ms, el, dp[dpn]['tags'], dp[dpn]['fields'])
            try:
                self.write(entry)
                if count > 0 and (count % 1000) == 0:
                    logger.info('{}Processed elements: {}.{}'.format(green, count, reset))
            except (InfluxDBServerError, InfluxDBClientError) as e:
                logger.error("{}Error({}) at pos.{}{}".format(red, e, count, reset))
            count += 1
        logger.info('Processed: ({}{}{}) elements.'.format(purple, count, reset))

    def save_data_to_db_a(self, ms, elems):
        count = 0
        logger.info('{}Process measurement: ({}){}'.format(yellow, ms, reset))
        for el in elems:
            entry = self.create_dp(ms, el, dp[ms]['tags'], dp[ms]['fields'])
            try:
                self.write(entry)
                if count > 0 and (count % 1000) == 0:
                    logger.info('{}Processed elements: {}.{}'.format(green, count, reset))
            except (InfluxDBServerError, InfluxDBClientError) as e:
                logger.error("{}Error({}) at pos.{}{}".format(red, e, count, reset))
            count += 1
        logger.info('Processed: ({}{}{}) elements.'.format(purple, count, reset))

    def drop_all_measures(self):
        msms = options.get('asns_measurements')
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

    @staticmethod
    def sorting(entries, tag, direction):
        label = tag if tag else 'ts'
        direction = True if str(direction).lower() == 'desc' else False
        return sorted(entries, key=lambda obj: int(obj[label]) if str(obj[label]).isdigit() else obj[label], reverse=direction)

    @staticmethod
    def create_dp(dp_name, dp_obj, dp_tags, dp_fields):
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
    #def validate_source_path(source):
        #result = False
        #try:
            #if source and os.path.exists(source) and os.access(source, os.R_OK):
                #result = True
            #if not result:
                #raise IOError("Cannot access directory <{}>".format(source))
        #except IOError as e:
            #logger.error(e)
            #exit(1)
        #return result

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
        for type,name in fnames.items():
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
