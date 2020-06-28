# -*- coding: utf-8 -*-

import os
import sys
import json
import operator
from calendar import monthrange
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import Json, execute_values
from config.config import getoptions

options = getoptions()
logger = options['logging']['logger']
color = options['logging']['color']


class PostgresImporter:
    # directory name formated as date
    DF = "%Y%m%d"

    # ip address types
    IP_TYPE = {
        'ipv4': 4,
        'ipv6': 6,
    }

    def __init__(self, kwargs):
        self.conn = None
        self.dsn = None
        self.opt = None
        self.meta = None
        self.datapoints = None
        self.mode = 'all'
        self.subdirs = {'ipv4': [], 'ipv6': []}
        self.opt = options.get('postgresql')
        self.dsn = {'database': self.opt['database'],
                    'host': self.opt['host'],
                    'port': self.opt['port'],
                    'user': self.opt['user'],
                    'password': self.opt['password']}
        if 'source' in kwargs and kwargs['source'] != self.source_path:
            self.source_path = kwargs['source']
        if 'datapoints' in kwargs:
            self.datapoints = kwargs['datapoints']
        if 'mode' in kwargs:
            self.mode = kwargs['mode']

        self.db_connect()
        self.get_table_meta()
        self.get_dir_paths()

    def start(self):
        try:
            # Iterate over dirs ipv4 and ipv6
            for key in self.subdirs.keys():
                # Iterate over subdirs of ipv4 and ipv6
                for folder in self.subdirs[key]:
                    self.process(folder, key)
        except Exception as e:
            print(e)

    def process(self, subdir: Path, ipt: str):
        self.process_datasets(subdir, ipt)
        self.process_orgs(subdir, ipt)
        self.process_asns(subdir, ipt)
        self.process_locations(subdir, ipt)
        self.process_links(subdir, ipt)

        # with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        #     executor.submit(self.process_datasets, subdir, ipt)
        #     executor.submit(self.process_orgs, subdir, ipt)
        #     executor.submit(self.process_asns, subdir, ipt)
        #     executor.submit(self.process_locations, subdir, ipt)


    # def process(self):
    #     self.process_dataset()
    #
    #     if 'all' in self.datapoints:
    #         self.process_dataset()
    #         self.process_asns()
    #         self.process_orgs()
    #         self.process_locations()
    #         self.process_links()
    #     else:
    #         if 'dataset' in self.datapoints:
    #             self.process_dataset()
    #         if 'asns' in self.datapoints:
    #             self.process_asns()
    #         if 'orgs' in self.datapoints:
    #             self.process_orgs()
    #         if 'locations' in self.datapoints:
    #             self.process_locations()
    #         if 'links' in self.datapoints:
    #             self.process_links()

    def process_datasets(self, subdir: Path, ipt: str):
        ms = 'dataset'
        subdir_name = subdir.name
        ip_type = self.IP_TYPE[ipt]
        timestamp = datetime.strptime(subdir_name, self.DF)
        folders = self.extract_global_dir(self.get_all_dirs(subdir))
        elems = []
        for folder in folders:
            country = folder.name
            datapoints = self.extract_data_gen(folder, ms)
            for dp in datapoints:
                # must be hashlib.sha224(str.encode(el.get('country')+el.get('type')+el.get('country'))).hexdigest())
                hs = '{}_{}_{}'.format(subdir_name, country, ip_type)
                el = {
                    'id': None,
                    'hid': hs,
                    'dataset_id': str(dp['dataset_id']) if 'dataset_id' in dp else '',
                    'clique': dp['clique'] if 'clique' in dp else [],
                    'asn_ixs': dp['asn_ixs'] if 'asn_ixs' in dp else [],
                    'number_asns': int(dp['number_asns']) if 'number_asns' in dp else 0,
                    'number_organizes': int(dp['number_organizes']) if 'number_organizes' in dp else 0,
                    'number_prefixes': int(dp['number_prefixes']) if 'number_prefixes' in dp else 0,
                    'number_addresses': int(dp['number_addresses']) if 'number_addresses' in dp else 0,
                    'sources': Json(dp['sources']) if 'sources' in dp else [],
                    'asn_reserved_ranges': Json(dp['asn_reserved_ranges']) if 'asn_reserved_ranges' in dp else [],
                    'asn_assigned_ranges': Json(dp['asn_assigned_ranges']) if 'asn_assigned_ranges' in dp else [],
                    'address_family': str(dp['address_family']) if 'address_family' in dp else '',
                    'address_country': country,
                    'address_type': ip_type,
                    'date_from': timestamp,
                    'date_to': self.date_one_month_delta(timestamp),
                    'ts': datetime.now()
                }
                elems.append(el)
            self.save_dataset(ms, elems)
            elems.clear()

    def process_asns(self, subdir: Path, ipt: str):
        ms = 'asns'
        subdir_name = subdir.name
        ip_type = self.IP_TYPE[ipt]
        timestamp = datetime.strptime(subdir_name, self.DF)
        folders = self.extract_global_dir(self.get_all_dirs(subdir))
        elems = []
        for folder in folders:
            country = folder.name
            datapoints = self.extract_data_gen(folder, ms)
            for dp in datapoints:
                asn = str(dp['asn'])
                hs = '{}_{}_{}'.format(asn, country, ip_type)
                el = {
                    'id': None,
                    'asn': str(dp['asn']) if 'asn' in dp else '',
                    'hid': hs,
                    'name': str(dp['asn_name']) if 'asn_name' in dp else '',
                    'source': str(dp['source']) if 'source' in dp else '',
                    'org_id': str(dp['org_id']) if 'org_id' in dp else '',
                    'org_name': str(dp['org_name']) if 'org_name' in dp else '',
                    'country': str(dp['country']) if 'country' in dp else '',
                    'country_name': str(dp['country_name']) if 'country_name' in dp else '',
                    'latitude': float(dp['latitude']) if 'latitude' in dp else 0.0,
                    'longitude': float(dp['longitude']) if 'longitude' in dp else 0.0,
                    'rank': int(dp['rank']) if 'rank' in dp else 0,
                    'customer_cone_asns': int(dp['customer_cone_asnes']) if 'customer_cone_asnes' in dp else 0,
                    'customer_cone_prefixes': int(dp['customer_cone_prefixes']) if 'customer_cone_prefixes' in dp else 0,
                    'customer_cone_addresses': int(dp['customer_cone_addresses']) if 'customer_cone_addresses' in dp else 0,
                    'degree_peer': int(dp['degree_peer']) if 'degree_peer' in dp else 0,
                    'degree_global': int(dp['degree_global']) if 'degree_global' in dp else 0,
                    'degree_customer': int(dp['degree_customer']) if 'degree_customer' in dp else 0,
                    'degree_sibling': int(dp['degree_sibling']) if 'degree_sibling' in dp else 0,
                    'degree_transit': int(dp['degree_transit']) if 'degree_transit' in dp else 0,
                    'degree_provider': int(dp['degree_provider']) if 'degree_provider' in dp else 0,
                    'address_country': country,
                    'address_type': ip_type,
                    'date_from': timestamp,
                    'date_to': self.date_one_month_delta(timestamp),
                    'ts': datetime.now()
                }
                elems.append(el)
            self.save_asns(ms, elems)
            elems.clear()

    def process_orgs(self, subdir: Path, ipt: str):
        ms = "orgs"
        subdir_name = subdir.name
        ip_type = self.IP_TYPE[ipt]
        timestamp = datetime.strptime(subdir_name, self.DF)
        folders = self.extract_global_dir(self.get_all_dirs(subdir))
        elems = []
        for folder in folders:
            country = folder.name
            datapoints = self.extract_data_gen(folder, ms)
            for dp in datapoints:
                org_id = str(dp['org_id'])
                hs = '{}_{}_{}'.format(org_id, country, ip_type)
                el = {
                    'id': None,
                    'hid': hs,
                    'rank': int(dp['rank']) if 'rank' in dp else 0,
                    'org_id': org_id if 'org_id' in dp else '',
                    'org_name': str(dp['org_name']) if 'org_name' in dp else '',
                    'country': str(dp['country']) if 'country' in dp else '',
                    'country_name': str(dp['country_name']) if 'country_name' in dp else '',
                    'asn_degree_global': int(dp['asn_degree_global']) if 'asn_degree_global' in dp else 0,
                    'asn_degree_transit': int(dp['asn_degree_transit']) if 'asn_degree_transit' in dp else 0,
                    'org_degree_global': int(dp['org_degree_global']) if 'org_degree_global' in dp else 0,
                    'org_degree_transit': int(dp['org_transit_degree']) if 'org_transit_degree' in dp else 0,
                    'customer_cone_asns': int(dp['customer_cone_asnes']) if 'customer_cone_asnes' in dp else 0,
                    'customer_cone_orgs': int(dp['customer_cone_orgs']) if 'customer_cone_orgs' in dp else 0,
                    'customer_cone_addresses': int(dp['customer_cone_addresses']) if 'customer_cone_addresses' in dp else 0,
                    'customer_cone_prefixes': int(dp['customer_cone_prefixes']) if 'customer_cone_prefixes' in dp else 0,
                    'number_members': int(dp['number_members']) if 'number_members' in dp else 0,
                    'number_members_ranked': str(dp['number_members_ranked']) if 'number_members_ranked' in dp else '0',
                    'members': dp['members'] if 'members' in dp else [],
                    'address_country': country,
                    'address_type': ip_type,
                    'date_from': timestamp,
                    'date_to': self.date_one_month_delta(timestamp),
                    'ts': datetime.now()
                }
                elems.append(el)
            self.save_orgs(ms, elems)
            elems.clear()

    def process_links(self, subdir: Path, ipt: str):
        ms = "links"
        subdir_name = subdir.name
        ip_type = self.IP_TYPE[ipt]
        timestamp = datetime.strptime(subdir_name, self.DF)
        folders = self.extract_global_dir(self.get_all_dirs(subdir))
        elems = []
        for folder in folders:
            country = folder.name
            datapoints = self.extract_data_gen(folder, ms)
            for dp in datapoints:
                asn0 = str(dp['asn0']) if 'asn0' in dp else ''
                asn1 = str(dp['asn1']) if 'asn1' in dp else ''
                hs = '{}_{}_{}_{}'.format(asn0, asn1, country, ip_type)
                el = {
                    'id': None,
                    'hid': hs,
                    'asn0': asn0,
                    'asn1': asn1,
                    'relationship': str(dp['relationship']) if 'relationship' in dp else '',
                    'locations': dp['locations'] if 'locations' in dp else [],
                    'number_paths': int(dp['number_paths']) if 'number_paths' in dp else 0,
                    'address_country': country,
                    'address_type': ip_type,
                    'date_from': timestamp,
                    'date_to': self.date_one_month_delta(timestamp),
                    'ts': datetime.now()
                }
                elems.append(el)
            self.save_links(ms, elems)
            elems.clear()

    def process_locations(self, subdir: Path, ipt: str):
        ms = "locations"
        subdir_name = subdir.name
        ip_type = self.IP_TYPE[ipt]
        timestamp = datetime.strptime(subdir_name, self.DF)
        folders = self.extract_global_dir(self.get_all_dirs(subdir))
        elems = []
        for folder in folders:
            country = folder.name
            datapoints = self.extract_data_gen(folder, ms)
            for dp in datapoints:
                lid = str(dp['lid'])
                hs = '{}_{}_{}'.format(lid, country, ip_type)
                el = {
                    'id': None,
                    'hid': hs,
                    'lid': lid,
                    'city': str(dp['city']) if 'city' in dp else '',
                    'country': str(dp['country']) if 'country' in dp else '',
                    'continent': str(dp['continent']) if 'continent' in dp else '',
                    'region': str(dp['region']) if 'region' in dp else '',
                    'population': int(dp['population']) if 'population' in dp else 0,
                    'longitude': float(dp['longitude']) if 'longitude' in dp else 0.0,
                    'latitude': float(dp['latitude']) if 'latitude' in dp else 0.0,
                    'address_country': country,
                    'address_type': ip_type,
                    'date_from': timestamp,
                    'date_to': self.date_one_month_delta(timestamp),
                    'ts': datetime.now()
                }
                elems.append(el)
            self.save_locations(ms, elems)
            elems.clear()

    def save_data_to_db(self, table_name, query, elems, batch_size=1000):
        self.create_table(table_name)
        if elems:
            print("{}Table ({}) - {} elem(s) for processing.{}".format(color['purple'], table_name.upper(), len(elems), color['reset']), file=sys.stdout)
            count = 0
            with self.conn.cursor() as cursor:
                try:
                    sels = []
                    for elem in elems:
                        count += 1
                        sels.append(elem)
                        if count % batch_size == 0:
                            execute_values(cursor, query, sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                        if count >= len(elems) and len(sels) > 0:
                            execute_values(cursor, query, sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                    print("{}\tTotal processed for table ({}): {} elems.{}".format(color['yellow'], table_name.upper(), count, color['reset']), file=sys.stdout)
                except Exception as e:
                    print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                    logger.error(e)

    def save_dataset(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM dataset WHERE hid = %s ORDER BY date_from ASC;"
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for el in elems:
                        hid1 = el.get('hid')
                        cursor.execute(qrow, (hid1,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            fields = [field.name for field in cursor.description]
                            found_identical = False
                            for row in rows:
                                r1 = dict(zip(fields, row))
                                hid2 = r1['hid']
                                if hid1 == hid2:
                                    found_identical = True
                                    el['id'] = r1['id']
                                    el['date_from'] = r1['date_from']
                                    update.append(el)
                                    break
                            if not found_identical:
                                insert.append(el)
                        else:
                            insert.append(el)
                else:
                    insert = elems
                self.update_data_to_db(ms, update)
                self.insert_data_to_db(ms, insert)
        except Exception as e:
            print(e)

    def save_orgs(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE hid = %s ORDER BY date_from ASC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for el in elems:
                        hid1 = el.get('hid')
                        cursor.execute(qrow, (hid1,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            fields = [field.name for field in cursor.description]
                            found_identical = False
                            for row in rows:
                                r1 = dict(zip(fields, row))
                                hid2 = r1['hid']
                                if hid1 == hid2:
                                    found_identical = True
                                    el['id'] = r1['id']
                                    el['date_from'] = r1['date_from']
                                    update.append(el)
                                    break
                            if not found_identical:
                                insert.append(el)
                        else:
                            insert.append(el)
                else:
                    insert = elems
                self.update_data_to_db(ms, update)
                self.insert_data_to_db(ms, insert)
        except Exception as e:
            print(e)

    def save_asns(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE hid = %s ORDER BY date_from ASC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for el in elems:
                        hid1 = el.get('hid')
                        cursor.execute(qrow, (hid1,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            fields = [field.name for field in cursor.description]
                            found_identical = False
                            for row in rows:
                                r1 = dict(zip(fields, row))
                                hid2 = r1['hid']
                                if hid1 == hid2:
                                    found_identical = True
                                    el['id'] = r1['id']
                                    el['date_from'] = r1['date_from']
                                    update.append(el)
                                    break
                            if not found_identical:
                                insert.append(el)
                        else:
                            insert.append(el)
                else:
                    insert = elems
                self.update_data_to_db(ms, update)
                self.insert_data_to_db(ms, insert)
        except Exception as e:
            print(e)

    def save_locations(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE hid = %s ORDER BY date_from ASC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for el in elems:
                        hid1 = el.get('hid')
                        cursor.execute(qrow, (hid1,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            fields = [field.name for field in cursor.description]
                            found_identical = False
                            for row in rows:
                                r1 = dict(zip(fields, row))
                                hid2 = r1['hid']
                                if hid1 == hid2:
                                    found_identical = True
                                    el['id'] = r1['id']
                                    el['date_from'] = r1['date_from']
                                    update.append(el)
                                    break
                            if not found_identical:
                                insert.append(el)
                        else:
                            insert.append(el)
                else:
                    insert = elems
                self.update_data_to_db(ms, update)
                self.insert_data_to_db(ms, insert)
        except Exception as e:
            print(e)

    def save_links(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE hid = %s ORDER BY date_from ASC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for el in elems:
                        hid1 = el.get('hid')
                        cursor.execute(qrow, (hid1,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            fields = [field.name for field in cursor.description]
                            found_identical = False
                            for row in rows:
                                r1 = dict(zip(fields, row))
                                hid2 = r1['hid']
                                if hid1 == hid2:
                                    found_identical = True
                                    el['id'] = r1['id']
                                    el['date_from'] = r1['date_from']
                                    update.append(el)
                                    break
                            if not found_identical:
                                insert.append(el)
                        else:
                            insert.append(el)
                else:
                    insert = elems
                self.update_data_to_db(ms, update)
                self.insert_data_to_db(ms, insert)
        except Exception as e:
            print(e)

    def insert_data_to_db(self, ms, elems, batch_size=1000):
        if elems:
            print("{}Insert into table ({}) - {} elem(s) for processing.{}".format(color['purple'], ms.upper(), len(elems), color['reset']), file=sys.stdout)
            count = 0
            with self.conn.cursor() as cursor:
                query, template = self.create_insert_query(ms)
                try:
                    sels = []
                    for elem in elems:
                        count += 1
                        sels.append(elem)
                        if count % batch_size == 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                        if count >= len(elems) and len(sels) > 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                    print("{}\tTotal processed for table ({}): {} elems.{}".format(color['yellow'], ms.upper(), count, color['reset']), file=sys.stdout)
                except Exception as e:
                    print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                    logger.error(e)

    def update_data_to_db(self, ms, elems, batch_size=1000):
        if elems:
            print("{}Update table ({}) - {} elem(s) for processing.{}".format(color['purple'], ms.upper(), len(elems), color['reset']), file=sys.stdout)
            count = 0
            with self.conn.cursor() as cursor:
                query, template = self.create_update_query(ms)
                try:
                    sels = []
                    for elem in elems:
                        count += 1
                        sels.append(elem)
                        if count % batch_size == 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tUpdated: {} elems.".format(count), file=sys.stdout)
                        if count >= len(elems) and len(sels) > 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tUpdated: {} elems.".format(count), file=sys.stdout)
                    print("{}\tTotal updated for table ({}): {} elems.{}".format(color['yellow'], ms.upper(), count, color['reset']), file=sys.stdout)
                except Exception as e:
                    print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                    logger.error(e)

    def test_update(self, ):
        with self.conn.cursor() as cursor:
            s1 = [{"name": "Country", "url": "https://www.geonames", "date": "20180427"}, {"name": "Netacuity", "url": "https://www.digitalelement.com", "date": "20180427"}]
            s2 = [{"name": "City", "url": "https://www.cityneos", "date": "20180427"}, {"name": "Moskow", "url": "https://www.digitalelement.com", "date": "20180427"}]

            elems = [{"id": 1, "sources": Json(s1)}, {"sources": Json(s2), "id": 2}]
            fields = self.meta['ds']
            template = "("+(', '.join(['%({})s'.format(x['name']) for x in fields]))+")"

            q = self.create_update_query('ds')
            if q is not None:
                execute_values(cursor, q, template=template, argslist=elems)

    def create_update_query(self, table_name):
        """
        Create update query using db meta for table columns name and type.

        :param table_name:  Table name in database.
        :return: Query string.
        """
        template = None
        query = None
        q = "UPDATE {} SET ({}) = ({}) FROM (VALUES %s) AS data ({}) WHERE {} = {};"
        try:
            fields = self.meta[table_name]
            data_columns = []
            for e in fields:
                pr = 'data.{}'.format(e['name'])
                if 'jsonb' in e['type']:
                    pr += '::jsonb'
                data_columns.append(pr)

            col1 = ', '.join([x['name'] for x in fields])
            col2 = ', '.join(data_columns)
            query = q.format(table_name, col1, col2, col1, table_name + '.id', 'data.id')
            template = "(" + (', '.join(['%({})s'.format(x['name']) for x in fields])) + ")"
        except Exception as e:
            print(e)
        return query, template

    def create_insert_query(self, table_name):
        """
        Create unsert query using db meta for table columns name.

        :param table_name:  Table name in database.
        :return: Query string.
        """
        template = None
        query = None
        q = 'INSERT INTO {} ({}) VALUES %s;'
        try:
            fields = self.meta[table_name][1:]
            col = ', '.join([x['name'] for x in fields])
            query = q.format(table_name, col)
            template = "(" + (', '.join(['%({})s'.format(x['name']) for x in fields])) + ")"
        except Exception as e:
            print(e)
        return query, template

    @staticmethod
    def resolve_file(subdir, name):
        result = None
        if subdir and name:
            fn = '{dir}.{name}.{ext}'.format(dir=subdir, name=name, ext='jsonl')
            file = os.path.join(PostgresImporter.source_path, subdir, fn)
            if PostgresImporter.validate_source_path(file):
                result = file
        return result

    @classmethod
    def extract_data_gen(cls, subdir: Path, fname: str):
        try:
            fn = subdir.joinpath(fname + ".jsonl")
            if fn.exists and fn.is_file():
                with fn.open() as f:
                    for line in f:
                        if line.startswith("#"):
                            continue
                        data = json.loads(line.strip())
                        yield data
        except IOError as e:
            print(e)

    @staticmethod
    def extract_data_from_file_gen(subdir, fname):
        fn = PostgresImporter.resolve_file(subdir, fname)
        if PostgresImporter.validate_source_path(fn):
            with open(fn) as f:
                for line in f:
                    data = json.loads(line.strip())
                    yield data

    @staticmethod
    def validate_source_path(source):
        result = False
        try:
            if source and os.path.exists(source) and os.access(source, os.R_OK):
                result = True
            if not result:
                raise IOError("Cannot access directory <{}>".format(source))
        except IOError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @classmethod
    def extract_last_dir(cls, path):
        result = None
        subdirs = [x for x in os.listdir(path) if os.path.isdir(os.path.join(path, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
        return result

    @staticmethod
    def date_one_month_delta(date, op=operator.add):
        result = None
        try:
            cday = date.day
            mday = monthrange(date.year, date.month)[1]
            delta = cday + (mday - cday)
            result = op(date, timedelta(days=delta))
        except TimeoutError as e:
            logger.error(e)
        return result

    @staticmethod
    def validate_date(date, df) -> bool:
        result = False
        try:
            datetime.strptime(date, df)
            result = True
        except ValueError:
            logger.error("Incorrect date: %s".format(date))
        return result

    @staticmethod
    def inter_dicts(dict1: dict, dict2: dict) -> bool:
        result = False
        keys = list(dict1.keys())[:-3]
        fields = []
        for k in keys:
            if dict1[k] == dict2[k]:
                fields.append(k)
        if len(keys) == len(fields):
            result = True
        return result

    @staticmethod
    def diff_asns(dict1: dict, dict2: dict) -> bool:
        result = False
        keys = list(dict1.keys())[:-3]
        for k in keys:
            if dict1[k] != dict2[k]:
                result = True
                break
        return result

    def get_dir_paths(self, mode='all'):
        """
        Get all children directories for root ipv4, ipv6,
        where directory name is a date in format 20190101.
        :param mode: If mode parameter is equal to 'single' get only last folder,
                     otherwise get all.
        :return:     List of directories path in sortrd order
        """
        root_path = Path(options['input_folder'])
        try:
            if root_path.exists():
                paths = {
                    "ipv4": root_path.joinpath('ipv4'),
                    "ipv6": root_path.joinpath('ipv6')
                }
                for p in paths.keys():
                    path = Path(paths[p])
                    if path.exists() and path.is_dir():
                        subdirs = self.get_all_dirs(path)
                        if 'single' in mode:
                            self.subdirs[p].extend([subdirs[-1]])
                        else:
                            self.subdirs[p].extend(subdirs)
        except IOError as e:
            print(e)
            exit(1)

    @classmethod
    def extract_global_dir(cls, folders):
        idx = -1
        for f in folders:
            if 'global' in f.name:
                idx = folders.index(f)
        return [folders.pop(idx)] + folders

    @classmethod
    def get_all_dirs(cls, path: Path) -> list:
        return sorted([p for p in path.iterdir() if p.is_dir()])

    # DB Helpers
    def db_exist(self, conn, dbname):
        result = False
        if not dbname and self.dsn['database']:
            dbname = self.dsn['database']
        try:
            with conn.cursor() as cursor:
                test_db = cursor.mogrify("""SELECT EXISTS (SELECT 1 datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(%s));""", (dbname,))
                cursor.execute(test_db)
                rows = cursor.fetchone()
                if rows is not None and len(rows) > 0:
                    if rows[0]:
                        result = True
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    def table_exist(self, table_name):
        result = False
        try:
            with self.conn.cursor() as cursor:
                test_db = cursor.mogrify("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s);", (table_name,))
                cursor.execute(test_db)
                rows = cursor.fetchone()
                if rows is not None and len(rows) > 0:
                    if rows[0]:
                        result = True
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    def create_table(self, table_name):
        if not self.table_exist(table_name):
            try:
                dn = "{}.sql".format(table_name)
                with open(os.path.join(options['SQL_PATH'], dn), 'rt') as sql:
                    sql = sql.read()
                    with self.conn.cursor() as cursor:
                        cursor.execute(sql)
            except Exception as e:
                print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                logger.error(e)
                exit(1)

    def show_tables(self):
        result = []
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
                rows = cursor.fetchall()
                if rows is not None and len(rows) > 0:
                    result = [row[0] for row in rows]
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    def db_connect(self):
        opt = options.get('postgresql')
        dsn = {'database': 'postgres',
               'host': opt['host'],
               'port': opt['port'],
               'user': opt['user'],
               'password': opt['password']}
        try:
            conn = connect(**dsn)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                if not self.db_exist(conn, opt['database']):
                    cursor.execute('CREATE DATABASE {};'.format(opt['database']))
            if self.db_exist(conn, opt['database']):
                self.conn = connect(**self.dsn)
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            for ds in options.get('ds').keys():
                if not self.table_exist(ds):
                    self.create_table(ds)

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
            exit(1)

    def get_table_meta(self):
        """
        Retrieve metadata (column namem column type) from database.
        Search only into public schema.
        :return:  save metadata into class instance var.
        """
        result = {}
        try:
            with self.conn.cursor() as cursor:
                query = cursor.mogrify('select "table_name" from information_schema.tables where table_catalog=%s AND table_schema=%s;', (self.dsn['database'], 'public'))
                cursor.execute(query)
                records = cursor.fetchall()
                for record in records:
                    key = record[0]
                    query = cursor.mogrify('select "column_name", "data_type" from information_schema.columns where table_catalog=%s and table_schema=%s and table_name=%s;',
                                           (self.dsn['database'], 'public', key))
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    fields = []
                    for row in rows:
                        el = {"name": row[0], "type": row[1]}
                        fields.append(el)
                    result[key] = fields
        except Exception as e:
            print(e)
        self.meta = result
