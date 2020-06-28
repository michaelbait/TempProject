# -*- coding: utf-8 -*-
from io import StringIO
import ipaddress
import math
import os
import sys
import json
import operator
import traceback
from pprint import pprint
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from config.config import getoptions
from dbutil import DBUtil
from psycopg2.extras import Json, execute_values

options = getoptions()
logger = options['logging']['logger']
color = options['logging']['color']

LIMIT = 10
CURSOR_LIMIT = 2000


class DataLoader:
    source_path = options['input_folder']
    conn = None

    # directory name formated as date
    DF = "%Y%m%d"

    def __init__(self, kwargs):
        self.conn = None
        self.opt = None
        self.meta = None
        self.datapoints = None
        self.mode = 'all'
        self.dirs = set()

        self.opt = options.get('postgresql')
        if 'source' in kwargs and kwargs['source'] != self.source_path:
            self.source_path = kwargs['source']
        if 'datapoints' in kwargs:
            self.datapoints = kwargs['datapoints']
        if 'mode' in kwargs:
            self.mode = kwargs['mode']

        self._fill_dirs()
        dbu = DBUtil()
        self.conn = dbu.get_db_connection()
        self.meta = dbu.get_table_meta()

    def start(self):
        for p in self.dirs:
            self.process(p)
        self.vacuum()

    def process(self, subdir):
        if self.is_valid_dir(str(subdir)):
            self.process_datasets(subdir.name)
            self.process_orgs(subdir.name)
            self.process_asns(subdir.name)
            self.process_links(subdir.name)
            self.process_cones(subdir.name)
            self.process_locations(subdir.name)
            self.process_prefixes(subdir.name)
            self.copy_cones_to_conescursor()

    def process_datasets(self, subdir):
        ms = 'dataset'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                oid = dp['datasetId'] if 'datasetId' in dp and dp['datasetId'] is not None else None
                if oid is not None:
                    if oid in processed: continue
                    el = {
                        'dataset_id': oid,
                        'ip_version': int(dp['ipVersion']) if 'ipVersion' in dp else 4,
                        'number_addresses': int(dp['numberAddresses']) if 'numberAddresses' in dp else 0,
                        'number_prefixes': int(dp['numberPrefixes']) if 'numberPrefixes' in dp else 0,
                        'number_asns': int(dp['numberAsns']) if 'numberAsns' in dp else 0,
                        'number_asns_seen': int(dp['numberAsnsSeen']) if 'numberAsnsSeen' in dp else 0,
                        'number_organizations': int(dp['numberOrganizations']) if 'numberOrganizations' in dp else 0,
                        'number_organizations_seen': int(dp['numberOrganizationsSeen']) if 'numberOrganizationsSeen' in dp else 0,
                        'country': Json({
                            'iso': '',
                            'name': '',
                            'languages': [],
                            'capital': '',
                            'population': 0,
                            'continent': dp['country'] if 'country' in dp else '',
                            'area': 0
                        }),
                        'clique': Json(dp['clique']) if 'clique' in dp else [],
                        'asn_ixs': Json(dp['asnIxs']) if 'asnIxs' in dp else [],
                        'sources': Json(dp['sources']) if 'sources' in dp else [],
                        'asn_reserved_ranges': Json(dp['asnReservedRanges']) if 'asnReservedRanges' in dp else [],
                        'asn_assigned_ranges': Json(dp['asnAssignedRanges']) if 'asnAssignedRanges' in dp else [],
                        'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                        'ts': datetime.now()
                    }
                    elems.append(el)
                    processed.add(oid)
            logger.info("Date {}".format(timestamp))
            print("{}Date {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)
            self.save_dataset(ms, elems)
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            logger.error(e, exc_info=True)

    def process_orgs(self, subdir):
        ms = 'organizations'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        grouped_countries = self.grouping_countries_by_iso(self.extract_data_from_file_gen(subdir, 'countries'))
        processed = set()
        elems = []
        count = 0
        try:
            for dp in datapoints:
                # if count >= LIMIT: break
                # count += 1
                oid = str(dp['orgId']) if 'orgId' in dp and dp['orgId'] is not None else None
                if oid is not None and oid in processed: continue

                country = {'iso': "", 'name': "", 'languages': "", 'capital': "", 'population': 0, 'continent': "", 'area': 0}
                ci1 = str(dp['countryIso']) if 'countryIso' in dp else "other"
                if ci1:
                    country_ = grouped_countries[ci1] if ci1 in grouped_countries else None
                    if country_:
                        country['iso'] = country_.get("countryIso", "ZZ")
                        country['name'] = country_.get("name", "")
                        country['languages'] = country_.get("languages", [])
                        country['capital'] = country_.get("capital", "")
                        country['population'] = country_.get("population", 0)
                        country['continent'] = country_.get("continent", "")
                        country['area'] = country_.get("area", 0)

                el = {'org_id': oid,
                      'org_name': str(dp['orgName']) if 'orgName' in dp else '',
                      'rank': int(dp['rank']) if 'rank' in dp else 0,
                      'seen': bool(dp['seen']) if 'seen' in dp else True,
                      'source': str(dp['source']) if 'source' in dp else '',
                      'country': Json(country),
                      'asns': Json(dp['members']['asns']) if 'members' in dp and 'asns' in dp['members'] else [],
                      'cone': Json(dp['cone']) if 'cone' in dp else [],
                      'members': Json(dp['members']) if 'members' in dp else [],
                      'orgdegree': Json(dp['orgDegree']) if 'orgDegree' in dp else [],
                      'asndegree': Json(dp['asnDegree']) if 'asnDegree' in dp else [],
                      'announcing': Json(dp['announcing']) if 'announcing' in dp else [],
                      'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                      'ts': datetime.now()
                      }

                elems.append(el)
                processed.add(oid)
            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)
            self.save_orgs(ms, elems)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_asns(self, subdir):
        ms = 'asns'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        orgs = self.grouping_orgs_by_id(self.extract_data_from_file_gen(subdir, 'organizations'))
        countries = list(self.extract_data_from_file_gen(subdir, 'countries'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                oid = str(dp['asn']) if 'asn' in dp and dp['asn'] is not None else None
                if oid is None or (oid and oid in processed): continue

                country = {'iso': "", 'name': "", 'languages': "", 'capital': "", 'population': 0, 'continent': "", 'area': 0}
                ci1 = str(dp['countryIso']) if 'countryIso' in dp else None
                if ci1:
                    country_ = None
                    for cnt in countries:
                        ci2 = cnt.get("countryIso", None)
                        if ci1 and ci2:
                            if ci1.lower() == ci2.lower():
                                country_ = cnt
                                break
                    if country_:
                        country = {
                            'iso': country_.get("countryIso", "ZZ"),
                            'name': country_.get("name", ""),
                            'languages': country_.get("languages", []),
                            'capital': country_.get("capital", ""),
                            'population': country_.get("population", 0),
                            'continent': country_.get("continent", ""),
                            'area': country_.get("area", 0)
                        }

                orgid = str(dp['orgId']) if 'orgId' in dp else ''
                orgn = ""
                if orgid.lower() in orgs:
                    orgn = orgs[orgid.lower()].get("orgName", "")

                el = {
                    'asn': int(oid),
                    'asn_name': str(dp['asnName']) if 'asnName' in dp else '',
                    'org_id': str(dp['orgId']) if 'orgId' in dp else '',
                    'org_name': orgn,
                    'rank': int(dp['rank']) if 'rank' in dp else 0,
                    'source': str(dp['source']) if 'source' in dp else '',
                    'seen': bool(dp['seen']) if 'seen' in dp else False,
                    'ixp': bool(dp['seen']) if 'seen' in dp else False,
                    'clique_member': bool(dp['seen']) if 'seen' in dp else False,

                    'longitude': float(dp['longitude']) if 'longitude' in dp else 0.0,
                    'latitude': float(dp['latitude']) if 'latitude' in dp else 0.0,

                    'country': Json(country),
                    'cone': Json(dp['cone']) if 'cone' in dp else {},
                    'asndegree': Json(dp['asnDegree']) if 'asnDegree' in dp else [],
                    'announcing': Json(dp['announcing']) if 'announcing' in dp else [],

                    'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                    'ts': datetime.now()
                }
                elems.append(el)
                processed.add(oid)
            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)
            self.save_asns(ms, elems)
            elems.clear()
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            logger.error(e)

    def process_cones(self, subdir):
        ms = 'cones'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, 'asnCones')
        asns = self.grouping_asns_by_id(self.extract_data_from_file_gen(subdir, 'asns'))
        prefs = self.grouping_prefs_by_id(self.extract_data_from_file_gen(subdir, 'prefixCones'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                date_ = datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp

                oid = int(dp['asn']) if 'asn' in dp and dp['asn'] is not None else None
                if oid is not None and oid in processed: continue
                pref = prefs[oid] if oid in prefs else None
                asn = asns[oid] if oid in asns else None
                asn['date'] = date_.strftime("%Y-%m-%d")

                cone = {
                    'numberAsns': asn['cone']['numberAsns'] if asn and 'cone' in asn and 'numberAsns' in asn['cone'] else 0,
                    'numberPrefixes': asn['cone']['numberPrefixes'] if asn and 'cone' in asn and 'numberPrefixes' in asn['cone'] else 0,
                    'numberAddresses': asn['cone']['numberAddresses'] if asn and 'cone' in asn and 'numberAddresses' in asn['cone'] else 0,
                }
                el = {
                    'aid': oid,
                    'rank': asn['rank'] if 'rank' in asn else 999999999,
                    'asn': Json(asn),
                    'cone': Json(cone),
                    'asns': Json(dp['asnMembers']) if 'asnMembers' in dp else [],
                    'pfx': Json(pref['prefixMembers'] if pref and 'prefixMembers' in pref else []),

                    'date': date_,
                    'ts': datetime.now()
                }
                elems.append(el)
                processed.add(oid)

            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)
            self.save_cones(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_prefixes(self, subdir):
        ms = 'prefixes'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, 'prefixCones')
        asns = self.grouping_asns_by_id(self.extract_data_from_file_gen(subdir, 'asns'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                oid = int(dp['asn']) if 'asn' in dp and dp['asn'] is not None else None
                if oid is not None and oid in processed: continue
                asn = asns[oid] if oid in asns else None
                el = {
                    'asn': str(oid),
                    'network': list(dp['prefixMembers']) if 'prefixMembers' in dp else [],
                    'length': len(dp['prefixMembers']) if 'prefixMembers' in dp else 0,
                    'origin': Json(asn),

                    'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                    'ts': datetime.now()
                }
                elems.append(el)
                processed.add(oid)

            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)

            self.save_prefixes(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_links(self, subdir):
        ms = 'links'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, 'asnLinks')
        asns = self.grouping_asns_by_id(self.extract_data_from_file_gen(subdir, 'asns'))
        countries = list(self.extract_data_from_file_gen(subdir, 'countries'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                an0 = dp['asn0'] if 'asn0' in dp else ''
                an1 = dp['asn1'] if 'asn1' in dp else ''
                oid = '{}_{}_'.format(an0, an1)
                if oid is not None:
                    if oid in processed: continue
                    co0 = asns[an0]['cone'] if 'cone' in asns[an0] else []
                    co1 = asns[an0]['cone'] if 'cone' in asns[an0] else []
                    el = {
                        'an0': an0,
                        'an1': an1,
                        'rank0': asns[an0]['rank'],
                        'rank1': asns[an1]['rank'],
                        'number_paths': int(dp['numberPaths']) if 'numberPaths' in dp else 0,
                        'relationship': str(dp['relationship']) if 'relationship' in dp else '',
                        'asn0_cone': Json(co0),
                        'asn1_cone': Json(co1),
                        'corrected_by': Json(dp['correctedBy']) if 'correctedBy' in dp else [],
                        'locations': Json(dp['locations']) if 'locations' in dp else [],
                        'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                        'ts': datetime.now()
                    }
                    _asns0 = asns[an0] if an0 in asns else []
                    _asns1 = asns[an1] if an1 in asns else []

                    country = self.extract_country_by_iso(_asns0.get('countryIso', ""), countries)
                    _asns0['country'] = country
                    _asns0['organization'] = _asns0.get('orgId', "")
                    _asns0['date'] = datetime.strftime(timestamp, "%Y-%m-%d")

                    el['asn0'] = Json(_asns0)

                    country = self.extract_country_by_iso(_asns1.get('countryIso', ""), countries)
                    _asns1['country'] = country
                    _asns1['organization'] = _asns1.get('orgId', "")
                    _asns1['date'] = datetime.strftime(timestamp, "%Y-%m-%d")
                    el['asn1'] = Json(_asns1)

                    elems.append(el)
                    processed.add(oid)

            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)

            lnks = self.grouping_links_by_rank(elems)
            self.save_links(ms, lnks)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_locations(self, subdir):
        ms = 'locations'
        timestamp = datetime.strptime(subdir, self.DF)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                lid = str(dp['locId']) if 'locId' in dp and dp['locId'] is not None else None
                if lid is not None and lid in processed: continue
                el = {
                    "locid": lid,
                    "city": str(dp['city']) if 'city' in dp else '',
                    "country": str(dp['country']) if 'country' in dp else '',
                    "continent": str(dp['continent']) if 'continent' in dp else '',
                    "region": str(dp['region']) if 'region' in dp else '',
                    "population": int(dp['population']) if 'population' in dp else 0,
                    "latitude": float(dp['latitude']) if 'latitude' in dp else "",
                    "longitude": float(dp['longitude']) if 'longitude' in dp else "",

                    'date': datetime.strptime(dp['date'], self.DF) if 'date' in dp else timestamp,
                    'ts': datetime.now()
                }
                elems.append(el)
                processed.add(lid)

            logger.info("Range {}".format(timestamp))
            print("{}Range {} {}".format(color['purple'], timestamp, color['reset']), file=sys.stdout)

            self.save_locations(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    # SAVING SECTIONS
    def save_dataset(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_orgs(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_asns(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_cones(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_prefixes(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_links(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_locations(self, ms, elems):
        insert = []
        update = []
        delete = []

        try:
            if 'update' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            elif 'delete' in self.mode:
                with self.conn.cursor() as cursor:
                    for nelem in elems:
                        pass
            else:
                insert.extend(elems)

            if len(update) > 0:
                self.update_data_to_db(ms, update)
            if len(insert) > 0:
                self.insert_data_to_db(ms, insert)
            if len(delete) > 0:
                pass

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def copy_cones_to_conescursor(self):
        try:
            with self.conn.cursor() as cursor:
                # input = StringIO()
                # cursor.copy_expert('COPY (select (aid, date) from cones) TO STDOUT', input)
                # input.seek(0)
                # cursor.copy_expert('COPY cc_test FROM STDOUT', input)
                # self.conn.commit()

                qq = "INSERT INTO cones_cursor (aid, date) SELECT aid, date FROM cones ORDER BY aid ASC;"
                cursor.execute(qq)

                qc = "CLUSTER cones_cursor USING cones_cursor_aid_idx;"
                cursor.execute(qc)

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def insert_data_to_db(self, ms, elems, batch_size=1000):
        if elems:
            print("{}Insert into table ({}) - {} elem(s) for processing.{}".format(color['purple'], ms.upper(), len(elems), color['reset']), file=sys.stdout)
            logger.info("Insert into table ({}) - {} elem(s) for processing".format(ms.upper(), len(elems)))
            count = 0

            with self.conn.cursor() as cursor:
                query, template = self.create_insert_query(ms)
                try:
                    sels = []
                    for elem in elems:
                        count += 1
                        sels.append(elem)
                        # print("\tInsertedYYY: {} elems.".format(query), file=sys.stdout)
                        if count % batch_size == 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                            logger.info("\tInserted: {} elems.".format(count))
                        if count >= len(elems) and len(sels) > 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tInserted: {} elems.".format(count), file=sys.stdout)
                            logger.info("\tInserted: {} elems.".format(count))
                    print("{}\tTotal processed for table ({}): {} elems.{}".format(color['yellow'], ms.upper(), count, color['reset']), file=sys.stdout)
                    logger.info("\tTotal processed for table ({}): {} elems.".format(ms.upper(), count))
                except Exception as e:
                    print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                    logger.error(e)

    def update_data_to_db(self, ms, elems, batch_size=1000):
        if elems:
            print("{}Update table ({}) - {} elem(s) for processing.{}".format(color['purple'], ms.upper(), len(elems), color['reset']), file=sys.stdout)
            logger.info("Update table ({}) - {} elem(s) for processing.".format(ms.upper(), len(elems)))
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
                            # print("\tUpdated: {} elems.".format(count), file=sys.stdout)
                            logger.info("\tUpdated: {} elems.".format(count))
                        if count >= len(elems) and len(sels) > 0:
                            execute_values(cursor, query, template=template, argslist=sels, page_size=batch_size)
                            sels.clear()
                            print("\tUpdated: {} elems.".format(count), file=sys.stdout)
                            logger.info("\tUpdated: {} elems.".format(count))
                    print("{}\tTotal updated for table ({}): {} elems.{}".format(color['yellow'], ms.upper(), count, color['reset']), file=sys.stdout)
                    logger.info("\tTotal updated for table ({}) elems.".format(count))
                except Exception as e:
                    print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                    logger.error(e)

    def create_update_query(self, table_name):
        """
        Create update query using db meta for table columns name and type.

        :param table_name:  Table name in database.
        :return: Query string.
        """
        template = None
        query = None
        q = 'UPDATE {} SET ({}) = ({}) FROM (VALUES %s) AS data ({}) WHERE {} = {};'
        try:
            fields = self.meta[table_name]
            data_columns = []
            for e in fields:
                pr = 'data.{}'.format(e['name'])
                if 'json' in e['type']:
                    pr += '::json'
                data_columns.append(pr)

            col1 = ', '.join([x['name'] for x in fields])
            col2 = ', '.join(data_columns)
            query = q.format(table_name, col1, col2, col1, table_name + '.id', 'data.id')
            template = "(" + (', '.join(['%({})s'.format(x['name']) for x in fields])) + ")"
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return query, template

    def create_insert_query(self, table_name):
        """
        Create unsert query using db meta for table columns name.

        :param table_name:  Table name in database.
        :return: Query string.
        """
        template = None
        query = None
        q = 'INSERT INTO {} ({}) VALUES %s'
        try:
            fields = self.meta[table_name][1:]
            col = ', '.join([x['name'] for x in fields])
            query = q.format(table_name, col)
            template = "(" + (', '.join(['%({})s'.format(x['name']) for x in fields])) + ")"
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return query, template

    def _fill_dirs(self):
        if self.is_valid_dir(self.source_path):
            root_dir = Path(self.source_path)
            self.dirs = sorted([p for p in root_dir.iterdir() if p.is_dir()])

    def extract_last_data_dir(self):
        result = None
        subdirs = [x for x in os.listdir(self.source_path) if os.path.isdir(os.path.join(self.source_path, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
        return result

    def vacuum(self):
        """
        Vacuum Postgresql DB.
        :return:
        """
        try:
            print('{}Start optimize db.{}'.format(color['blue'], color['reset']), file=sys.stdout)
            with self.conn.cursor() as cursor:
                cursor.execute("VACUUM FULL")
            print('{}End optimize db.{}'.format(color['blue'], color['reset']), file=sys.stdout)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)

    @staticmethod
    def extract_country_by_iso(iso, countries):
        result = {
            'iso': "",
            'name': "",
            'languages': "",
            'capital': "",
            'population': 0,
            'continent': "",
            'area': 0
        }
        for cnt in countries:
            c = cnt.get("countryIso", None)
            if c and (c.lower() == iso.lower()):
                result['iso'] = cnt.get("countryIso", "")
                result['name'] = cnt.get("name", "")
                result['languages'] = cnt.get("languages", [])
                result['capital'] = cnt.get("capital", "")
                result['population'] = cnt.get("population", 0)
                result['continent'] = cnt.get("continent", "")
                result['area'] = cnt.get("area", 0)
                break
        return result

    @staticmethod
    def extract_org_by_orgid(orgid, orgs):
        result = {
            'orgId': "",
            'orgName': "",
            'rank': 0,
            'range': None,
            'seen': True,
            'country': None,
            'asnDegree': None,
            'orgDegree': None,
            'cone': None,
            'members': None,
            'neighbors': None,
            'asnLinks': None,
            'source': ""
        }

        try:
            if orgid is not None and orgid.lower() in orgs:
                org = orgs[orgid.lower()]
                result = {
                    'orgId': org.get('orgId', ''),
                    'orgName': org.get('orgName', ''),
                    'rank': org.get('rank', 999999999),
                    'range': org.get('rank', None),
                    'seen': org.get('seen', True),
                    'country': org.get('country', {}),
                    'asnDegree': org.get('asnDegree', {}),
                    'orgDegree': org.get('orgDegree', {}),
                    'cone': org.get('cone', {}),
                    'members': org.get('members', {}),
                    'neighbors': org.get('neighbors', {}),
                    'asnLinks': org.get('asnLinks', {}),
                    'source': org.get('source', ''),
                }
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def grouping_countries_by_iso(counties):
        group = {}
        for country in counties:
            iso = country['countryIso'] if 'countryIso' in country else "other"
            if iso not in group:
                group[iso] = country
        return group

    @staticmethod
    def grouping_links_by_rank(links):
        result = []

        def sorter(link):
            r0 = int(link["rank0"]) if "rank0" in link else 999999999
            r1 = int(link["rank1"]) if "rank1" in link else 999999999
            if r0 <= r1:
                return r0, r1
            else:
                return r1, r0

        ranks_last = None
        num_above = 0
        rank = 0
        for link in sorted(links, key=sorter):
            ranks = sorter(link)
            if not ranks_last or not ranks_last == ranks:
                rank = num_above + 1
            link["rank"] = rank
            num_above += 1
            ranks_last = ranks
            result.append(link)
        return result

    @staticmethod
    def grouping_orgs_by_id(orgs):
        result = defaultdict(dict)
        try:
            if orgs:
                for org in orgs:
                    oid = org['orgId']
                    if oid:
                        result[oid.lower()] = org
        except Exception as e:
            logger.error(e)
        return result

    @staticmethod
    def grouping_asns_by_id(asns):
        result = defaultdict(dict)
        try:
            if asns:
                for asn in asns:
                    aid = asn['asn']
                    if aid:
                        result[aid] = asn
        except Exception as e:
            logger.error(e)
        return result

    @staticmethod
    def grouping_prefs_by_id(prefs):
        result = defaultdict(dict)
        try:
            if prefs:
                for asn in prefs:
                    aid = int(asn['asn'])
                    if aid:
                        result[aid] = asn
        except Exception as e:
            logger.error(e)
        return result

    @staticmethod
    def resolve_file(subdir, name):
        """
        Construct right path to resource
        :param subdir: Directory name
        :param name: File name
        :return:   boolean
        """
        result = None
        if subdir and name:
            fn = '{dir}.{name}.{ext}'.format(dir=subdir, name=name, ext='jsonl')
            file = os.path.join(DataLoader.source_path, subdir, fn)
            if DataLoader.is_valid_file(file):
                result = file
        return result

    @staticmethod
    def is_valid_dir(source):
        """
        Validate directory with source files.
        :param source: Directory name
        :return:   boolean
        """
        result = False
        try:
            if source and os.path.exists(source) \
                    and os.path.isdir(source) \
                    and os.access(source, os.R_OK) \
                    and os.listdir(source):
                result = True
            else:
                raise IOError("Unexistent or empty directory <{}>".format(source))
        except IOError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def is_valid_file(source):
        """
        Validate source file.
        :param source: File path
        :return:   boolean
        """
        result = False
        try:
            if source and os.path.exists(source) and os.path.isfile(source) and os.access(source, os.R_OK):
                result = True
            else:
                raise IOError("Cannot access file <{}>".format(source))
        except IOError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def extract_data_from_file_gen(subdir, fname):
        try:
            fn = DataLoader.resolve_file(subdir, fname)
            if DataLoader.is_valid_file(fn):
                with open(fn) as f:
                    for line in f:
                        data = json.loads(line.strip())
                        yield data
        except IOError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
            exit(1)

    @staticmethod
    def date_one_month_delta(date, op=operator.add):
        """
        Build one month delta for source date
        :param date: Date source
        :param op:  Operator +/-
        :return:   One month date delta
        """
        result = None
        try:
            cday = date.day
            mday = monthrange(date.year, date.month)[1]
            delta = cday + (mday - cday)
            result = op(date, timedelta(days=delta))
        except TimeoutError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def get_ip_ver(ip: str):
        result = 'Unknown'
        ips = {4: "IPv4", 6: "IPv6"}
        try:
            v = ipaddress.ip_network(ip, False).version
            result = ips[v]
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def list_equal(l1: list, l2: list):
        """
        Compare two lists for equality.
        :param l1:  List first
        :param l2:  List second
        :return:  boolean
        """
        result = False
        try:
            result = not any(set(l1) ^ set(l2))
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def dict_equal(d1: dict, d2: dict):
        result = False
        try:
            k1 = d1.keys()
            k2 = d2.keys()
            l1 = d1.values()
            l2 = d2.values()
            f1 = not any(set(k1) ^ set(k2))
            f2 = not any(set(l1) ^ set(l2))
            result = f1 and f2
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return result

    @staticmethod
    def distance(origin, destination):
        """
        Haversine formula for calculate distance on sphere
        https://en.wikipedia.org/wiki/Haversine_formula
        :param origin:
        :param destination:
        :return:
        """
        lat1, lon1 = origin
        lat2, lon2 = destination
        radius = 6371  # km

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1)) \
            * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        d = radius * c
        return d

    @staticmethod
    def longlat_equal(pair1, pair2):
        """
        Calculate distance based on 1 degree distance epsilon = 1.0000000000000
        :param pair1 Long/Lat pair
        :param pair2: Long/Lat pair
        :return: boolean Distance less that epsilon.
        """

        result = False
        epsilon = 1.0000000000000

        lat1, lon1 = pair1
        lat2, lon2 = pair2

        lts = abs(lat1 - lat2)
        lgs = abs(lon1 - lon2)
        if lts < epsilon and lgs < epsilon:
            result = True
        return result

    @staticmethod
    def build_prefs(prefixes: list, asn) -> list:
        result = []
        if prefixes:
            for pref in prefixes:
                el = {
                    "network": None,
                    "length": 0,
                    "origin": asn
                }
                try:
                    _n, _l = pref.split("/")
                    el['network'] = _n
                    el['length'] = _l
                    result.append(el)
                except Exception as e:
                    logger.error(e)
        return result
