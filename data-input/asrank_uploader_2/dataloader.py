# -*- coding: utf-8 -*-
import ipaddress
import math
import os
import sys
import json
import operator
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
        self.conn = DBUtil().get_db_connection()
        self.meta = DBUtil().get_table_meta()

    def start(self):
        for p in self.dirs:
            self.process(p)

    def process(self, subdir):
        if self.is_valid_dir(str(subdir)):
           self.process_datasets(subdir.name)
           self.process_orgs(subdir.name)
           self.process_asns(subdir.name)
           self.process_links(subdir.name)
           self.process_locations(subdir.name)
           self.process_cones(subdir.name)
           self.process_prefixes(subdir.name)

    def process_datasets(self, subdir):
        ms = 'dataset'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
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

                        'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
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
                        'modified_at': dp['modifiedAt'] if 'modifiedAt' in dp else datetime.now().strftime(self.DF),
                        'valid_date_first': timestamp,
                        'valid_date_last': timediff
                    }
                    elems.append(el)
                    processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_dataset(ms, elems)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_orgs(self, subdir):
        ms = 'organizations'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        countries = list(self.extract_data_from_file_gen(subdir, 'countries'))
        processed = set()
        elems = []
        count = 0
        try:
            for dp in datapoints:
                # if count >= LIMIT: break
                # count += 1
                oid = str(dp['orgId']) if 'orgId' in dp and dp['orgId'] is not None else None
                if oid is not None and oid in processed: continue

                country = None
                ci1 = str(dp['countryIso']) if 'countryIso' in dp else None
                if ci1:
                    for cnt in countries:
                        ci2 = cnt.get("countryIso", None)
                        if ci1 and ci2:
                            if ci1.lower() == ci2.lower():
                                country = cnt
                                break
                    country = {
                        'iso': country.get("countryIso", ""),
                        'name': country.get("name", ""),
                        'languages': country.get("languages", []),
                        'capital': country.get("capital", ""),
                        'population': country.get("population", 0),
                        'continent': country.get("continent", ""),
                        'area': country.get("area", 0)
                    }
                else:
                    country = {
                        'iso': "",
                        'name': "",
                        'languages': "",
                        'capital': "",
                        'population': 0,
                        'continent': "",
                        'area': 0
                    }

                el = {'org_id': oid,
                      'org_name': str(dp['orgName']) if 'orgName' in dp else '',
                      'rank': int(dp['rank']) if 'rank' in dp else 0,
                      'seen': bool(dp['seen']) if 'seen' in dp else True,
                      'source': str(dp['source']) if 'source' in dp else '',
                      'country': Json(country),
                      'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                      'asns': Json(dp['members']['asns']) if 'members' in dp and 'asns' in dp['members'] else [],
                      'cone': Json(dp['cone']) if 'cone' in dp else [],
                      'members': Json(dp['members']) if 'members' in dp else [],
                      'orgdegree': Json(dp['orgDegree']) if 'orgDegree' in dp else [],
                      'asndegree': Json(dp['asnDegree']) if 'asnDegree' in dp else [],
                      'announcing': Json(dp['announcing']) if 'announcing' in dp else [],
                      'valid_date_first': timestamp,
                      'valid_date_last': timediff
                      }
                elems.append(el)
                processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_orgs(ms, elems)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_asns(self, subdir):
        ms = 'asns'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        orgs = self.grouping_orgs_by_id(self.extract_data_from_file_gen(subdir, 'organizations'))
        countries = list(self.extract_data_from_file_gen(subdir, 'countries'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                oid = str(dp['asn']) if 'asn' in dp and dp['asn'] is not None else None
                if oid is not None and oid in processed: continue

                country = None
                ci1 = str(dp['countryIso']) if 'countryIso' in dp else None
                if ci1:
                    for cnt in countries:
                        ci2 = cnt.get("countryIso", None)
                        if ci1 and ci2:
                            if ci1.lower() == ci2.lower():
                                country = cnt
                                break
                    country = {
                        'iso': country.get("countryIso", ""),
                        'name': country.get("name", ""),
                        'languages': country.get("languages", []),
                        'capital': country.get("capital", ""),
                        'population': country.get("population", 0),
                        'continent': country.get("continent", ""),
                        'area': country.get("area", 0)
                    }
                else:
                    country = {
                        'iso': "",
                        'name': "",
                        'languages': "",
                        'capital': "",
                        'population': 0,
                        'continent': "",
                        'area': 0
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

                    'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                                   "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                    'cone': Json(dp['cone']) if 'cone' in dp else [],
                    'asndegree': Json(dp['asnDegree']) if 'asnDegree' in dp else [],
                    'announcing': Json(dp['announcing']) if 'announcing' in dp else [],

                    'valid_date_first': timestamp,
                    'valid_date_last': timediff
                }
                elems.append(el)
                processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_asns(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_cones(self, subdir):
        ms = 'cones'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
        datapoints = self.extract_data_from_file_gen(subdir, 'asnCones')
        asns = self.grouping_asns_by_id(self.extract_data_from_file_gen(subdir, 'asns'))
        prefs = self.grouping_prefs_by_id(self.extract_data_from_file_gen(subdir, 'prefixCones'))
        processed = set()
        elems = []
        try:
            for dp in datapoints:
                oid = int(dp['asn']) if 'asn' in dp and dp['asn'] is not None else None
                if oid is not None and oid in processed: continue
                asn = asns[oid] if oid in asns else None
                pref = prefs[oid] if oid in prefs else None
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
                    'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                    'valid_date_first': timestamp,
                    'valid_date_last': timediff
                }
                elems.append(el)
                processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_cones(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_prefixes(self, subdir):
        ms = 'prefixes'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
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
                    'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                    'valid_date_first': timestamp,
                    'valid_date_last': timediff
                }
                elems.append(el)
                processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_prefixes(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_links(self, subdir):
        ms = 'links'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
        datapoints = self.extract_data_from_file_gen(subdir, 'asnLinks')
        asns = self.grouping_asns_by_id(self.extract_data_from_file_gen(subdir, 'asns'))
        orgs = self.grouping_orgs_by_id(self.extract_data_from_file_gen(subdir, 'organizations'))
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
                    el = {
                        'an0': an0,
                        'an1': an1,
                        'rank0': asns[an0]['rank'],
                        'rank1': asns[an1]['rank'],
                        'number_paths': int(dp['numberPaths']) if 'numberPaths' in dp else 0,
                        'relationship': str(dp['relationship']) if 'relationship' in dp else '',
                        'asn0_cone': Json(asns[an0]['cone']) if 'cone' in asns[an0] else [],
                        'asn1_cone': Json(asns[an1]['cone']) if 'cone' in asns[an1] else [],
                        'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                        'corrected_by': Json(dp['correctedBy']) if 'correctedBy' in dp else [],
                        'locations': Json(dp['locations']) if 'locations' in dp else [],
                        'valid_date_first': timestamp,
                        'valid_date_last': timediff
                    }
                    _asns0 = asns[an0] if an0 in asns else []
                    _asns1 = asns[an1] if an1 in asns else []

                    country = self.extract_country_by_iso(_asns0.get('countryIso', ""), countries)
                    _asns0['country'] = country
                    _asns0['organization'] = _asns0.get('orgId', "")
                    el['asn0'] = Json(_asns0)

                    country = self.extract_country_by_iso(_asns1.get('countryIso', ""), countries)
                    _asns1['country'] = country
                    _asns1['organization'] = _asns1.get('orgId', "")
                    el['asn1'] = Json(_asns1)

                    elems.append(el)
                    processed.add(oid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            lnks = self.grouping_links_by_rank(elems)
            self.save_links(ms, lnks)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_locations(self, subdir):
        ms = 'locations'
        timestamp = datetime.strptime(subdir, self.DF)
        timediff = self.date_one_month_delta(timestamp)
        datapoints = self.extract_data_from_file_gen(subdir, ms)
        processed = set()
        elems = []
        count = 0
        try:
            for ds in datapoints:
                #if count >= LIMIT: break
                count += 1
                lid = str(ds['locId']) if 'locId' in ds and ds['locId'] is not None else None
                if lid is not None and lid in processed: continue
                el = {
                    "locid": lid,
                    "city": str(ds['city']) if 'city' in ds else '',
                    "country": str(ds['country']) if 'country' in ds else '',
                    "continent": str(ds['continent']) if 'continent' in ds else '',
                    "region": str(ds['region']) if 'region' in ds else '',
                    "population": int(ds['population']) if 'population' in ds else 0,
                    "latitude": float(ds['latitude']) if 'latitude' in ds else "",
                    "longitude": float(ds['longitude']) if 'longitude' in ds else "",
                    'range': Json({"start": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "end": timediff.strftime("%Y-%m-%d %H:%M:%S")}),
                    'valid_date_first': timestamp,
                    'valid_date_last': timediff
                }
                elems.append(el)
                processed.add(lid)
            logger.info("Range {} -- {}".format(timestamp, timediff))
            print("{}Range {} -- {} {}".format(color['purple'], timestamp, timediff, color['reset']), file=sys.stdout)
            self.save_locations(ms, elems)
            elems.clear()
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def process_cursores(self, cones):
        cursores = []


        cone_cur = 0
        asn_cur = 0
        pref_cur = 0

        ct = 0
        for cone in cones:
            cone_cur += 1
            asns = len(cone['asns'])
            pfxs = len(cone['pfx'])

            if ct + cone_cur == CURSOR_LIMIT:
                cursor = {'cone': cone_cur, 'asn': None, 'pfx': None}
                cursores.append(cursor)


    def save_dataset(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE dataset_id = %s ORDER BY valid_date_first DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('dataset_id')
                        if new_id:
                            cursor.execute(qrow, (new_id,))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['dataset_id', 'number_addresses',  'number_prefixes',  'number_asns']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_orgs(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE org_id = %s ORDER BY valid_date_first DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('org_id')
                        if new_id:
                            cursor.execute(qrow, (new_id,))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['org_id', 'org_name', 'rank']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_asns(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE asn = %s ORDER BY valid_date_last DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('asn')
                        if new_id:
                            cursor.execute(qrow, (new_id,))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['asn', 'asn_name', 'org_id', 'rank', 'source', 'seen', 'ixp', 'clique_member']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break

                                    lt1 = (last_elem['longitude'], last_elem['latitude'])
                                    lt2 = (new_elem['longitude'], new_elem['latitude'])
                                    if equal and DataLoader.longlat_equal(lt1, lt2):
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_cones(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE aid = %s ORDER BY valid_date_last DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('aid')
                        if new_id:
                            cursor.execute(qrow, (str(new_id),))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['aid']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_prefixes(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE aid = %s ORDER BY valid_date_last DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('aid')
                        if new_id:
                            cursor.execute(qrow, (new_id,))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['aid', 'network', 'length']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

    def save_links(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE an0 = '%s' AND an1 = '%s' ORDER BY valid_date_first DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        lid = new_elem.get('an0')
                        rid = new_elem.get('an1')
                        if lid and rid:
                            cursor.execute(qrow, (lid, rid))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['an0', 'an1', 'range', 'number_paths', 'relationship']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)

        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)

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
                    'asnDegree':  org.get('asnDegree', {}),
                    'orgDegree':  org.get('orgDegree', {}),
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

    def save_locations(self, ms, elems):
        insert = []
        update = []
        try:
            with self.conn.cursor() as cursor:
                qcount = "SELECT COUNT(id) FROM {} LIMIT 1;".format(ms)
                qrow = "SELECT * FROM {} WHERE locid = %s ORDER BY valid_date_first DESC;".format(ms)
                cursor.execute(qcount)
                result = cursor.fetchone()
                count = result[0]
                if count > 0:
                    for new_elem in elems:
                        new_id = new_elem.get('locid')
                        if new_id:
                            cursor.execute(qrow, (new_id,))
                            rows = cursor.fetchall()
                            if len(rows) > 0:
                                last_elem = None
                                identical = False
                                for row in rows:
                                    fields = [field.name for field in cursor.description]
                                    last_elem = dict(zip(fields, row))
                                    equal = True
                                    for field in ['locid', 'city', 'country', 'continent', 'region', 'population']:
                                        if new_elem[field] != last_elem[field]:
                                            equal = False
                                            break
                                    if equal:
                                        identical = True
                                        break

                                new_first_date = new_elem['valid_date_first'].strftime(self.DF)
                                new_last_date = new_elem['valid_date_last'].strftime(self.DF)
                                old_first_date = last_elem['valid_date_first'].strftime(self.DF)
                                old_last_date = last_elem['valid_date_last'].strftime(self.DF)

                                if new_first_date == old_first_date and new_last_date == old_last_date:
                                    continue

                                if old_last_date == new_first_date and identical:

                                    r = new_elem['range'].adapted
                                    r['start'] = last_elem['valid_date_first'].strftime("%Y-%m-%d %H:%M:%S")

                                    new_elem['id'] = last_elem['id']
                                    new_elem['valid_date_first'] = last_elem['valid_date_first']
                                    new_elem['range'] = Json(r)

                                    update.append(new_elem)
                                else:
                                    insert.append(new_elem)
                            else:
                                insert.append(new_elem)
                else:
                    insert = elems
                if len(update) > 0:
                    self.update_data_to_db(ms, update)
                if len(insert) > 0:
                    self.insert_data_to_db(ms, insert)
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
                        #print("\tInsertedYYY: {} elems.".format(query), file=sys.stdout)
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
                            print("\tUpdated: {} elems.".format(count), file=sys.stdout)
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
            print("\tInserted: {} elems.".format(query), file=sys.stdout)
            print("\tInserted: {} elems.".format(template), file=sys.stdout)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        return query, template

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
    def distance(origin, destination):
        """
            Haversine formula for calqulate distance on sphere
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
            Calculate distance based on 1 degree distance eps = 1.0
        :param pair1 Long/Lat pair
        :param pair2: Long/Lat pair
        :return: boolean
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
    def resolve_file(subdir, name):
        result = None
        if subdir and name:
            fn = '{dir}.{name}.{ext}'.format(dir=subdir, name=name, ext='jsonl')
            file = os.path.join(DataLoader.source_path, subdir, fn)
            if DataLoader.is_valid_file(file):
                result = file
        return result

    @staticmethod
    def is_valid_dir(source):
        result = False
        try:
            if source and os.path.exists(source) \
                    and os.path.isdir(source) \
                    and os.access(source, os.R_OK)\
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
