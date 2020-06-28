# -*- coding: utf-8 -*-
import json
import os
import sys
from datetime import date, datetime
from operator import itemgetter

import requests
from pygments import highlight
from collections import defaultdict
from config.config import getoptions
from pygments.lexers.python import PythonLexer
from pygments.formatters.terminal import TerminalFormatter

options = getoptions()
logger = options['logging']['logger']
logger2 = options['logging']['logger2']
color = options['logging']['color']


class TestError(Exception):
    def __init__(self, msg, error=""):
        super().__init__(msg + (": %s" % error))
        self.error = error


class TestUtil:
    source_path = options['input_folder']
    dest_path = options['output_folder']
    api_url = options['api_url']
    content = None

    def __init__(self, kwargs):
        self.args = kwargs
        self.subdir, self.dirpath = TestUtil.extract_last_data_dir()
        if 'u' in kwargs and kwargs['u']:
            self.api_url = kwargs['u']

    def start(self):
        self.process_asns()

    def process_asns(self):
        q = """{{asns(first:{},offset:{},sort:"rank",seen:true){{totalCount,edges{{node{{rank,asn,asnName,
                    organization{{orgId,orgName}},
                    country{{iso}},
                    cone{{numberAsns,numberPrefixes,numberAddresses}},
                    asnDegree {{transit}}}}}}}}}}
                    """
        try:
            print("{}START TEST ASNS ...{}".format(color['cyan'], color['reset']))
            logger2.debug('START TEST ASNS ({})...'.format(datetime.today()))

            asns = self._build_asns()
            cnt = asns['totalCount']
            prx = cnt
            first = 1000
            offset = 0
            count = 0
            processed = 0
            failed = []

            print("{}Elements to test: {} {}".format(color['yellow'], cnt, color['reset']))
            logger2.debug("Elements to test: {}".format(cnt))
            while prx > 0:
                query = q.format(first, offset)
                response = requests.get(options['api_url'], params={"query": query})
                data = response.json()
                count += len(data['data']['asns']['edges'])
                offset += first
                prx -= first
                print("Remaining: {}".format(prx))
                if 'errors' in data:
                    raise TestError(data['errors'][0]['message'])
                processed += self.compare_asns(data['data']['asns'], asns, failed)

            print("{}Processed {} elements.{}".format(color['blue'], processed, color['reset']))
            print("{}Failed {} elements.{}".format(color['red'], len(failed), color['reset']))
            logger2.info("Processed {} elements.".format(processed))
            logger2.info("Failed {} elements.".format(len(failed)))
            if len(failed) > 0:
                logger2.info("Failed list: ")
                for a in failed:
                    logger2.info("\tAsn Nr. {}".format(a))

            print("{}END TEST ASNS ...{}".format(color['cyan'], color['reset']))
            print("-" * 40 + '\n')
            logger2.debug('END TEST ASNS ...')
        except TestError as e:
            logger.error(e)
            logger2.error(e)
            logger2.debug('ABORT TEST ASNS.')
            print("{}ABORT TEST ASNS ...{}".format(color['red'], color['reset']))

    def compare_asns(self, l1, l2, failed):
        processed = 0

        def __grouping(asns):
            r = defaultdict(dict)
            try:
                if asns:
                    for asn in asns['edges']:
                        node = asn['node']
                        aid = str(node['asn'])
                        if aid:
                            r[aid] = node
            except Exception as e:
                logger.error(e)
            return r

        def test_asn(asn1, asn2):
            result = True
            try:
                if asn1['asn'] != str(asn2['asn']):
                    result = False
                    raise TestError("{} != {}".format(asn1['asn'], str(asn2['asn'])))

                if asn1['asnName'] != asn2['asnName']:
                    result = False
                    raise TestError("{} != {}".format(asn1['asnName'], asn2['asnName']))

                if asn1['organization'] != asn2['organization']:
                    result = False
                    raise TestError("{} != {}".format(asn1['organization'], asn2['organization']))

                if asn1['cone'] != asn2['cone']:
                    result = False
                    raise TestError("{} != {}".format(asn1['cone'], asn2['cone']))

                if asn1['country'] != asn2['country']:
                    result = False
                    raise TestError("{} != {}".format(asn1['country'], asn2['country']))

                if asn1['asnDegree'] != asn2['asnDegree']:
                    result = False
                    raise TestError("{} != {}".format(asn1['asnDegree'], asn2['asnDegree']))

            except TestError as e:
                logger.error(e)
            return result

        _asns = __grouping(l2)

        try:
            for edge in l1['edges']:
                asn1 = edge['node']
                asn2 = _asns[asn1['asn']]

                r = test_asn(asn1, asn2)
                if not r:
                    failed.append(asn1['asn'])
                processed += 1
        except TestError as e:
                logger.error(e)
        return processed

    def _build_asns(self):
        result = {}
        lines = list(TestUtil.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.asns.jsonl')))
        lines.sort(key=itemgetter('rank'))

        tc = 0
        for l in lines:
            if ('seen' in l and l['seen'] is True) and ('rank' in l and l['rank'] > 0):
                tc += 1
        result["totalCount"] = tc

        def build_orgs():
            _lines = list(TestUtil.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.organizations.jsonl')))
            _lines.sort(key=itemgetter('orgId'))
            _countries = self.__build_countries()
            orgs = {}
            for org in _lines:
                key = org['orgId']
                org['country'] = {"iso": None}
                _c = _countries.get(org['countryIso'], None)
                if _c and 'iso' in _c:
                    org['country']['iso'] = _c['iso'] if 'iso' in _c else None
                orgs[key] = org
            return orgs

        orgs = build_orgs()
        countries = self.__build_countries()
        result["edges"] = []

        for line in lines:
            org = orgs.get(line['orgId'], None) if 'orgId' in line else {}
            o = None
            if org:
                o = {}
                if 'orgId' in org:
                    o['orgId'] = org['orgId']
                if 'orgName' in org:
                    o['orgName'] = org['orgName']

            c = None
            if 'countryIso' in line:
                c = countries[line['countryIso']]
            elif 'world' in line:
                c = countries[line['world']]

            el = {
                "rank": line["rank"] if "rank" in line else 999999999999,
                "asn": line["asn"] if "asn" in line else "",
                "asnName": line["asnName"] if "asnName" in line else "",
                "organization": o,
                "country": {"iso": ""},
                "cone": None,
                "asnDegree": {"transit": None},
                "seen": line["seen"] if "seen" in line else None,
            }

            cn = {
                'numberAsns': int(line['cone']['numberAsns']) if "cone" in line and 'numberAsns' in line['cone'] else None,
                'numberPrefixes': int(line['cone']['numberPrefixes']) if "cone" in line and 'numberPrefixes' in line['cone'] else None,
                'numberAddresses': int(line['cone']['numberAddresses']) if "cone" in line and 'numberAddresses' in line['cone'] else None
            }
            el['cone'] = cn
            el['country']['iso'] = c['countryIso'] if c and 'countryIso' in c else ""
            el["asnDegree"]['transit'] = int(line["asnDegree"]['transit']) if "asnDegree" in line and 'transit' in line["asnDegree"] else None
            node = {"node": el}
            result['edges'].append(node)
        return result

    def __build_countries(self):
        lines = list(TestUtil.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.countries.jsonl')))
        lines.sort(key=itemgetter('countryIso'))
        r = {}
        for c in lines:
            key = c['countryIso']
            r[key] = c
        return r

    @staticmethod
    def pprint_json(json_text):
        try:
            js = json.dumps(json.loads(json_text), indent=2, sort_keys=True)
            print(highlight(js, PythonLexer(), TerminalFormatter()))
        except Exception as e:
            logger.error(e)

    @staticmethod
    def extract_last_data_dir():
        result = None
        subdirs = [x for x in os.listdir(TestUtil.source_path) if os.path.isdir(os.path.join(TestUtil.source_path, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
        return result, os.path.join(TestUtil.source_path, result)

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
    def extract_data_from_file(fn):
        try:
            with open(fn) as f:
                for line in f:
                    data = json.loads(line.strip())
                    yield data
        except IOError as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
            exit(1)
