# -*- coding: utf-8 -*-
import os
import sys
import json
import unittest
import requests
import argparse
from pygments import highlight
from collections import defaultdict
from operator import itemgetter
from config.config import getoptions
from pygments.lexers.python import PythonLexer
from pygments.formatters.terminal import TerminalFormatter

from testutil.testutil import TestUtil

options = getoptions()
logger = options['logging']['logger']
tlogger = options['logging']['logger2']
color = options['logging']['color']


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=False, type=str, default="", help="Path to folder with json data sources.")
    parser.add_argument("--compare", required=False, type=str, default='all', help="Compare objects argument.")
    parser.add_argument("--print", required=False, type=str, default='true', help="Print objects representation in console. Default 100 objects.")
    parser.add_argument("--u", required=False, type=str, default='', help="Graphql end point.")
    args = parser.parse_args()
    return vars(args)


class LoggerWriter:
    def __init__(self, logger):
        # self.level is really like using log.debug(message)
        # at least in my case
        self.logger = logger

    def write(self, message):
        # if statement reduces the amount of newlines that are
        # printed to the logger
        if message != '\n':
            self.logger.info(message)

    def flush(self):
        # create a flush method so things can be flushed when
        # the system wants to. Not sure if simply 'printing'
        # sys.stderr is the correct way to do it, but it seemed
        # to work properly for me.
        self.logger.error(sys.stderr)


class TestAPI(unittest.TestCase):
    source_path = options['input_folder']
    dest_path = options['output_folder']
    api_url = options['api_url']
    print_count = options.get('print_count', 100)
    max_top = options.get('pmax_top', 20)
    content = None

    def setUp(self):
        self.args = args_parser()
        self.subdir, self.dirpath = TestAPI.extract_last_data_dir()
        if 'u' in self.args and self.args['u']:
            self.api_url = self.args['u']

    def test_dataset(self):
        q = """{dataset{datasetId,modifiedAt,date,ipVersion,sources{url,date,name},
            numberAddresses,numberPrefixes,numberAsns,numberAsnsSeen,numberOrganizations,numberOrganizationsSeen}}
            """
        print("{}START TEST DATASET ...{}".format(color['cyan'], color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text
        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])

            if 'data' in data and 'dataset' in data['data']:
                dataset = self._build_dataset()
                if dataset:
                    self.compare_dataset(data['data']['dataset'], dataset)
                if dataset and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_jsond(dataset, 'dataset.json')
        print("{}END TEST DATASET ...{}".format(color['cyan'], color['reset']))
        print("-" * 40 + '\n')

    def compare_dataset(self, o1, o2):
        if o1 and o2:
            self.assertEqual(o1['datasetId'], o2['datasetId'])
            self.assertEqual(o1['ipVersion'], o2['ipVersion'])
            self.assertEqual(o1['numberAddresses'], o2['numberAddresses'])
            self.assertEqual(o1['numberPrefixes'], o2['numberPrefixes'])
            self.assertEqual(o1['numberAsns'], o2['numberAsns'])
            self.assertEqual(o1['numberAsnsSeen'], int(o2['numberAsnsSeen']))
            self.assertEqual(o1['numberOrganizations'], o2['numberOrganizations'])
            self.assertEqual(o1['numberOrganizationsSeen'], int(o2['numberOrganizationsSeen']))
            self.assertListEqual(o1['sources'], o2['sources'])

    def test_asn(self):
        q = """{asns(name:"LEVEL3",first:10,offset:0,sort:"rank",seen:true){totalCount,edges{node{rank,asn,asnName,
            organization{orgId,orgName}}}}}
            """
        print("{}START TEST ASN ...{}".format(color['cyan'], color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text
        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])

            if 'data' in data and 'asns' in data['data']:
                asns = self._build_asns()
                self.compare_asn(data['data']['asns'], asns)
                if asns and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_jsona(asns, 'asn.json')
        print("{}END TEST ASN ...{}".format(color['cyan'], color['reset']))
        print("-" * 40 + '\n')

    def compare_asn(self, o1, o2):
        def __grouping(name, asns):
            r = {}
            try:
                if asns:
                    for asn in asns['edges']:
                        node = asn['node']
                        if (node['asnName'] and name.lower() in node['asnName'].lower()) or \
                                (node['organization']['orgName'] and name.lower() in node['organization']['orgName'].lower()):
                            r[str(node['asn'])] = node
            except Exception as e:
                logger.error(e)
            return r

        count = 0
        _asns = __grouping("LEVEL3", o2)

        for edge in o1['edges']:
            if count > self.max_top: break
            asn1 = edge['node']
            asn2 = _asns[asn1['asn']]
            self.assertEqual(asn1['asn'], str(asn2['asn']))
            self.assertEqual(asn1['rank'], asn2['rank'])
            self.assertEqual(asn1['asnName'], str(asn2['asnName']))
            self.assertDictEqual(asn1['organization'], asn2['organization'])

    def test_asns(self):
        q = """{asns(first:10,offset:0,sort:"rank",seen:true){totalCount,edges{node{rank,asn,asnName,
            organization{orgId,orgName},
            country{iso},
            cone{numberAsns,numberPrefixes,numberAddresses},
            asnDegree {transit}}}}}
            """
        print("{}START TEST ASNS ...{}".format(color['cyan'],color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text
        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])
            if 'data' in data and 'asns' in data['data'] and 'totalCount' in data['data']['asns']:
                asns = self._build_asns()
                self.compare_asns(data['data']['asns'], asns)
                if asns and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_json(asns, 'asns.json')
        print("{}END TEST ASNS ...{}".format(color['cyan'],color['reset']))
        print("-"*40 + '\n')

    def compare_asns(self, o1, o2):
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
        _asns = __grouping(o2)
        count = 0
        for edge in o1['edges']:
            if count > self.max_top: break
            asn1 = edge['node']
            asn2 = _asns[asn1['asn']]
            self.assertEqual(asn1['asn'], str(asn2['asn']))
            self.assertEqual(asn1['asnName'], str(asn2['asnName']))
            self.assertDictEqual(asn1['organization'], asn2['organization'])
            self.assertDictEqual(asn1['cone'], asn2['cone'])
            self.assertDictEqual(asn1['country'], asn2['country'])
            self.assertDictEqual(asn1['asnDegree'], asn2['asnDegree'])

    def test_org(self):
        q = """{organization(orgId:"LPL-141-ARIN"){members{numberAsns,numberAsnsSeen,asns(first:10,offset:0,sort:"rank",seen:true){
            edges{node{rank,asn,asnName,organization{orgId,orgName},country{iso},cone{numberAsns,numberPrefixes,numberAddresses},asnDegree{transit}}}}}}}
           """
        print("{}START TEST ORG ...{}".format(color['cyan'], color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text
        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])

            if 'data' in data and 'organization' in data['data']:
                org = self._build_org('LPL-141-ARIN')
                self.compare_org(data['data']['organization'], org)
                if org and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_json(org, 'org.json')
        print("{}END TEST ORG ...{}".format(color['cyan'], color['reset']))
        print("-" * 40 + '\n')

    def compare_org(self, o1, o2):
        m1 = o1['members']
        m2 = o2['members']
        self.assertEqual(m1['numberAsns'], m2['numberAsns'])
        self.assertEqual(m1['numberAsnsSeen'], m2['numberAsnsSeen'])

    def test_orgs(self):
        q = """{organizations(first:10,offset:0,sort:"rank",seen:true){totalCount,edges{node{rank,orgId,orgName,country{iso},
            cone{numberAsns,numberPrefixes,numberAddresses},
            asnDegree{transit},
            members{numberAsnsSeen}}}}}
            """
        print("{}START TEST ORGS ...{}".format(color['cyan'],color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text

        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])
            if 'data' in data and 'organizations' in data['data'] and 'totalCount' in data['data']['organizations']:
                orgs = self._build_orgs()
                self.assertEqual(data['data']['organizations']['totalCount'], orgs['totalCount'])
                self.compare_orgs(data['data']['organizations'], orgs)

                if orgs and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_json(orgs, 'orgs.json')

        print("{}END TEST ORGS ...{}".format(color['cyan'],color['reset']))
        print("-"*40 + '\n')

    def compare_orgs(self, o1, o2):
        def __grouping(elems):
            r = defaultdict(dict)
            try:
                if elems:
                    for el in elems['edges']:
                        node = el['node']
                        aid = str(node['orgId'])
                        if aid:
                            r[aid] = node
            except Exception as e:
                logger.error(e)
            return r

        _orgs = __grouping(o2)
        count = 0
        for edge in o1['edges']:
            if count > self.max_top: break
            org1 = edge['node']
            org2 = _orgs[org1['orgId']]
            self.assertEqual(org1['orgId'], org2['orgId'])
            self.assertEqual(org1['orgName'], org2['orgName'])
            self.assertDictEqual(org1['cone'], org2['cone'])
            self.assertDictEqual(org1['country'], org2['country'])
            self.assertDictEqual(org1['asnDegree'], org2['asnDegree'])
            self.assertDictEqual(org1['members'], org2['members'])

    def test_links(self):
        q = """{asnLinks(asn:"3356",first:40,offset:0,sort:"rank"){totalCount,edges{node{numberPaths,relationship,
            asn1{asn,asnName,rank,cone{numberAsns},organization{orgId,orgName},country{iso}}}}}}
            """
        print("{}START TEST LINKS ...{}".format(color['cyan'], color['reset']))
        response = requests.get(options['api_url'], params={"query": q})
        content = response.text

        if content:
            data = response.json()
            if 'errors' in data:
                self.assertNotIn('errors', data, msg=data['errors'][0]['message'])

            if 'data' in data and 'asnLinks' in data['data'] and 'totalCount' in data['data']['asnLinks']:
                links = self._build_links(3356)
                self.assertEqual(data['data']['asnLinks']['totalCount'], links['totalCount'])
                self.compare_links(data['data']['asnLinks'], links, 3356)
                if links and 'print' in self.args and 'true' in self.args['print']:
                    self.pprint_jsonl(links, 'links.json')

        print("{}END TEST LINKS ...{}".format(color['cyan'], color['reset']))
        print("-" * 40 + '\n')

    def compare_links(self, o1, o2, asn0):
        def __grouping(elems):
            r = defaultdict(dict)
            try:
                if elems:
                    for el in elems['edges']:
                        node = el['node']
                        aid = str(node['asn0']['asn']) + "_" + str(node['asn1']['asn'])
                        if aid:
                            r[aid] = node
            except Exception as e:
                logger.error(e)
            return r

        _links = __grouping(o2)
        count = 0
        for edge in o1['edges']:
            if count > self.max_top: break
            e1 = edge['node']
            k = str(asn0) + "_" + str(e1['asn1']['asn'])
            e2 = _links[k]
            self.assertEqual(e1['numberPaths'], e2['numberPaths'])
            self.assertEqual(e1['relationship'], e2['relationship'])
            self.assertDictEqual(e1['asn1']['cone'], e2['asn1']['cone'])
            count += 1

    def _build_dataset(self):
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.dataset.jsonl')))
        return lines[0] if len(lines) > 0 else None

    def _build_asns(self):
        result = {}
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.asns.jsonl')))
        lines.sort(key=itemgetter('rank'))

        tc = 0
        for l in lines:
            if ('seen' in l and l['seen'] is True) and ('rank' in l and l['rank'] > 0):
                tc += 1
        result["totalCount"] = tc

        def build_orgs():
            _lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.organizations.jsonl')))
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
            o = {
                'orgId': org['orgId'] if 'orgId' in org else None,
                'orgName': org['orgName'] if 'orgName' in org else None
            }
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
                "country": {"iso": None},
                "cone": line["cone"] if "cone" in line else None,
                "asnDegree": {"transit": None},
                "seen": line["seen"] if "seen" in line else None,
            }
            el['country']['iso'] = c['countryIso'] if c and 'countryIso' in c else None
            el["asnDegree"]['transit'] = line["asnDegree"]['transit'] if "asnDegree" in line and 'transit' in line["asnDegree"] else None
            node = {"node": el}
            result['edges'].append(node)
        return result

    def _build_orgs(self):
        result = {}
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.organizations.jsonl')))
        lines.sort(key=itemgetter('rank'))

        result["totalCount"] = len(lines)
        result["edges"] = []

        countries = self.__build_countries()

        for line in lines:
            c = None
            if 'countryIso' in line:
                c = countries[line['countryIso']]
            elif 'world' in line:
                c = countries[line['world']]

            el = {
                "rank": line["rank"] if "rank" in line else 999999999999,
                "orgId": line["orgId"] if "orgId" in line else "",
                "orgName": line["orgName"] if "orgName" in line else "",
                "country": {"iso": None},
                "cone": line["cone"] if "cone" in line else None,
                "asnDegree": {"transit": None},
                "members": {"numberAsnsSeen": None}
            }
            m = line['members']['numberAsnsSeen'] if 'members' in line and 'numberAsnsSeen' in line['members'] else None
            el["members"]['numberAsnsSeen'] = m
            el['country']['iso'] = c['countryIso'] if c and 'countryIso' in c else None
            el["asnDegree"]['transit'] = line["asnDegree"]['transit'] if "asnDegree" in line and 'transit' in line["asnDegree"] else None
            node = {"node": el}
            result['edges'].append(node)
        return result

    def _build_links(self, asn):
        result = {}
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.asnLinks.jsonl')))

        result["totalCount"] = 0
        result["edges"] = []

        a = self._build_asns()

        def __bds(a):
            nodes = {}
            for edge in a['edges']:
                key = edge['node']['asn']
                nodes[key] = edge['node']
            return nodes

        asns = __bds(a)
        links = []
        for line in lines:
            asn0 = line['asn0']
            asn1 = line['asn1']
            if asn0 == asn or asn1 == asn:
                el = {
                    "numberPaths": line["numberPaths"] if "numberPaths" in line else 0,
                    "relationship": line["relationship"] if "relationship" in line else "",
                    "asn0": asns[line['asn0']] if 'asn0' in line else None,
                    "asn1": asns[line['asn1']] if 'asn1' in line else None
                }
                links.append(el)

        lss = self.grouping_links_by_rank(links)
        ls = self._rebuild_links(lss, '3356')
        ls.sort(key=itemgetter('relationship'), reverse=True)
        ls.sort(key=itemgetter('rank'))
        result["totalCount"] = len(ls)
        k = []
        for l in ls:
            k.append(l['asn1'])
            node = {"node": l}
            result['edges'].append(node)

        return result

    def _build_org(self, orgid):
        result = {"members": None}
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.organizations.jsonl')))
        orgs = self.grouping_orgs_by_id(lines)
        a = self._build_asns()

        def __bds(a):
            nodes = {}
            for edge in a['edges']:
                key = edge['node']['asn']
                nodes[key] = edge['node']
            return nodes

        asns = __bds(a)

        if orgid:
            _org = orgs[orgid.lower()]
            if _org:
                _members = _org['members']
                m = {
                   "numberAsns": _members["numberAsns"] if "numberAsns" in _members else None,
                   "numberAsnsSeen": _members["numberAsnsSeen"] if "numberAsnsSeen" in _members else None,
                }
                _as = _members['asns']
                edges = []
                for _a in _as:
                    a = asns[_a] if _a in asns else None
                    if a and 'seen' in a and a['seen'] is True:
                        edges.append(a)
                if len(edges) > self.print_count:
                    edges = edges[0:self.print_count]
                m['asns'] = {"edges": edges}
                result['members'] = m
        return result

    def __build_countries(self):
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.countries.jsonl')))
        lines.sort(key=itemgetter('countryIso'))
        r = {}
        for c in lines:
            key = c['countryIso']
            r[key] = c
        return r

    def __build_orgs(self):
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.organizations.jsonl')))
        lines.sort(key=itemgetter('orgId'))
        r = {}
        for c in lines:
            key = c['orgId']
            r[key] = c
        return r

    def __build_asns(self):
        lines = list(TestAPI.extract_data_from_file(os.path.join(self.dirpath, self.subdir + '.asns.jsonl')))
        lines.sort(key=itemgetter('asn'))
        r = {}
        for c in lines:
            key = c['asn']
            r[key] = c
        return r


    @staticmethod
    def _rebuild_links(links, asn):
        result = []
        for value in links:
            try:
                if value['asn1']['asn'] == int(asn):
                    value['asn0'], value['asn1'] = value['asn1'], value['asn0']
                    if 'provider' in value['relationship']:
                        value['relationship'] = 'customer'
                    if 'customer' in value['relationship']:
                        value['customer'] = 'provider'
                if 'cone' in value['asn1'] and value['asn1']['cone'] and 'numberAddresses' in value['asn1']['cone']:
                    del value['asn1']['cone']['numberAddresses']
                if 'cone' in value['asn1'] and value['asn1']['cone'] and 'numberPrefixes' in value['asn1']['cone']:
                    del value['asn1']['cone']['numberPrefixes']
                result.append(value)
            except Exception as e:
                print(e)
        return result

    @staticmethod
    def grouping_asns_by_id(asns):
        result = defaultdict(dict)
        try:
            if asns:
                for asn in asns:
                    aid = str(asn['asn'])
                    if aid:
                        result[aid] = asn
        except Exception as e:
            logger.error(e)
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
    def grouping_links_by_rank(links):
        result = []

        def sorter(link):
            r0 = int(link['asn0']["rank"]) if 'asn0' in link and "rank" in link['asn0'] else 999999999
            r1 = int(link['asn1']["rank"]) if 'asn1' in link and "rank" in link['asn1'] else 999999999
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
    def pprint_json(elems, file):
        try:
            if 'edges' in elems:
                if len(elems['edges']) > TestAPI.print_count:
                    ee = elems['edges'][0:TestAPI.print_count]
                    elems['edges'] = ee
            with open(os.path.join(TestAPI.dest_path, file), 'w') as fp:
                json.dump(elems, fp)
            js = json.dumps(elems, indent=2, sort_keys=True)
            print(highlight(js, PythonLexer(), TerminalFormatter()))
        except Exception as e:
            logger.error(e)

    @staticmethod
    def pprint_jsona(rows, file):
        def __grouping(name, asns):
            r = {"totalCount": 0, "edges": []}
            try:
                if asns:
                    for asn in asns['edges']:
                        node = asn['node']
                        if (node['asnName'] and name.lower() in node['asnName'].lower()) or \
                                (node['organization']['orgName'] and name.lower() in node['organization']['orgName'].lower()):
                            r['edges'].append(node)
                r['totalCount'] = len(r['edges'])
            except Exception as e:
                logger.error(e)
            return r

        elems = __grouping("LEVEL3", rows)

        try:
            if 'edges' in elems:
                if len(elems['edges']) > TestAPI.print_count:
                    ee = elems['edges'][0:TestAPI.print_count]
                    for node in ee:
                        e = node['node']
                        if 'country' in e:
                            del e['country']
                        if 'cone' in e:
                            del e['cone']
                        if 'asnDegree' in e:
                            del e['asnDegree']
                        if 'seen' in e:
                            del e['seen']
                    elems['edges'] = ee
            with open(os.path.join(TestAPI.dest_path, file), 'w') as fp:
                json.dump(elems, fp)
            js = json.dumps(elems, indent=2, sort_keys=True)
            print(highlight(js, PythonLexer(), TerminalFormatter()))
        except Exception as e:
            logger.error(e)

    @staticmethod
    def pprint_jsonl(elems, file):
        try:
            if 'edges' in elems:
                if len(elems['edges']) > TestAPI.print_count:
                    ee = elems['edges'][0:TestAPI.print_count]
                    for node in ee:
                        e = node['node']
                        if 'asn0' in e:
                            del e['asn0']
                        if 'rank' in e:
                            del e['rank']
                        if 'asn1' in e and 'asnDegree' in e['asn1']:
                            del e['asn1']['asnDegree']
                        if 'asn1' in e and 'seen' in e['asn1']:
                            del e['asn1']['seen']

                    elems['edges'] = ee
            with open(os.path.join(TestAPI.dest_path, file), 'w') as fp:
                json.dump(elems, fp)
            js = json.dumps(elems, indent=2, sort_keys=True)
            print(highlight(js, PythonLexer(), TerminalFormatter()))
        except Exception as e:
            logger.error(e)

    @staticmethod
    def pprint_jsond(elem, file):
        try:
            if 'clique' in elem:
                del elem['clique']
            if 'asnIxs' in elem:
                del elem['asnIxs']
            if 'countryIso' in elem:
                del elem['countryIso']
            if 'asnReservedRanges' in elem:
                del elem['asnReservedRanges']
            if 'asnAssignedRanges' in elem:
                del elem['asnAssignedRanges']

            with open(os.path.join(TestAPI.dest_path, file), 'w') as fp:
                json.dump(elem, fp)
            js = json.dumps(elem, indent=2, sort_keys=True)
            print(highlight(js, PythonLexer(), TerminalFormatter()))
        except Exception as e:
            logger.error(e)

    @staticmethod
    def extract_last_data_dir():
        result = None
        subdirs = [x for x in os.listdir(TestAPI.source_path) if os.path.isdir(os.path.join(TestAPI.source_path, x))]
        if subdirs:
            if len(subdirs) > 1:
                result = max(subdirs)
            else:
                result = subdirs[0]
        return result, os.path.join(TestAPI.source_path, result)

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


if __name__ == '__main__':
    try:
        args = args_parser()
        suite = unittest.TestSuite()
        if 'dataset' == args['compare']:
            suite.addTest(TestAPI("test_dataset"))
        elif 'asn' == args['compare']:
            suite.addTest(TestAPI("test_asn"))
        elif 'asns' == args['compare']:
            #suite.addTest(TestAPI("test_asns"))
            TestUtil(args_parser()).start()
        elif 'org' == args['compare']:
            suite.addTest(TestAPI("test_org"))
        elif 'orgs' == args['compare']:
            suite.addTest(TestAPI("test_orgs"))
        elif 'links' == args['compare']:
            suite.addTest(TestAPI("test_links"))
        else:
            suite.addTest(TestAPI("test_dataset"))
            suite.addTest(TestAPI("test_asn"))
            #suite.addTest(TestAPI("test_asns"))
            TestUtil(args_parser()).start()
            suite.addTest(TestAPI("test_org"))
            suite.addTest(TestAPI("test_orgs"))
            suite.addTest(TestAPI("test_links"))
        result = unittest.TextTestRunner().run(suite)

    except (Exception, SystemExit) as e:
        print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
        logger.error(e)
        exit(1)
