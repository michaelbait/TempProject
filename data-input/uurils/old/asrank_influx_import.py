# -*- coding: utf-8 -*-
from pathlib import Path

import sys
import os
import re
import glob
import json
import getopt
from datetime import datetime, date, time, timedelta
from influxdb import InfluxDBClient
from collections import defaultdict


def resolve_file(subdir, name):
    result = None
    if subdir and name:
        fn = '{dir}.{name}.{ext}'.format(dir=subdir, name=name, ext='jsonl')
        file = os.path.join(self.source_path, subdir, fn)
        if validate_source_path(file):
            result = file
    return result


def validate_source_path(source):
    result = False
    try:
        if source and \
                os.path.exists(source) and \
                os.access(source, os.R_OK):
            result = True
        if not result:
            raise IOError("Cannot access directory <{}>".format(source))
    except IOError as e:
        logger.error(e)
        exit(1)
    return result


def extract_data_from_file_gen(subdir, fname):
    fn = resolve_file(subdir, fname)
    if validate_source_path(fn):
        with open(fn) as f:
            for line in f:
                data = json.loads(line.strip())
                yield data


def process_relations():
    asn_processed = set()
    asnsm = {}
    for asn in extract_data_from_file_gen(subdir, "asns"):
        asnid = int(asn['asn'])
        if asnid not in asnsm.keys():
            asnsm[asnid] = asn
    elems = []
    for org in extract_data_from_file_gen(subdir, "orgs"):
        oid = int(org['org_id'])
        if oid in asnsm.keys():
            al = asnsm[oid]
            el = {}

    # orgfn = datapath + "/" + timestamp + ".orgs.jsonl"
    # ORG = open_file(orgfn)
    # for line in ORG:
    #     org_obj = json.loads(line.strip())
    #     org_obj['oid'] = org_obj['org_id']
    #     entry = create_entry("org_info_v4", influx_t, org_obj, org_info_v4_tags, org_info_v4_fields)
    #     # print entry
    #     json_body.append(entry)
    # ORG.close()
    # # print "number of points",len(json_body)
    # batch_write(json_body, client)

def create_entry(meas_name, timestamp, raw_json_obj, tags_list, fields_list):
    entry = {
        "measurement": meas_name,
        "time": timestamp,
        "tags": {},
        "fields": {},
    }

    # populate tags
    for t in tags_list:
        if t in raw_json_obj:
            entry['tags'][t] = raw_json_obj[t]
        else:
            entry['tags'][t] = ""

    # populate fields
    for f in fields_list:
        if f in raw_json_obj:
            # print f,type(raw_json_obj[f])
            if type(raw_json_obj[f]) is dict or type(raw_json_obj[f]) is list:
                entry['fields'][f] = json.dumps(raw_json_obj[f])
            else:
                if f is 'longitude' or f is 'latitude':
                    entry['fields'][f] = float(raw_json_obj[f])
                elif f is 'region':
                    entry['fields'][f] = str(raw_json_obj[f])
                elif f is 'degree_global':
                    entry['fields'][f] = int(raw_json_obj[f])
                else:
                    entry['fields'][f] = raw_json_obj[f]

    return entry


def open_file(filename):
    try:
        return open(filename, "r")
    except IOError as e:
        sys.stderr.write(e)
        exit(1)


def batch_write(points_list, client):
    # print points_list[0]
    count = 0
    write_list = []
    while count < len(points_list):
        write_list.append(points_list[count])
        count += 1
        if count % 5000 == 0:
            client.write_points(write_list)
            # print "hit mod 0",len(write_list)
            write_list = []

    # print "remnant",len(write_list)
    client.write_points(write_list)


def main():
    path = '/media/documents/asrank_data/'
    client = InfluxDBClient('127.0.0.1', 8086, 'asrankuser', 'rankas', 'asrank', ssl=False, verify_ssl=False)

    for root, subdirs, files in os.walk(path):
        for subdir in subdirs:
            print("Process <{}>".format(subdir))
            timestamp = subdir
            DEBUG = False
            monfn = None

            try:
                opts, args = getopt.getopt(sys.argv[1:], "hdt:", ["help", "debug", "timestamp"])
            except getopt.GetoptError as err:
                print(err)
                sys.exit(1)

            for o, a in opts:
                if o in ("-h", "--help"):
                    #usage()
                    sys.exit(2)
                elif o in ("-d", "--debug"):
                    DEBUG = 1
                elif o in ("-t", "--timestamp"):
                    timestamp = a
                else:
                    assert False, "unhandled option"

                datapath = path + timestamp

                d = datetime.strptime(timestamp, "%Y%m%d")
                influx_t = int((d - datetime(1970, 1, 1)).total_seconds()) * 1000000000

                dataset_info_v4_tags = []
                dataset_info_v4_fields = ['dataset_id', 'date', 'clique', 'number_addresses', 'sources', 'asn_ixes',
                                          'asn_reserved_ranges', 'asn_assigned_ranges', 'number_asnes', 'address_family',
                                          'number_organizes', 'number_prefixes', 'number_addresses']

                asn_info_v4_tags = ['asn', 'org_id']
                asn_info_v4_fields = ['asn_name', 'org_name', 'source', 'latitude', 'longitude',
                                      'country', 'rank', 'customer_cone_addresses', 'customer_cone_asnes',
                                      'customer_cone_prefixes', 'degree_transit', 'degree_provider',
                                      'degree_sibling', 'degree_customer', 'degree_peer', 'degree_global']

                asn_rel_v4_tags = ['asn', 'org_id', 'neighbor']
                asn_rel_v4_fields = ['asnf', 'relationship', 'neighbor_rank', 'number_paths', 'locations', 'ts']

                asn_cone_v4_tags = ['asn']
                asn_cone_v4_fields = ['cone', 'in_cone']

                org_info_v4_tags = ['org_id']
                org_info_v4_fields = ['oid', 'org_name', 'country', 'members', 'number_members', 'rank',
                                      'customer_cone_asnes', 'customer_cone_orgs', 'customer_cone_addresses',
                                      'customer_cone_prefixes', 'org_transit_degree', 'org_degree_global',
                                      'asn_degree_transit', 'asn_degree_global']

                locations_tags = ['lid']
                locations_fields = ['city', 'country', 'region', 'continent', 'latitude', 'longitude', 'population']

                # dicts to store the org and rank of each ASN
                as2org = {}
                as2rank = {}

                json_body = []

                datasetfn = datapath + "/" + timestamp + ".dataset.jsonl"
                DATASET = open_file(datasetfn)
                for line in DATASET:
                    dataset_obj = json.loads(line.strip())
                    entry = create_entry("dataset_info_v4", influx_t, dataset_obj, dataset_info_v4_tags, dataset_info_v4_fields)
                    json_body.append(entry)
                    # print entry
                DATASET.close()
                # print "number of points",len(json_body)
                batch_write(json_body, client)
                json_body = []

                asnfn = datapath + "/" + timestamp + ".asns.jsonl"
                ASN = open_file(asnfn)
                for line in ASN:
                    asn_obj = json.loads(line.strip())
                    entry = create_entry("asn_info_v4", influx_t, asn_obj, asn_info_v4_tags, asn_info_v4_fields)

                    # populate info that will be required by other structures
                    t_asn = asn_obj['asn']
                    as2org[t_asn] = asn_obj['org_id'] if 'org_id' in asn_obj else ""
                    as2rank[t_asn] = asn_obj['rank'] if 'rank' in asn_obj else None

                    json_body.append(entry)
                    # print entry
                ASN.close()
                # print "number of points",len(json_body)
                batch_write(json_body, client)
                json_body = []

                orgfn = datapath + "/" + timestamp + ".orgs.jsonl"
                ORG = open_file(orgfn)
                for line in ORG:
                    org_obj = json.loads(line.strip())
                    org_obj['oid'] = org_obj['org_id']
                    entry = create_entry("org_info_v4", influx_t, org_obj, org_info_v4_tags, org_info_v4_fields)
                    # print entry
                    json_body.append(entry)
                ORG.close()
                # print "number of points",len(json_body)
                batch_write(json_body, client)
                json_body = []

                locfn = datapath + "/" + timestamp + ".locations.jsonl"
                LOC = open_file(locfn)
                for line in LOC:
                    loc_obj = json.loads(line.strip())
                    entry = create_entry("locations", influx_t, loc_obj, locations_tags, locations_fields)
                    # print entry
                    json_body.append(entry)
                LOC.close()
                # print "number of points",len(json_body)
                batch_write(json_body, client)
                json_body = []

                conefn = datapath + "/" + timestamp + ".asn_cones.jsonl"
                cone = {}
                in_cone = defaultdict(list)
                CONES = open_file(conefn)
                for line in CONES:
                    cone_obj = json.loads(line.strip())
                    t_asn = cone_obj['asn']
                    cone[t_asn] = cone_obj['cone']
                    for a in cone_obj['cone']:
                        in_cone[a].append(t_asn)
                CONES.close()

                for a in cone:
                    cone_obj = {'cone': cone[a],
                                'in_cone': in_cone[a],
                                'asn': a}
                    entry = create_entry("asn_cone_v4", influx_t, cone_obj, asn_cone_v4_tags, asn_cone_v4_fields)
                    json_body.append(entry)
                    # print entry
                # print "number of points",len(json_body)
                batch_write(json_body, client)
                json_body = []

                # asn_rel_v4 has to be built using the links.jsonl file and
                # info from the asns.jsonl file
                relfn = datapath + "/" + timestamp + ".links.jsonl"
                REL = open_file(relfn)
                for line in REL:
                    rel_obj = json.loads(line.strip())
                    rel_obj['org_id'] = as2org[int(rel_obj['asn0'])]
                    rel_obj['neighbor'] = rel_obj['asn1']
                    rel_obj['neighbor_rank'] = as2rank[int(rel_obj['asn1'])]
                    rel_obj['asn'] = rel_obj['asn0']
                    rel_obj['asnf'] = rel_obj['asn0']
                    rel_obj['ts'] = datetime.now().timestamp()
                    entry = create_entry("asn_rel_v4", influx_t, rel_obj, asn_rel_v4_tags, asn_rel_v4_fields)
                    json_body.append(entry)
                    # print entry
                REL.close()
                # print "number of points",len(json_body)
                batch_write(json_body, client)


if __name__ == "__main__":
    main()
