# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)
LOGS = os.path.join(BASE_PATH, 'logs')

DEBUG = True

OPTIONS = {
    'input_folder': '/www/as-rank-2/data',
    'influxdb': {
        'host': '127.0.0.1',
        'port': '8086',
        'dbname': 'asrankw',
        'user': 'asrank',
        'password': 'rankas',
    },
    'postgresql': {
        "user": "postgres",
        "password": "Aqpl308E",
        "host": "localhost",
        "port": "5432",
        "database": "asrankw",
        "sql": ""
    },
    'asns_measurements': [
            'asns.rank.asc',
            'asns.rank.desc',
            'asns.customer_cone_asns.asc',
            'asns.customer_cone_asns.desc',
            'asns.customer_cone_addresses.asc',
            'asns.customer_cone_addresses.desc',
            'asns.customer_cone_prefixes.asc',
            'asns.customer_cone_prefixes.desc',
            'asns.degree_transit.asc',
            'asns.degree_transit.desc',
    ],
    'orgs_measurements': [
        'orgs.rank.asc',
        'orgs.rank.desc',
        'orgs.customer_cone_asns.asc',
        'orgs.customer_cone_asns.desc',
        'orgs.customer_cone_addresses.asc',
        'orgs.customer_cone_addresses.desc',
        'orgs.customer_cone_prefixes.asc',
        'orgs.customer_cone_prefixes.desc',
        'orgs.degree_transit.asc',
        'orgs.degree_transit.desc',
    ],
    "fnames": {
        "asns": 'asns',
        "orgs": 'orgs',
        "cones": 'asn_cones',
        "datasets": 'dataset',
        "links": 'links',
        "relations": 'links',
        "locations": 'locations',

    },
    "dp": {
        'datasets': {
            'tags': ['ds_id'],
            'fields': ['ds_id_f', 'dataset_id', 'date', 'clique', 'sources', 'address_family',
                     'asn_ixes', 'asn_reserved_ranges', 'asn_assigned_ranges',
                     'number_asns', 'number_organizes', 'number_prefixes', 'number_addresses']
        },
        'orgs': {
            'tags': ['org_id'],
            'fields': ['org_id_f', 'org_name', 'rank', 'country', 'country_name', 'org_transit_degree', 'org_degree_global',
                     'degree_transit', 'degree_global', 'customer_cone_asns', 'customer_cone_orgs', 'customer_cone_addresses', 'customer_cone_prefixes',
                     'members', 'number_members', 'number_members_ranked']
        },
        'asns': {
            'tags': ['id', 'asn'],
            'fields': ['asn_f', 'ts', 'asn_name', 'rank', 'source', 'org_id', 'org_name', 'country', 'country_name', 'latitude', 'longitude',
                     'customer_cone_addresses', 'customer_cone_asns', 'customer_cone_prefixes',
                     'degree_peer', 'degree_global', 'degree_customer', 'degree_sibling', 'degree_transit', 'degree_provider']
        },
        'locations': {
            'tags': ['lid'],
            'fields': ['lid_f', 'city', 'country', 'continent', 'region', 'population', 'latitude', 'longitude']

        },
        'links': {
            'tags': ['asn0', 'asn1'],
            'fields': ['asn0_f', 'asn1_f','relationship', 'locations', 'number_paths']
        },
        'relations': {
            'tags': ['asn0', 'asn1'],
            'fields': ['asn0_f', 'asn1_f', 'rank0', 'rank1', 'relationship', 'paths', 'source', 'name', 'locations',
                     'country0', 'country1', 'country_name', 'latitude', 'longitude', "org0_id", 'org0_name', "org1_id", 'org1_name',
                     'cone_asns0', 'cone_asns1', 'cone_addresses', 'cone_prefixes',
                     'degree_customers', 'degree_peers', 'degree_globals', 'degree_transits', 'degree_providers']
        },
        'counts': {
            'tags': ['cid'],
            'fields': ["datasets", "asns", "orgs", "cones", "locations", "links", "relations"]

        }
    }
}

if DEBUG:
    OPTIONS['input_folder'] = '/media/efanchic/documents/asrank_data/dev'
    OPTIONS['influxdb']['dbname'] = 'asrankw'
    OPTIONS['rurl'] = 'http://127.0.0.1:8000/as_core/'

# Loggging
logging.config.fileConfig('config/logging.ini')
logger = logging.getLogger('asrankw')

# Console Color
reset = '\033[0m'  # Reset
black = '\033[0;30m'  # Black
white = '\033[0;37m'  # White
red = '\033[0;91m'  # Red
green = '\033[0;92m'  # Green
yellow = '\033[0;93m'  # Yellow
blue = '\033[0;34m'  # Blue
purple = '\033[0;95m'  # Purple
cyan = '\033[0;96m'  # Cyan

with open(os.path.join(DIR_PATH, 'db.sql'), 'r') as sql:
    OPTIONS['postgresql']['sql'] = sql.read()


def getoptions(): return OPTIONS
