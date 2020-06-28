# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(__file__)
LOGS = os.path.join(os.path.dirname(BASE_PATH), 'logs')

DEBUG = True

OPTIONS = {
    'input_folder': '/www/data-import/data',
    'influxdb': {
        'host': '127.0.0.1',
        'port': '8086',
        'dbname': 'asrank',
        'user': 'asrank',
        'password': 'rankas'
    },
    'measurements': [
        'datasets',
        'asns',
        'orgs',
        'cones',
        'links',
        'locations',
        'relations'
    ],
    'fnames': {
            "asns": 'asns',
            "orgs": 'orgs',
            "cones": 'asn_cones',
            "datasets": 'dataset',
            "links": 'links',
            "relations": 'links',
            "locations": 'locations'

        },
    "dp": {
        'datasets': {
                'tags': ['dataset_id', 'ts'],
                'fields': ['ds_id_f', 'date', 'clique', 'sources', 'address_family',
                         'asn_ixes', 'asn_reserved_ranges', 'asn_assigned_ranges',
                         'number_asnes', 'number_organizes', 'number_prefixes', 'number_addresses']},

        'asns': {
            'tags': ['asn', 'ts'],
            'fields': ['asn_f', 'asn_name', 'rank', 'source', 'org_id', 'org_name', 'country', 'country_name', 'latitude', 'longitude',
                     'customer_cone_addresses', 'customer_cone_asns', 'customer_cone_prefixes',
                     'degree_peer', 'degree_global', 'degree_customer', 'degree_sibling', 'degree_transit', 'degree_provider']},

        'orgs': {
                'tags': ['org_id', 'ts'],
                'fields': ['org_id_f', 'org_name', 'rank', 'country', 'org_transit_degree', 'org_degree_global',
                         'asn_degree_transit', 'asn_degree_global',
                         'customer_cone_asns', 'customer_cone_orgs', 'customer_cone_addresses', 'customer_cone_prefixes',
                         'members', 'number_members', 'number_members_ranked']},

        'locations': {
                'tags': ['lid', 'ts'],
                'fields': ['lid_f', 'city', 'country', 'continent', 'region', 'population', 'latitude', 'longitude']},

        'links': {
                'tags': ['asn0', 'asn1', 'ts'],
                'fields': ['asn0_f', 'asn1_f', 'relationship', 'number_paths']},

        'cones': {
                'tags': ['asn', 'ts'],
                'fields': ['asn_f', 'cone', 'in_cone']},

        'relations': {
            'tags': ['asn0', 'asn1', 'ts'],
            'fields': ['asn0_f', 'asn1_f', 'rank', 'relationship','paths', 'source', 'name', 'locations',
                       'org0_id', 'org0_name', 'rank0',
                       'country', 'country_name', 'latitude', 'longitude', "org_id", 'org_name',
                       'cone_asns', 'cone_addresses', 'cone_prefixes',
                       'degree_customers', 'degree_peers', 'degree_globals', 'degree_transits']}
    }
}

if DEBUG:
    OPTIONS['input_folder'] = '/www/data-import/data'
    OPTIONS['influxdb']['dbname'] = 'asrankt'

# Loggging
logging.config.fileConfig(os.path.join(BASE_PATH, 'logging.ini'))

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
