# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)

ENV = "test"

# Loggging
logging.config.fileConfig('config/logging.ini')

OPTIONS = {
    'input_folder': '/www/data-import/data',
    'postgresql': {
        "dsn": "postgresql://{user}:{passw}@{host}:{port}/{name}",
        "user": "postgres",
        "password": "Aqpl308E",
        "host": "localhost",
        "port": "5434",
        "database": "asrank",
    },
    'ds': {
        'dataset': {
            'fields': ['hid', 'dataset_id', 'clique', 'asn_ixs', 'number_asns', 'number_organizes', 'number_prefixes', 'number_addresses',
                       'sources', 'asn_reserved_ranges', 'asn_assigned_ranges', 'address_family', 'country', 'type', 'date_from', 'date_to', 'ts']},
        'asns': {
            'fields': ['asn', 'name', 'source', 'org_id', 'org_name', 'country', 'country_name', 'latitude', 'longitude', 'rank',
                       'customer_cone_asns', 'customer_cone_prefixes', 'customer_cone_addresses', 'degree_peer', 'degree_global',
                       'degree_customer', 'degree_sibling', 'degree_transit', 'degree_provider', 'date_from', 'date_to', 'ts']},
        'orgs': {
            'fields': ['rank', 'org_id', 'org_name', 'country', 'country_name', 'asn_degree_global', 'asn_degree_transit',
                       'org_degree_global', 'org_degree_transit', 'customer_cone_asns', 'customer_cone_orgs', 'customer_cone_addresses', 'customer_cone_prefixes',
                       'number_members', 'number_members_ranked', 'members', 'date_from', 'date_to', 'ts']},
        'links': {
            'fields': ['asn0', 'asn1', 'relationship', 'locations', 'number_paths', 'date_from', 'date_to', 'ts']},

        'locations': {
            'fields': ['lid', 'city', 'country', 'continent', 'region', 'population', 'longitude', 'latitude', 'date_from', 'date_to', 'ts']}
    },
    'SQL_PATH': os.path.join(DIR_PATH, 'sql'),
    'logging': {
        'logger': logging.getLogger('asrank'),
        'color': {
            # Console Color
            'reset': '\033[0m',     # Reset
            'black': '\033[0;30m',  # Black
            'white': '\033[0;37m',  # White
            'red': '\033[0;91m',    # Red
            'green': '\033[0;92m',  # Green
            'yellow': '\033[0;93m', # Yellow
            'blue': '\033[0;34m',   # Blue
            'purple': '\033[0;95m', # Purple
            'cyan': '\033[0;96m',   # Cyan
        }
    }
}

if 'test' in ENV:
    OPTIONS['postgresql']['database'] = "asrank"
    OPTIONS['input_folder'] = '/media/efanchic/documents/asrank_data/dev'
    OPTIONS['rurl'] = 'http://127.0.0.1:8000/as_core/'


def getoptions(): return OPTIONS
