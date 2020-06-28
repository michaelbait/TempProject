# -*- coding: utf-8 -*-

import os
import logging.config

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DIR_PATH = os.path.dirname(__file__)

ENV = "dev"

# Loggging
logging.config.fileConfig('config/logging.ini')

OPTIONS = {
    'input_folder': '/home/baitaluk/Projects/Historic_ipv',
    'postgresql': {
        "dsn": "postgresql://{user}:{passw}@{host}:{port}/{name}",
        #"user": "postgres",
        #"password": "asrank",
        #"host": "192.172.226.57",
        #"port": "5433",
        #"database": "asrankq"
        "user": "asrank_rw",
        "password": "veiTahn8",
        "host": "clayface.caida.org",
        "port": "5432",
        "database": "asrank_test_B",
    },
    'ds': {
        'dataset': {
            'fields': ['dataset_id', 'ip_version', 'modified_at', 'country', 'ip_version',
                       'number_addresses',  'number_prefixes',  'number_asns',  'number_seen',  'number_organizations', 'number_organizations_seen',
                       'asn_reserved_ranges', 'asn_assigned_ranges', 'clique', 'asn_ixs', 'sources', 'date', 'ts']},
        'asns': {
            'fields': [
                'asn', 'asn_name', 'org_id', 'org_name', 'rank', 'source', 'seen', 'ixp', 'clique_member', 'longitude', 'latitude', 'ip_version',
                'range', 'country', 'cone', 'asndegree', 'announcing', 'date', 'ts']},

        'organizations': {
            'fields': ['org_id', 'org_name', 'rank', 'seen', 'source', 'range', 'asns', 'cone', 'members', 'country', 'asndegree', 'orgdegree', 'announcing', 'ip_version',
                       'date', 'ts']},

        'links': {
            'fields': ['an0', 'an1', 'asn0', 'asn1', 'rank0', 'rank1', 'rank', 'range', 'number_paths', 'relationship', 'ip_version',
                       'asn0_cone', 'asn1_cone', 'locations', 'corrected_by', 'date', 'ts']},

        'cones': {
            'fields': ['aid', 'rank', 'asn', 'cone', 'asns', 'pfx', 'ip_version', 'range', 'date', 'ts']},

        'prefixes': {
            'fields': ['asn', 'network', 'length', 'origin', 'range', 'ip_version', 'date', 'ts']},

        'locations': {
            'fields': ['locid', 'city', 'country', 'continent', 'region', 'population', 'longitude', 'latitude', 'ip_version', 'range', 'date', 'ts']},

        'cones_cursor': {
            'fields': ['aid', 'date']}
    },

    'SQL_PATH': os.path.join(DIR_PATH, 'sql'),
    'logging': {
        'logger': logging.getLogger('asrank33'),
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
    OPTIONS['input_folder'] = '/media/efanchic/documents/asrank_data/test'
    OPTIONS['postgresql']["user"] = "postgres"
    OPTIONS['postgresql']["password"] = "Aqpl308E"
    OPTIONS['postgresql']["host"] = "localhost"
    OPTIONS['postgresql']["port"] = "5434"
    OPTIONS['postgresql']["database"] = "asrank33"


def getoptions(): return OPTIONS
