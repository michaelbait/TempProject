# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

import os
import sys
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config.config import getoptions

options = getoptions()
logger = options['logging']['logger']
color = options['logging']['color']


class DBUtil:

    def __init__(self):
        self.conn = None
        self.dsn = None
        self.opt = None
        self.meta = None
        self.db_connect()
        self.get_table_meta()

    def get_db_connection(self):
        return self.conn

    # DB Helpers
    def db_exist(self, conn, dbname):
        """
        Test if interesed DB exists.
        :param conn:    Postresql DB connection
        :param dbname:  Postgresql DB name
        :return: True (present) / False (otherwise)
        """
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

    def truncate_table(self, table_name):
        if self.table_exist(table_name):
            try:
                sql = 'TRUNCATE {};'.format(table_name)
                with self.conn.cursor() as cursor:
                    cursor.execute(sql)
            except Exception as e:
                print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                logger.error(e)
                exit(1)

    def drop_table(self, table_name):
        if self.table_exist(table_name):
            try:
                sql = 'DROP TABLE IF EXISTS {};'.format(table_name)
                with self.conn.cursor() as cursor:
                    cursor.execute(sql)
            except Exception as e:
                print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
                logger.error(e)
                exit(1)

    def recreate_table(self, table_name):
        self.drop_table(table_name)
        self.create_table(table_name)

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
        self.opt = options.get('postgresql')
        self.dsn = {'database': self.opt['database'],
                    'host': self.opt['host'],
                    'port': self.opt['port'],
                    'user': self.opt['user'],
                    'password': self.opt['password']}
        try:
            conn = connect(**self.dsn)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                if not self.db_exist(conn, self.opt['database']):
                    cursor.execute('CREATE DATABASE {};'.format(opt['database']))
            if self.db_exist(conn, self.opt['database']):
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
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
            logger.error(e)
        self.meta = result
        return result

    def vacuum(self):
        try:
            print('{}Start optimize db.{}'.format(color['blue'], color['reset']), file=sys.stdout)
            with self.conn.cursor() as cursor:
                cursor.execute("VACUUM FULL")
            print('{}End optimize db.{}'.format(color['blue'], color['reset']), file=sys.stdout)
        except Exception as e:
            print("{} {} {}".format(color['red'], e, color['reset']), file=sys.stderr)
