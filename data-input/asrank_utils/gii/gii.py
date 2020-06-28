# -*- coding: utf-8 -*-
__author__ = 'baitaluk'

from psycopg2 import connect, DatabaseError
from config.config import *

options = getoptions()
opt = options.get('postgresql')


class PostgresImporter:
    dsn = {
        'database': opt.get('database') if 'database' in opt else 'asrankw',
        'host': opt.get('host') if 'host' in opt else 'localhost',
        'port': opt.get('port') if 'port' in opt else 5432,
        'user': opt.get('user') if 'user' in opt else 'postgres',
        'password': opt.get('password') if 'password' in opt else 'Aqpl308E'}
    conn = None

    def __init__(self):
        self.conn = connect(**self.dsn)
        self.process()

    def process(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(opt.get('sql'))
                self.conn.commit()
                print(self.conn.status)
        except (Exception, DatabaseError) as error:
            print(error)
            self.conn.rollback()
        finally:
            pass
