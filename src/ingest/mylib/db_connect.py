import os
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python
import s3fs
import glob_func

"""
sqlalchemy and sqlalchemy-redshift were used as the ODBC connector for PostgreSQL and Redshift, respectively.
"""

class db_connect:
    def __init__(self, redshift=True):
        self.conn = None
        self.cur = None
        self.engine = None
        self.con = None
        self.user = None
        self.pswd = None
        self.host = None
        self.port = None
        self.dbname = None
        self.surl = None
        self.s3fuse = None
        if redshift:
            # Connect to Redshift
            self.user = os.getenv('REDSHIFT_USER', 'default')
            self.pswd = os.getenv('REDSHIFT_PASSWORD', 'default')
            self.host = os.getenv('REDSHIFT_HOST_IP', 'default')
            self.port = os.getenv('REDSHIFT_PORT', 'default')
            self.dbname = os.getenv('REDSHIFT_DATABASE', 'default')
            self.surl = 'redshift+psycopg2://'
            self.s3fuse = s3fs.S3FileSystem(anon=False)
        else:
            # Connect to PostgreSQL
            self.user = os.getenv('POSTGRESQL_USER', 'default')
            self.pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
            self.host = os.getenv('POSTGRESQL_HOST_IP', 'default')
            self.port = str(os.getenv('POSTGRESQL_PORT', 'default'))
            self.dbname = os.getenv('POSTGRESQL_DATABASE', 'default')
            self.surl = 'postgresql://'
        self.engine = sa.create_engine(self.surl+self.user+':'+self.pswd+'@'+self.host+':'+self.port+'/'+self.dbname,echo=False)
        self.con = self.engine.connect()
        self.conn = self.engine.raw_connection()
        self.cur = self.conn.cursor()

    def engine_connect(self):
        print(glob_func.time_stamp()+' Engine connected.')
        return self.engine, self.con

    def raw_connect(self):
        print(glob_func.time_stamp()+' Cursor ready.')
        return self.conn, self.cur

    def s3_fuse(self):
        return self.s3fuse

    def close_conn(self):
        self.cur.close()
        self.conn.close()
        print(glob_func.time_stamp()+' Cursor closed.')

    def close_engine(self):
        self.con.close()
        print(glob_func.time_stamp()+' Engine closed.')