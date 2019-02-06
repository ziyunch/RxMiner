import os
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python
import s3fs
import glob_func

class db_connect:
    def __init__(self, redshift=True):
        self.conn = None
        self.cur = None
        self.engine = None
        self.con = None
        if redshift:
            # Connect to Redshift
            user = os.getenv('REDSHIFT_USER', 'default')
            pswd = os.getenv('REDSHIFT_PASSWORD', 'default')
            host = os.getenv('REDSHIFT_HOST_IP', 'default')
            port = os.getenv('REDSHIFT_PORT', 'default')
            dbname = os.getenv('REDSHIFT_DATABASE', 'default')
            surl = 'redshift+psycopg2://'
            s3fuse = s3fs.S3FileSystem(anon=False)
        else:
            # Connect to PostgreSQL
            user = os.getenv('POSTGRESQL_USER', 'default')
            pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
            host = os.getenv('POSTGRESQL_HOST_IP', 'default')
            port = str(os.getenv('POSTGRESQL_PORT', 'default'))
            dbname = os.getenv('POSTGRESQL_DATABASE', 'default')
            surl = 'postgresql://'
        self.engine = sa.create_engine(surl+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
        self.con = engine.connect()
        self.conn = engine.raw_connection()
        cur = conn.cursor()

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