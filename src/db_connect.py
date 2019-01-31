import os
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python

class db_connect:
    def __init__(self, psql=True):
    self.conn = None
    self.cur = None
    self.engine = None
    self.con = None
    if psql:
        user = os.getenv('POSTGRESQL_USER', 'default')
        pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
        host = os.getenv('POSTGRESQL_HOST_IP', 'default')
        port = os.getenv('POSTGRESQL_PORT', 'default')
        dbname = os.getenv('POSTGRESQL_DATABASE', 'default')
    else:
        # Connect to localhost
        user = os.getenv('REDSHIFT_USER', 'default')
        pswd = os.getenv('REDSHIFT_PASSWORD', 'default')
        host = os.getenv('REDSHIFT_HOST_IP', 'default')
        port = os.getenv('REDSHIFT_PORT', 'default')
        dbname = os.getenv('REDSHIFT_DATABASE', 'default')
    self.engine = sa.create_engine('postgresql://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    self.con = engine.connect()

def get_engine(self):
    return self.engine, self.con

def get_conn(self):
    return self.conn, self.cur

def close_conn(self):
    self.con.close()

def close_engine(self):
    self.cur.close()