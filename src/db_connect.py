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
        db = os.getenv('POSTGRESQL_DATABASE', 'default')
    else:
        # Connect to localhost
        psql_user = 'dbuser'
        psql_pswd = 'password'
        psql_host = '127.0.0.1'
        psql_port = '5432'
        psql_db = 'rxdata'
    self.conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=pswd)
    self.cur = self.conn.cursor()
    self.engine = sa.create_engine('postgresql://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    self.con = engine.connect()

def get_connection(self):
    return self.conn, self.cur

def get_

def close_connection(self):
    self.conn.close()
    self.cur.close()