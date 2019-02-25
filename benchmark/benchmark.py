import os
import sys
import time
import datetime
import pytz
import boto
import boto3
import s3fs
import pandas as pd
import json
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python
import StringIO
from aenum import Enum

class TargetDB(Enum):
    psql = 1
    redshift = 2

class OperateMode(Enum):
    replace = 1
    append = 2

class ODBCMode(Enum):
    psycopg = 1
    sqlalchemy = 2

def time_stamp():
    eastern = pytz.timezone('US/Eastern')
    ts = datetime.datetime.now(eastern).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return ts

def pd_to_postgres(df, table_name, mode):
    """
    Save DataFrame to PostgreSQL by to_sql() function.
    """
    # Writing Dataframe to PostgreSQL and replacing table if it already exists
    df.to_sql(name=table_name, con=engine, if_exists = mode, index=False)

def cleanColumns(columns):
    cols = []
    for col in columns:
        col = col.replace(' ', '_')
        cols.append(col)
    return cols

def df_to_sql(df, table_name, mode, new_table, target_database, cur, engine):
    """
    Save DataFrame to .csv, read csv as sql table in memory and copy the table
     directly in batch to PostgreSQL or Redshift.
    """
    # Prepare schema for table
    df[:0].to_sql('temp', engine, if_exists = "replace", index=False)
    df.columns = cleanColumns(df.columns)
    # replace mode
    if mode == OperateMode.replace.name:
        cur.execute("DROP TABLE " + table_name)
        cur.connection.commit()
    # Prepare data to temp
    if target_database == TargetDB.psql.name:
        data = StringIO.StringIO()
        df.to_csv(data, header=False, index=False)
        data.seek(0)
        sql_query = """
            COPY temp FROM STDIN WITH
                CSV
                HEADER;
        """
        cur.copy_expert(sql=sql_query, file=data)
        cur.connection.commit()
        # Copy or append table temp to target table
        if (mode == 'replace' or new_table == 0):
            sql_query2 = """
                ALTER TABLE temp
                RENAME TO %s;
            """
        else:
            sql_query2 = """
                INSERT INTO %s SELECT * FROM temp;
                DROP TABLE temp;
            """
        cur.execute(sql_query2 % table_name)
        cur.connection.commit()
    else:
        with s3.open('rxminer/temp.csv', 'wb') as f:
            df.to_csv(f, index=False, header=False)
        sql_query = """
            COPY temp
            FROM 's3://rxminer/temp.csv'
            IAM_ROLE 'arn:aws:iam::809946175142:role/RedshiftCopyUnload'
            CSV
            IGNOREHEADER 1;
            COMMIT;
        """
        cur.execute(sql_query)
        # Copy or append table temp to target table
        if (mode == 'replace' or new_table == 0):
            sql_query2 = """
                ALTER TABLE temp
                RENAME TO %s;
                COMMIT;VACUUM;COMMIT;
            """
        else:
            sql_query2 = """
                INSERT INTO %s SELECT * FROM temp;
                DROP TABLE temp;
                COMMIT;VACUUM;COMMIT;
            """
        cur.execute(sql_query2 % table_name)

def read_medicaid(year, mode):
    type_dir = 'sdud/medicaid_sdud_'
    table_name = 'sdudtest'
    cols_to_keep = {
        1:'state', 5:'year',7:'drug_name',
        9:'unit_reimbursed',10:'num_prescriptions',11:'tot_reimbursed',
        12:'medicaid_reimbursed',13:'nonmedicaid_reimbursed',19:'ndc'
        }
    d_type = {'ndc': str}
    chunks = pd.read_csv(
        s3_path+type_dir+str(year)+'.csv',
        usecols = cols_to_keep.keys(),
        names = cols_to_keep.values(),
        dtype = d_type,
        skiprows = 1,
        chunksize = chunk_size)
    new_table = 0
    for chunk in chunks:
        chunk.dropna(subset=['tot_reimbursed'], inplace=True)
        if pd2db == "yes":
            pd_to_postgres(chunk, table_name, mode)
        else:
            df_to_sql(chunk, table_name, mode, new_table, target_database, cur, engine)
        new_table += 1
        print(time_stamp()+' Medicaid data: reading in progress...')
    print(time_stamp()+' Finish Reading Medicaid data and save in table '+table_name)

def convert_ndc(ndc):
    temp = ndc.split('-')
    if len(temp) == 3:
        ndc = temp[0].zfill(5) + temp[1].zfill(4) + temp[2].zfill(2)
    return ndc

def read_drugndc(mode):
    new_table = 0
    table_name = 'ndctest'
    s3 = boto3.resource('s3')
    content_object = s3.Object('rxminer', 'openfda/drug-ndc-0001-of-0001.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    # Flattern the deeply nested json files
    # Key Error bug when meta=['product_id', 'generic_name']
    df1 = pd.io.json.json_normalize(json_content['results'], 'packaging', meta=['product_id'])
    df2 = pd.io.json.json_normalize(json_content['results'])
    df = df1.merge(df2, on='product_id')
    # Compress the dataframe by dropping unneccssary information
    df = df[['package_ndc', 'generic_name', 'brand_name', 'labeler_name']]
    # Standardlize dashed NDC to CMS 11 digits NDC
    df.package_ndc = df.package_ndc.apply(convert_ndc)
    df.generic_name = df.generic_name.str[:100]
    if pd2db == "yes":
        pd_to_postgres(df, table_name, mode)
    else:
        df_to_sql(df, table_name, mode, new_table, target_database, cur, engine)
    print(time_stamp()+' Finish Reading NDC and save in table ndcdata')

def merge_table():
    query = """
        SELECT
            sdudtest.ndc,
            sdudtest.tot_reimbursed,
            ndctest.generic_name
        INTO
            sdud_cleaned
        FROM
            sdudtest
        LEFT JOIN ndctest ON ndctest.package_ndc = sdudtest.ndc;
        SELECT * FROM sdud_cleaned LIMIT 10;
    """
    cur.execute(query)
    conn.commit()

def sum_by_state():
    query = """
        SELECT SUM(tot_reimbursed), generic_name
        FROM sdud_cleaned
        GROUP BY generic_name;
    """
    cur.execute(query)
    conn.commit()

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    eastern = pytz.timezone('US/Eastern')
    new_table = 0
    chunk_size = int(sys.argv[1])
    target_database = sys.argv[2]
    psyc = sys.argv[3]
    sraw = sys.argv[4]
    pd2db = sys.argv[5]
    if target_database == TargetDB.psql.name:
        print(time_stamp()+' Connect to PostgreSQL on AWS RDS')
        user = os.getenv('POSTGRESQL_USER', 'default')
        pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
        host = 'psql-test.csjcz7lmua3t.us-east-1.rds.amazonaws.com'
        port = '5432'
        dbname = 'postgres'
        surl = 'postgresql://'
    else:
        print(time_stamp()+' Connect to AWS Redshift')
        user = os.getenv('REDSHIFT_USER', 'default')
        pswd = os.getenv('REDSHIFT_PASSWORD', 'default')
        host = 'redshift-test.cgcoq5ionbrp.us-east-1.redshift.amazonaws.com'
        port = '5439'
        dbname = 'rxtest'
        surl = 'redshift+psycopg2://'
        s3 = s3fs.S3FileSystem(anon=False)
    if psyc == ODBCMode.psycopg.name:
        print("Using Psycopg2")
        conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port, password=pswd)
    else:
        engine = sa.create_engine(surl+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
        if sraw == "raw":
            conn = engine.raw_connection()
        else:
            con = engine.connect()
            conn = con.connection
    cur = conn.cursor()
    print(time_stamp()+' Connected')
    s3_path = 's3n://rxminer/'
    start0 = time.time()
    start = time.time()
    if (psyc == ODBCMode.sqlalchemy.name and sraw == "no") :
        read_medicaid(2016, 'append')
    end = time.time()
    print(end - start)
    start = time.time()
    if (psyc == ODBCMode.sqlalchemy.name and sraw == "no"):
        read_drugndc('append')
    end = time.time()
    print(end - start)
    start = time.time()
    merge_table()
    sum_by_state()
    end = time.time()
    print(end - start)
    print(end - start0)
    cur.close()
    conn.close()
    if (psyc == ODBCMode.sqlalchemy.name and sraw == "no") : con.close()