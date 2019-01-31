import os
import sys
import time
import boto
import boto3
import pandas as pd
import json
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python

test = True

def df_to_postgres(df, df_name, mode):
    """
    Save DataFrame to PostgreSQL by providing sqlalchemy engine
    """
    # Writing Dataframe to PostgreSQL and replacing table if it already exists
    df.to_sql(name=df_name, con=engine, if_exists = mode, index=False)

def read_medicaid(year, mode):
    type_dir = 'sdud/medicaid_sdud_'
    psql_table = 'sdudtest'
    cols_to_keep = [1,5,7,9,10,11,12,13,19]
    column_names = [
        'state', 'year', 'drug_name',
        'unit_reimbursed', 'num_prescriptions',
        'tot_reimbursed', 'medicaid_reimbursed',
        'nonmedicaid_reimbursed', 'ndc'
        ]
    d_type = {'ndc': str}
    df = pd.read_csv(
        s3_path+type_dir+str(year)+'.csv',
        usecols = cols_to_keep,
        names = column_names,
        dtype = d_type,
        skiprows = 1,
        nrows = test_limit)
        df = df.dropna(subset=['tot_reimbursed'])
        df_to_postgres(df, psql_table, mode)
    print('Finish Reading Medicaid data and save in table '+psql_table)

def convert_ndc(ndc):
    temp = ndc.split('-')
    if len(temp) == 3:
        ndc = temp[0].zfill(5)+temp[1].zfill(4)+temp[2].zfill(2)
    return ndc

def read_drugndc(mode):
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
    df_to_postgres(df, 'ndctest', mode)
    print('Finish Reading NDC and save in table ndcdata')

def merge_table():
    query = """
        SELECT
            sdudtest.ndc,
            ndctest.generic_name
        FROM
            sdudtest
        LEFT JOIN ndctest ON ndctest.package_ndc = sdudtest
    """
    cur.execute(query)
    print("The number of parts: ", cur.rowcount)
    row = cur.fetchone()
    print(row)

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    test_limit = int(sys.argv[1])
    psql = int(sys.argv[2])
    if psql:
        user = os.getenv('POSTGRESQL_USER', 'default')
        pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
        host = 'psql-test.csjcz7lmua3t.us-east-1.rds.amazonaws.com'
        port = '5432'
        dbname = 'postgres'
        engine = sa.create_engine('postgresql://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    else:
        user = os.getenv('REDSHIFT_USER', 'default')
        pswd = os.getenv('REDSHIFT_PASSWORD', 'default')
        host = 'redshift-test.cgcoq5ionbrp.us-east-1.redshift.amazonaws.com:5439'
        port = '5439'
        dbname = 'rxtest'
        engine = sa.create_engine('redshift+psycopg2://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    con = engine.connect()
    # conn = psycopg2.connect(dbname='rxtest', user='dbuser', host='localhost', password='password')
    # cur = conn.cursor()
    chunk_size = 100000
    s3_path = 's3n://rxminer/'
    #s3_path = '../test/rxdata/'
    start0 = time.time()
    read_drugndc('replace')
    end = time.time()
    print(end - start0)
    start = time.time()
    read_medicaid(2016, 'append')
    end = time.time()
    print(end - start)
    start = time.time()
    merge_talbe()
    end = time.time()
    print(end - start)
    print(end - start0)
    cur.close()
    con.close()