import os
import boto
import boto3
import pandas as pd
import json
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python

def df_to_postgres(df, df_name, mode):
    """
    Save DataFrame to PostgreSQL by providing sqlalchemy engine
    """
    # Writing Dataframe to PostgreSQL and replacing table if it already exists
    df.to_sql(name=df_name, con=engine, if_exists = mode, index=False)

def read_medicare(year, mode):
    type_dir = 'pupd/medicare_pupd_'
    psql_table = 'pupd'
    chunks = pd.read_csv(s3_path+type_dir+str(year)+'.csv', chunksize=chunk_size)
    for chunk in chunks:
        chunk["year"] = year
        df_to_postgres(chunk, psql_table, mode)
        print('Medicare data: reading in progress...')
    print('Finish Reading Medicare data and save in table '+psql_table)

def read_medicaid(year, mode):
    type_dir = 'sdud/medicaid_sdud_'
    psql_table = 'sdud'
    cols_to_keep = [1,5,7,9,10,11,12,13,19]
    column_names = [
        'state', 'year', 'drug_name',
        'unit_reimbursed', 'num_prescriptions',
        'tot_reimbursed', 'medicaid_reimbursed',
        'nonmedicaid_reimbursed', 'ndc'
        ]
    d_type = {'ndc': str}
    chunks = pd.read_csv(
        s3_path+type_dir+str(year)+'.csv',
        usecols = cols_to_keep,
        names = column_names,
        dtype = d_type,
        skiprows = 1,
        chunksize = chunk_size)
    for chunk in chunks:
        chunk = chunk.dropna(subset=['tot_reimbursed'])
        df_to_postgres(chunk, psql_table, mode)
        print('Medicaid data: reading in progress...')
    print('Finish Reading Medicaid data and save in table '+psql_table)

def clean_npi(df):
    """
    Clean up NPI table
    """
    # Drop NPI records with no providers' information
    df = df.dropna(subset=['entity_type_code'])
    # Truncate Zip code to 5 digits
    df.practice_postal = df.practice_postal.str.slice(0, 5)
    # Assign state and postal code of foreign country with XX and 00000
    df.loc[df.practice_country != 'US', 'practice_state'] = "XX"
    df.loc[df.practice_country != 'US', 'practice_postal'] = "00000"
    return(df)

def read_npi(file_name, mode):
    cols_to_keep = [0,1,4,5,6,31,32,33]
    column_names = [
        'npi', 'entity_type_code',
        'organize_name',
        'last_name', 'first_name',
        'practice_state', 'practice_postal', 'practice_country'
        ]
    d_type = {'practice_postal': str}
    # Read NPI file in chunk to reduce memory usage
    chunks = pd.read_csv(
        s3_path+'npi/'+file_name+'.csv',
        usecols = cols_to_keep,
        names = column_names,
        dtype = d_type,
        chunksize = chunk_size,
        skiprows = 1)
    for chunk in chunks:
        clean_npi(chunk)
        df_to_postgres(chunk, 'npidata', mode)
        print('NPI data: reading in progress...')
    print('Finish Reading NPI and save in table npidata')

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
    df_to_postgres(df, 'ndcdata', mode)
    print('Finish Reading NDC and save in table ndcdata')

def merge_table():
    query = """
        SELECT
            pupd.npi,
            npidata.practice_state
        FROM
            pupd
        LEFT JOIN npidata ON npidata.npi = pupd.npi
    """
    cur.execute(query)
    print("The number of parts: ", cur.rowcount)
    rows = cur.fetchmany(size=10)
    print(rows)

def delete_table():
    cur.execute("DELETE TABLE pupd, npidata")

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    user = os.getenv('POSTGRESQL_USER', 'default')
    pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
    host = os.getenv('POSTGRESQL_HOST_IP', 'default')
    port = os.getenv('POSTGRESQL_PORT', 'default')
    dbname = 'postgres'
    # engine = sa.create_engine('postgresql://dbuser:password@localhost/rxdata')
    engine = sa.create_engine('postgresql://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    con = engine.connect()
    # conn = psycopg2.connect(dbname='rxdata', user='dbuser', host='localhost', password='password')
    # cur = conn.cursor()
    chunk_size = 100000
    s3_path = 's3n://rxminer/'
    #s3_path = '../test/rxdata/'
    # read_drugndc('replace')
    read_medicaid(2016, 'append')
    read_npi('npidata_pfile_20050523-20190113', 'append')
    read_medicare(2016, 'append')
    # cur.close()
    con.close()