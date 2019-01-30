import os
import boto
import pandas as pd
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python

def clean_npi(df):
    """
    Clean up NPI table
    """
    df = df.dropna(subset=['entity_type_code'])
    df.practice_postal = df.practice_postal.str.slice(0, 5)
    df.loc[df.practice_country != 'US', 'practice_state'] = "XX"
    df.loc[df.practice_country != 'US', 'practice_postal'] = "00000"
    return(df)

def df_to_postgres(df, df_name, mode):
    """
    Save DataFrame to PostgreSQL by providing sqlalchemy engine
    """
    # Connecting to PostgreSQL by providing a sqlachemy engine
    #engine = sa.create_engine('postgresql://'+psql_user+':'+psql_pswd+'@'+psql_host+':'+psql_port+'/'+psql_db,echo=False)
    engine = sa.create_engine('postgresql://dbuser:password@localhost/rxdata')
    conn = engine.connect()
    # Writing Dataframe to PostgreSQL and replacing table if it already exists
    df.to_sql(name=df_name, con=engine, if_exists = mode, index=False)

def read_pupd(year):
    chunks = pd.read_csv(s3_path+'pupd/medicare_pupd_'+str(year)+'.csv', chunksize=chunk_size)
    for chunk in chunks:
        chunk["year"] = year
        df_to_postgres(chunk, 'pupd', 'append')

def read_npi(file_name):
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
        df_to_postgres(chunk, 'npidata', 'append')

def merge_table():
    query = """
        SELECT
            pupd.npi,
            npi.practice_state
        FROM
            pupd
        LEFT JOIN npidata ON npidata.npi = pupd.npi
    """
    engine = sa.create_engine('postgresql://dbuser:password@localhost/rxdata')
    conn = engine.connect()
    engine.execute(query)
    for row in iter_row(engine, 10):
        print(row)

def main():
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    # Connect to
    psql_user = os.getenv('POSTGRESQL_USER', 'default')
    psql_pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
    psql_host = os.getenv('POSTGRESQL_HOST_IP', 'default')
    psql_port = os.getenv('POSTGRESQL_PORT', 'default')
    psql_db = os.getenv('POSTGRESQL_DATABASE', 'default')
    chunk_size = 100000
    #s3_path = 's3n://rxminer/'
    s3_path = '../test/rxdata/'
    read_npi('npidata_pfile_20050523-20190113')
    read_pupd(2016)
    merge_table()

if __name__ == "__main__":
    main()