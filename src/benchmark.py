import os
import sys
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
    # Writing Dataframe to PostgreSQL and replacing table if it already exists
    df.to_sql(name=df_name, con=engine, if_exists = mode, index=False)

def import_test_db(read_limit):
    client = Socrata("data.medicaid.gov", rxminer_token)
    results = client.get("neai-csgh", limit=read_limit)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/medicaid-sdud-2016-'+read_limit+'.csv', index=None, header=True)

def read_npi_test(file_name, read_limit, table_name, mode):
    cols_to_keep = [0,1,4,5,6,31,32,33]
    column_names = [
        'npi', 'entity_type_code',
        'organize_name',
        'last_name', 'first_name',
        'practice_state', 'practice_postal', 'practice_country'
        ]
    d_type = {'practice_postal': str}
    # Read NPI file in chunk to reduce memory usage
    df = pd.read_csv(
        s3_path+'npi/'+file_name+'.csv',
        usecols = cols_to_keep,
        names = column_names,
        dtype = d_type,
        nrows = read_limit,
        skiprows = 1)
    clean_npi(df)
    df_to_postgres(df, table_name, mode)

def read_pupd_test(year, read_limit, table_name, mode):
    df = pd.read_csv(s3_path+'pupd/medicare_pupd_'+str(year)+'.csv', nrows=read_limit)
    df["year"] = year
    df_to_postgres(df, 'pupd', 'replace')

def merge_table():
    query = """
        SELECT
            pupd.npi,
            npidata.practice_state
        FROM
            pupd
        LEFT JOIN npidata ON npidata.npi = pupd.npi
    """
    engine.execute(query)
    print("The number of parts: ", engine.rowcount)
    row = engine.fetchone()
    print(row)

# Disable `SettingWithCopyWarning`
pd.options.mode.chained_assignment = None
test_limit = int(sys.argv[1])
# Connecting to PostgreSQL by providing a sqlachemy engine
#engine = sa.create_engine('postgresql://'+psql_user+':'+psql_pswd+'@'+psql_host+':'+psql_port+'/'+psql_db,echo=False)
engine = sa.create_engine('postgresql://dbuser:password@localhost/rxdata')
con = engine.connect()
s3_path = '../test/rxdata/'
read_npi_test('npidata_pfile_20050523-20190113', test_limit, 'npidata', 'replace')
read_pupd_test(2016, test_limit, 'pupd', 'replace')
merge_table()
con.close()
engine.close()
