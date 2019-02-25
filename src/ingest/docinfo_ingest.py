import pandas as pd
from mylib import glob_func
from mylib import db_connect

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

def read_npi(file_name, mode, new_table):
    type_dir = 'npi/'
    table_name = 'npidata'
    cols_to_keep = {
        0:'npi', 1:'entity_type_code',4:'organize_name',
        5:'last_name', 6:'first_name',
        31:'practice_state', 32:'practice_postal', 33:'practice_country'
        }
    d_type = {'practice_postal': str}
    # Read NPI file in chunk to reduce memory usage
    chunks = pd.read_csv(
        s3_path+type_dir+file_name+'.csv',
        usecols = cols_to_keep.keys(),
        names = cols_to_keep.values(),
        dtype = d_type,
        chunksize = chunk_size,
        skiprows = 1)
    for chunk in chunks:
        clean_npi(chunk)
        glob_func.df_to_redshift(chunk, table_name, mode, new_table, cur, engine, s3f)
        new_table = False
        print(glob_func.time_stamp()+' NPI data: reading in progress...')
    print(glob_func.time_stamp()+' Finish Reading NPI and save in table npidata')

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    db_connection = db_connect.db_connect()
    engine, con = db_connection.engine_connect()
    conn, cur = db_connection.raw_connect()
    s3f = db_connection.s3_fuse()
    s3_path = 's3n://rxminer/'
    chunk_size = 200000
    read_npi('npidata_pfile_20050523-20190113', 'append', True)
    db_connection.close_engine()
    db_connection.close_conn()