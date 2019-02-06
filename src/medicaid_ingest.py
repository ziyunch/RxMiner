import pandas as pd
import glob_func
import db_connect

def read_medicaid(year, mode):
    type_dir = 'sdud/medicaid_sdud_'
    table_name = 'sdud'
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
        glob_func.df_to_redshift(chunk, table_name, mode, new_table, cur, engine, s3f)
        new_table += 1
        print(glob_func.time_stamp()+' Medicaid data: reading in progress...')
    print(glob_func.time_stamp()+' Finish Reading Medicaid data and save in table '+psql_table)

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    db_connection = db_connect.db_connect()
    engine, con = db_connection.engine_connect()
    conn, cur = db_connection.raw_connect()
    s3f = db_connection.s3_fuse()
    s3_path = 's3n://rxminer/'
    new_table = 0
    chunk_size = 200000
    for year in [2013,2014,2015,2016]:
        read_medicaid(year, 'append')
    db_connection.close_engine()
    db_connection.close_conn()