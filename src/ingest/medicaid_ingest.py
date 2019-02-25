import pandas as pd
from mylib import glob_func
from mylib import db_connect

def read_medicaid(year, mode, new_table):
    type_dir = 'sdud/medicaid_sdud_'
    table_name = 'sdud'
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
    for chunk in chunks:
        chunk = chunk.dropna(subset=['tot_reimbursed'])
        chunk['ndc9'] = chunk.ndc.str[:9]
        glob_func.df_to_redshift(chunk, table_name, mode, new_table, cur, engine, s3f)
        new_table = False
        print(glob_func.time_stamp()+' Medicaid data: reading in progress...')
    print(glob_func.time_stamp()+' Finish Reading Medicaid data and save in table '+table_name)

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    db_connection = db_connect.db_connect()
    engine, con = db_connection.engine_connect()
    conn, cur = db_connection.raw_connect()
    s3f = db_connection.s3_fuse()
    s3_path = 's3n://rxminer/'
    chunk_size = 200000
    read_medicaid(2013, 'append', True)
    for year in [2014,2015,2016]:
        read_medicaid(year, 'append', False)
    db_connection.close_engine()
    db_connection.close_conn()