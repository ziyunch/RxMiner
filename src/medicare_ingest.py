import pandas as pd
import glob_func
import db_connect
import rxgen_parser

def read_medicare(year, mode, new_table, gen_ds):
    type_dir = 'pupd/medicare_pupd_'
    table_name = 'pupd'
    chunks = pd.read_csv(s3_path+type_dir+str(year)+'.csv', chunksize=chunk_size)
    for chunk in chunks:
        chunk["year"] = year
        chunk = rxgen_parser.rxgen_class(regex_df, chunk, 'generic_name')
        glob_func.df_to_redshift(chunk, table_name, mode, new_table, cur, engine, s3f)
        new_table = False
        print(glob_func.time_stamp()+' Medicare data: reading in progress...')
    print(glob_func.time_stamp()+' Finish Reading Medicare data and save in table '+table_name)
    return gen_ds

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
    url = 'https://druginfo.nlm.nih.gov/drugportal/jsp/drugportal/DrugNameGenericStems.jsp'
    regex_df = rxgen_parser.regex_file(url)
    read_medicare(2013, 'append', True)
    for year in [2014,2015,2016]:
        read_medicare(year, 'append', False)
    db_connection.close_engine()
    db_connection.close_conn()