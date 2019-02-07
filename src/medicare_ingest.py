import pandas as pd
import glob_func
import db_connect
import rxgen_parser

def collect_gen(df, df1):
    df.append(df1['generic_name'])
    df.drop_duplicates(subset ='generic_name', keep = 'first', inplace = True)
    return df

def read_medicare(year, mode, new_table, gen_df):
    type_dir = 'pupd/medicare_pupd_'
    table_name = 'pupd'
    chunks = pd.read_csv(s3_path+type_dir+str(year)+'.csv', chunksize=chunk_size)
    for chunk in chunks:
        chunk["year"] = year
        gen_df = collect_gen(gen_df, chunk)
        glob_func.df_to_redshift(chunk, table_name, mode, new_table, cur, engine, s3f)
        new_table = False
        print(glob_func.time_stamp()+' Medicare data: reading in progress...')
    print(glob_func.time_stamp()+' Finish Reading Medicare data and save in table '+table_name)
    return gen_df

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
    gen_df = []
    gen_df = read_medicare(2013, 'append', True, gen_df)
    for year in [2014,2015,2016]:
        gen_df = read_medicare(year, 'append', False, gen_df)
    gen_df = rxgen_parser.rxgen_class(regex_df, gen_df, 'generic_name')
    glob_func.df_to_redshift(gen_df, 'pupd_genclass', 'append', True, cur, engine, s3f)
    db_connection.close_engine()
    db_connection.close_conn()