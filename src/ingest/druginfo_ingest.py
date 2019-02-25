import pandas as pd
import boto3
import json
import glob_func
from mylib import db_connect
from mylib import rxgen_parser

def convert_ndc_11(ndc):
    temp = ndc.split('-')
    if len(temp) == 3:
        ndc = temp[0].zfill(5) + temp[1].zfill(4) + temp[2].zfill(2)
    return ndc

def convert_ndc_9(ndc):
    temp = ndc.split('-')
    if len(temp) == 2:
        ndc = temp[0].zfill(5) + temp[1].zfill(4)
    return ndc

def read_drugndc(mode, new_table):
    s3 = boto3.resource('s3')
    content_object = s3.Object('rxminer', 'openfda/drug-ndc-0001-of-0001.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    # Flattern the deeply nested json files
    # Key Error bug when meta=['product_id', 'generic_name']
    df1 = pd.io.json.json_normalize(json_content['results'], 'packaging', meta=['product_id'])
    df2 = pd.io.json.json_normalize(json_content['results'])
    df = df1.merge(df2, on='product_id')
    # Standardlize dashed NDC to CMS 11/9 digits NDC
    df.package_ndc = df.package_ndc.apply(convert_ndc_11)
    df.product_ndc = df.product_ndc.apply(convert_ndc_9)
    df.generic_name = df.generic_name.str[:100]
    # Compress the dataframe by dropping unneccssary information
    df11 = df[['package_ndc', 'generic_name', 'product_ndc']]
    glob_func.df_to_redshift(df11, 'ndc11', mode, new_table, cur, engine, s3f)
    df9 = df[['product_ndc', 'generic_name', 'brand_name', 'labeler_name']]
    df9 = rxgen_parser.rxgen_class(regex_df, df9, 'generic_name')
    glob_func.df_to_redshift(df9, 'ndc9', mode, new_table, cur, engine, s3f)
    genclass = df9[['generic_name', 'rxclass']]
    genclass.drop_duplicates(subset ='generic_name', keep = 'first', inplace = True)
    glob_func.df_to_redshift(genclass, 'genclass', mode, new_table, cur, engine, s3f)
    print(glob_func.time_stamp()+' Finish Reading NDC and save in table ndcdata')

if __name__ == "__main__":
    # Disable `SettingWithCopyWarning`
    pd.options.mode.chained_assignment = None
    db_connection = db_connect.db_connect()
    engine, con = db_connection.engine_connect()
    conn, cur = db_connection.raw_connect()
    s3f = db_connection.s3_fuse()
    s3_path = 's3n://rxminer/'
    chunk_size = 200000
    url = 'https://druginfo.nlm.nih.gov/drugportal/jsp/drugportal/DrugNameGenericStems.jsp'
    regex_df = rxgen_parser.regex_file(url)
    read_drugndc('append', True)
    db_connection.close_engine()
    db_connection.close_conn()