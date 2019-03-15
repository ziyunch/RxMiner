import os
import boto
import pandas as pd
from sodapy import Socrata

"""
Obtain Medicaid and Medicare data from Socrata's SODA API and store them in the S3 bucket.
"""

def import_soda(client_path, end_point, file_name, rxminer_token):
    """
    Obtain data from Socrata's SODA API
    """
    client = Socrata(client_path, rxminer_token)
    results = client.get(end_point)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/rxdata/'+file_name, index=None, header=True)
    print('Finish downloading '+file_name)

def import_medicare(rxminer_token, pupd_dict):
    """
    Obtain Medicare data from CMS's endpoint
    """
    for year,end_point in pupd_dict.items():
        file_name = 'pupd/medicare-pupd-'+year+'.csv'
        client_path = 'data.cms.gov'
        import_soda(client_path, end_point, file_name, rxminer_token)

def import_medicaid(rxminer_token, sdud_dict):
    """
    Obtain Medicaid data from Medicaid's endpoint
    """
    for year,end_point in sdud_dict.items():
        file_name = 'sdud/medicaid-sdud-'+year+'.csv'
        client_path = 'data.medicaid.gov'
        import_soda(client_path, end_point, file_name, rxminer_token)

def upload_s3():
    """
    Connect to s3 bucket for uploading files
    """
    # Create an S3 client
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bucket_name = os.getenv('AWS_BUCKET_NAME', 'default')
    bucket = conn.get_bucket(bucket_name)
    object_key = "rx_data/"
    s3 = boto3.client('s3')
    filename = 'file.txt'
    bucket_name = 'my-bucket'
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    s3.upload_file(filename, bucket_name, filename)

if __name__ == "__main__":
    # Connect to S3
    rxminer_token = os.getenv("SODAPY_APPTOKEN", 'default')
    sdud_dict = {'2018':'e5ds-i36p', '2017':'3v5r-x5x9', '2016':'3v6v-qk5s', '2015':'ju2h-vcgs', '2014':'955u-9h9g', '2013':'rkct-3tm8'}
    pupd_dict = {'2016':'yvpj-pmj2', '2015':'3z4d-vmhm', '2014':'465c-49pb', '2013':'4uvc-gbfz'}
    import_medicare(rxminer_token, pupd_dict)
    import_medicaid(rxminer_token, sdud_dict)