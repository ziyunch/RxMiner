import os
import boto
import pandas as pd
from sodapy import Socrata

def import_soda(client_path, end_point, file_name):
    client = Socrata(client_path, rxminer_token)
    results = client.get(end_point)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/rxdata/'+file_name, index=None, header=True)
    print('Finish downloading '+file_name)

def import_pupd():
    pupd_dict = {'2016':'yvpj-pmj2', '2015':'3z4d-vmhm', '2014':'465c-49pb', '2013':'4uvc-gbfz'}
    for year,end_point in pupd_dict.items():
        file_name = 'pupd/medicare-pupd-'+year+'.csv'
        client_path = 'data.cms.gov'
        import_soda(client_path, end_point, file_name)

def upload_pupd():
    # Create an S3 client
    s3 = boto3.client('s3')
    filename = 'file.txt'
    bucket_name = 'my-bucket'
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    s3.upload_file(filename, bucket_name, filename)

if __name__ == "__main__":
    # Connect to S3
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    rxminer_token = os.getenv("SODAPY_APPTOKEN", 'default')
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bucket_name = "rxminer"
    # bucket = conn.create_bucket(bucket_name)
    bucket = conn.get_bucket(bucket_name)
    object_key = "rx_data/"
    import_pupd()