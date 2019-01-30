import math, os
import boto
import requests
import pandas as pd
from sodapy import Socrata
from filechunkio import FileChunkIO

def import_soda(client_path, end_point, file_name):
    clinet = Socrate(client_path, rxminer_token)
    results = client.get(end_point)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/rxdata/'+file_name, index=None, header=True)

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

def import_file():
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.medicaid.gov", rxminer_token)
    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.medicaid.gov,
    #                  MyAppToken,
    #                  userame="user@example.com",
    #                  password="AFakePassword")

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("neai-csgh", limit=10000)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/medicaid-sdud-2016.csv', index=None, header=True)
    # read puf file
    client = Socrata("data.cms.gov", rxminer_token)
    results = client.get("haqy-eqp7", limit=10000)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/medicare-puf-2016.csv', index=None, header=True)
    #read medicare-part d puf file
    results = client.get("ep4w-t37d", limit=10000)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/medicare-partd-puf-2016.csv', index=None, header=True)
    # read open payment data
    client = Socrata("openpaymentsdata.cms.gov", rxminer_token)
    results = client.get("a3k9-9uq3", limit=10000)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/openpayment-gen-2016.csv', index=None, header=True)
    # read wa sample
    client = Socrata("data.wa.gov", rxminer_token)
    results = client.get("t8zq-rh9q", limit=10000, clinicUse="y")
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/wa-pmp-2016q1.csv', index=None, header=True)
    """
    soda_api = "https://data.medicaid.gov/api/views/3v6v-qk5s/rows.csv?accessType=DOWNLOAD"
    stream data into aws S3, doesn't work
    s3_object = boto3.resource('s3').Object(bucket_name, object_key)
    with requests.get(soda_api, stream=True) as r:
        s3_object.put(Body=r.content)
    """

def main():
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

if __name__ == "__main__":
    main()