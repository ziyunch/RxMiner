import math, os
import boto
import boto3
import requests
import pandas as pd
from sodapy import Socrata
from filechunkio import FileChunkIO

def multi_upload_s3(source_path):
    """Store large dataset in s3: split such files into smaller components,
        upload each component in turn and then S3 combines them into the
        final object.
    Args:
        source_path: (string) SODA api.
    Returns:
        hour: (int) A integer designating the hour;
    """
    # Calculate size of the source file
    source_size = os.stat(source_path).st_size

    # Create a multipart upload request
    mp = b.initiate_multipart_upload(os.path.basename(source_path))

    # Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    # Finish the upload
    mp.complete_upload()
    return source_size

def main():
    # Connect to S3
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bucket_name = "rxminer"
    bucket = conn.create_bucket(bucket_name)
    bucket = conn.get_bucket(bucket_name)
    object_key = "rx_data/"
    rxminer_token = os.getenv("SODAPY_APPTOKEN", 'default')

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.cms.gov", rxminer_token)
    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.medicaid.gov,
    #                  MyAppToken,
    #                  userame="user@example.com",
    #                  password="AFakePassword")

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("xbte-dn4t", limit=10000)
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../pupd/medicare_pupd_2016.csv', index=None, header=True)

    results = client.get("3z4d-vmhm")
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../pupd/medicare-puf-2015.csv', index=None, header=True)

    results = client.get("ep4w-t37d")
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../test/sample/medicare-partd-puf-2016.csv', index=None, header=True)

    client = Socrata("openpaymentsdata.cms.gov", rxminer_token)
    results = client.get("a3k9-9uq3")
    results_df = pd.DataFrame.from_records(results)
    export_csv = results_df.to_csv(r'../openpayment/openpayment-gen-2016.csv', index=None, header=True)

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

if __name__ == "__main__":
    main()