import requests
import boto3
import os
from ingestion_location import ingest_location
from ingestion_raw import ingest_raw

s3 = boto3.client('s3')
bucket_name = 'taxi-data-hub'  

def lambda_handler(event, context):
    base_s3_prefix = event.get('s3_prefix') or os.getenv('S3_PREFIX', 'dev/raw/') #if receives s3_prefix, uses it. Else, use from environment (dev/raw/ is the default)

    # download taxi zone location csv file
    ingest_location(s3, bucket_name, base_s3_prefix)

    # download raw vehicles parquet files
    ingest_raw(s3, bucket_name, base_s3_prefix)

    return {
        'statusCode': 200,
        'body': 'Lambda completed.'
    }
