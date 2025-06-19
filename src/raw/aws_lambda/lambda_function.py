import requests
import boto3
import os
from ingestion_location import ingest_location
from ingestion_raw import ingest_raw

s3 = boto3.client('s3')
bucket_name = 'taxi-data-hub'  

def lambda_handler(event, context):

    # download taxi zone location csv file
    ingest_location(s3, bucket_name)

    # download raw vehicles parquet files
    ingest_raw(s3, bucket_name)

    return {
        'statusCode': 200,
        'body': 'Lambda completed.'
    }
