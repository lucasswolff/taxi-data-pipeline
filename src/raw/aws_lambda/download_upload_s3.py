import requests
import boto3
import os

def download_and_upload_to_s3(url, s3, bucket_name, s3_key):
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        s3.upload_fileobj(response.raw, Bucket=bucket_name, Key=s3_key)
        print(f'Uploaded to s3://{bucket_name}/{s3_key} \n')
    elif response.status_code == 403:
        print(f'Failed to download {url}. Error code: {response.status_code}. The file might not exist. \n')
    else:
        print(f'Failed to download {url}. Error code: {response.status_code} \n')
