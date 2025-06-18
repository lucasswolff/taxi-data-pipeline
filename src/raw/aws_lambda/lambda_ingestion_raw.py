import requests
import boto3
import os
from download_upload_s3 import download_and_upload_to_s3

s3 = boto3.client('s3')
bucket_name = 'taxi-data-hub'  

def main():
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    base_s3_prefix = 'raw/'  

    vehicles = ['yellow']
    years = ['2024']
    months = [f'{i:02}' for i in range(1, 13)]

    for vehicle in vehicles:
        print(f"Processing vehicle: {vehicle} \n")
        for year in years:
            for month in months:
                print(f"Checking year {year}, month {month}")
                
                file_name = f'{vehicle}_tripdata_{year}-{month}.parquet'
                url = base_url + file_name
                s3_key = f'{base_s3_prefix}{vehicle}/{file_name}'

                # skip upload if file exists in S3
                try:
                    s3.head_object(Bucket=bucket_name, Key=s3_key)
                    print(f'{s3_key} already in S3, skipping. \n')
                except s3.exceptions.ClientError:
                    print(f'Downloading and uploading {s3_key}')
                    download_and_upload_to_s3(url, s3, bucket_name, s3_key)


if __name__ == '__main__':
    print("Starting download and upload process")
    main()
