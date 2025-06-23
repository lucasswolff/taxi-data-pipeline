from download_upload_s3 import download_and_upload_to_s3
from datetime import datetime

def get_year_list():
    current_year = datetime.now().year
    current_month = datetime.now().month

    # data is updated with two months delay. If it's Jan or Feb, skips current year
    if current_month <= 2:
        year = current_year - 1
    else: 
        year = current_year

    year_list = [str(x) for x in range(2020, year + 1)]

    return year_list

def get_files_name(base_s3_prefix):

    if base_s3_prefix == 'prd/':
        vehicles = ['yellow', 'green', 'fhv', 'fhvhv']
        years = get_year_list()
        months = [f'{i:02}' for i in range(1, 13)]

    else:
        # retricts data download for dev
        vehicles = ['yellow', 'green', 'fhv', 'fhvhv']
        years = ['2024', '2025']
        months = ['01', '02', '03']

    return vehicles, years, months

def ingest_raw(s3, bucket_name, base_s3_prefix):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    vehicles, years, months = get_files_name(base_s3_prefix)

    prefix = 'raw/'

    print('Downloading raw files \n')

    for vehicle in vehicles:
        print(f"Processing vehicle: {vehicle} \n")
        for year in years:
            for month in months:
                print(f"Checking year {year}, month {month}")
                
                file_name = f'{vehicle}_tripdata_{year}-{month}.parquet'
                url = base_url + file_name
                s3_key = f'{base_s3_prefix}{prefix}{vehicle}/{file_name}'

                # skip upload if file exists in S3
                try:
                    s3.head_object(Bucket=bucket_name, Key=s3_key)
                    print(f'{s3_key} already in S3, skipping. \n')
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print(f'Downloading and uploading {s3_key}')
                        download_and_upload_to_s3(url, s3, bucket_name, s3_key)
                    else:
                        raise 

