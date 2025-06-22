from download_upload_s3 import download_and_upload_to_s3

def ingest_raw(s3, bucket_name, base_s3_prefix):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    vehicles = ['green']
    years = ['2024']
    months = [f'{i:02}' for i in range(1, 13)]
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

