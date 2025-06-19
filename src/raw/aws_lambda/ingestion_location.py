from download_upload_s3 import download_and_upload_to_s3

def ingest_location(s3, bucket_name):
   
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    base_s3_prefix = 'lockup_tables/' 
    file_name = 'taxi_zone.csv'
    s3_key = f'{base_s3_prefix}{file_name}'

    print(f'Downloading location file')

    # skip upload if file exists in S3
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        print(f'{s3_key} already in S3, skipping. \n')
    except s3.exceptions.ClientError:
        print(f'Downloading and uploading {s3_key}')
        download_and_upload_to_s3(url, s3, bucket_name, s3_key)
        
               
