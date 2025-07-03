import boto3
from urllib.parse import urlparse

def list_s3_files(months, folder_path): 
    s3 = boto3.client('s3')

    parsed = urlparse(folder_path)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip('/')  # Remove leading '/' 

    # List all objects under the prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Filter out directories (if any) and sort by LastModified
    files = sorted(
        [obj for obj in response.get('Contents', []) if not obj['Key'].endswith('/')],
        key=lambda x: x['LastModified']
    )

    # Get the last N files
    last_n = files[-months:]

    # Create full S3 paths
    file_paths = [f's3://{bucket_name}/{obj["Key"]}' for obj in last_n]

    return file_paths



