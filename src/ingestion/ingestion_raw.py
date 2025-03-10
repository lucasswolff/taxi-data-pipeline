import requests

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
response = requests.get(url)

file_path = 'sample_data/raw/yellow_tripdata_2024-01.parquet'

if response.status_code == 200:
    
    with open(file_path, 'wb') as f:
        f.write(response.content)
        
    print(f'File downloaded to {file_path}')
    
else:
    print(f'Download failed. Error code: {response.status_code}')