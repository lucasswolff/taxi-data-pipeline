import requests
import os

def download_data(url, file_path):
    response = requests.get(url)

    if response.status_code == 200:
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
            
        print(f'File downloaded to {file_path}')
        
    else:
        print(f'Download failed. Error code: {response.status_code}')


def main():
   
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    base_file_path = 'sample_data/raw/'  

    vehicles = ['yellow', 'green', 'fhv', 'fhvhv']
    years = ['2024','2025']
    months = [f'{i:02}' for i in range(1, 13)] # 01, 02, ... 12

    for vehicle in vehicles:
        for year in years:
            for month in months:
                url = base_url + vehicle + '_tripdata_' + year + '-' + month + '.parquet'
                file_path = base_file_path + vehicle + '/' + vehicle +  '_tripdata_' + year + '-' + month + '.parquet'
                
                # skip if file exists already
                if os.path.exists(file_path):
                    print(f'{file_path} already exists, skipping download.')
                else:
                    print(f'Downloading {file_path}')
                    download_data(url, file_path)
            
            
if __name__ == '__main__':
    main()
        
        



