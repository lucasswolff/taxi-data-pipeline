import requests
import os
from datetime import datetime

def download_data(url, file_path):
    response = requests.get(url)

    if response.status_code == 200:
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
            
        print(f'File downloaded to {file_path}')
    
    elif response.status_code == 403:
        print(f'Download failed. Error code: {response.status_code}. The file might not exist.')

    else:
        print(f'Download failed. Error code: {response.status_code}')

def get_year_list():
    current_year = datetime.now().year
    current_month = datetime.now().month

    if current_month <= 2:
        year = current_year - 1
    else: 
        year = current_year

    year_list = [str(x) for x in range(2020, year + 1)]

    return year_list

def main():
   
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    base_file_path = 'sample_data/raw/'  

    #vehicles = ['yellow', 'green', 'fhv', 'fhvhv']
    vehicles = ['green']
    years = get_year_list()
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
        
        



