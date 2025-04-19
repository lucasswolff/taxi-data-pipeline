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
   
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    file_path = 'lockup_tables/taxi_zone.csv'
    
    print(f'Downloading {file_path}')
    download_data(url, file_path)
        
               
if __name__ == '__main__':
    main()
        
        



