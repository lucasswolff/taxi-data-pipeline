import sys
import os
import shutil

class TransformMode():
    
    def parse_args(self):
        
        # user provided only one parameter
        if len(sys.argv) == 2: 
            if sys.argv[1] == 'full_load':
                run_mode = sys.argv[1]
                months = 0 # placeholder. Won't be used
            else:
                print('Invalid parameter.\nPlease use full_load, past_months or specific_month.')
                print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
                sys.exit(1)
        
        # user provided 2 parameters
        elif len(sys.argv) > 2:
            run_mode = sys.argv[1]
            
            if sys.argv[1] == 'full_load':
                run_mode = sys.argv[1]
                months = 0 # placeholder. Won't be used
            else:
                try:
                    months = sys.argv[2]
                    months = int(months)
                except IndexError:
                    print("Invalid argument value. When using past_months or specific_month provide an integer.")
                    print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
                    sys.exit(1)
                except ValueError:
                    print("Invalid argument value. Please provide an integer bigger than zero.")
                    print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
                    sys.exit(1)
        
        # user didn't provide parameters - use past 3 months as default
        else:
            run_mode = 'past_months'
            months = 3
            
        return run_mode, months
    
    def get_run_mode_files(self, run_mode, months, folder_path, taxi):
        # from function parameters, get files names for spark df 
        
        if run_mode == 'full_load':
            print('Run mode: full load')
            file_path = [folder_path + taxi + '_tripdata_*.parquet']
        
        elif run_mode == 'past_months':
            if isinstance(months, int) and months > 0 and months < 300:
                print(f'Run mode: load past {months} months')
                
                files = sorted(os.listdir(folder_path))
                last_three = files[-months:]
                
                file_path = [os.path.join(folder_path, f) for f in last_three]
            else:
                print('Invalid parameter.\nPlease, when using past_months, provide an int bigger between zero and 300.')
                sys.exit(1)
    
        elif run_mode == 'specific_month':
            
            valid_month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
            
            if len(str(months)) == 6 and str(months)[-2:] in valid_month_list and months > 200900:
                print(f'Run mode: load specific month - {months}')
                year_file = str(months)[:4]
                month_file = str(months)[-2:]
                file_path = [folder_path + taxi + f'_tripdata_{year_file}-{month_file}.parquet']
            else:
                print('Invalid parameter.\nPlease, when using specific_month, provide yyyymm (e.g. 202404) from 2009 or after.')
                sys.exit(1)
        
        else:
            print('Invalid parameter.\nPlease use full_load, past_months or specific_month.')
            print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
            sys.exit(1)
        
        return file_path

    def delete_parquet_folders(self, run_mode, months, curated_folder_path, raw_folder_path):
        if run_mode == 'specific_month':
            year = str(months)[:4]
            month = str(months)[-2:]

            folder_path = curated_folder_path + '/file_year=' + year + '/file_month=' + month

            # Check if the folder exists and deletes it
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)
                print(f"Deleted partition: {folder_path}")
            else:
                print(f"Partition does not exist: {folder_path}")
                
        else: # run_mode = past_months
                files = sorted(os.listdir(raw_folder_path))
                last_three_files = files[-months:]
                
                for file in last_three_files:
                    year, month = file.split('_')[2].replace('.parquet', '').split('-')
                    
                    folder_path = curated_folder_path + '/file_year=' + year + '/file_month=' + month
                    
                    # Check if the folder exists and delete it
                    if os.path.exists(folder_path):
                        shutil.rmtree(folder_path)
                        print(f"Deleted partition: {folder_path}")
                    else:
                        print(f"Partition does not exist: {folder_path}")
