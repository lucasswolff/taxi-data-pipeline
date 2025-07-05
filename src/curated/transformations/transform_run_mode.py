import sys
import os
import shutil
from urllib.parse import urlparse
from curated.utils.list_s3_files import list_s3_files

class TransformMode():
    
    def parse_args(self):

        ## get env (dev/prd) when running in AWS EMR
        env  = sys.argv[1]

        if env != 'prd':
            env = 'dev' #make sure uses dev, regardless of what user types (except if it's prd)
        
        # user provided only two parameter
        if len(sys.argv) == 3: 
            if sys.argv[2] == 'full_load':
                run_mode = sys.argv[2]
                months = 0 # placeholder. Won't be used
            else:
                print('Invalid parameter.\nPlease use full_load, past_months or specific_month.')
                print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
                sys.exit(1)
        
        # user provided 3 parameters
        elif len(sys.argv) > 3:
            run_mode = sys.argv[2]
            
            if sys.argv[2] == 'full_load':
                run_mode = sys.argv[2]
                months = 0 # placeholder. Won't be used
            else:
                try:
                    months = sys.argv[3]
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
            env = 'dev'

        return run_mode, months, env 
    
    def get_run_mode_local_files(self, run_mode, months, folder_path, taxi, running_on):
        # from function parameters, get files names for spark df 
        
        if run_mode == 'full_load':
            print('Run mode: full load')
            file_path = [folder_path + taxi + '_tripdata_*.parquet']
        
        elif run_mode == 'past_months':
            if isinstance(months, int) and months > 0 and months < 300:
                print(f'Run mode: load past {months} months')

                if running_on == 'local':
                    files = sorted(os.listdir(folder_path))
                    last_n = files[-months:]
                    
                    file_path = [os.path.join(folder_path, f) for f in last_n]

                else:
                    file_path = list_s3_files(months, folder_path)
                    
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

