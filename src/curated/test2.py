import os
import shutil



if run_mode == 'specific_month':
    year = str(months)[:4]
    month = str(months)[-2:]

    folder_path = curated_folder_path + '/file_year=' + year + '/file_month=' + month

    # Check if the folder exists and delete it
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Deleted folder: {folder_path}")
    else:
        print(f"Folder does not exist: {folder_path}")
        
else: # run_mode = past_months
        files = sorted(os.listdir(raw_folder_path))
        last_three_files = files[-months:]
        
        for file in last_three_files:
            year, month = file.split('_')[2].replace('.parquet', '').split('-')
            
            folder_path = curated_folder_path + '/file_year=' + year + '/file_month=' + month
            
            # Check if the folder exists and delete it
            if os.path.exists(folder_path):
                shutil.rmtree(folder_path)
                print(f"Deleted folder: {folder_path}")
            else:
                print(f"Folder does not exist: {folder_path}")
        
    
    
    
        




