import sys
import os

running_on = 'AWS EMR' if 'HADOOP_CONF_DIR' in os.environ else 'local'

if running_on == 'local':
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from curated.jobs.yellow_taxi_curate import run_yellow_taxi_curate
from curated.jobs.green_taxi_curate import run_green_taxi_curate
from curated.transformations.transform_run_mode import TransformMode


def get_files_path(env, taxi):

    if running_on == 'local':
        raw_folder_path = f"sample_data/raw/{taxi}/"
        lockup_folder_path = "lockup_tables/"
        curated_folder_path = f"sample_data/curated/{taxi}/"
        
    else: #runnig on AWS
        print(f"Detected environment: {env}")

        raw_folder_path = f"s3://taxi-data-hub/{env}/raw/{taxi}/"
        lockup_folder_path = f"s3://taxi-data-hub/{env}/lockup_tables/"
        curated_folder_path = f"s3://taxi-data-hub/{env}/curated/{taxi}/"

    return raw_folder_path, lockup_folder_path, curated_folder_path


def main():
    trans_mode = TransformMode()
    env, jobs, run_mode, months = trans_mode.parse_args()

    if jobs == 'yellow':

        taxi = 'yellow'
        print(f'\n Running {taxi} job \n')

        raw_folder_path, lockup_folder_path, curated_folder_path = get_files_path(env, taxi)
        run_yellow_taxi_curate(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)

    elif jobs == 'green':
        taxi = 'green'
        print(f'\n Running {taxi} job \n')

        raw_folder_path, lockup_folder_path, curated_folder_path = get_files_path(env, taxi)
        run_green_taxi_curate(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)

    elif jobs == 'both':
        taxi = 'green'
        print(f'\n Running {taxi} job \n')

        raw_folder_path, lockup_folder_path, curated_folder_path = get_files_path(env, taxi)
        run_green_taxi_curate(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)

        taxi = 'yellow'
        print(f'\n Running {taxi} job \n')

        raw_folder_path, lockup_folder_path, curated_folder_path = get_files_path(env, taxi)
        run_yellow_taxi_curate(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path) 

    else:
        print('Please provide a valid job: yellow, green or both')
        sys.exit(1)


if __name__ == '__main__':
    main()
