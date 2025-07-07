import sys
import os

running_on = 'AWS EMR' if 'HADOOP_CONF_DIR' in os.environ else 'local'

if running_on == 'local':
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from curated.utils.create_spark import create_spark_session
from curated.transformations.taxi_transformer import TransformerBase, TransformerYellowGreen
from curated.tests.test_curated_taxi import run_tests
from curated.utils.read_lockup_tables import ReadLockup
from curated.transformations.transform_run_mode import TransformMode



def run_green_taxi_curate(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path):

    #### CREATE SPARK
    spark = create_spark_session(app_name="curated_layer")
    
    #### READ FILES
    print('Reading files...')
    
    taxi = 'green'
    file_path = trans_mode.get_run_mode_local_files(run_mode, months, raw_folder_path, taxi, running_on) # get file path based on run parameters
    
    print(f'File path: {file_path}')

    print('Reading Raw')

    df_green_raw = spark.read.option("mergeSchema", "true").parquet(*file_path)

    print(df_green_raw.columns)
    
    print('reading Lockup tables')

    lockup_reader = ReadLockup()
    df_payment_type, df_ratecode, df_trip_type, df_taxi_zone = lockup_reader.read_lockup_tables(spark, lockup_folder_path)
    
    print('Successfully read all input tables!')

    #### TRANSFORM
    print('Initiating dataframe transformation...')
    
    transformer_green = TransformerYellowGreen()
    df_green = transformer_green.make_green_consistent(df_green_raw) # add columns with defaut values to be consistent with yellow taxi
    
    transformer = TransformerBase()
    df_green = transformer.transform_df(df_green, df_payment_type, df_ratecode, df_trip_type, df_taxi_zone) 

    #### TEST 
    run_tests(df_green)
    print('All tests passed!')    
    
    #### SAVE FILES
    print('Saving files...')

    df_green.coalesce(1).write \
            .partitionBy('file_year', 'file_month') \
            .mode('overwrite') \
            .parquet(curated_folder_path)

    print('Successfully wrote files to Curated')