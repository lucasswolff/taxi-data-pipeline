import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from curated.utils.create_spark import create_spark_session
from curated.transformations.taxi_transformer import TransformerBase, TransformerYellowGreen
from curated.tests.test_curated_taxi import run_tests
from curated.utils.read_lockup_tables import ReadLockup
from curated.transformations.transform_run_mode import TransformMode

def main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path):
    #### create spark session
    spark = create_spark_session(app_name="curated_layer")
    
    #### read parquet files
    print('Reading files...')
    
    taxi = 'green'
    file_path = trans_mode.get_run_mode_files(run_mode, months, raw_folder_path, taxi) # get file path based on run parameters
    df_green_raw = spark.read.option("mergeSchema", "true").parquet(*file_path) 
    
    lockup_reader = ReadLockup()
    df_payment_type, df_ratecode, df_trip_type, df_taxi_zone = lockup_reader.read_lockup_tables(spark, lockup_folder_path)
    
    #### transform df
    print('Initiating dataframe transformation...')
    
    transformer_green = TransformerYellowGreen()
    df_green = transformer_green.make_green_consistent(df_green_raw) # add columns with defaut values to be consistent with yellow taxi
    
    transformer = TransformerBase()
    df_green = transformer.transform_df(df_green, df_payment_type, df_ratecode, df_trip_type, df_taxi_zone) 
    
    run_tests(df_green)
    print('All tests passed!')    
    
    #### upload into curated layer
    print('Saving files...')
    
    # use df_green.coalesce(2).write when in AWS
    
    if run_mode == 'full_load':
        df_green.coalesce(2).write \
            .partitionBy('file_year', 'file_month') \
            .mode('overwrite') \
            .parquet(curated_folder_path)
    
    else: 
        trans_mode.delete_parquet_folders(run_mode, months, curated_folder_path, raw_folder_path)
        
        df_green.coalesce(2).write \
            .partitionBy('file_year', 'file_month') \
            .mode('append') \
            .parquet(curated_folder_path)
    
        
    print('Successfully wrote files to Curated')
    
if __name__ == '__main__':

    trans_mode = TransformMode()
    run_mode, months = trans_mode.parse_args()
    
    ### change it when in AWS
    raw_folder_path = "sample_data/raw/green/"
    lockup_folder_path = "lockup_tables/"
    curated_folder_path = "sample_data/curated/green"

    main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)


