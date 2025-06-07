import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from curated.utils.create_spark import create_spark_session
from curated.transformations.taxi_transformer import TransformerBase, TransformerYellowGreen
from curated.utils.read_lockup_tables import ReadLockup
from curated.transformations.transform_run_mode import TransformMode

def main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path):
    #### create spark session
    spark = create_spark_session(app_name="curated_layer")
    
    #### read parquet files
    print('Reading files...')
    
    taxi = 'yellow'
    file_path = trans_mode.get_run_mode_files(run_mode, months, raw_folder_path, taxi) # get file path based on run parameters
    df_yellow_raw = spark.read.parquet(*file_path) 
    
    lockup_reader = ReadLockup()
    df_payment_type, df_ratecode, df_trip_type, df_taxi_zone = lockup_reader.read_lockup_tables(spark, lockup_folder_path)
    
    
    #### transform df
    print('Initiating dataframe transformation...')
    
    transformer_yellow = TransformerYellowGreen()
    df_yellow = transformer_yellow.make_yellow_consistent(df_yellow_raw) # add columns with defaut values to be consistent with green taxi
    
    transformer = TransformerBase()
    df_yellow = transformer.transform_df(df_yellow, df_payment_type, df_ratecode, df_trip_type, df_taxi_zone) 
    
    
    #### upload into curated layer
    print('Saving files...')
    
    # use df_yellow.coalesce(2).write when in AWS
    
    if run_mode == 'full_load':
        df_yellow.coalesce(2).write \
            .partitionBy('file_year', 'file_month') \
            .mode('overwrite') \
            .parquet(curated_folder_path)
    
    else: 
        trans_mode.delete_parquet_folders(run_mode, months, curated_folder_path, raw_folder_path)
        
        df_yellow.coalesce(2).write \
            .partitionBy('file_year', 'file_month') \
            .mode('append') \
            .parquet(curated_folder_path)
    
        
    print('Successfully wrote files to Curated')
    
if __name__ == '__main__':

    trans_mode = TransformMode()
    run_mode, months = trans_mode.parse_args()
    
    ### change it when in AWS
    raw_folder_path = "sample_data/raw/yellow/"
    lockup_folder_path = "lockup_tables/"
    curated_folder_path = "sample_data/curated/yellow"

    main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)


