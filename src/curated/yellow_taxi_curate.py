import sys
import os
import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, year, month, day, expr, to_timestamp, concat_ws, lpad, coalesce
from pyspark.sql.functions import input_file_name, regexp_extract, unix_timestamp, row_number, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F 

def get_run_mode_files(run_mode, months, folder_path):
    
    # from function parameters, get files names for spark df 
    
    if run_mode == 'full_load':
        print('Run mode: full load')
        file_path = [folder_path + 'yellow_tripdata_*.parquet']
    
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
            file_path = [folder_path + f'yellow_tripdata_{year_file}-{month_file}.parquet']
        else:
            print('Invalid parameter.\nPlease, when using specific_month, provide yyyymm (e.g. 202404) from 2009 or after.')
            sys.exit(1)
    
    else:
        print('Invalid parameter.\nPlease use full_load, past_months or specific_month.')
        print('If past_months provide the number of months (e.g. 3). If specific_month provide yyyymm (e.g. 202404)')
        sys.exit(1)
    
    return file_path

def read_lockup_tables(spark, folder_path):
    # load payment_type df
    schema_payment_type = StructType([
        StructField('payment_type', IntegerType()),
        StructField('payment_type_desc', StringType()),
    ])

    path = folder_path + '/payment_type.csv'
    df_payment_type = spark.read.csv(path, schema=schema_payment_type, header=True)
    
    
    # load ratecode df
    schema_ratecode = StructType([
        StructField('RatecodeID', IntegerType()),
        StructField('RatecodeDesc', StringType()),
    ])

    path = folder_path + '/ratecode.csv'
    df_ratecode = spark.read.csv(path, schema=schema_ratecode, header=True)


    # load taxi zone
    schema_taxi_zone = StructType([
        StructField('LocationID', IntegerType()),
        StructField('Borough', StringType()),
        StructField('Zone', StringType()),
        StructField('service_zone', StringType()),
    ])

    path = folder_path + '/taxi_zone.csv'
    df_taxi_zone = spark.read.csv(path, schema=schema_taxi_zone, header=True)

    return df_payment_type, df_ratecode, df_taxi_zone

def add_year_month_file(df):
    # add file year and month columns 

    # Add filename column
    df = df.withColumn("source_file", input_file_name())

    # Extract year and month from filename using regex
    df = df.withColumn("file_year", regexp_extract("source_file", r"yellow_tripdata_(\d{4})-(\d{2})", 1))
    df = df.withColumn("file_month", regexp_extract("source_file", r"yellow_tripdata_(\d{4})-(\d{2})", 2))

    df = df.drop("source_file")
    
    return df

def correct_total_amount(df):
    # correct total amount column
    df = df.withColumn(
        'total_amount',
        coalesce(col('fare_amount'), lit(0)) +
        coalesce(col('extra'), lit(0)) +
        coalesce(col('mta_tax'), lit(0)) +
        coalesce(col('tip_amount'), lit(0)) +
        coalesce(col('tolls_amount'), lit(0)) +
        coalesce(col('improvement_surcharge'), lit(0)) +
        coalesce(col('congestion_surcharge'), lit(0)) +
        coalesce(col('Airport_fee'), lit(0))
    )
    return df

def correct_timestamp(df):
    # corrects year data quality issue by assigning the year of the file 
    # we need to be carefull here, because some trips might happen during new year's eve (who spends the turn of the year in a Taxi??)

    # pickup datetime
    df = df.withColumn(
        'tpep_pickup_datetime',
        when(year('tpep_pickup_datetime') == col('file_year'), col('tpep_pickup_datetime'))
        .when( # new's year eve (e.g. file 2024, check if trips in in 2023-24)
            (year('tpep_pickup_datetime') == col('file_year') - 1) & (month('tpep_pickup_datetime') == 12) & (day('tpep_pickup_datetime') == 31) &
            (year('tpep_dropoff_datetime') == col('file_year')) & (month('tpep_dropoff_datetime') == 1) & (day('tpep_dropoff_datetime') == 1)
            ,col('tpep_pickup_datetime'))
        .when( # new's year eve (e.g. file 2024, check if trips in in 2024-25)
            (year('tpep_pickup_datetime') == col('file_year')) & (month('tpep_pickup_datetime') == 12) & (day('tpep_pickup_datetime') == 31) &
            (year('tpep_dropoff_datetime') == col('file_year') + 1) & (month('tpep_dropoff_datetime') == 1) & (day('tpep_dropoff_datetime') == 1)
            ,col('tpep_pickup_datetime')) 
        .otherwise(expr("make_timestamp(file_year, month(tpep_pickup_datetime), day(tpep_pickup_datetime), hour(tpep_pickup_datetime), minute(tpep_pickup_datetime), second(tpep_pickup_datetime))"))
    )
        
    # dropoff datetime
    df = df.withColumn(
        'tpep_dropoff_datetime',
        when(year('tpep_dropoff_datetime') == col('file_year'), col('tpep_dropoff_datetime'))
        .when( # new's year eve (e.g. file 2024, check if trips in in 2023-24)
            (year('tpep_pickup_datetime') == col('file_year') - 1) & (month('tpep_pickup_datetime') == 12) & (day('tpep_pickup_datetime') == 31) &
            (year('tpep_dropoff_datetime') == col('file_year')) & (month('tpep_dropoff_datetime') == 1) & (day('tpep_dropoff_datetime') == 1)
            ,col('tpep_dropoff_datetime'))
        .when( # new's year eve (e.g. file 2024, check if trips in in 2024-25)
            (year('tpep_pickup_datetime') == col('file_year')) & (month('tpep_pickup_datetime') == 12) & (day('tpep_pickup_datetime') == 31) &
            (year('tpep_dropoff_datetime') == col('file_year') + 1) & (month('tpep_dropoff_datetime') == 1) & (day('tpep_dropoff_datetime') == 1)
            ,col('tpep_dropoff_datetime')) 
        .otherwise(expr("make_timestamp(file_year, month(tpep_dropoff_datetime), day(tpep_dropoff_datetime), hour(tpep_dropoff_datetime), minute(tpep_dropoff_datetime), second(tpep_dropoff_datetime))"))
    )
    
    # if dropoff ealier than pickup, replaces dropoff by pickup
    df = df.withColumn(
        'tpep_dropoff_datetime',
        when(col('tpep_dropoff_datetime') < col('tpep_pickup_datetime'), col('tpep_pickup_datetime'))
        .otherwise(col('tpep_dropoff_datetime'))
    )
    
    return df

def add_pickup_dropoff_year_month_day(df):
    
    df = df\
            .withColumn('pickup_year',year(col('tpep_pickup_datetime')))\
            .withColumn('pickup_month',month(col('tpep_pickup_datetime')))\
            .withColumn('pickup_day',day(col('tpep_pickup_datetime')))\
            .withColumn('dropoff_year',year(col('tpep_dropoff_datetime')))\
            .withColumn('dropoff_month',month(col('tpep_dropoff_datetime')))\
            .withColumn('dropoff_day',day(col('tpep_dropoff_datetime')))
    
    return df

def filter_reversal(df):
    # removes both rows when there's a reversal

    # finds both reversal rows
    # count = 2 and sum == 0 (e.g. 10 + (-10) = 0 -- -10 is the reversal of 10)
    df_reversal = df\
                .groupBy('VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID','trip_distance')\
                .agg(
                    F.sum('total_amount').alias('total_amount_sum'),
                    F.count('*').alias('count'))\
                .filter((col('total_amount_sum') == 0) & (col('count') == 2))
            
    # antijoin removes from df everything that's df_reversal           
    df = df.join(
                    df_reversal.select('VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID','trip_distance'),
                    on = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID','trip_distance'],
                    how = 'anti'  
    )
    
    return df

def filter_zero_negatives(df):
    # filter out negatives
    df = df.filter(
                (col('trip_distance') >= 0) &
                (col('fare_amount') >= 0) &
                (col('total_amount') >= 0)
    )

    # filter out when both trip_distance and total_amount is zero
    df = df.filter(
                (col('trip_distance') > 0) |
                (col('total_amount') > 0)
    )  
    
    return df

def filter_duplicates(df):
    # remove duplicated data based on the bellow keys
    # select the one with the high total_amount. In some duplicated cases the tip or other fees was missing in one of the rows
    key_cols = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'trip_distance', 'fare_amount', 'PULocationID', 'DOLocationID']

    window_dup = Window.partitionBy(key_cols).orderBy(df.total_amount.desc())

    df = df.withColumn('row_number', row_number().over(window_dup))

    df = df.filter('row_number == 1').drop('row_number') 
    
    return df

def replace_null_and_zero(df):
    # replace null and 0 passenger_count with 1
    df = df.withColumn(
            'passenger_count',
            when((col('passenger_count').isNull()) | (col('passenger_count') == 0), 1)
            .otherwise(col('passenger_count'))
    )

    #replace null store_and_fwd_flag
    df = df.withColumn(
        'store_and_fwd_flag',
        coalesce(col('store_and_fwd_flag'), lit('N'))
    )
    #replace null RatecodeID
    df = df.withColumn(
        'RatecodeID',
        coalesce(col('RatecodeID'), lit(99))
    )
    
    # create file timestamp based on file year and month
    df = df.withColumn(
        'file_timestamp',
        to_timestamp(
            concat_ws("-", col("file_year"), lpad(col("file_month"), 2, "0"), lit("01"))
            )
    )
    
    # replaces null datetime with the file_timestamp 
    df = df.withColumn(
        'tpep_pickup_datetime',
        coalesce(col('tpep_pickup_datetime'), col('file_timestamp'))
    )
    df = df.withColumn(
        'tpep_dropoff_datetime',
        coalesce(col('tpep_dropoff_datetime'), col('file_timestamp'))
    )
    df = df.drop('file_timestamp')
    
    # replace null locations
    df = df.withColumn(
        'PULocationID',
        coalesce(col('PULocationID'), lit(264))
    )

    df = df.withColumn(
        'DOLocationID',
        coalesce(col('DOLocationID'), lit(264))
    )
    
    return df

def joins(df, df_ratecode, df_payment_type, df_taxi_zone):
    # join with ratecode df and bring Rate code Description
    df = df.join(df_ratecode, on = 'RatecodeID', how = 'left')
    #replace null payment_type 
    df = df.withColumn(
        'payment_type',
        coalesce(col('payment_type'), lit(5))
    )

    # join with ratecode df and bring Rate code Description
    df = df.join(df_payment_type, on = 'payment_type', how = 'left')

    # join df with taxi zones for Pickup
    df = df\
                        .join(df_taxi_zone, 
                        df['PULocationID'] == df_taxi_zone['LocationID'], 
                        how = 'left')\
                        .withColumnRenamed('Borough', 'PU_Borough') \
                        .withColumnRenamed('Zone', 'PU_Zone') \
                        .withColumnRenamed('service_zone', 'PU_service_zone')
    
    df = df.drop('LocationID')
                    
    # join df with taxi zones for Dropoff                
    df = df\
                        .join(df_taxi_zone, 
                        df['DOLocationID'] == df_taxi_zone['LocationID'], 
                        how = 'left')\
                        .withColumnRenamed('Borough', 'DO_Borough') \
                        .withColumnRenamed('Zone', 'DO_Zone') \
                        .withColumnRenamed('service_zone', 'DO_service_zone')
                        
    df = df.drop('LocationID')  
    
    return df

def create_measures(df):
    # creates trip duration column
    df = df.withColumn('trip_duration_min', 
                                    (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime'))/60)

    # miles per minute
    df = df.withColumn(
        'miles_per_minute', 
        coalesce(col('trip_distance')/col('trip_duration_min'), lit(0))
    )

    # miles per hour
    df = df.withColumn(
        'miles_per_hour', 
        col('miles_per_minute')*60
    )

    # fare amount per mile
    df = df.withColumn(
        'fare_amount_per_mile', 
        coalesce(col('fare_amount')/col('trip_distance'), lit(0))
    )

    # fare amount per minute
    df = df.withColumn(
        'fare_amount_per_min', 
        coalesce(col('fare_amount')/col('trip_duration_min'), lit(0))
    )
    return df

def flag_outliers(df):
    # creates the trip outlier flag
    # trip outlier --> discrepancy between the time and the distance (e.g. to many miles in a few minutes - the trip can't be that fast)

    df = df.withColumn(
        'trip_outlier_flag',
        when((col('miles_per_minute') < 0.01) | (col('miles_per_minute') > 2), True)
        .when((col('trip_distance') > 100) & (col('trip_duration_min') < 60), True)
        .when((col('trip_distance') < 20) & (col('trip_duration_min') > 60*4), True)
        .when((col('trip_distance') < 50) & (col('trip_duration_min') > 60*8), True)
        .when((col('trip_distance') < 100) & (col('trip_duration_min') > 60*12), True)
        .otherwise(False)
    )
    # creates the amount outlier flag
    # amount outlier --> discrepancy between fare amount and the time or distance (e.g. to expensive miles for a few minutes or to cheap for long trips)

    df = df.withColumn(
        'fare_amount_outlier_flag',
        when((col('fare_amount_per_mile') < 0) | (col('fare_amount_per_mile') > 30), True)
        .when((col('fare_amount_per_min') < 0) | (col('fare_amount_per_min') > 15), True)
        .when((col('fare_amount_per_min') == 0) & (col('fare_amount_per_min') == 0), True)
        .otherwise(False)
    )
    
    return df

def rename_reorder_df(df):
    # rename columns to use snake_case and reorder dataframe

    rename_reorder_dict = {
        "file_year": "file_year",
        "file_month": "file_month",
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "pickup_year": "pickup_year",
        "pickup_month": "pickup_month",
        "pickup_day": "pickup_day",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "dropoff_year": "dropoff_year",
        "dropoff_month": "dropoff_month",
        "dropoff_day": "dropoff_day",
        "PULocationID": "pu_location_id",
        "PU_Borough": "pu_borough",
        "PU_Zone": "pu_zone",
        "PU_service_zone": "pu_service_zone",
        "DOLocationID": "do_location_id",
        "DO_Borough": "do_borough",
        "DO_Zone": "do_zone",
        "DO_service_zone": "do_service_zone",
        "RatecodeID": "ratecode_id",
        "RatecodeDesc": "ratecode_desc",
        "payment_type": "payment_type",
        "payment_type_desc": "payment_type_desc",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
        "total_amount": "total_amount",
        "trip_duration_min": "trip_duration_min",
        "miles_per_minute": "miles_per_minute",
        "miles_per_hour": "miles_per_hour",
        "trip_outlier_flag": "trip_outlier_flag",
        "fare_amount_per_mile": "fare_amount_per_mile",
        "fare_amount_per_min": "fare_amount_per_min",
        "fare_amount_outlier_flag": "fare_amount_outlier_flag",
    }

    # rename using loop
    for old_name, new_name in rename_reorder_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
        
    # reorder 
    ordered_columns = list(rename_reorder_dict.values())
    df = df.select(*ordered_columns)
    return df

def transform_df(df, df_payment_type, df_ratecode, df_taxi_zone):

    df = add_year_month_file(df) # adds file year and month
    df = correct_total_amount(df) #corrects total amount column
    df = correct_timestamp(df) #if timestamp is wrong, replaces if file year month
    df = add_pickup_dropoff_year_month_day(df) #add year, month and day for PU and DO
    df = filter_reversal(df) #filter both rows when reversal
    df = filter_zero_negatives(df) #filter zero and negatives when suitable
    df = filter_duplicates(df) #filter duplicated records
    df = replace_null_and_zero(df) #replace nulls and zeros
    df = joins(df, df_ratecode, df_payment_type, df_taxi_zone) #join with lockup tables
    df = create_measures(df) #create measures like miles_per_hour, amount_per_min
    df = flag_outliers(df) #flag outliers based on criteria
    df = rename_reorder_df(df) #rename columns and reorder them

    return df

def delete_parquet_folders(run_mode, months, curated_folder_path, raw_folder_path):
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

def main(run_mode, months):
    #### create spark session
    spark = SparkSession.builder \
        .appName("curated_layer") \
        .getOrCreate()
    
    #### read parquet files
    print('Reading files...')
    raw_folder_path = "sample_data/raw/yellow/" ### mudar isso quando for subir na AWS
    
    # get file path based on run parameters
    file_path = get_run_mode_files(run_mode, months, raw_folder_path)
    df_yellow_raw = spark.read.parquet(*file_path) ### mudar isso quando for subir na AWS
  
    folder_path_lockup = "lockup_tables/" #mudar o source das lockup quando subir na aws
    df_payment_type, df_ratecode, df_taxi_zone = read_lockup_tables(spark, folder_path_lockup)
    
    #### transform df
    print('Initiating dataframe transformation...')
    df_yellow = transform_df(df_yellow_raw, df_payment_type, df_ratecode, df_taxi_zone) 
    
    #### upload into curated layer
    print('Saving files...')
    
    # mudar isso quando for subir na AWS
    
    # df_yellow.coalesce(2).write \
    #     .partitionBy('file_year', 'file_month') \
    #     .mode('overwrite') \
    #     .parquet('../sample_data/curated/yellow')
    
    curated_folder_path = 'sample_data/curated/yellow'
    
    if run_mode == 'full_load':
        df_yellow.write \
            .partitionBy('file_year', 'file_month') \
            .mode('overwrite') \
            .parquet(curated_folder_path)
    
    else: 
        delete_parquet_folders(run_mode, months, curated_folder_path, raw_folder_path)
        
        df_yellow.write \
            .partitionBy('file_year', 'file_month') \
            .mode('append') \
            .parquet(curated_folder_path)
    
        
    print('Successfully wrote files to Curated')
    
if __name__ == '__main__':
    
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
    
    # user didn't provide parameters
    else:
        run_mode = 'past_months'
        months = 3

    main(run_mode, months)


