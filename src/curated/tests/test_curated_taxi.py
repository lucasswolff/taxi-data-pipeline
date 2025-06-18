from pyspark.sql.functions import col, sum
from pyspark.sql.types import StringType, IntegerType, LongType, TimestampType, DoubleType, BooleanType

def run_tests(df):
    check_columns_present(df)    
    check_data_types(df)
    check_zero(df)
    check_negative(df)
    check_dupplicated(df)
    check_null(df)

def check_columns_present(df):
    print('Testing if columns are present')
    
    columns_list = [ "file_year", "file_month", "vendor_id", "pickup_datetime", "pickup_year", "pickup_month", "pickup_day", "dropoff_datetime", "dropoff_year", "dropoff_month", "dropoff_day", "pu_location_id", "pu_borough", "pu_zone", "pu_service_zone", "do_location_id", "do_borough", "do_zone", "do_service_zone", "ratecode_id", "ratecode_desc", "payment_type", "payment_type_desc", "store_and_fwd_flag", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee", 'ehail_fee', "total_amount", "trip_duration_min", "miles_per_minute", "miles_per_hour", "trip_outlier_flag", "fare_amount_per_mile", "fare_amount_per_min", "fare_amount_outlier_flag"]
    
    for column in columns_list: 
        assert column in df.columns, f"Missing '{column}'"
 
def check_data_types(df):
    print('Testing data types')
    
    expected_types = {
        "file_year": StringType,
        "file_month": StringType,
        "vendor_id": IntegerType,
        "pickup_datetime": TimestampType,
        "pickup_year": IntegerType,
        "pickup_month": IntegerType,
        "pickup_day": IntegerType,
        "dropoff_datetime": TimestampType,
        "dropoff_year": IntegerType,
        "dropoff_month": IntegerType,
        "dropoff_day": IntegerType,
        "pu_location_id": IntegerType,
        "pu_borough": StringType,
        "pu_zone": StringType,
        "pu_service_zone": StringType,
        "do_location_id": IntegerType,
        "do_borough": StringType,
        "do_zone": StringType,
        "do_service_zone": StringType,
        "ratecode_id": LongType,
        "ratecode_desc": StringType,
        "payment_type": LongType,
        "payment_type_desc": StringType,
        "store_and_fwd_flag": StringType,
        "passenger_count": LongType,
        "trip_distance": DoubleType,
        "fare_amount": DoubleType,
        "extra": DoubleType,
        "mta_tax": DoubleType,
        "tip_amount": DoubleType,
        "tolls_amount": DoubleType,
        "improvement_surcharge": DoubleType,
        "congestion_surcharge": DoubleType,
        "airport_fee": DoubleType,
        "ehail_fee": DoubleType,
        "total_amount": DoubleType,
        "trip_duration_min": DoubleType,
        "miles_per_minute": DoubleType,
        "miles_per_hour": DoubleType,
        "trip_outlier_flag": BooleanType,
        "fare_amount_per_mile": DoubleType,
        "fare_amount_per_min": DoubleType,
        "fare_amount_outlier_flag": BooleanType,
    }
    
    for col_name, expected_type in expected_types.items():
        actual_type = dict(df.dtypes)[col_name]
        assert actual_type == expected_type().simpleString(), f"Column {col_name} expected {expected_type} but got {actual_type}"
               
def check_null(df):
    print('Testing for nulls')
    
    not_nullable_columns_list = [ "file_year", "file_month", "vendor_id", "pickup_datetime", "pickup_year", "pickup_month", "pickup_day",
                                  "dropoff_datetime", "dropoff_year", "dropoff_month", "dropoff_day", "pu_location_id", "pu_borough", "pu_zone", "pu_service_zone", 
                                  "do_location_id", "do_borough", "do_zone", "do_service_zone", "ratecode_id", "ratecode_desc", "payment_type", "payment_type_desc", 
                                  "store_and_fwd_flag", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
                                  "improvement_surcharge", "congestion_surcharge", "airport_fee", 'ehail_fee', "total_amount"]
    
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in not_nullable_columns_list
    ]).collect()[0].asDict()
    
    for col_name, null_count in null_counts.items():
        assert null_count == 0, f"Null values found in column: {col_name}"
        
def check_zero(df):
    print('Testing for zeros')
    
    not_zero_columns_list = ["passenger_count"]
    
    zero_counts = df.select([
        sum((col(c) == 0).cast("int")).alias(c) for c in not_zero_columns_list
    ]).collect()[0].asDict()
    
    for col_name, zero_count in zero_counts.items():
        assert zero_count == 0, f"Zero found in column: {col_name}"
  
def check_negative(df):
    print('Testing for negatives')
    
    negative_columns_list = ["passenger_count", "trip_distance", "fare_amount", "total_amount"]
    
    negative_counts = df.select([
        sum((col(c) < 0).cast("int")).alias(c) for c in negative_columns_list
    ]).collect()[0].asDict()
    
    for col_name, negative_count in negative_counts.items():
        assert negative_count == 0, f"Negative found in column: {col_name}"      

def check_dupplicated(df):
    print('Testing for duplicated data')
    
    key_cols = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
                'trip_distance', 'fare_amount', 'pu_location_id', 'do_location_id']
    
    dup_count = df.groupBy(key_cols).count().filter('count > 1').count()
    
    assert dup_count == 0, f"{dup_count} duplicate rows found" 