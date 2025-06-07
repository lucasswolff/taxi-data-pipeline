import sys
import os
import shutil
from pyspark.sql import Window
from pyspark.sql.functions import col, when, year, month, day, expr, to_timestamp, concat_ws, lpad, coalesce
from pyspark.sql.functions import input_file_name, regexp_extract, unix_timestamp, row_number, lit
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F 

class TransformerBase:
    
    def transform_df(self, df, df_payment_type, df_ratecode, df_trip_type, df_taxi_zone):
        
        df = self.add_year_month_file(df) # adds file year and month
        df = self.correct_total_amount(df) #corrects total amount column
        df = self.correct_timestamp(df) #if timestamp is wrong, replaces if file year month
        df = self.add_pickup_dropoff_year_month_day(df) #add year, month and day for PU and DO
        df = self.filter_reversal(df) #filter both rows when reversal
        df = self.filter_zero_negatives(df) #filter zero and negatives when suitable
        df = self.filter_duplicates(df) #filter duplicated records
        df = self.replace_null_and_zero(df) #replace nulls and zeros
        df = self.joins(df, df_ratecode, df_payment_type, df_trip_type, df_taxi_zone) #join with lockup tables
        df = self.create_measures(df) #create measures like miles_per_hour, amount_per_min
        df = self.flag_outliers(df) #flag outliers based on criteria
        df = self.rename_reorder_df(df) #rename columns and reorder them
        
        return df
    
    def add_year_month_file(self, df):
        # add file year and month columns 
        
        #print(df)

        # Add filename column
        df = df.withColumn("source_file", input_file_name())

        # Extract year and month from filename using regex
        df = df.withColumn("file_year", regexp_extract("source_file", r"(\d{4})-\d{2}", 1))
        df = df.withColumn("file_month", regexp_extract("source_file", r"\d{4}-(\d{2})", 1))

        df = df.drop("source_file")
        
        return df

    def correct_total_amount(self, df):
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
            coalesce(col('Airport_fee'), lit(0)) + 
            coalesce(col('ehail_fee'), lit(0))
        )
        return df

    def correct_timestamp(self, df):
        # corrects year data quality issue by assigning the year of the file 
        # we need to be carefull here, because some trips might happen during new year's eve (who spends the turn of the year in a Taxi??)

        # pickup datetime
        df = df.withColumn(
            'pickup_datetime',
            when(year('pickup_datetime') == col('file_year'), col('pickup_datetime'))
            .when( # new's year eve (e.g. file 2024, check if trips in in 2023-24)
                (year('pickup_datetime') == col('file_year') - 1) & (month('pickup_datetime') == 12) & (day('pickup_datetime') == 31) &
                (year('dropoff_datetime') == col('file_year')) & (month('dropoff_datetime') == 1) & (day('dropoff_datetime') == 1)
                ,col('pickup_datetime'))
            .when( # new's year eve (e.g. file 2024, check if trips in in 2024-25)
                (year('pickup_datetime') == col('file_year')) & (month('pickup_datetime') == 12) & (day('pickup_datetime') == 31) &
                (year('dropoff_datetime') == col('file_year') + 1) & (month('dropoff_datetime') == 1) & (day('dropoff_datetime') == 1)
                ,col('pickup_datetime')) 
            .otherwise(expr("make_timestamp(file_year, month(pickup_datetime), day(pickup_datetime), hour(pickup_datetime), minute(pickup_datetime), second(pickup_datetime))"))
        )
            
        # dropoff datetime
        df = df.withColumn(
            'dropoff_datetime',
            when(year('dropoff_datetime') == col('file_year'), col('dropoff_datetime'))
            .when( # new's year eve (e.g. file 2024, check if trips in in 2023-24)
                (year('pickup_datetime') == col('file_year') - 1) & (month('pickup_datetime') == 12) & (day('pickup_datetime') == 31) &
                (year('dropoff_datetime') == col('file_year')) & (month('dropoff_datetime') == 1) & (day('dropoff_datetime') == 1)
                ,col('dropoff_datetime'))
            .when( # new's year eve (e.g. file 2024, check if trips in in 2024-25)
                (year('pickup_datetime') == col('file_year')) & (month('pickup_datetime') == 12) & (day('pickup_datetime') == 31) &
                (year('dropoff_datetime') == col('file_year') + 1) & (month('dropoff_datetime') == 1) & (day('dropoff_datetime') == 1)
                ,col('dropoff_datetime')) 
            .otherwise(expr("make_timestamp(file_year, month(dropoff_datetime), day(dropoff_datetime), hour(dropoff_datetime), minute(dropoff_datetime), second(dropoff_datetime))"))
        )
        
        # if dropoff ealier than pickup, replaces dropoff by pickup
        df = df.withColumn(
            'dropoff_datetime',
            when(col('dropoff_datetime') < col('pickup_datetime'), col('pickup_datetime'))
            .otherwise(col('dropoff_datetime'))
        )
        
        return df

    def add_pickup_dropoff_year_month_day(self, df):
        
        df = df\
                .withColumn('pickup_year',year(col('pickup_datetime')))\
                .withColumn('pickup_month',month(col('pickup_datetime')))\
                .withColumn('pickup_day',day(col('pickup_datetime')))\
                .withColumn('dropoff_year',year(col('dropoff_datetime')))\
                .withColumn('dropoff_month',month(col('dropoff_datetime')))\
                .withColumn('dropoff_day',day(col('dropoff_datetime')))
        
        return df

    def filter_reversal(self, df):
        # removes both rows when there's a reversal

        # finds both reversal rows
        # count = 2 and sum == 0 (e.g. 10 + (-10) = 0 -- -10 is the reversal of 10)
        df_reversal = df\
                    .groupBy('VendorID','pickup_datetime','dropoff_datetime','PULocationID','DOLocationID','trip_distance')\
                    .agg(
                        F.sum('total_amount').alias('total_amount_sum'),
                        F.count('*').alias('count'))\
                    .filter((col('total_amount_sum') == 0) & (col('count') == 2))
                
        # antijoin removes from df everything that's df_reversal           
        df = df.join(
                        df_reversal.select('VendorID','pickup_datetime','dropoff_datetime','PULocationID','DOLocationID','trip_distance'),
                        on = ['VendorID','pickup_datetime','dropoff_datetime','PULocationID','DOLocationID','trip_distance'],
                        how = 'anti'  
        )
        
        return df

    def filter_zero_negatives(self, df):
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

    def filter_duplicates(self, df):
        # remove duplicated data based on the bellow keys
        # select the one with the high total_amount. In some duplicated cases the tip or other fees was missing in one of the rows
        key_cols = ['VendorID', 'pickup_datetime', 'dropoff_datetime',
                'trip_distance', 'fare_amount', 'PULocationID', 'DOLocationID']

        window_dup = Window.partitionBy(key_cols).orderBy(df.total_amount.desc())

        df = df.withColumn('row_number', row_number().over(window_dup))

        df = df.filter('row_number == 1').drop('row_number') 
        
        return df

    def replace_null_and_zero(self, df):
        # replace null and 0 passenger_count with 1
        df = df.withColumn(
                'passenger_count',
                when((col('passenger_count').isNull()) | (col('passenger_count') == 0), 1)
                .otherwise(col('passenger_count'))
        )
        
        # replace null locations, trip_type, RatecodeID, store_and_fwd_flag, payment_type
        df = df\
            .withColumn('PULocationID', coalesce(col('PULocationID'), lit(264)))\
            .withColumn('DOLocationID',coalesce(col('DOLocationID'), lit(264)))\
            .withColumn('trip_type',coalesce(col('trip_type'), lit(3)))\
            .withColumn('RatecodeID',coalesce(col('RatecodeID'), lit(99)))\
            .withColumn('store_and_fwd_flag',coalesce(col('store_and_fwd_flag'), lit('N')))\
            .withColumn('payment_type',coalesce(col('payment_type'), lit(5)))
        
        
        # create file timestamp based on file year and month
        df = df.withColumn(
            'file_timestamp',
            to_timestamp(
                concat_ws("-", col("file_year"), lpad(col("file_month"), 2, "0"), lit("01"))
                )
        )
        
        # replaces null datetime with the file_timestamp 
        df = df.withColumn(
            'pickup_datetime',
            coalesce(col('pickup_datetime'), col('file_timestamp'))
        )
        df = df.withColumn(
            'dropoff_datetime',
            coalesce(col('dropoff_datetime'), col('file_timestamp'))
        )
        df = df.drop('file_timestamp')
            
        return df

    def joins(self, df, df_ratecode, df_payment_type, df_trip_type, df_taxi_zone):
        # join with ratecode df and bring Rate Description
        df = df.join(df_ratecode, on = 'RatecodeID', how = 'left')

        # join with payment_type df and bring payment type Description
        df = df.join(df_payment_type, on = 'payment_type', how = 'left')

        # join with trip_type df and bring trip type Description
        df = df.join(df_trip_type, on = 'trip_type', how = 'left')

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

    def create_measures(self, df):
        # creates trip duration column
        df = df.withColumn('trip_duration_min', 
                                        (unix_timestamp('dropoff_datetime') - unix_timestamp('pickup_datetime'))/60)

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

    def flag_outliers(self, df):
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

    def rename_reorder_df(self, df):
        # rename columns to use snake_case and reorder dataframe

        rename_reorder_dict = {
            "file_year": "file_year",
            "file_month": "file_month",
            "VendorID": "vendor_id",
            "pickup_datetime": "pickup_datetime",
            "pickup_year": "pickup_year",
            "pickup_month": "pickup_month",
            "pickup_day": "pickup_day",
            "dropoff_datetime": "dropoff_datetime",
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
            'ehail_fee': 'ehail_fee',
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


class TransformerYellowGreen:
    def make_yellow_consistent(self, df):
        # add columns with defaut values to be consistent with green taxi
        df = df\
                .withColumn('ehail_fee', lit(None).cast(DoubleType()))\
                .withColumn('trip_type', lit(3))
        
        df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
        
        return df
    
    def make_green_consistent(self, df):
        # add columns with defaut values to be consistent with yellow taxi
        df = df.withColumn('Airport_fee',  lit(None).cast(DoubleType()))
        
        df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
        
        return df