import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class ReadLockup:
   
    def read_lockup_tables(self, spark, folder_path):
        # load payment_type df
        schema_payment_type = StructType([
            StructField('payment_type', IntegerType()),
            StructField('payment_type_desc', StringType()),
        ])

        path = os.path.join(folder_path, 'payment_type.csv')
        df_payment_type = spark.read.csv(path, schema=schema_payment_type, header=True)
        
        
        # load ratecode df
        schema_ratecode = StructType([
            StructField('RatecodeID', IntegerType()),
            StructField('RatecodeDesc', StringType()),
        ])

        path = os.path.join(folder_path, 'ratecode.csv')
        df_ratecode = spark.read.csv(path, schema=schema_ratecode, header=True)

        # load trip_type df
        schema_trip_type = StructType([
            StructField('trip_type', IntegerType()),
            StructField('trip_type_desc', StringType()),
        ])

        path = os.path.join(folder_path, 'trip_type.csv')
        df_trip_type = spark.read.csv(path, schema=schema_trip_type, header=True)


        # load taxi zone
        schema_taxi_zone = StructType([
            StructField('LocationID', IntegerType()),
            StructField('Borough', StringType()),
            StructField('Zone', StringType()),
            StructField('service_zone', StringType()),
        ])

        path = os.path.join(folder_path, 'taxi_zone.csv')
        df_taxi_zone = spark.read.csv(path, schema=schema_taxi_zone, header=True)

        return df_payment_type, df_ratecode, df_trip_type, df_taxi_zone