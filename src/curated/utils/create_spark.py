from pyspark.sql import SparkSession

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
    return spark


