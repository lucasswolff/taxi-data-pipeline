from pyspark.sql import SparkSession
import pytest

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
        
    return spark


