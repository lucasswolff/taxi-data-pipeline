from pyspark.sql import SparkSession
import pytest

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
        
    return spark

# #@pytest.fixture
# def create_spark_session_test():
#     spark = SparkSession.builder \
#         .appName('test_pipeline') \
#         .getOrCreate()
        
#     yield spark
