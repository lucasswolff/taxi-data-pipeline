import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from curated.utils.create_spark import create_spark_session

spark = create_spark_session('test_pipeline')

df = spark.read.parquet('sample_data/curated/yellow/file_year=2024/file_month=12')

df.printSchema()