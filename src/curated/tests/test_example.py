import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from curated.utils.create_spark import create_spark_session
from pyspark.sql.functions import col, regexp_replace
from pyspark.testing.utils import assertDataFrameEqual

def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed


spark_test = create_spark_session('test_pipeline')

sample_data = [{"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28}]

original_df = spark_test.createDataFrame(sample_data)

transformed_df = remove_extra_spaces(original_df, "name")

expected_data = [{"name": "John D.", "age": 30},
    {"name": "Alice G.", "age": 25},
    {"name": "Bob T.", "age": 35},
    {"name": "Eve A.", "age": 28}]

expected_df = spark_test.createDataFrame(expected_data)

assertDataFrameEqual(transformed_df, expected_df)
assert 'name1' in transformed_df.columns, "Missing 'name1'"

print("Test passed: DataFrames are equal.")