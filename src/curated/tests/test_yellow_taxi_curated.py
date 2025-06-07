import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from curated.utils.create_spark import create_spark_session
from pyspark.testing.utils import assertDataFrameEqual
import pyspark.pandas as ps

spark = create_spark_session('test_pipeline')

def assert_data_type():
    expected_types = {
        "vendor_id": "int",
        "pickup_datetime": "timestamp",
        "pickup_year": "int",
        "pickup_month": "int",
        "pickup_day": "int",
        "dropoff_datetime": "timestamp",
        "dropoff_year": "int",
        "dropoff_month": "int",
        "dropoff_day": "int",
        "pu_location_id": "int",
        "pu_borough": "string",
        "pu_zone": "string",
        "pu_service_zone": "string",
        "do_location_id": "int",
        "do_borough": "string",
        "do_zone": "string",
        "do_service_zone": "string",
        "ratecode_id": "bigint",
        "ratecode_desc": "string",
        "payment_type": "bigint",
        "payment_type_desc": "string",
        "store_and_fwd_flag": "string",
        "passenger_count": "bigint",
        "trip_distance": "double",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "ehail_fee": "double",
        "total_amount": "double",
        "trip_duration_min": "double",
        "miles_per_minute": "double",
        "miles_per_hour": "double",
        "trip_outlier_flag": "boolean",
        "fare_amount_per_mile": "double",
        "fare_amount_per_min": "double",
        "fare_amount_outlier_flag": "boolean"
    }


df = ps.read_excel("src\curated\tests\raw_yellow_test_input.xlsx")

