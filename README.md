# OVERVIEW
This project builds a complete data pipeline using open data from the New York City Trip Record Data. The data consists of Yellow and Green Taxis, FHV (For-Hire Vehicles), and HVFHV (High Volume For-Hire Vehicles, such as Uber, Lyft, etc.). The data can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page#

The goal of this project is to provide users with curated data and metrics related to Ride services in New York. The data can be used for dashboards, analytics, and data science applications. Plus, the project serves as a guide on how to structure an end-to-end solution leveraging open-source tools and cloud services. And to showcase how spark can be used for massive data processing. 

# DATA ARCHITECTURE

The below diagram ilustrates the Data Architecture.

![Project Structure](architecture_diagram.png)

The project is organized into these layers:

## From files and api to Raw
**AWS Lambda**
Data will be loaded into S3 in its as-is format (parquet).
In the cloud (AWS), this is executed using Lambda. The job is triggered every month to fetch data from the previous months. 

## From Raw to Curated
**Pyspark in AWS EMR**
Data is cleaned, deduplicated and filtered. 
Data is joined with the lockup tables, and business metrics are created. No aggregation is done.

Spark is also used for testing the data. 
In the cloud this is done using AWS EMR which is spinned up by CLI commands. There are three way to run it:
• full_load: backfill the whole data (very heavy as the data has many partition with billions of rows)
• past_monhts: take a positive int parameter. E.g. past_monhts 3 will load the data from the past 3 months of available data. The spark job will overwrite the previous data with the new, so in cases of update in the sourc for past data, the curated layer will be updated as well. And will insert the new data if not available in curated yet. 
• specific_month: take a yyyymm parameter. E.g. 202501 will overwrite the 2025 January data in the curated layer, or insert with not available yet. 


# PROJECT STRUCTURE

```
src/
 └── curated/
     ├── jobs/        # Spark jobs
     ├── utils/       # Utility functions (e.g., create Spark session)
     └── tests/       # Test scripts for validating transformations
sample_data/
 └── raw/             # Raw input data
 └── curated/         # Output of curated (cleaned) jobs
 └── analytics/       # Output of analytics jobs in dbt
lockup_tables/        # Reference tables (e.g., payment types, rate codes)
notebooks/            # Jupyter Notebooks used for data exploration
```

