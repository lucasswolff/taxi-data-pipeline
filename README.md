# OVERVIEW
This project builds a complete data pipeline using open data from the New York City Trip Record Data. The data consists of Yellow and Green Taxis, FHV (For-Hire Vehicles), and HVFHV (High Volume For-Hire Vehicles, such as Uber, Lyft, etc.). The data can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page#

The goal of this project is to provide users with curated data and metrics related to Ride services in New York. The data can be used for dashboards, analytics, and data science applications. Plus, the project serves as a guide on how to structure an end-to-end solution leveraging open-source tools and cloud services.

# DATA ARCHITECTURE

The below diagram ilustrates the Data Architecture.

![Project Structure](architecture_diagram.png)

The project is organized into these layers:

## From files and api to Raw
**AWS Lambda**
Data will be loaded into S3 in its as-is format (parquet).

## From Raw to Curated
**Pyspark in AWS EMR**
Data is cleaned, deduplicated and filtered. Spark is also used for testing the data. 

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

