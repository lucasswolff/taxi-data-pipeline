# From files and api to Raw (bronze)
**AWS Lambda**
Data will be loaded into s3 in the as-is format (parquet and json*)
*or convert into parquet to reduce storage cost

# From Raw to Curated (silver)
**Pyspark in AWS EMR**
Data will be cleaned, deduplicated and filtered. It will be stored in apache iceberg.

# From Curated to Analytics (gold): 
**dbt**
From iceberg, create business logic and metric, and store the data in redshift for querying and dashboards

# Orchestration
Apache Airflow - MWAA (Managed Workflows for Apache Airflow)

# Monitoring and Logging
AWS CloudWatch and Great expectations (maybe)


# CI/CD
**Branches:** main, dev. create new branch for new features

**Data testing**
When test fails, don't change the data
• presence: is the data there (in the file)
• data types
• not null
• uniqueness
• allowed values for enums
• freshness - is the data up to do?
• statistical methods (ex: monthly rows increased 20%)

Setup to not allow direct commit to main, but pushed through dev
Create automation so that when it goes to dev (or main) it does the ETL accordingly in the AWS _env. Dev will have just part of the data (a few months)
Once deployed in dev, triggers to change the data. For example, if created a filter in silver, it will reload all the data using this filter


# Considerations
**Bronze**
Use S3 Event Notifications + SQS to trigger AWS Lambda only when new files arrive.
Lambda has a 15 minutes limit. For the historical, do I use EMR? I think is not worth

**Silver**
Use Athena for quick Silver-layer queries (faster debugging)
Find a way to just run the cluster when needed, then terminate it

**Gold**
Use Materialized Views in Redshift for frequently used business metrics

**Airflow**
Add retries, alerts, and data quality checks (e.g., Great Expectations) in Airflow DAGs.