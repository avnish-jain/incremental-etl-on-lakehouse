# Simplify, optimize and improve your data pipelines with incremental ETL on the Lakehouse

In this guide, we will walkthrough how to build an incremental ETL data pipeline on the Databricks Lakehouse platform.

Futher details can be found here in this [blogpost.](https://medium.com/@avnishjain22/simplify-optimise-and-improve-your-data-pipelines-with-incremental-etl-on-the-lakehouse-61b279afadea)

## Use-Case Overview

Your organization would like to understand and report on the total number of visitors each country receives. 

The data comes from an operational database that tracks the number of visitors in a given district at a point in time. The database has been configured to enable Change Data Capture (CDC) transaction logs, which you ingest into your Lakehouse to clean and report on. The CDC logs include new visitor measurements inserted to the operational database, as well as updates and corrections to previously recorded measurements. Additionally, it is not guaranteed that each record is delivered only once with the source system potentially providing you duplicates within a log file (intra batch/window duplicates) as well as across files (inter-batch/window duplicates). 

## Prerequisites

- A Databricks account
- A Databricks cluster
  - Unity Catalog enabled
  - attached with an Instance Profile with appropriate S3 privileges 

### [ACTION REQUIRED] - Input Necessary Configurations

Once the above pre-requisites have been met, please ensure you fill in the `config-notebook` with the necessary input configurations. 

```
#Input AWS Configurations

# Omit the "s3://"
s3_bucket = '<bucket name>'

# This is just the path, do not repeat the bucket name.
# Ensure path does not begin with a "/"
s3_parent_key = '<parent key>'

#Input Unity Catalog Configurations

#Note: Ensure database does not have any objects you need persisted
#This Demo notebook will perform a DROP CASCADE on the database

catalog_name = '<catalog name>'
database_name = '<database name>'
table_name = '<table name suffix>'
```

For more details on these parameters, please refer to the below table:

| Parameter Name | Data Type | Description | Example Value |
|---|---|---|---|
| s3_bucket | String | Name of the S3 Bucket where sample data files will be written into. **Omit the "s3://".** | databricks-incremental-etl-demo-bucket |
| s3_parent_key | String | Name of the S3 Prefix within the bucket where files will be created under. **Ensure it does not begin with "/"** |  incremental-etl/demo/ |
| catalog_name | String | Name of the Unity Catalog catalog name. This can be an existing or new catalog name. |  databricks-demo-catalog |
| database_name | String | Name of the Unity Catalog database name. **Please ensure this is a new database or one where you don't need the objects persisted.** | incremental-etl-db |
| table_name | String | Name of the suffix used for the table names. "Bronze_", "Silver_" and "Gold_" will be automatically added in as prefixes in the notebooks. | visitor_tbl |


## How To Run

Once the `config-notebook` has been filled with the neccessary input configurations, you need only to run the `demo-notebook` which will make calls to the other notebooks. 


### Mock Data Generation

The notebook will generate the necessary data sets and write them to the configured S3 location.
You can also have a look at the sample data file under the `/data/` folder in the repository.



