# Simplify, optimize and improve your data pipelines with incremental ETL on the Lakehouse

In this guide, we will walkthrough how to build an incremental ETL data pipeline on the Databricks Lakehouse platform. 

## Use-Case Overview

Your organization would like to understand and report on the total number of visitors each country receives. 

The data comes from an operational database that tracks the number of visitors in a given district at a point in time. The database has been configured to enable Change Data Capture (CDC) transaction logs, which you ingest into your Lakehouse to clean and report on. The CDC logs include new visitor measurements inserted to the operational database, as well as updates and corrections to previously recorded measurements. Additionally, it is not guaranteed that each record is delivered only once with the source system potentially providing you duplicates within a log file (intra batch/window duplicates) as well as across files (inter-batch/window duplicates). 

A sample schema of a generated change data record is shown below.

```
[
     {
         "id": 7, 
         "country": "England", 
         "district": "District_1", 
         "visit_timestamp": "2023-01-08 11:02:17", 
         "num_visitors": 10000, 
         "cdc_operation": "UPDATE", 
         "cdc_timestamp": "2023-01-08 21:32:22.987432"
     }
     ,
     ...
]
```

## Solution Overview 


![Diagram - Database CDC Log Ingest into Databricks](https://www.databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg)


We will leverage the Medallion Architecture design pattern to organize our data tables in our Lakehouse.

- **Ingest source data into the Bronze layer:** Data will be incrementally ingested and appended into the Bronze layer using Databricks Autoloader.
- **Curate and conform data into the Silver layer:** The CDC data captured in our Bronze table will be used to re-create an up-to-date snapshot of our external operational database, using Spark Structured Streaming to incrementally process new rows in batches or continuously.
- **Aggregating into a business level table in the Gold layer:** We will incrementally perform the necessary aggregations over our Silver table data, leveraging Delta Change Data Feed (CDF) to track changes.

### Prerequisites

- A Databricks account
- A Databricks cluster
  - Unity Catalog enabled
  - attached with an Instance Profile with appropriate S3 privileges 

### [ACTION] - Input Necessary Configurations

Once the above pre-requisites have been met and the `demo-notebook` cloned or imported into your Databricks workspace, please ensure you fill in the following cell (Cmd 4 of the `demo-notebook`) with the appropriate input configurations.

```
#Input AWS Configurations
s3_bucket = '<bucket name>'
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
| s3_parent_key | String | Name of the S3 Prefix within the bucket where files will be created under. **Ensure it does not begin with "/" but ends with "/".** |  incremental-etl/demo/ |
| catalog_name | String | Name of the Unity Catalog catalog name. This can be an existing or new catalog name. |  databricks-demo-catalog |
| database_name | String | Name of the Unity Catalog database name. **Please ensure this is a new database or one where you don't need the objects persisted.** | incremental-etl-db |
| table_name | String | Name of the suffix used for the table names. "Bronze_", "Silver_" and "Gold_" will be automatically added in as prefixes in the notebooks. | visitor_tbl |


### Mock Data Generation

The notebook will generate the necessary data sets and write them to the above specified S3 location.

You can also have a look at the sample data file under the `/data/` folder in the repository.


## Step 1 - Ingesting CDC Logs into Bronze Layer

Firstly, we need to ingest our source data into Databricks. We will leverage [Databricks Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html), which allows you to automatically ingest data from a variety of cloud storage sources into a Bronze layer in your Databricks workspace. 

```
raw_cdc_stream = spark.readStream \
               .format("cloudFiles") \
               .option("cloudFiles.format", "json") \
               .option("multiLine", "true") \
               .option("cloudFiles.inferColumnTypes", "true") \
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSS][XXX]") \
               .option("cloudFiles.schemaLocation",  <bronze_schema_path>) \
               .load(<raw_data_path>) \
               .withColumn("data_hash", md5(concat(col("id"), col("country"), col("district"), col("visit_timestamp"), col("num_visitors")))) \
               .withColumn("file_name", input_file_name()) \
               .withColumn("insert_timestamp", current_timestamp()) \
               .writeStream    \
               .option("checkpointLocation", <bronze_checkpoint_path>) \
               .table("<catalog>.<database>.<bronze_table>")

```

Files landed in the specified in the `<raw_data_path>` parameter will be automatically loaded into the Bronze Table incrementally through Databricks Autoloader. As files are detected and loaded, their metadata is stored under the `checkpointLocation` option ensuring that data is processed exactly once. Furthermore, Autoloader can sample and infer the schema of the files. It stores this information in the path specified in the `cloudFiles.schemaLocation` option. 

The above code snippet will load data, discover the schema and automatically convert from JSON format to more optimized-format, Delta Lake. Additionally, the code snippet appends the below columns using built-in functions to assist with downstream processing & auditing:

| Column Name | Data Type | Description | 
|---|---|---|
| data_hash | string | MD5 Hash of the specified business data columns. This will be used later in the Silver layer to handle special data edge-cases such as de-duplication. |
| file_name | string | Each record ingested will be mapped to the originating file path to enable data provenance. |
| insert_timestamp | timestamp | Denotes the timestamp when data was inserted into the Bronze table for auditing purposes |

## Step 2 - Apply CDC changes into Silver Layer

We now want to process records from our Bronze table and incrementally maintain an up-to-date version of our source in our Silver layer.

[Spark Structured Streaming](https://docs.databricks.com/structured-streaming/index.html), together with Delta Lake’s automated data versioning, allows us to incrementally read batches of changes from our Bronze table and apply them to our Silver table, similarly to how we processed new files using AutoLoader:


```
def merge_and_dedup_stream(df, i):

    # Create a temporary view based on the incremental data feed coming from Bronze table
    temp_view = 'silver_' + table_name + '_microbatch'
    df.createOrReplaceTempView(temp_view)

    # Perform a MERGE operation comparing on ID from the incremental feed and the Silver table
    #    --> INSERT when a new ID has been detected
    #    --> UPDATE only when a ID has matched and the DATA_HASH is different (i.e no duplicate values)

    # There is also an ROW_NUMBER() function performed on the incremental feed to de-duplicate on ID 
    # and retrieve only the latest within the window
    df._jdf.sparkSession().sql(f"""
                                  MERGE INTO {silver_table} target
                                  USING
                                  (
                                        SELECT id
                                            , country
                                            , district
                                            , visit_timestamp
                                            -- Append new column with converted data timestamps to UTC timestamp
                                            , to_utc_timestamp(visit_timestamp, 'Europe/Paris') as utc_visit_timestamp
                                            , num_visitors
                                            , file_name
                                            , data_hash
                                            , cdc_timestamp
                                            , insert_timestamp
                                            , cdc_operation
                                        FROM
                                        (
                                                -- Remove duplicates within a batch (e.g. quick succession UPDATES)
                                                SELECT    *
                                                        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY cdc_timestamp DESC) as rnk
                                                FROM {temp_view}
                                                QUALIFY rnk = 1
                                        )
                                    ) source
                                    on source.id = target.id
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'DELETE'
                                        THEN DELETE
                                    -- Only processes UPDATEs when there is a data change / removes inter-batch duplicates
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'UPDATE' 
                                        AND source.data_hash <> target.data_hash
                                        THEN UPDATE SET *
                                    WHEN NOT MATCHED
                                        THEN INSERT *
                                """)

spark.readStream \
       .table(bronze_table) \
       .writeStream \
       .foreachBatch(merge_and_dedup_stream) \
       .option("checkpointLocation", silver_checkpoint_path) \
     .start()
```

We will leverage foreachBatch to define a custom function to handle duplicates and merge the latest values into our Silver table using the SQL MERGE statement. We apply further curation and cleansing on matched rows marked for update ensuring we only trigger a change if the latest record value in our batch is different from our current Silver record value by comparing on the `data_hash` column created in our Bronze layer:

**Note:** It is important to highlight that Structured Spark Streaming pipelines can be set up to run continuously to support near real-time requirements or in batch mode. You can opt to process data as soon as new data becomes available; or only process the new data since your last run incrementally. Your data refresh should be scheduled to efficiently meet the SLAs required by your business. This is achieved through [trigger intervals](https://docs.databricks.com/structured-streaming/triggers.html) which define and control the timing of when your data processing occurs. 


## Step 3 - Aggregating into a Gold Layer Table

Finally, we want to aggregate our data to serve accurate and up-to-date information to our end-users on the number of visitors per country. As we are performing UPSERT and DELETE operations to our Silver table, we need a way to track changes and process only the modified rows.
We can capture these changes incrementally through the use of Delta Change Data Feed (CDF). CDF leverages Delta Lake native history tracking capabilities, to continuously track and expose the changes made to a Delta table. The data exposed by CDF tracks inserts, deletes, and updates - with special inclusion for the pre- and post-update values for updated rows. This will be key for accurately refreshing our aggregations incrementally.

CDF can easily be enabled on any Delta table by specifying the property on table creation, or by altering an existing Delta Lake table:

```

alter table <table_name> set tblproperties (delta.enableChangeDataFeed = true)

```

To perform our incremental aggregation we will process all record changes observed on the Silver table exposed by CDF to calculate the delta in the number of visitors. Depending on whether change type is one that invalidates a value previously accounted for in the aggregate (DELETE, UPDATE_PREIMAGE) or introduces a new value to add (INSERT, UPDATE_POSTIMAGE):


```
def merge_into_gold_agg(df, i):
    # Create a temporary view based on the incremental data feed coming 
    # from the Change Data Feed generated from the Silver table
    temp_view = 'gold_' + table_name + '_microbatch'
    df.createOrReplaceTempView(temp_view)

    # Perform a MERGE operation comparing on ID from the incremental CDF feed and the Gold table
    #    --> INSERT when a new COUNTRY has been detected
    #    --> UPDATE SUM_VISITORS when a COUNTRY has matched by adding DELTA
    #               DELTA aggregation is calculated by subtracting update_preimage and adding update_post_image
    #               For example: Num_visitors for a country has been updated from 5 [update_preimage] to 20 [update_postimage]
    #                            This is a delta change of +15 visitors.
    # 
    #                            This can be calculated by performing (-1 * update_preimage) + update_postimage
    #                            or (-1 * 5) + 20 = +15
    df._jdf.sparkSession().sql(f"""
                                    MERGE INTO {gold_table} target
                                    USING 
                                    (   
                                        select country, sum(delta_visitors) as delta_visitors 
                                        from
                                        (
                                            select    country
                                                    , case
                                                            -- Multiply previous value by -1 in order to subtract from total when aggregated
                                                            when _change_type = 'update_preimage'
                                                            then -1 * num_visitors

                                                            -- Multiply previous value by -1 in order to subtract from total when aggregated
                                                            when _change_type = 'delete'
                                                            then -1 * num_visitors

                                                            -- Handles case when _change_type = 'update_postimage' and 'insert'
                                                            -- In effect, adds the number of visitors to the changed or new value
                                                            else num_visitors
                                                    end as delta_visitors 
                                            from {temp_view}
                                        ) 
                                        group by country
                                    ) as source
                                    on source.country = target.country
                                    -- Update only the modified records based on delta calculated
                                    WHEN MATCHED 
                                        THEN UPDATE SET target.sum_visitors = target.sum_visitors + source.delta_visitors
                                    -- Insert new records where a country has not been seen before
                                    WHEN NOT MATCHED
                                        THEN INSERT (country, sum_visitors) values (source.country, source.delta_visitors)
                                """)

# Read CDF feed from Silver table and process micro-batches
spark.readStream \
       .option("readChangeData", "true") \
       .option("startingVersion", 1) \
       .table(silver_table) \
       .writeStream \
       .foreachBatch(merge_into_gold_agg) \
       .option("checkpointLocation", gold_checkpoint_path) \
      .start()
```

Using Delta Change Data Feed brings you full CDC capabilities for any data managed in your Lakehouse, bringing several benefits:

- **Improve ETL Pipelines:** Process less data during your ETL pipelines to increase efficiency and performance
- **Unify batch and streaming:** Apply a common change format for both batch and streaming updates, inserts and deletes
- **Optimize BI on your Lakehouse:** Incrementally update your pipelines as opposed to slow full refreshes or expensive dynamic query computation, over cheap cloud object storage and an open file format, lowering storage costs and removing vendor lock-in

## Conclusion

Adopting an incremental refresh strategy for your pipelines in your Lakehouse enables your business to improve performance and cost efficiency whilst reducing time to derive more accurate insights. Operating over new or changed data rather than full historical datasets processes less data, requires less compute resources, and reduces the amount of time needed to execute, enabling you to meet your SLAs more efficiently, consistently, and at a lower cost. 

The Databricks Lakehouse offers your teams an open environment with a simple, comprehensive and integrated set of tools to efficiently meet all your Data & AI needs, empowering your business to make better and more timely decisions.
