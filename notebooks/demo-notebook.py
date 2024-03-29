# Databricks notebook source
# MAGIC %run ../setup/incremental-etl-helper

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Simplify, optimize and improve your data pipelines with incremental ETL on the Lakehouse 
# MAGIC 
# MAGIC In this guide, we will walkthrough how to build an event-driven, incremental ETL data pipeline on the Databricks Lakehouse platform. 
# MAGIC 
# MAGIC We will leverage the Medallion Architecture design pattern to organize our data tables in our Lakehouse.
# MAGIC 
# MAGIC - **Ingest source data into the Bronze layer:** Data will be incrementally ingested and appended into the Bronze layer using Databricks Autoloader.
# MAGIC - **Curate and conform data into the Silver layer:** The CDC data captured in our Bronze table will be used to re-create an up-to-date snapshot of our external operational database, using Spark Structured Streaming to incrementally process new rows in batches or continuously.
# MAGIC - **Aggregating into a business level table in the Gold layer:** We will incrementally perform the necessary aggregations over our Silver table data, leveraging Delta Change Data Feed (CDF) to track changes.
# MAGIC 
# MAGIC ### Prerequisites
# MAGIC 
# MAGIC - A Databricks account
# MAGIC - A Databricks cluster
# MAGIC   - with Unity Catalog enabled
# MAGIC   - attached with an Instance Profile with appropriate S3 privileges 
# MAGIC 
# MAGIC 
# MAGIC ### [ACTION REQUIRED] - Input Necessary Configurations
# MAGIC 
# MAGIC Once the above pre-requisites have been met, please ensure you fill in the `config-notebook` with the necessary input configurations. 
# MAGIC 
# MAGIC ```
# MAGIC #Input AWS Configurations
# MAGIC 
# MAGIC # Omit the "s3://"
# MAGIC s3_bucket = '<bucket name>'
# MAGIC 
# MAGIC # This is just the path, do not repeat the bucket name.
# MAGIC # Ensure path does not begin with a "/"
# MAGIC s3_parent_key = '<parent key>'
# MAGIC 
# MAGIC #Input Unity Catalog Configurations
# MAGIC 
# MAGIC #Note: Ensure database does not have any objects you need persisted
# MAGIC #This Demo notebook will perform a DROP CASCADE on the database
# MAGIC 
# MAGIC catalog_name = '<catalog name>'
# MAGIC database_name = '<database name>'
# MAGIC table_name = '<table name suffix>'
# MAGIC ```
# MAGIC 
# MAGIC For more information on these parameters, please refer to the README file

# COMMAND ----------

# MAGIC %run ./config-notebook

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
import os

# COMMAND ----------

# DBTITLE 1,Below configuration variables are derived from the input configurations
now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Strip any leading/trailing whitespaces
s3_bucket = s3_bucket.strip()
s3_parent_key = s3_parent_key.strip()
catalog_name = catalog_name.strip()
database_name = database_name.strip()
table_name = table_name.strip()

# AWS S3 Configurations
s3_parent_path = os.path.join('s3://', s3_bucket, s3_parent_key)
s3_raw_data_path = os.path.join(s3_parent_path, 'data/raw/')
s3_raw_data_key = os.path.join(s3_parent_key, 'data/raw/')

# Unity Catalog (UC) Configurations
bronze_table = '{0}.{1}.bronze_{2}'.format(catalog_name, database_name, table_name)
silver_table = '{0}.{1}.silver_{2}'.format(catalog_name, database_name, table_name)
gold_table = '{0}.{1}.gold_{2}'.format(catalog_name, database_name, table_name)

# UC made available in Spark Conf for SQL parameterization
spark.conf.set('db.catalog_name', catalog_name)
spark.conf.set('db.database_name', f"{catalog_name}.{database_name}")
spark.conf.set('db.bronze_table', bronze_table)
spark.conf.set('db.silver_table', silver_table)
spark.conf.set('db.gold_table', gold_table)

# Autoloader Configurations
bronze_schema_path = os.path.join(s3_parent_path,'autoloader/{0}/{1}/{2}/bronze_{3}/schema_path/'.format(now, catalog_name, database_name, table_name))
bronze_checkpoint_path = os.path.join(s3_parent_path,'autoloader/{0}/{1}/{2}/bronze_{3}/checkpoint_path/'.format(now, catalog_name, database_name, table_name))

# Delta Stream Configurations
silver_checkpoint_path = os.path.join(s3_parent_path,'streams/{0}/{1}/{2}/silver_{3}/checkpoint_path/'.format(now, catalog_name, database_name, table_name))
gold_checkpoint_path = os.path.join(s3_parent_path,'streams/{0}/{1}/{2}/gold_{3}/checkpoint_path/'.format(now, catalog_name, database_name, table_name))

# COMMAND ----------

# DBTITLE 1,Create and reset Unity Catalog databases
# MAGIC %sql 
# MAGIC 
# MAGIC CREATE CATALOG IF NOT EXISTS ${db.catalog_name};
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS ${db.database_name} CASCADE;
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS ${db.database_name};
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${db.bronze_table}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Uploading Sample CDC Logs Data into S3
# MAGIC 
# MAGIC To simulate Database CDC Logs, a sample file has been generated and kept in the `/data/` folder in the Git repository.
# MAGIC 
# MAGIC We will upload this initial raw file to our defined S3 path to mock our input from source.

# COMMAND ----------

# DBTITLE 1,Upload DB CDC Log sample file to S3 path
delete_files(s3_bucket, s3_raw_data_key)
body = open('../data/db_cdc_log_demo_sample.json').read()
upload_file(s3_bucket, s3_raw_data_key, 'db_cdc_log_demo_sample_' + now + '.json', body)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingesting CDC Logs into Bronze Layer
# MAGIC 
# MAGIC First, we need to ingest the CDC logs into Databricks. There are several ways to do this, but in this guide we will leverage Databricks Autoloader.

# COMMAND ----------

# DBTITLE 1,Source DB CDC Log file(s) will be loaded in JSON format to object storage
# Listing Amazon S3 path to see what and how many files have been landed
dbutils.fs.ls(s3_raw_data_path)

# COMMAND ----------

# DBTITLE 1,Let's explore our incoming data set!
json_df = spark.read.option("multiline","true").json(s3_raw_data_path)
display(json_df)

# COMMAND ----------

# DBTITLE 1,Let's dive deeper into an UPDATE operation
sample_json_update_df = json_df.where("id = 298")
display(sample_json_update_df)

# COMMAND ----------

# DBTITLE 1,Files landed will be automatically loaded, converted to Delta and augmented with Databricks Auto Loader
raw_cdc_stream = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.maxFilesPerTrigger", "1")  \
                .option("multiLine", "true") \
                .option("cloudFiles.inferColumnTypes", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSS][XXX]") \
                .option("cloudFiles.schemaHints", "id BIGINT, num_visitors BIGINT, visit_timestamp TIMESTAMP, cdc_timestamp TIMESTAMP") \
                .option("cloudFiles.schemaLocation",  bronze_schema_path) \
                .load(s3_raw_data_path) \
                .withColumn("data_hash", md5(concat(col("id"), col("country"), col("district"), col("visit_timestamp"), col("num_visitors")))) \
                .withColumn("file_name", input_file_name()) \
                .withColumn("insert_timestamp", current_timestamp()) \
                .writeStream    \
                .option("checkpointLocation", bronze_checkpoint_path) \
                .table(bronze_table)

# COMMAND ----------

# DBTITLE 1,Let's see what our Bronze table looks like
# MAGIC %sql
# MAGIC 
# MAGIC -- Bronze Table is loaded incrementally as Autoloader continuously listens to input path
# MAGIC -- Data is automatically converted from JSON format to more optimized-format, Delta Lake
# MAGIC -- Additional data columns using built-in functions have been added to assist with downstream processing & auditing
# MAGIC 
# MAGIC select    id
# MAGIC         , country
# MAGIC         , district
# MAGIC         , visit_timestamp
# MAGIC         , num_visitors
# MAGIC         , cdc_operation
# MAGIC         , cdc_timestamp
# MAGIC         , data_hash           -- Appended column to assist with de-duplication in Silver layer
# MAGIC         , file_name           -- Appended column to easily enable data provenance (i.e. tracking record to the original data file) 
# MAGIC         , insert_timestamp    -- Appended column for auditing purposes 
# MAGIC from ${db.bronze_table}
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Cleanse and conform data into your Silver Layer
# MAGIC 
# MAGIC Delta Streaming is a feature in Databricks that allows you to stream data into a Delta table in a Silver layer continuously. 
# MAGIC 
# MAGIC Used in combination with MERGE operation, we can incrementally update a Silver layer table with new or changed data from a streaming source.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists ${db.silver_table};
# MAGIC 
# MAGIC create table ${db.silver_table} (
# MAGIC       id                  bigint
# MAGIC     , country             string
# MAGIC     , district            string
# MAGIC     , visit_timestamp     timestamp
# MAGIC     , utc_visit_timestamp timestamp
# MAGIC     , num_visitors        bigint
# MAGIC     , file_name           string    
# MAGIC     , data_hash           string
# MAGIC     , cdc_timestamp       timestamp
# MAGIC     , insert_timestamp    timestamp
# MAGIC )
# MAGIC tblproperties (
# MAGIC       delta.enableChangeDataFeed = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Merge incremental data from Bronze with applied business logic
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

# COMMAND ----------

# DBTITLE 1,Let's explore what our Silver table looks like for a particular record!
# MAGIC %sql
# MAGIC 
# MAGIC -- Silver Table is loaded incrementally as Delta Streams continuously listens to Bronze Table
# MAGIC -- Data is MERGED into the Silver table which reflects the latest 'version' of the records
# MAGIC -- Additional business data columns using built-in functions have been added to assist with formal consumption (utc_visit_timestamp)
# MAGIC 
# MAGIC select    *
# MAGIC from      ${db.silver_table}
# MAGIC where     id = 298
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Let's compare that ID to our Bronze table to see the MERGE in action
# MAGIC %sql
# MAGIC 
# MAGIC -- Bronze Table stores ALL records as ingested from source
# MAGIC -- Leverage the Bronze layer to understand lineage of record over time (as-was reporting use-cases)
# MAGIC -- Leverage the Silver layer for current version of a record (as-is reporting use-cases)
# MAGIC 
# MAGIC select    *
# MAGIC from      ${db.bronze_table}
# MAGIC where     id = 298
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Aggregating into a business level table in the Gold layer
# MAGIC 
# MAGIC We can leverage Delta Streaming and the MERGE operation again to effectively and continuously update a Gold layer table with real-time data, providing near-instantaneous insights and analytics on the data.
# MAGIC 
# MAGIC As we are performing UPSERTS to our Silver table, we can capture those changes incrementally through the use of the Delta Change Data Feed (CDF). CDF is a feature that allows you to continuously track and expose the changes made to a Delta table as a stream.
# MAGIC 
# MAGIC Using Delta Change Data Feed brings you full CDC capabilities for any data managed in your Lakehouse, bringing several benefits:
# MAGIC 
# MAGIC - **Improve ETL Pipelines:** Process less data during your ETL pipelines to increase efficiency and performance
# MAGIC - **Unify batch and streaming:** Apply a common change format for both batch and streaming updates, inserts and deletes
# MAGIC - **Optimize BI on your Lakehouse:** Incrementally update your pipelines as opposed to slow full refreshes or expensive dynamic query computation, over cheap cloud object storage and an open file format, lowering storage costs and removing vendor lock-in
# MAGIC 
# MAGIC We enabled CDF on our Silver table on DDL creation: 
# MAGIC 
# MAGIC ```
# MAGIC create table ${db.silver_table} (
# MAGIC   ...
# MAGIC )
# MAGIC tblproperties (
# MAGIC       delta.enableChangeDataFeed = true
# MAGIC );
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Gold Layer table will be aggregating number of visitors by country
# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists ${db.gold_table};
# MAGIC 
# MAGIC create table ${db.gold_table}
# MAGIC (
# MAGIC       country             string
# MAGIC     , sum_visitors        bigint
# MAGIC )
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,You can directly query the Delta Lake Change Data Feed via SQL
# MAGIC %sql 
# MAGIC 
# MAGIC select    id
# MAGIC         , country
# MAGIC         , district
# MAGIC         , visit_timestamp
# MAGIC         , num_visitors
# MAGIC         , _change_type          -- Databricks CDF special columns denoting if its an insert, previous image of the update, post image of the update or delete
# MAGIC         , _commit_version       -- Databricks CDF special column denoting the commit version; previous image and post image of the update will have the same commit version
# MAGIC         , _commit_timestamp     -- Databricks CDF special colummn denoting commit timestamp
# MAGIC from table_changes('${db.silver_table}', 1)
# MAGIC order by _commit_version desc, _commit_timestamp desc, _change_type asc
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Perform incremental ETL of aggregate table (as opposed to constantly refreshing or by views)
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

# COMMAND ----------

# DBTITLE 1,Let's explore our Gold aggregate table
# MAGIC %sql
# MAGIC 
# MAGIC select      *
# MAGIC from        ${db.gold_table}
# MAGIC order by    country;

# COMMAND ----------

# DBTITLE 1,Let's add a new data file with INSERTs, UPDATEs, and DUPLICATEs
new_file_create_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
new_file_name = 'custom_cdc_' + new_file_create_timestamp + '.json'

# New Data File will consist of the following records
#       ID -1   = INSERT                  [Australia, +10K Visitors]
#       ID 7    = UPDATE                  [England, 934 -> 10,934 = +10K Visitors]
#       ID -1   = INTRA-BATCH DUPLICATE   [Australia, +10K Visitors]
#       ID 298  = INTER-BATCH DUPLICATE   [Northern Ireland, 994]

new_file_body = """
[
    {
        "id": -1,
        "country": "Australia",
        "district": "District_1",
        "visit_timestamp": "2023-01-08 11:02:17",
        "num_visitors": 10000,
        "cdc_operation": "INSERT",
        "cdc_timestamp": "2023-01-08 21:32:22.987432"
    }
    ,
    {
        "id": 7,
        "country": "England",
        "district": "District_2",
        "visit_timestamp": "2023-01-08 11:02:17",
        "num_visitors": 10934,
        "cdc_operation": "UPDATE",
        "cdc_timestamp": "2023-01-09 21:37:22.987432"
    }
    ,
    {
        "id": -1,
        "country": "Australia",
        "district": "District_1",
        "visit_timestamp": "2023-01-08 11:02:17",
        "num_visitors": 10000,
        "cdc_operation": "INSERT",
        "cdc_timestamp": "2023-01-08 21:32:22.987432"
    }
    ,
    {
        "id": 298,
        "country": "Northern Ireland",
        "district": "District_8",
        "visit_timestamp": "2023-01-07 23:34:18",
        "num_visitors": 994,
        "cdc_operation": "UPDATE",
        "cdc_timestamp": "2023-01-08 21:32:23.850593"
    }
]
"""

upload_file(s3_bucket, s3_raw_data_key, new_file_name, new_file_body)

# COMMAND ----------

# DBTITLE 1,Let's see how our Gold Table has handled our edge-cases!
# MAGIC %sql
# MAGIC 
# MAGIC select      *
# MAGIC from        ${db.gold_table}
# MAGIC order by    country;

# COMMAND ----------

# DBTITLE 1,Compare & validate incremental aggregate against previous version using Databricks Time Travel
# MAGIC %sql
# MAGIC 
# MAGIC -- Performs a comparison to the previous version of the table using Databricks Time Travel
# MAGIC 
# MAGIC -- Based on our test cases, we can see:
# MAGIC --    New Insert for Australia with 10K visitors; not 20K (ignoring intra-batch duplicates)
# MAGIC --    Update on England with 10K new visitors
# MAGIC --    No change for Northern Ireland (ignoring inter-batch duplicates)
# MAGIC --    No change for Wales (no modification)
# MAGIC --    No change for Scotland (no modification)
# MAGIC 
# MAGIC select    curr.country
# MAGIC         , nvl(past.sum_visitors, 0) as prev_sum_visitors
# MAGIC         , curr.sum_visitors as curr_sum_visitors
# MAGIC         , curr.sum_visitors - nvl(past.sum_visitors, 0) as delta_visitors
# MAGIC from
# MAGIC (
# MAGIC   select      *
# MAGIC   from        ${db.gold_table} VERSION AS OF 1
# MAGIC ) past
# MAGIC right join ${db.gold_table} curr
# MAGIC on curr.country = past.country
# MAGIC ;
