# Ingesting and aggregating database CDC logs into your Lakehouse Platform

In this guide, we will walk through the process of ingesting database Change Data Capture (CDC) logs into Databricks and performing basic analysis on the data.

![Diagram: Database CDC Log Ingest into Databricks](https://github.com/avnish-jain/db-cdc-log-medallion/blob/development/images/cdc-log-ingest-into-medallion.png?raw=true)

## Prerequisites

- A Databricks account
- A Databricks cluster
  - Unity Catalog enabled
  - attached with an Instance Profile with appropriate S3 privileges 

The notebooks have already been configured to write to the following S3 bucket and location:
[s3://databricks-avnishjain/repo/db-cdc-log-medallion/data/raw/](https://s3.console.aws.amazon.com/s3/buckets/databricks-avnishjain?region=us-west-2&prefix=repo/db-cdc-log-medallion/data/raw/&showversions=false).

The output tables can be queried in Databricks SQL under the following namespace:
`avnish_jain.db_gen_cdc_demo.bronze_cdc`

In the Databricks E2 Field Eng West Workspace, please ensure your cluster that you have created is **Unity Catalog enabled** and has the **shard-demo-s3-access** Instance Profile attached.

In the E2 Field Eng West AWS Account, the S3 bucket has been configured to provide the pre-configured `shard-demo-s3-access` IAM role and Instance Profile with the appropriate privileges.


## Mock Data Generation

In this guide, the notebook `notebook-data-gen` will handle this for us and write JSON files into the following S3 Location: [s3://databricks-avnishjain/repo/db-cdc-log-medallion/data/raw/](https://s3.console.aws.amazon.com/s3/buckets/databricks-avnishjain?region=us-west-2&prefix=repo/db-cdc-log-medallion/data/raw/&showversions=false).

The notebook generates mock data with the assistance of the Faker library. A sample of a record is shown below. 

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
     {
         ...
      }
]
```

## Ingesting CDC Logs into Bronze Layer

First, we need to ingest the CDC logs into Databricks. There are several ways to do this, but in this guide we will leverage Databricks Autoloader.

Databricks Autoloader is a feature that allows you to automatically ingest data from a variety of sources into a Bronze layer in your Databricks workspace.

A Bronze layer is a fast and cost-effective storage layer that is optimized for storing and querying large amounts of data. It is often used as a landing zone for raw data, before it is transformed and stored in a Silver or Gold layer for further analysis.

```
raw_cdc_stream = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("multiLine", "true") \
                .option("cloudFiles.inferColumnTypes", "true") \
                .option("cloudFiles.schemaLocation",  <bronze_schema_path>) \
                .load(raw_data_path) \
                .writeStream    \
                .option("checkpointLocation", <bronze_checkpoint_path>) \
                .table("<table_name>")
```

Using Databricks Autoloader to load data into a Bronze layer has several benefits:

- Automation: With Autoloader, you can set up a continuous data ingestion process without the need for manual intervention. This can save time and reduce the risk of errors.
- Scalability: Autoloader can handle large volumes of data and can scale up or down as needed to meet changing data ingestion demands.
- Data transformation: Autoloader allows you to perform basic data transformations, such as filtering, renaming, and aggregating, as part of the ingestion process. This can save time and resources by reducing the need for additional data transformation steps later on.
- Connectivity: Autoloader supports a wide range of data sources, including databases, cloud storage, and streaming platforms, making it easy to connect to and load data from multiple sources.

Overall, Databricks Autoloader is a useful tool for quickly and efficiently ingesting data into a Bronze layer for further analysis and processing.

## Apply CDC changes into Silver Layer

Delta Streaming is a feature in Databricks that allows you to stream data into a Delta table in a Silver layer in real-time. A Silver layer is a higher-performance storage layer that is optimized for storing and querying large amounts of structured data.

One way to use Delta Streaming is to perform a MERGE operation, which allows you to update or insert data into a Delta table based on the values of certain columns. You can use the MERGE syntax in the SELECT statement of the streaming query to specify the columns to match on and the actions to take for each type of update or insert. This can be useful when you want to continuously update a Silver layer table with new or changed data from a streaming source.

To use Delta Streaming and the MERGE operation, you will need to set up a streaming query in Databricks that reads the data from the streaming source and performs the MERGE operation on the Silver layer Delta table. In this guide, we will treat our Bronze table as a streaming source.

Using Delta Streaming and the MERGE operation, we can optimize our ETL pipeline by:
1. Processing changes made to our Bronze table incrementally and in micro-batches
2. Matching it to existing records in the Silver table based on a unique identifier (i.e the `id` field)
3. Either updating the existing records or inserting new records by including source columns in our transformation, such as the `cdc_operation`

```
def merge_stream(df, i):
    df.createOrReplaceTempView("silver_cdc_microbatch")
    df._jdf.sparkSession().sql("""
                                  MERGE INTO <target table> target
                                  USING silver_cdc_microbatch source
                                  ON source.id = target.id
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'DELETE'
                                        THEN DELETE
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'UPDATE' 
                                        THEN UPDATE SET *
                                    WHEN NOT MATCHED
                                        THEN INSERT *
                                """)

spark.readStream \
       .table("<source table>") \
       .writeStream \
       .foreachBatch(merge_stream) \
       .option("checkpointLocation", <silver_checkpoint_path>) \
     .start()
```

## Aggregating into a Gold Layer Table

We can leverage Delta Streaming and the MERGE operation again to effectively and continuously update a Gold layer table with real-time data, providing near-instantaneous insights and analytics on the data.

We can capture the changes made to our Silver table through the use of the Delta Change Data Feed (CDF). CDF is a feature that allows you to continuously track and expose the changes made to a Delta table in real-time. 

You can enable CDF on a Delta table by altering an existing table or specifying the following property on DDL creation:

```
delta.enableChangeDataFeed = true
```

Using Delta Change Data Feed has several benefits:

- Improve ETL Pipelines: Process less data during your ETL pipelines to increase efficiency and performance
- Unify batch and streaming: Apply a common change format for both batch and streaming updates, inserts and deletes
- Optimize BI on your Lakehouse: Incrementally update the data as opposed to full refresh

## Conclusion

In this guide, we learned how to ingest database CDC logs into Databricks and perform basic analysis on the data. With the power of Databricks and Apache Spark, you can now easily analyze your CDC logs to get insights into your database activity.
