# Databricks notebook source
# MAGIC %run ../setup/notebook-data-gen

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

# COMMAND ----------

# AUTOLOADER
now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
parent_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/'
raw_data_path = parent_path + 'data/raw/'
bronze_schema_path = parent_path + 'autoloader/bronze_cdc/' + now + '/schema_path/'
bronze_checkpoint_path = parent_path + 'autoloader/bronze_cdc/' + now + '/checkpoint_path/'

# COMMAND ----------

# DBTITLE 1,Let's explore our incoming data
sample_file_path = dbutils.fs.ls(raw_data_path)[0][0]
dbutils.fs.head(sample_file_path)
sample_raw_data = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(sample_file_path)
display(sample_raw_data)

# COMMAND ----------

# DBTITLE 1,Let's dive deeper into an UPDATE operation
sample_update_id = sample_raw_data \
                        .groupby("id") \
                        .count() \
                        .orderBy(desc("count")) \
                        .limit(1) \
                        .select("id") \
                        .collect()[0]['id']

sample_update = sample_raw_data.where("id = " + str(sample_update_id))
display(sample_update)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists db_gen_cdc_demo;
# MAGIC drop table if exists db_gen_cdc_demo.bronze_cdc;

# COMMAND ----------

raw_cdc_stream = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.maxFilesPerTrigger", "1")  \
                .option("multiLine", "true") \
                .option("cloudFiles.inferColumnTypes", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSS][XXX]") \
                .option("cloudFiles.schemaHints", "id BIGINT, num_visitors BIGINT, visit_timestamp TIMESTAMP, cdc_timestamp TIMESTAMP") \
                .option("cloudFiles.schemaLocation",  bronze_schema_path) \
                .load(raw_data_path) \
                .withColumn("data_hash", md5(concat(col("id"), col("country"), col("district"), col("visit_timestamp"), col("num_visitors")))) \
                .withColumn("file_name", input_file_name()) \
                .withColumn("insert_timestamp", current_timestamp()) \
                .writeStream    \
                .option("checkpointLocation", bronze_checkpoint_path) \
                .trigger(processingTime='5 seconds') \
                .table("db_gen_cdc_demo.bronze_cdc")

time.sleep(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- let's make sure our table has the proper compaction settings to support streaming
# MAGIC alter table db_gen_cdc_demo.bronze_cdc set tblproperties (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
# MAGIC 
# MAGIC select    id
# MAGIC         , country
# MAGIC         , district
# MAGIC         , visit_timestamp
# MAGIC         , num_visitors
# MAGIC         , cdc_operation
# MAGIC         , cdc_timestamp
# MAGIC         , data_hash
# MAGIC         , file_name
# MAGIC         , insert_timestamp 
# MAGIC from db_gen_cdc_demo.bronze_cdc
# MAGIC order by cdc_timestamp
# MAGIC ;
