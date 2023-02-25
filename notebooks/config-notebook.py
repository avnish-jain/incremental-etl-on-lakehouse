# Databricks notebook source
# MAGIC %run ../setup/incremental-etl-helper

# COMMAND ----------

# DBTITLE 1,[ACTION] - Input Necessary Configurations
#Input AWS Configurations

# Omit the "s3://"
s3_bucket = 'databricks-avnishjain'

# Ensure path does not begin with a "/"
s3_parent_key = 'repo/db-cdc-log-medallion/'

#Input Unity Catalog Configurations

#Note: Ensure database does not have any objects you need persisted
#This Demo notebook will perform a DROP CASCADE on the database

catalog_name = 'avnish_jain'
database_name = 'db_gen_cdc_demo'
table_name = 'cdc'

# COMMAND ----------

assert_s3_bucket(s3_bucket)
assert_s3_parent_key(s3_parent_key)
