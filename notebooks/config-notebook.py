# Databricks notebook source
# MAGIC %run ../setup/incremental-etl-helper

# COMMAND ----------

# DBTITLE 1,[ACTION] - Input Necessary Configurations
#Input AWS Configurations

# Omit the "s3://"
s3_bucket = 'your-bucket-name'

# This is just the path, do not repeat the bucket name.
# Ensure path does not begin with a "/"
s3_parent_key = 'path/to/parent-key/'

#Input Unity Catalog Configurations

#Note: Ensure database does not have any objects you need persisted
#This Demo notebook will perform a DROP CASCADE on the database

catalog_name = 'your-catalog-name'
database_name = 'your-database-name'
table_name = 'cdc'

# COMMAND ----------

assert_s3_bucket(s3_bucket)
assert_s3_parent_key(s3_parent_key, s3_bucket)
assert_uc_object_name(catalog_name, 'Catalog')
assert_uc_object_name(database_name, 'Database')
assert_uc_object_name(table_name, 'Table')
