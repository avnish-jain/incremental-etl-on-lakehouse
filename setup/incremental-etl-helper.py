# Databricks notebook source
import boto3

# COMMAND ----------

def upload_file(bucket, key, file_name, body):
    client = boto3.client('s3')
    client.put_object(
        Body=body, 
        Bucket=bucket, 
        Key=key + file_name
    )

# COMMAND ----------

def delete_files(bucket, key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=key).delete()

# COMMAND ----------

def assert_s3_bucket(s3_bucket):
    assert s3_bucket.strip().startswith('s3://') == False, 'ERROR:\tInput Variable "s3_bucket" should not start with "s3://"'

def assert_s3_parent_key(s3_parent_key, s3_bucket):
    assert s3_parent_key.strip().startswith('/') == False, 'ERROR:\tInput Variable "s3_parent_key" should not start with "/"'
    assert s3_bucket.strip() not in s3_parent_key.strip(), 'ERROR:\tInput Variable "s3_parent_key" should not include bucket name'

def assert_uc_object_name(uc_object, object_type):
    assert '.' not in uc_object.strip(), f"ERROR:\t {object_type} name should not include '.'"
