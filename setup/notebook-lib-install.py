# Databricks notebook source
pip install Faker==15.3.4

# COMMAND ----------

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

def display_slide(slide_id, slide_number):
	displayHTML(f'''
	<div style="width:1750px; margin:auto">
	<iframe
		src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}" 
		frameborder="0" 
		width="1150" 
		height="683"
	></iframe></div>
	''')
