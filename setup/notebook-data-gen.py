# Databricks notebook source
# MAGIC %run ./notebook-lib-install

# COMMAND ----------

from faker import Faker
import random
import datetime
import time
import json
import boto3

# COMMAND ----------

### CONSTANTS

#AWS
s3_bucket = 'databricks-avnishjain'
out_file_key = 'repo/db-cdc-log-medallion/data/raw/'

# OUTPUTS
out_file_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/data/raw/'
num_files = 5
num_records_per_file = 20
sleep_between_files = 5

# RANDOM GENERATION 
id_gen_randomizer_percentage = 0.8
num_seconds_in_day = 60*60*24
now = datetime.datetime.now()
dict_record = {}

# FAKER LIB INIT
fake = Faker()

# DATA
column_names = ['id','country','district','visit_timestamp','num_visitors','cdc_operation','cdc_timestamp']
country_list = ['England', 'Wales', 'Scotland', 'Northern Ireland']
district_list = ['District_1', 'District_2', 'District_3', 'District_4', 'District_5', 'District_6', 'District_7', 'District_8', 'District_9', 'District_10']


# COMMAND ----------

def gen_random_id():
    max_rand_id = int(num_files * num_records_per_file * id_gen_randomizer_percentage)
    return random.randint(1, max_rand_id)

def gen_random_country():
    index = random.randint(0, len(country_list)-1)
    return country_list[index]

def gen_random_district():
    index = random.randint(0, len(district_list)-1)
    return district_list[index]

def gen_random_datetime():
    num_seconds = random.randint(0, num_seconds_in_day)
    return (now - datetime.timedelta(seconds=num_seconds)).strftime('%Y-%m-%d %H:%M:%S')

def gen_random_num_visitors():
    return random.randint(100, 1000)

def gen_random_record(record_id):
    record_country = gen_random_country()
    record_district = gen_random_district()
    record_datetime = gen_random_datetime()
    record_num_visitors = gen_random_num_visitors()
    record = [record_id, record_country, record_district, record_datetime, record_num_visitors]
    return record

# COMMAND ----------

def format_record(record, format_type='json'):
    if format_type == 'csv':
        str_record = ','.join(record)
        return str_record
    else:
        dict_record = dict(zip(column_names, record))
        return json.dumps(dict_record)

def deformat_record(record, format_type='json'):
    if format_type == 'csv':
        return record.split(',')
    else:
        return list(json.loads(record).values())

def gen_cdc_insert_record(record_id):
    record = gen_random_record(record_id)
    record.append('INSERT')
    record.append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
    str_record = format_record(record, 'json')
    return str_record

def gen_cdc_update_record(record_id, record):
    record_list = deformat_record(record, 'json')
    new_visit = gen_random_num_visitors()
    while(new_visit == record_list[-2]):
        new_visit = gen_random_num_visitors()
    record_list[-3] = new_visit
    record_list[-2] = 'UPDATE'
    record_list[-1] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    str_record = format_record(record_list, 'json')
    return str_record

def gen_random_cdc_record():
    record_id = gen_random_id()
    cdc_record = ''
    
    if record_id in dict_record:
        prev_record = dict_record[record_id]
        cdc_record = gen_cdc_update_record(record_id, prev_record)
        dict_record[record_id] = cdc_record
    else:
        cdc_record = gen_cdc_insert_record(record_id)
        dict_record[record_id] = cdc_record

    return cdc_record

# COMMAND ----------

def gen_file_name(extension='.json'):
    file_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return 'db_cdc_log_medallion_'+file_datetime+extension

def format_file_content(content, format_type='json'):
    if format_type == 'csv':
        return '/n'.join(content)
    else:
        return '[' + ','.join(content) + ']'

def gen_file_content():
    contents = []
    for _ in range(num_records_per_file):
        record = gen_random_cdc_record()
        contents.append(record)
    return format_file_content(contents, 'json')

def clear_dbfs_location(path):
    dbutils.fs.rm(path, True)
    dbutils.fs.mkdirs(path)

def write_dbfs_location(path, name, body):
    dbutils.fs.put(path+name, body, True)

def write_s3_location(bucket, name, body):
    client = boto3.client('s3')
    client.put_object(
        Body=body, 
        Bucket=bucket, 
        Key=name
    )

# COMMAND ----------

dict_record = {}
for _ in range(num_files):
    file_name = gen_file_name()
    file_content = gen_file_content()
    write_s3_location(s3_bucket, out_file_key + file_name, file_content)
    time.sleep(sleep_between_files)
