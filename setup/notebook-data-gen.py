# Databricks notebook source
# MAGIC %run ./notebook-lib-install

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Mock Data Generation 
# MAGIC 
# MAGIC Generates random CDC data and stores into DBFS

# COMMAND ----------

from faker import Faker
import random
import datetime
import time

# COMMAND ----------

### CONSTANTS

# OUTPUTS
out_file_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/data/raw/'
num_files = 3
num_records_per_file = 20
sleep_between_files = 10

# RANDOM GENERATION 
id_gen_randomizer_percentage = 0.8
num_seconds_in_day = 60*60*24
now = datetime.datetime.now()
dict_record = {}

# FAKER LIB INIT
fake = Faker()

# DATA
schema = [('id, int'), ('country', 'string'), ('district', 'string'), ('visit_datetime', 'date'), ('num_visitors', 'int')]
country_list = ['England', 'Wales', 'Scotland', 'Northern Ireland']
district_list = ['District_1', 'District_2', 'District_3', 'District_4', 'District_5', 'District_6', 'District_7', 'District_8', 'District_9', 'District_10']


# COMMAND ----------

def gen_random_id():
    max_rand_id = int(num_files * num_records_per_file * id_gen_randomizer_percentage)
    return str(random.randint(1, max_rand_id))

def gen_random_country():
    index = random.randint(0, len(country_list)-1)
    return str(country_list[index])

def gen_random_district():
    index = random.randint(0, len(district_list)-1)
    return str(district_list[index])

def gen_random_datetime():
    num_seconds = random.randint(0, num_seconds_in_day)
    return str((now - datetime.timedelta(seconds=num_seconds)).strftime('%Y-%m-%d %H:%M:%S'))

def gen_random_num_visitors():
    return str(random.randint(100, 1000))

def gen_random_record(record_id):
    record_country = gen_random_country()
    record_district = gen_random_district()
    record_datetime = gen_random_datetime()
    record_num_visitors = gen_random_num_visitors()
    record = [record_id, record_country, record_district, record_datetime, record_num_visitors]
    return record

# COMMAND ----------

def gen_cdc_insert_record(record_id):
    record = gen_random_record(record_id)
    record.append('INSERT')
    str_record = ','.join(record)
    return str_record

def gen_cdc_update_record(record_id, record):
    record_list = record.split(',')
    new_visit = gen_random_num_visitors()
    while(new_visit == record_list[-2]):
        new_visit = gen_random_num_visitors()
    record_list[-2] = new_visit
    record_list[-1] = 'UPDATE'
    str_record = ','.join(record_list)
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

def gen_file_name():
    file_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return 'db_cdc_log_medallion_'+file_datetime+'.csv'

def gen_file_content():
    contents = ''
    for _ in range(num_records_per_file):
        line = gen_random_cdc_record() + '\n'
        contents = contents + line
    return contents

def clear_dbfs_location(path):
    dbutils.fs.rm(path, True)
    dbutils.fs.mkdirs(path)

def write_dbfs_location(path, name, body):
    dbutils.fs.put(path+name, body, True)

# COMMAND ----------

clear_dbfs_location(out_file_path)
assert (dbutils.fs.ls(out_file_path) == [])

dict_record = {}
for _ in range(num_files):
    file_name = gen_file_name()
    file_content = gen_file_content()
    write_dbfs_location(out_file_path, file_name, file_content)
    time.sleep(sleep_between_files)

# COMMAND ----------

assert( len(dbutils.fs.ls(out_file_path)) == num_files)
dbutils.fs.ls(out_file_path)
