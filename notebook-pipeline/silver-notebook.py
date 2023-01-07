# Databricks notebook source
# MAGIC %run ./bronze-notebook

# COMMAND ----------

import datetime

# COMMAND ----------

# STREAM
now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
parent_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/'
silver_checkpoint_path = parent_path + 'stream/silver_cdc/' + now + 'checkpoint_path/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists db_gen_cdc_demo.silver_cdc;
# MAGIC 
# MAGIC create table db_gen_cdc_demo.silver_cdc (
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
# MAGIC tblproperties (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

# COMMAND ----------

def merge_and_dedup_stream(df, i):
    df.createOrReplaceTempView("silver_cdc_microbatch")
    df._jdf.sparkSession().sql("""
                                  MERGE INTO db_gen_cdc_demo.silver_cdc target
                                  USING
                                  (
                                        SELECT id
                                            , country
                                            , district
                                            , visit_timestamp
                                            , to_utc_timestamp(visit_timestamp, 'Europe/Paris') as utc_visit_timestamp
                                            , num_visitors
                                            , file_name
                                            , data_hash
                                            , cdc_timestamp
                                            , insert_timestamp
                                            , cdc_operation
                                        FROM
                                        (
                                                SELECT    *
                                                        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY cdc_timestamp DESC, cdc_operation DESC) as rnk
                                                FROM silver_cdc_microbatch
                                                QUALIFY rnk = 1
                                        )
                                    ) source
                                    on source.id = target.id
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'DELETE'
                                        THEN DELETE
                                    WHEN MATCHED 
                                        AND source.cdc_operation = 'UPDATE' 
                                        AND source.data_hash <> target.data_hash 
                                        THEN UPDATE SET *
                                    WHEN NOT MATCHED
                                        THEN INSERT *
                                """)

spark.readStream \
       .table("db_gen_cdc_demo.bronze_cdc") \
       .writeStream \
       .foreachBatch(merge_and_dedup_stream) \
       .option("checkpointLocation", silver_checkpoint_path) \
       .trigger(processingTime='5 seconds') \
     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select cdc_operation, cdc_timestamp, id, country, district, visit_timestamp, num_visitors 
# MAGIC from db_gen_cdc_demo.bronze_cdc
# MAGIC where id in
# MAGIC (
# MAGIC     select id 
# MAGIC     from 
# MAGIC     (
# MAGIC         select id, count(*) from db_gen_cdc_demo.bronze_cdc
# MAGIC         group by id
# MAGIC         order by 2 desc
# MAGIC         limit 1
# MAGIC     )
# MAGIC )
# MAGIC order by cdc_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select id, country, district, visit_timestamp, num_visitors, cdc_timestamp
# MAGIC from db_gen_cdc_demo.silver_cdc
# MAGIC where id in
# MAGIC (
# MAGIC     select id 
# MAGIC     from 
# MAGIC     (
# MAGIC         select id, count(*) from db_gen_cdc_demo.bronze_cdc
# MAGIC         group by id
# MAGIC         order by 2 desc
# MAGIC         limit 1
# MAGIC     )
# MAGIC )
# MAGIC order by cdc_timestamp
