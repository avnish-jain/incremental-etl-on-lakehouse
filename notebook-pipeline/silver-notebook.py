# Databricks notebook source
import datetime

# COMMAND ----------

# STREAM
now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
dbfs_parent_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/'
parent_path = 's3://databricks-avnishjain/repo/db-cdc-log-medallion/'
silver_checkpoint_path = parent_path + 'stream/silver_cdc/' + now + 'checkpoint_path/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists avnish_jain.db_gen_cdc_demo.silver_cdc;
# MAGIC 
# MAGIC create table avnish_jain.db_gen_cdc_demo.silver_cdc (
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
# MAGIC     , delta.autoOptimize.optimizeWrite = true
# MAGIC     , delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Merge stream based on CDC operation and de-duplicate within and across batches
def merge_and_dedup_stream(df, i):
    df.createOrReplaceTempView("silver_cdc_microbatch")
    df._jdf.sparkSession().sql("""
                                  MERGE INTO avnish_jain.db_gen_cdc_demo.silver_cdc target
                                  USING
                                  (
                                        SELECT id
                                            , country
                                            , district
                                            , visit_timestamp
                                            -- Convert data timestamps to UTC timestamp
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
                                                        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY cdc_timestamp DESC, cdc_operation DESC) as rnk
                                                FROM silver_cdc_microbatch
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
       .table("avnish_jain.db_gen_cdc_demo.bronze_cdc") \
       .writeStream \
       .foreachBatch(merge_and_dedup_stream) \
       .option("checkpointLocation", silver_checkpoint_path) \
     .start()

# COMMAND ----------

# DBTITLE 1,Let's find the ID with the most amount of UPDATEs in our Bronze Table
# MAGIC %sql
# MAGIC 
# MAGIC select cdc_operation, cdc_timestamp, id, country, district, visit_timestamp, num_visitors 
# MAGIC from avnish_jain.db_gen_cdc_demo.bronze_cdc
# MAGIC where id in
# MAGIC (
# MAGIC     select id 
# MAGIC     from 
# MAGIC     (
# MAGIC         select id, count(*) as cnt from avnish_jain.db_gen_cdc_demo.bronze_cdc
# MAGIC         group by id
# MAGIC         order by cnt desc, id desc
# MAGIC         limit 1
# MAGIC     )
# MAGIC )
# MAGIC order by cdc_timestamp

# COMMAND ----------

# DBTITLE 1,Silver table only holds the latest version of that ID
# MAGIC %sql
# MAGIC 
# MAGIC select id, country, district, visit_timestamp, num_visitors, cdc_timestamp
# MAGIC from avnish_jain.db_gen_cdc_demo.silver_cdc
# MAGIC where id in
# MAGIC (
# MAGIC     select id 
# MAGIC     from 
# MAGIC     (
# MAGIC         select id, count(*) as cnt from avnish_jain.db_gen_cdc_demo.bronze_cdc
# MAGIC         group by id
# MAGIC         order by cnt desc, id desc
# MAGIC         limit 1
# MAGIC     )
# MAGIC )
# MAGIC order by cdc_timestamp
