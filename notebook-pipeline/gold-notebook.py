# Databricks notebook source
import datetime

# COMMAND ----------

# STREAM
now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
dbfs_parent_path = 'dbfs:/avnishjain/repos/db-cdc-log-medallion/'
parent_path = 's3://databricks-avnishjain/repo/db-cdc-log-medallion/'
gold_checkpoint_path = parent_path + 'stream/gold_cdc/' + now + 'checkpoint_path/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists avnish_jain.db_gen_cdc_demo.gold_cdc_agg;
# MAGIC 
# MAGIC create table avnish_jain.db_gen_cdc_demo.gold_cdc_agg 
# MAGIC (
# MAGIC       country             string
# MAGIC     , sum_visitors        bigint
# MAGIC )
# MAGIC tblproperties (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

# COMMAND ----------

# DBTITLE 1,Let's explore what the Delta Lake Change Data Feed provides
# MAGIC %sql 
# MAGIC 
# MAGIC select    id
# MAGIC         , country
# MAGIC         , district
# MAGIC         , visit_timestamp
# MAGIC         , num_visitors
# MAGIC         , _change_type
# MAGIC         , _commit_version
# MAGIC         , _commit_timestamp 
# MAGIC from table_changes('avnish_jain.db_gen_cdc_demo.silver_cdc', 1)
# MAGIC where id =  43
# MAGIC order by _commit_version desc, _commit_timestamp desc, _change_type asc
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Incremental update of aggregate table (as opposed to constantly refreshing or by views)
def merge_into_gold_agg(df, i):
    df.createOrReplaceTempView("gold_cdc_microbatch")
    df._jdf.sparkSession().sql("""
                                    MERGE INTO avnish_jain.db_gen_cdc_demo.gold_cdc_agg target
                                    USING 
                                    (   
                                        select country, sum(delta_visitors) as delta_visitors 
                                        from
                                        (
                                            select country, delta_visitors from
                                            (
                                                select pre.country as country, (post.num_visitors - pre.num_visitors) as delta_visitors 
                                                from
                                                (select * from gold_cdc_microbatch where _change_type = 'update_preimage') pre
                                                inner join
                                                (select * from gold_cdc_microbatch where _change_type = 'update_postimage') post
                                                on pre.id = post.id
                                                and pre._commit_version = post._commit_version
                                            ) 
                                            union all
                                            (
                                                select country, num_visitors as delta_visitors
                                                from gold_cdc_microbatch inserts
                                                where _change_type = 'insert'
                                            ) 
                                            union all
                                            (
                                                select country, num_visitors * -1 as delta_visitors
                                                from gold_cdc_microbatch deletes
                                                where _change_type = 'delete'
                                            )
                                        ) 
                                        group by country
                                    ) as source
                                    on source.country = target.country
                                    WHEN MATCHED 
                                        THEN UPDATE SET target.sum_visitors = target.sum_visitors + source.delta_visitors
                                    WHEN NOT MATCHED
                                        THEN INSERT (country, sum_visitors) values (source.country, source.delta_visitors)
                                """)

spark.readStream \
       .option("readChangeData", "true") \
       .option("startingVersion", 1) \
       .table("avnish_jain.db_gen_cdc_demo.silver_cdc") \
       .writeStream \
       .foreachBatch(merge_into_gold_agg) \
       .option("checkpointLocation", gold_checkpoint_path) \
       .trigger(processingTime='5 seconds') \
      .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from avnish_jain.db_gen_cdc_demo.gold_cdc_agg
# MAGIC order by  country;

# COMMAND ----------

# DBTITLE 1,Reconciles with Silver
# MAGIC %sql 
# MAGIC 
# MAGIC select    country, sum(num_visitors) as sum_visitors
# MAGIC from      avnish_jain.db_gen_cdc_demo.silver_cdc 
# MAGIC group by  country
# MAGIC order by  country;

# COMMAND ----------

# DBTITLE 1,Reconciles with Bronze
# MAGIC %sql 
# MAGIC 
# MAGIC select country, sum(num_visitors) as sum_visitors
# MAGIC from
# MAGIC (
# MAGIC select      country
# MAGIC           , num_visitors
# MAGIC           , row_number() over (partition by id order by cdc_timestamp desc) as rnk
# MAGIC from      avnish_jain.db_gen_cdc_demo.bronze_cdc
# MAGIC qualify   rnk = 1
# MAGIC )
# MAGIC group by  country
# MAGIC order by  country;
