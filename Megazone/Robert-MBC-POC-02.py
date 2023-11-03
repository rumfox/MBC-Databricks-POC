# Databricks notebook source
# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# COMMAND ----------

# Show schemas in the catalog that was set earlier.
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED mbc_youtube_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table mbc_youtube_video

# COMMAND ----------

import dlt 
from pyspark.sql.functions import date_format
from pyspark.sql.functions import from_unixtime, unix_timestamp

mbc_youtube_video_schema = "VIDEO_ID string, CHANNEL_ID string, CNTS_CD string, CNTS_NM string, REGI_DATE date, UPDT_DATE date, partition_dt string"

@dlt.table(
    name="mbc_youtube_video",
    schema=mbc_youtube_video_schema,
    partition_cols=["partition_dt"]
)
def mbc_youtube_video():
    return (
        spark.readStream.format("cloudFiles")
        .schema(mbc_youtube_video_schema)
        .option("cloudFiles.format", "csv")  
        .load("s3://mbcinfo/upload/mbc_youtube_video/")
        .withColumn("partition_dt", from_unixtime(unix_timestamp("UPDT_DATE", "yyyy-MM-dd"), "yyyy-MM-dd").alias("partition_dt"))
    )

mbc_youtube_video() 





# COMMAND ----------


# Register the DLT table as a temporary view
spark.sql("CREATE OR REPLACE TEMPORARY VIEW mbc_youtube_video_view AS SELECT * FROM mbc_youtube_video")

# Perform a SQL query
result_df = spark.sql("SELECT * FROM mbc_youtube_video_view")

# Show the results
result_df.show()




