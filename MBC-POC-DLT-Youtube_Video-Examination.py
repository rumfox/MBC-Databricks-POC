# Databricks notebook source
# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Live Table : mbc_youtube_video_examination 데이터 적재 

# COMMAND ----------

import dlt 
from pyspark.sql.functions import date_format
from pyspark.sql.functions import from_unixtime, unix_timestamp

mbc_youtube_video_schema = "VIDEO_ID string, CHANNEL_ID string, CNTS_CD string, CNTS_NM string, REGI_DATE date, UPDT_DATE date, partition_dt string"

@dlt.table(
    name="mbc_youtube_video_examination",
    schema=mbc_youtube_video_schema,
    partition_cols=["partition_dt"]
)
def mbc_youtube_video_examination():
    return (
        spark.readStream.format("cloudFiles")
        .schema(mbc_youtube_video_schema)
        .option("cloudFiles.format", "csv")
        .load("s3://mbcinfo/upload/mbc_youtube_video_examination/")
        .withColumn("partition_dt", from_unixtime(unix_timestamp("UPDT_DATE", "yyyyMMdd"), "yyyyMMdd").alias("partition_dt"))
    )

mbc_youtube_video_examination()
