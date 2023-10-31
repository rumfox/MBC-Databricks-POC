# Databricks notebook source
# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Live Table : content_owner_demographics_a1 데이터 적재 

# COMMAND ----------

import dlt 
from pyspark.sql.functions import date_format
from pyspark.sql.functions import from_unixtime, unix_timestamp

content_owner_demographics_a1_schema = "date DATE,	channel_id STRING,	video_id STRING,	claimed_status STRING,	uploader_type STRING,	live_or_on_demand STRING,	subscribed_status STRING,	country_code STRING,	age_group STRING,	gender STRING,	views_percentage DOUBLE,	video_title STRING,	channel_title STRING,	program_id STRING,	program_title STRING,	matching_method STRING,	total_view_count INT,	total_comment_count INT,	total_like_count INT,	total_dislike_count INT,	video_publish_datetime TIMESTAMP, month STRING"

@dlt.table(
    name="content_owner_demographics",
    schema=content_owner_demographics_a1_schema,
    partition_cols=["month"]
)
    
def content_owner_demographics_a1():
    return (
        spark.readStream.format("cloudFiles")
        .schema(content_owner_demographics_a1_schema)
        .option("cloudFiles.format", "csv")  
        .load("s3://mbcinfo/upload/content_owner_demographics_a1/")
        .withColumn("month", from_unixtime(unix_timestamp("date", "MM"), "MM").alias("month"))
    )

content_owner_demographics_a1() 
