# Databricks notebook source
# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) RDB 데이터 이관 : Notebook --> JDBC --> MBC Oracle DB --> JDBC --> S3 

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
connection_properties = {
    "user"    : "mbceis",
    "password": "eismbc",
    "driver"  : "oracle.jdbc.OracleDriver"
}

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# MAGIC
# MAGIC location = "s3://mbcinfo/upload/mbc_youtube_video/"
# MAGIC query    = "(SELECT * FROM mbceis.mbc_youtube_video)"  
# MAGIC df       = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format

# ------------------------------------------------------------------------------------------------------------
#  Get Yesterday 
# ------------------------------------------------------------------------------------------------------------
utc_yesterday   = spark.sql("SELECT date_sub(current_timestamp(), 2) as utc_time")
seoul_yesterday = utc_yesterday.select(from_utc_timestamp(utc_yesterday["utc_time"], "Asia/Seoul").alias("seoul_time"))
seoul_yesterday = seoul_yesterday.select(date_format(seoul_yesterday["seoul_time"], "yyyyMMdd").alias("today"))
yesterday       = seoul_yesterday.first()[0]

# ------------------------------------------------------------------------------------------------------------
#  Get Today 
# ------------------------------------------------------------------------------------------------------------
utc_today   = spark.sql("SELECT date_sub(current_timestamp(), 1) as utc_time")
seoul_today = utc_today.select(from_utc_timestamp(utc_today["utc_time"], "Asia/Seoul").alias("seoul_time"))
seoul_today = seoul_today.select(date_format(seoul_today["seoul_time"], "yyyyMMdd").alias("today"))
today       = seoul_today.first()[0]

# COMMAND ----------

#s3 전일 파일 삭제 
dbutils.fs.rm(f"s3://mbcinfo/upload/mbc_youtube_video/{yesterday}/", True)

# COMMAND ----------

# DLT 전일 데이터 삭제
spark.sql("DELETE FROM mbc_youtube_video")

# COMMAND ----------

#s3 스테이징 영역 적재 (for recgonize by dlt)
df.write.mode("overwrite").option("delimiter", ",").option("encoding", "UTF-8").csv(f"s3://mbcinfo/upload/mbc_youtube_video/{today}")
