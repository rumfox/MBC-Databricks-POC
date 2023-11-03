# Databricks notebook source
# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) RDB 데이터 이관 : Notebook --> JDBC --> MBC Oracle DB --> JDBC --> S3 

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
# MAGIC from pyspark.sql.functions import from_utc_timestamp, date_format
# MAGIC
# MAGIC location = "s3://mbcinfo/upload/mbc_youtube_video/"
# MAGIC query = "(SELECT * FROM mbceis.mbc_youtube_video)"  
# MAGIC df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# MAGIC
# MAGIC # ------------------------------------------------------------------------------------------------------------
# MAGIC # Get Today 
# MAGIC # ------------------------------------------------------------------------------------------------------------
# MAGIC # utc_today   = spark.sql("SELECT date_sub(current_timestamp(), 1) as utc_time")
# MAGIC # seoul_today = utc_today.select(from_utc_timestamp(utc_today["utc_time"], "Asia/Seoul").alias("seoul_time"))
# MAGIC # seoul_today = seoul_today.select(date_format(seoul_today["seoul_time"], "yyyyMMdd").alias("today"))
# MAGIC # today       = seoul_today.first()[0]
# MAGIC
# MAGIC # location = f"s3://mbcinfo/upload/mbc_youtube_video/{today}/"
# MAGIC
# MAGIC # query = f"""(
# MAGIC #     SELECT * 
# MAGIC #     FROM mbceis.mbc_youtube_video 
# MAGIC #    WHERE 1=1
# MAGIC #      AND TO_CHAR(UPDT_DATE, 'YYYYMMDD') = '{today}'
# MAGIC # )""" 
# MAGIC # ------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

#s3 스테이징 영역 적재 

df.write.mode("overwrite").csv("s3://mbcinfo/upload/mbc_youtube_video/")

#df.write.mode("overwrite").csv(f"s3://mbcinfo/upload/mbc_youtube_video/{today}")
