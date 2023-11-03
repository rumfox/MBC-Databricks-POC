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
# MAGIC
# MAGIC location = "s3://mbcinfo/upload/mbc_youtube_video_examination/"
# MAGIC query    = "(SELECT * FROM mbceis.mbc_youtube_video)" 
# MAGIC
# MAGIC df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# MAGIC

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# COMMAND ----------

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

#s3 스테이징 영역 적재 
df.write.mode("overwrite").csv(location)
