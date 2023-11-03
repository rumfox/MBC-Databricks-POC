# Databricks notebook source
# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) RDB 데이터 이관 : Notebook --> JDBC --> MBC Oracle DB --> JDBC --> S3 
# MAGIC ##     Streaming 테스트 : 새로운 파일 또는 폴더가 생성되면 실시간으로 읽어와 테이블에 적재 

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
# MAGIC yyyymm   = '202305'
# MAGIC query    = f"""(
# MAGIC     SELECT * 
# MAGIC       FROM mbceis.mbc_youtube_video
# MAGIC      WHERE TO_CHAR(UPDT_DATE, 'yyyyMM') = '{yyyymm}'
# MAGIC )"""
# MAGIC df       = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# MAGIC df.write.mode("overwrite").option("delimiter", ",").option("encoding", "UTF-8").csv(f"s3://mbcinfo/upload/mbc_youtube_video/{yyyymm}")
