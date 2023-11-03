# Databricks notebook source
# MAGIC %md
# MAGIC #Oracle DB Read

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

# ---------------------------------------------------
# JDBC --> Delta Table Overwrite Every Day 
# ---------------------------------------------------
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
connection_properties = {
    "user"    : "mbceis",
    "password": "eismbc",
    "driver"  : "oracle.jdbc.OracleDriver"
}

query = "(SELECT * FROM mbceis.mbc_youtube_video)"  
df    = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)


# COMMAND ----------

from pyspark.sql.functions import date_format

df_with_partition = df.withColumn("partition_dt", date_format(df["updt_date"], "yyyyMM"))
df_with_partition.write.mode("overwrite").option("header", "true").partitionBy("partition_dt").saveAsTable("tb_youtube_video") 

# COMMAND ----------

# --------------------------------------
#  for solve problem query performance 
# --------------------------------------
df_with_partition.write.mode("overwrite").option("header", "true").partitionBy("partition_dt").parquet("s3://mbcinfo/oracle2/tb_youtube_video/")

#df        = oracle_df.withColumn("partition_dt", col("UPDT_DATE").cast("string"))
#s3_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e"
#df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("UPDT_DATE").save(s3_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # 데이터 Json 형태 저장 

# COMMAND ----------

# json_df = spark.sql("""
#     SELECT
#         a.video_id,
#         a.video_title,
#         a.channel_id, 
#         b.cnts_cd, 
#         sum(a.estimated_partner_revenue) as estimated_partner_revenue
#     FROM content_owner_estimated_revenue_a1 a, 
#         tb_youtube_video                   b
#     WHERE 1=1
#       --AND date_format(a.date, 'yyyyMMdd') between '20230901' and '20231031'
#       AND b.channel_id = a.channel_id
#       AND b.video_id   = a.video_id
#       --AND B.channel_id = 'UC9idb-NIhZrI6wkPesc3MUg'
#       --AND b.cnts_cd    = 'M00094G'
#   GROUP BY ALL
#   ORDER BY estimated_partner_revenue DESC
# """)

# COMMAND ----------

df.write.mode("overwrite").json("s3://mbcinfo/json/video-channel.json")
