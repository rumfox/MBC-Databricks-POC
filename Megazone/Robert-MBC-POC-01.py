# Databricks notebook source
# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# Show schemas in the catalog that was set earlier.
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*) from tb_youtube_video

# COMMAND ----------

# MAGIC %fs ls 
# MAGIC s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e

# COMMAND ----------

# MAGIC %fs ls  
# MAGIC `s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta.`s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e`
# MAGIC limit 5 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP SCHEMA mbc_poc_sc

# COMMAND ----------

# MAGIC %python
# MAGIC # dbutils를 사용하여 S3 버킷의 파일 목록을 확인합니다.
# MAGIC dbutils.fs.ls("s3://mbcinfo/upload/")
# MAGIC

# COMMAND ----------

# MAGIC %sh nc -vz 203.238.227.108 1521

# COMMAND ----------

# MAGIC %fs ls s3://mbcinfo/jar/dfa29338_1657_4e95_af50_4c32de68c6ec-ojdbc14.jar
# MAGIC

# COMMAND ----------

# MAGIC %fs ls s3://mbcinfo/upload
# MAGIC

# COMMAND ----------

# ----------------------------------------
# Read data from Oracle into a DataFrame
# ----------------------------------------
jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
connection_properties = {
    "user"    : "mbceis",
    "password": "eismbc",
    "driver"  : "oracle.jdbc.OracleDriver"
}

query = "(SELECT * FROM mbceis.mbc_youtube_video)"

"""select * from mbc_poc_unity_catalog.mbc_poc_schema.mbc_youtube_video
where VIDEO_ID like 'Jl9eLoNrHtg';
"""
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

# --------------------------------------------------------------
# Add a new column for partitioning using the UPDT_DATE column
# --------------------------------------------------------------
from pyspark.sql.functions import col

df = df.withColumn("partition_dt", col("UPDT_DATE").cast("string"))

# Write the data to a Delta Lake table, partitioned by UPDT_DATE
mbc_youtube_video = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/c2e4960f-2b07-424a-a963-3453afa5dad6"
df.write.format("delta").partitionBy("partition_dt").save(mbc_youtube_video)

# COMMAND ----------

# MAGIC %python
# MAGIC # dbutils를 사용하여 S3 버킷의 파일 목록을 확인합니다.
# MAGIC dbutils.fs.ls("s3://mbcinfo/upload/mbc_youtube_video/")
# MAGIC
# MAGIC #dbutils.fs.rm("s3://mbcinfo/upload/mbc_youtube_video/20231016", True)

# COMMAND ----------

df.write.mode("overwrite").csv("s3://mbcinfo/upload/mbc_youtube_video/")

# COMMAND ----------

df.write.mode("overwrite").csv("s3://mbcinfo/upload/mbc_youtube_video/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from tb_youtube_video WHERE VIDEO_ID like 'Jl9eLoNrHtg'
# MAGIC

# COMMAND ----------

624366 - 623738

# COMMAND ----------

# MAGIC %sql select * from mbc_youtube_video where updt_date is  null

# COMMAND ----------

# MAGIC %sql select count(*) from csv.`s3://mbcinfo/upload/mbc_youtube_video/20231025/`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from mbc_youtube_video a
# MAGIC where a.VIDEO_ID not in 
# MAGIC (
# MAGIC    select b.video_id from tb_youtube_video b )
# MAGIC

# COMMAND ----------

# MAGIC %sql DELETE from mbc_youtube_video

# COMMAND ----------

#s3 전일 파일 삭제 
dbutils.fs.rm(f"s3://mbcinfo/upload/mbc_youtube_video/20231025/", True)

# COMMAND ----------

dbutils.fs.ls("s3://mbcinfo/upload/mbc_youtube_video/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`s3://mbcinfo/upload/mbc_youtube_video/` limit 3; 

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW mbc_youtube_video_temp
# MAGIC (
# MAGIC   VIDEO_ID     string,
# MAGIC   CHANNEL_ID   string,
# MAGIC   CNTS_CD      string,
# MAGIC   CNTS_NM      string,
# MAGIC   REGI_DATE    date,
# MAGIC   UPDT_DATE    date,
# MAGIC   VIDEO_TAGS   string
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "s3://mbcinfo/upload/mbc_youtube_video/",
# MAGIC   header = "false"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mbc_youtube_video_temp LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_youtube_video 
# MAGIC USING DELTA
# MAGIC --PARTITIONED BY (month)
# MAGIC COMMENT "mbc_youtube_video from oracle db"
# MAGIC AS SELECT *  
# MAGIC FROM mbc_youtube_video_temp;  

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(1) FROM mbc_youtube_video;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.functions import from_utc_timestamp, date_format
# MAGIC
# MAGIC # ----------------------------------------
# MAGIC # Read data from Oracle into a DataFrame
# MAGIC # ----------------------------------------
# MAGIC jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
# MAGIC connection_properties = {
# MAGIC     "user"    : "mbceis",
# MAGIC     "password": "eismbc",
# MAGIC     "driver"  : "oracle.jdbc.OracleDriver"
# MAGIC }
# MAGIC
# MAGIC #utc_today = spark.sql(f"SELECT date_format(date_sub(current_date(), 1), 'yyyyMMdd')").first()[0]
# MAGIC
# MAGIC utc_today   = spark.sql("SELECT date_sub(current_timestamp(), 1) as utc_time")
# MAGIC seoul_today = utc_today.select(from_utc_timestamp(utc_today["utc_time"], "Asia/Seoul").alias("seoul_time"))
# MAGIC seoul_today = seoul_today.select(date_format(seoul_today["seoul_time"], "yyyyMMdd").alias("today"))
# MAGIC today       = int(seoul_today.first()[0])
# MAGIC
# MAGIC print(today)
# MAGIC
# MAGIC location = f"s3://mbcinfo/upload/mbc_youtube_video/{today}/"
# MAGIC
# MAGIC query = f"""(
# MAGIC     SELECT count(*) 
# MAGIC     FROM mbceis.mbc_youtube_video 
# MAGIC    WHERE 1=1
# MAGIC      AND TO_NUMBER(TO_CHAR(UPDT_DATE, 'YYYYMMDD')) >= '{today}'
# MAGIC )""" 
# MAGIC
# MAGIC df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# MAGIC df.show()
# MAGIC

# COMMAND ----------

#from pyspark.sql.functions import date_sub
# Get the current date
# current_date = spark.sql("SELECT current_date()").first()[0]

exatract_day = spark.sql(f"SELECT date_format(date_sub(current_date(), 355), 'yyyyMMdd')").first()[0]
print(exatract_day)

# COMMAND ----------

# MAGIC %fs ls s3://mbcinfo/upload/mbc_youtube_video/
# MAGIC

# COMMAND ----------

# MAGIC %sql select count(*) from mbc_youtube_video
# MAGIC

# COMMAND ----------

#dbutils.fs.ls("s3://mbcinfo/upload/mbc_youtube_video/")
#%fs ls /mbcinfo/upload/mbc_youtube_video

# COMMAND ----------

dbutils.fs.rm("s3://mbcinfo/upload/mbc_youtube_video/20231025/", True)

# COMMAND ----------

# MAGIC %python 
# MAGIC
# MAGIC delta_table_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/c2e4960f-2b07-424a-a963-3453afa5dad6"
# MAGIC df_video_channel = spark.read.format("delta").load(delta_table_path)
# MAGIC df_video_channel2 = df_video_channel.limit(10)
# MAGIC

# COMMAND ----------

display(df_video_channel2)

# COMMAND ----------

df_video_channel2.write.mode("overwrite").json("s3://mbcinfo/json/video-channel.json")

# COMMAND ----------

# Read the JSON file from the S3 path
df_video_channel_json = spark.read.json("s3://mbcinfo/json/video-channel.json")

# Show the first few rows of the DataFrame
df_video_channel_json.show()


# COMMAND ----------

# -------------------------------------------------
#  s3 파일 삭제 
# -------------------------------------------------
dbutils.fs.rm("s3://mbcinfo/json/video-channel.json", recurse=True)


# COMMAND ----------

dbutils.fs.ls("s3://mbcinfo/json/video-channel.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from json.`s3://mbcinfo/json/video-channel.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from json.`s3://mbcinfo/json/video-channel.json`
# MAGIC limit 3 

# COMMAND ----------

# MAGIC %sql desc detail mbc_youtube_video

# COMMAND ----------

# MAGIC %fs ls s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/b6b0f352-4d64-4af7-8f38-ff4bb714d544

# COMMAND ----------

spark.sql("select COUNT(*) from mbc_youtube_video").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Oracle DB Read & Write 

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

spark.sql("SHOW SCHEMAS").show()

# COMMAND ----------

# Specify the S3 path for the folder you want to create
s3_folder_path = "s3://mbcinfo/oracle/tb_youtube_video/"

# Use dbutils.fs.mkdirs() to create the S3 folder
dbutils.fs.mkdirs(s3_folder_path)

# COMMAND ----------

# -----------------------------------------------------------------------------------------------------------
#  trouble parsing the date or timestamp values in the column, likely due to a data inconsistency issue.
#  The error message suggests two potential solutions:
# -----------------------------------------------------------------------------------------------------------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW tb_youtube_video_temp
# MAGIC (
# MAGIC   VIDEO_ID     string,
# MAGIC   CHANNEL_ID   string,
# MAGIC   CNTS_CD      string,
# MAGIC   CNTS_NM      string,
# MAGIC   REGI_DATE    date,
# MAGIC   UPDT_DATE    date,
# MAGIC   VIDEO_TAGS   string
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "s3://mbcinfo/oracle/tb_youtube_video/",
# MAGIC   header = "false"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---------------------------------
# MAGIC -- Create Table for Oracle Data 
# MAGIC ---------------------------------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE tb_youtube_video
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (partition_dt)
# MAGIC AS
# MAGIC SELECT
# MAGIC   VIDEO_ID,
# MAGIC   CHANNEL_ID,
# MAGIC   CNTS_CD,
# MAGIC   CNTS_NM,
# MAGIC   REGI_DATE,
# MAGIC   UPDT_DATE,
# MAGIC   VIDEO_TAGS,
# MAGIC   CAST(UPDT_DATE AS string) AS partition_dt
# MAGIC FROM tb_youtube_video_temp;

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

query     = "(SELECT * FROM mbceis.mbc_youtube_video)"  
oracle_df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df        = oracle_df.withColumn("partition_dt", col("UPDT_DATE").cast("string"))

#s3_path = "s3://mbcinfo/oracle/tb_youtube_video"
s3_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("UPDT_DATE").save(s3_path)



# COMMAND ----------

# --------------------------------
# Confirmation Execute Result 
# --------------------------------
delta_table_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e"
df_video_channel = spark.read.format("delta").load(delta_table_path)
print(df_video_channel.count())



# COMMAND ----------

spark.sql("select COUNT(*) from tb_youtube_video").show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 'tb_youtube_video', count(*) from tb_youtube_video
# MAGIC

# COMMAND ----------

# MAGIC %fs ls "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/f939e272-d621-4f89-b572-841d3b61f42e`

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mbcinfo/oracle/tb_youtube_video"
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc detail tb_youtube_video
# MAGIC

# COMMAND ----------

# MAGIC %fs ls "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/fe66778f-bbbe-4bbf-8d9b-d8c5c5bf1ef0"

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mbcinfo/oracle/tb_youtube_video/'

# COMMAND ----------

# MAGIC %md
# MAGIC # JDBC --> Delta Lake Test

# COMMAND ----------


# -----------------------------------------------------------------------------------------
#  두번째 방법
# -----------------------------------------------------------------------------------------

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

query     = "(SELECT * FROM mbceis.mbc_youtube_video)"  
oracle_df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df        = oracle_df.withColumn("partition_dt", col("UPDT_DATE").cast("string"))

# Define the Delta Lake table location
delta_table_path = "dbfs:/mbcinfo/oracle/tb_youtube_video/"

# Define the schema for the Delta table
delta_table_schema = StructType([
    StructField("VIDEO_ID", StringType(), nullable=True),
    StructField("CHANNEL_ID", StringType(), nullable=True),
    StructField("CNTS_CD", StringType(), nullable=True),
    StructField("CNTS_NM", StringType(), nullable=True),
    StructField("REGI_DATE", DateType(), nullable=True),
    StructField("UPDT_DATE", DateType(), nullable=True),
    StructField("VIDEO_TAGS", StringType(), nullable=True)
])

spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{delta_table_path}` USING delta")

#s3_path = "s3://mbcinfo/oracle/tb_youtube_video"
s3_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/fe66778f-bbbe-4bbf-8d9b-d8c5c5bf1ef0"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("UPDT_DATE").save(s3_path)




# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE delta.`dbfs:/mbcinfo/oracle/tb_youtube_video/`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Query Performance Check 

# COMMAND ----------

sql_df = spark.sql("""
  SELECT
      a.video_id,
      a.video_title,
      b.channel_id, 
      b.cnts_cd, 
      sum(a.estimated_partner_revenue) as estimated_partner_revenue
  FROM content_owner_estimated_revenue_a1 a, 
       tb_youtube_video                   b
  WHERE 1=1
    AND date_format(a.date, 'yyyyMMdd') between '20230901' and '20230902'
    AND b.channel_id = a.channel_id
    AND b.video_id   = a.video_id
    AND B.channel_id = 'UC9idb-NIhZrI6wkPesc3MUg'
    AND b.cnts_cd    =  'M00094G'
    --AND b.partition_dt between '202307' and '202310' 
GROUP BY ALL
ORDER BY estimated_partner_revenue DESC

""")

# COMMAND ----------

sql_df.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from  tb_youtube_video 
# MAGIC where 1=1
# MAGIC   and regi_date between '2023-01-01' and '2023-01-31'
# MAGIC   --and partition_dt = '202301'
# MAGIC limit 5

# COMMAND ----------

sql_df.show()

# COMMAND ----------

sql_df.count()

# COMMAND ----------

sql_df.write.mode("overwrite").json("s3://mbcinfo/json/video-channel.json")

# COMMAND ----------

# Read the JSON file from the S3 path
df_video_channel_json = spark.read.json("s3://mbcinfo/json/video-channel.json")

# Show the first few rows of the DataFrame
df_video_channel_json.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Linage 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS LINEAGE_VIDEO_CHANNEL 
# MAGIC     SELECT
# MAGIC       a.video_id,
# MAGIC       a.video_title,
# MAGIC       b.channel_id, 
# MAGIC       b.cnts_cd, 
# MAGIC       sum(a.estimated_partner_revenue) as estimated_partner_revenue
# MAGIC   FROM content_owner_estimated_revenue_a1 a, 
# MAGIC        tb_youtube_video                   b
# MAGIC   WHERE 1=1
# MAGIC     AND date_format(a.date, 'yyyyMMdd') between '20230901' and '20230902'
# MAGIC     AND b.channel_id = a.channel_id
# MAGIC     AND b.video_id   = a.video_id
# MAGIC     AND B.channel_id = 'UC9idb-NIhZrI6wkPesc3MUg'
# MAGIC     AND b.cnts_cd    =  'M00094G'
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC DESC EXTENDED mbc_youtube_video

# COMMAND ----------

# MAGIC %fs ls s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/b6b0f352-4d64-4af7-8f38-ff4bb714d544
