# Databricks notebook source
# MAGIC %md 
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 데이터 거버넌스 
# MAGIC 데이터 리니지를 관리하고 계정과 역할에 따라 데이터 접근 권한을 부여하여 보안 관리
# MAGIC <br><br/>
# MAGIC 1. Lineage
# MAGIC <br>1.1. 데이터 workflow 관리 - Pipeline Lineage, Data Transformation Lineage
# MAGIC 2. 보안
# MAGIC <br>2.1. 계정 관리 - 데이터 접근 제어
# MAGIC <br>2.2. Role 관리 - Workspace 내 권한관리 기능
# MAGIC <br>2.3. 데이터 공유 관리 - 대시보드 등 데이터 활용 솔루션 연동

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Lineage

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. 데이터 workflow 관리 - Pipeline Lineage, Data Transformation Lineage
# MAGIC 테이블을 조인해 파이프라인을 생성하고 데이터 트랜스포메이션 (집계함수 : sum, count, min, max 등) 을 통한 데이터 리니지 관리 

# COMMAND ----------

# DBTITLE 1,oracle로 가져올 데이터의 S3 폴더 생성 
# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# Specify the S3 path for the folder you want to create
s3_folder_path = "s3://mbcinfo/oracle/tb_youtube_video/"

# Use dbutils.fs.mkdirs() to create the S3 folder
dbutils.fs.mkdirs(s3_folder_path)

# -----------------------------------------------------------------------------------------------------------
#  trouble parsing the date or timestamp values in the column, likely due to a data inconsistency issue.
#  The error message suggests two potential solutions:
# -----------------------------------------------------------------------------------------------------------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# DBTITLE 1,S3에 적재된 csv 파일로 temp view 생성
# MAGIC %sql
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

# DBTITLE 1,temp view 데이터로 delta table 생성
# MAGIC %sql
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

# DBTITLE 1,data lineage 를 위해 테이블을 조인하여 리니지 테이블 생성
# MAGIC %sql 
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

# MAGIC %md
# MAGIC ![Image Alt Text](https://github.com/seungheejo/test/blob/main/img/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA%202023-10-31%20%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB%2010.46.04.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 보안

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. 계정 관리 - 데이터 접근 제어
# MAGIC workspace, table, user, group의 접근 권한을 부여하여 제어

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Role 관리 - Workspace 내 권한관리 기능
# MAGIC workspace에 엑세스할 수 있는 user 및 group에 admin 또는 user 권한을 부여하여 접근을 제어
# MAGIC <br>add permissions로 권한 추가 가능 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. 데이터 공유 관리 - 대시보드 등 데이터 활용 솔루션 연동
# MAGIC 대시보드 등 고객사내 데이터 활용 솔루션 연동 기능 검증
