# Databricks notebook source
# MAGIC %md
# MAGIC # 0. Catalog, Schema 선택

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Oracle DB 데이터 확인

# COMMAND ----------

jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
connection_properties = {
    "user"    : "mbceis",
    "password": "eismbc",
    "driver"  : "oracle.jdbc.OracleDriver"
}
query = "(SELECT COUNT(*) FROM mbceis.mbc_youtube_video)"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. 실시간 스트리밍 데이터 적재 현황 확인 
# MAGIC #### 파일생성 Event, 실시간 데이터 적재 기능 테스트 
# MAGIC #### DLT 폴더 파일 생성 --> DLT 감지 --> Auto Loader --> 데이터 변화 확인 

# COMMAND ----------

# MAGIC %fs ls s3://mbcinfo/upload/mbc_youtube_video/

# COMMAND ----------

# MAGIC %sql select count(*) from mbc_youtube_video

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. 조회 성능 테스트
# MAGIC #### 조회 조건 조정을 통한 조회 성능 비교

# COMMAND ----------

sql_df = spark.sql("""
          SELECT
              a.video_id,
              a.video_title,
              b.channel_id, 
              b.cnts_cd, 
              sum(a.estimated_partner_revenue) as estimated_partner_revenue, 
              b.VIDEO_TAGS
          FROM content_owner_estimated_revenue_a1 a, 
              tb_youtube_video                   b
          WHERE 1=1
            AND a.date BETWEEN '2022-10-01' AND '2023-10-31'
            AND b.channel_id = a.channel_id
            AND b.video_id   = a.video_id
            --AND B.channel_id = 'UC9idb-NIhZrI6wkPesc3MUg'
            --AND b.cnts_cd    =  'M00094G'
            --AND a.video_title like '%추석%'
            AND b.partition_dt between '202210' and '202310' 
        GROUP BY ALL
        ORDER BY estimated_partner_revenue DESC
""")

sql_df.show(5, truncate=True)

# COMMAND ----------

sql_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC # 4. 데이터 가공 편의성 
# MAGIC ##### SQL로 만들기 어려웠던 데이터의 간편한 가공 
# MAGIC ##### 별도 솔루션 불필요 
# MAGIC ##### Assistant 질문 1: sql_df의 "video_title" 컬럼 데이터를 space로 잘라 video_id와 word 컬럼의 데이터 프레임을 만드는 pyspark코드를 만들고 싶어.
# MAGIC ##### Assistant 질문 2: sql_df의 "VIDEO_TAGS" 컬럼 데이터를 ',' 로 잘라 video_id와 VIDEO_TAGS 컬럼의 데이터 프레임을 만드는 pyspark코드를 만들고 싶어.

# COMMAND ----------

from pyspark.sql.functions import split, explode, col

# # 'video_title'을 split 함수를 통해 분할하여 'words' 컬럼 생성
# sql_df = sql_df.withColumn("words", split(col("video_title"), " "))

# # explode 함수를 통해 'video_id'와 'word'로 구성된 새로운 row 생성
# exploded_df = sql_df.select("video_id", explode("words").alias("word"))


# 'VIDEO_TAGS' 컬럼을 split 함수를 통해 분할하여 'tags' 컬럼을 생성
split_df = sql_df.withColumn("tags", split("VIDEO_TAGS", ","))

# explode 함수를 통해 'video_id'와 'tags' 컬럼 데이터로 새로운 row 생성
exploded_df = split_df.select("video_id", explode("tags").alias("video_tag"))

# 결과 출력 
exploded_df.show(20, truncate=False)

# COMMAND ----------

#exploded_df.write.mode("overwrite").option("header", "true").saveAsTable("tb_video_id_word")

exploded_df.write.mode("overwrite").option("header", "true").saveAsTable("tb_video_tags") 

# COMMAND ----------

# MAGIC %md 
# MAGIC # 5. 추출 데이터 JSON 저장
# MAGIC #### 조회 결과 데이터 Json형태 저장
# MAGIC #### 다양한 형태로 Json 파일 읽기 : Notebook, Visual Studio, BI 등
# MAGIC #### 별도 컴퓨팅 리소스 필요 없이 데이터 다양한 활용

# COMMAND ----------

sql_df.write.mode("overwrite").json("s3://mbcinfo/json/video-channel.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC # 6. S3 저장 JSON 파일 읽어오기

# COMMAND ----------

# Read the JSON file from the S3 path
df_video_channel_json = spark.read.json("s3://mbcinfo/json/video-channel.json")

# Show the first few rows of the DataFrame
df_video_channel_json.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Databricks Linage 
# MAGIC #### 데이터 출처, 활용처 확인

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
