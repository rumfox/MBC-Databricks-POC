# Databricks notebook source
# MAGIC %md
# MAGIC ####1. tag 단어 추출

# COMMAND ----------

from pyspark.sql.functions import split, explode, col

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# Set the current schema.
spark.sql("USE mbc_poc_schema")

sql_df = spark.sql("""
          SELECT
              video_id
             ,video_tags
          FROM tb_youtube_video
""")

# 'VIDEO_TAGS' 컬럼을 split 함수를 통해 분할하여 'tags' 컬럼을 생성
split_df = sql_df.withColumn("tags", split("VIDEO_TAGS", ","))

# explode 함수를 통해 'video_id'와 'tags' 컬럼 데이터로 새로운 row 생성
exploded_df = split_df.select("video_id", explode("tags").alias("video_tag"))

# 결과 출력 
exploded_df.show(20, truncate=False)

# 결과 저장, 'tb_video_tags' 테이블 덮어쓰기 모드로 실행 (불용어를 제거한 후 저장해야 함)
exploded_df.write.mode("overwrite").saveAsTable("tb_video_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. 불용어 제거
# MAGIC #####2.1 라이브러리 추가 설치

# COMMAND ----------

!pip install nltk


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 영문 불용어 확인 및 제거 (아직 못했음)
# MAGIC -- exploded_df에서 불용어를 제거한 후 테이블로 저장을 해야 함

# COMMAND ----------

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
print(stopwords.words('english'))



# COMMAND ----------

# MAGIC %md
# MAGIC ####3. 비디오/컨텐츠 정보 추가

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 콘텐츠를 전참시, 나혼자산다로 한정 
# MAGIC CREATE OR REPLACE TABLE z_report_by_tags_subset AS (
# MAGIC WITH videdo AS (
# MAGIC SELECT date, video_id, video_title
# MAGIC --     ,cnts_cd, cnts_nm 
# MAGIC     , sum(views)              AS views
# MAGIC     , sum(comments)           AS comments
# MAGIC     , sum(shares)             AS shares
# MAGIC     , sum(watch_time_minutes) AS watch_time_minutes
# MAGIC     , sum(estimated_youtube_ad_revenue)  AS estimated_youtube_ad_revenue
# MAGIC     , sum(estimated_monetized_playbacks) AS estimated_monetized_playbacks
# MAGIC     , sum(ad_impressions)     AS ad_impressions
# MAGIC FROM z_report_by_daily_video_country_cnt_rev x
# MAGIC WHERE cnts_nm like '%나혼자산다%' or cnts_nm like '%전참시%' 
# MAGIC group by date, video_id, video_title
# MAGIC --     ,cnts_cd, cnts_nm
# MAGIC )
# MAGIC
# MAGIC , tags AS (   --- 중복제거
# MAGIC SELECT video_id
# MAGIC       , UPPER(video_tag) AS video_tag
# MAGIC   FROM tb_video_tags T01
# MAGIC  GROUP BY video_id
# MAGIC       , UPPER(video_tag)
# MAGIC )
# MAGIC
# MAGIC      
# MAGIC SELECT T02.video_tag
# MAGIC      , T01.date
# MAGIC      , T01.video_id
# MAGIC      , T01.video_title
# MAGIC      , T01.views
# MAGIC      , T01.comments
# MAGIC      , T01.shares
# MAGIC      , T01.watch_time_minutes
# MAGIC      , T01.estimated_youtube_ad_revenue
# MAGIC      , T01.estimated_monetized_playbacks
# MAGIC      , T01.ad_impressions
# MAGIC   FROM videdo T01
# MAGIC LEFT OUTER JOIN tags T02
# MAGIC              ON T01.video_id = T02.video_id
# MAGIC ); 
# MAGIC
# MAGIC SELECT * FROM z_report_by_tags_subset;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -- 태그별 랭킹 순위

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT video_tag
# MAGIC      --, date --날자별
# MAGIC      --, month(date) --월별
# MAGIC      , COUNT(DISTINCT video_id)    AS video_cnt
# MAGIC      , SUM(views)                  AS views
# MAGIC      , SUM(comments)               AS comments
# MAGIC      , SUM(shares)                 AS shares
# MAGIC      , SUM(watch_time_minutes)     AS watch_time_minutes
# MAGIC      , SUM(estimated_youtube_ad_revenue)   AS estimated_youtube_ad_revenue
# MAGIC      , SUM(estimated_monetized_playbacks)  AS estimated_monetized_playbacks
# MAGIC      , SUM(ad_impressions)                AS ad_impressions
# MAGIC FROM z_report_by_tags_subset
# MAGIC GROUP BY video_tag
# MAGIC      -- , date --날자별
# MAGIC      --, month(date) --월별
# MAGIC ORDER BY 3 DESC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --월별/일별로 조회했을 때 이상치 발견(전현무, 박나래 등의 건수가 동일)
# MAGIC -- 아래와 같은 예시로 해보니 기안84내용의 video이었으나 video tag는 출연진이 다 있음

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct video_tag, video_title  FROM z_report_by_tags_subset
# MAGIC where video_id = 'mHqLbiWA3Jg'
# MAGIC

# COMMAND ----------


