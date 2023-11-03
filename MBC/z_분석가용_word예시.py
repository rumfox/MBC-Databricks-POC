# Databricks notebook source
# MAGIC %md
# MAGIC - 중복제거

# COMMAND ----------

create or replace  table mbc_poc_unity_catalog.mbc_poc_schema.tb_video_id_word_distinct as
(select video_id, word
 from mbc_poc_unity_catalog.mbc_poc_schema.tb_video_id_word
 group by  video_id, word
 )

# COMMAND ----------

# MAGIC %md
# MAGIC - 단어별 일자별 video제목 및 views 추출

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word AS (
# MAGIC WITH video_cnt AS (
# MAGIC   SELECT date
# MAGIC        , video_id
# MAGIC        , MAX(video_title) AS video_title
# MAGIC        , SUM(views)  AS views
# MAGIC  FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3  
# MAGIC GROUP BY date
# MAGIC        , video_id
# MAGIC )
# MAGIC SELECT T01.word
# MAGIC      , T02.date
# MAGIC      , T02.video_id
# MAGIC        , MAX(T02.video_title) AS video_title
# MAGIC        , SUM(T02.views) AS views
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.tb_video_id_word_distinct T01
# MAGIC   LEFT OUTER JOIN  video_cnt T02
# MAGIC     ON T01.video_id = T02.video_id
# MAGIC GROUP BY T01.word
# MAGIC      , T02.date
# MAGIC      , T02.video_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC - 건수가 많고 불용어 등이 많은 것 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from  mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word 
# MAGIC --select count(*)  from  mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word 

# COMMAND ----------

# MAGIC %md
# MAGIC - 월별 단어의 순위를 추정(views/video수로)
# MAGIC - veiws순으로 하지 않은 이유는 일반적인 단어의 경우([], of, BC 등등) 많은 비디오명에 있으므로 views수가 높게 나옴.  따라서 보정하기 위해서 video ID수로 나누어서 보정

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC SELECT word
# MAGIC      , MONTH(date)              AS month
# MAGIC      , SUM(views)                AS views
# MAGIC      , COUNT(distinct video_id)   AS video_cnt
# MAGIC      , ROUND(SUM(views) / COUNT(distinct video_id), 2) AS adj_value
# MAGIC   FROM  mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word 
# MAGIC  GROUP BY word, month(date)
# MAGIC  -- ORDER BY 1, 2 DESC
# MAGIC   )
# MAGIC , BASE2 AS (
# MAGIC SELECT word
# MAGIC      , month
# MAGIC      , views
# MAGIC      , video_cnt
# MAGIC      , adj_value
# MAGIC      , rank() OVER(PARTITION BY month order by adj_value desc) AS rank
# MAGIC  FROM BASE 
# MAGIC  WHERE month is not null
# MAGIC  )
# MAGIC
# MAGIC /*
# MAGIC SELECT MAX(case when month = 5 then rank else 0 end) AS MAX_5
# MAGIC      , MAX(case when month = 6 then rank else 0 end) AS MAX_6
# MAGIC      , MAX(case when month = 7 then rank else 0 end) AS MAX_7     
# MAGIC      , MAX(case when month = 8 then rank else 0 end) AS MAX_8
# MAGIC      , MAX(case when month = 9 then rank else 0 end) AS MAX_9
# MAGIC      , MAX(case when month = 10 then rank else 0 end) AS MAX_10
# MAGIC  FROM BASE2 
# MAGIC */
# MAGIC
# MAGIC /*CREATE TA
# MAGIC SELECT word
# MAGIC      , SUM(case when month = 5 then rank else 0 end) AS rank_5
# MAGIC      , SUM(case when month = 6 then rank else 0 end) AS rank_6
# MAGIC      , SUM(case when month = 7 then rank else 0 end) AS rank_7     
# MAGIC      , SUM(case when month = 8 then rank else 0 end) AS rank_8
# MAGIC      , SUM(case when month = 9 then rank else 0 end) AS rank_9
# MAGIC      , SUM(case when month = 10 then rank else 0 end) AS rank_10
# MAGIC  FROM BASE2 
# MAGIC  GROUP BY word
# MAGIC  

# COMMAND ----------

# MAGIC %md 
# MAGIC - 10월 rak 100위 위에 있는 단어의 최근 추이를, 뽑아서 봄

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word_monthly AS (
# MAGIC
# MAGIC WITH base AS (
# MAGIC SELECT word
# MAGIC      , MONTH(date)              AS month
# MAGIC      , SUM(views)                AS views
# MAGIC      , COUNT(distinct video_id)   AS video_cnt
# MAGIC      , ROUND(SUM(views) / COUNT(distinct video_id), 2) AS adj_value
# MAGIC   FROM  mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_word 
# MAGIC  GROUP BY word, month(date)
# MAGIC  -- ORDER BY 1, 2 DESC
# MAGIC   )
# MAGIC , BASE2 AS (
# MAGIC SELECT word
# MAGIC      , month
# MAGIC      , views
# MAGIC      , video_cnt
# MAGIC      , adj_value
# MAGIC      , rank() OVER(PARTITION BY month order by adj_value desc) AS rank
# MAGIC  FROM BASE 
# MAGIC  WHERE month is not null
# MAGIC  )
# MAGIC /*
# MAGIC SELECT word
# MAGIC      , SUM(case when month = 5 then rank else 0 end) AS rank_5
# MAGIC      , SUM(case when month = 6 then rank else 0 end) AS rank_6
# MAGIC      , SUM(case when month = 7 then rank else 0 end) AS rank_7     
# MAGIC      , SUM(case when month = 8 then rank else 0 end) AS rank_8
# MAGIC      , SUM(case when month = 9 then rank else 0 end) AS rank_9
# MAGIC      , SUM(case when month = 10 then rank else 0 end) AS rank_10
# MAGIC  FROM BASE2
# MAGIC GROUP BY word
# MAGIC */
# MAGIC )
# MAGIC
