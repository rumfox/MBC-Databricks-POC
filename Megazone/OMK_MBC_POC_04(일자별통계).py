# Databricks notebook source
# MAGIC %md
# MAGIC ### 일자별 집계를 만듬
# MAGIC ##### 1. 일자별 이상치 여부 확인
# MAGIC ##### 2. 일자별 집계 
# MAGIC       - 일자속성 추가: PowerBI에서도 제공되나, 요일별 등등 추이현상 등을 확인하기 위해 추가
# MAGIC       - 비디오/콘텐츠 마스터 정보 추가
# MAGIC       - 총계(누계)에서 일자별 차이를 계산에서 증감수 추가 (Total은 상태값-속성이므로 절대 계산식을 적용하면 안됨)
# MAGIC       - 비디오발행일 대비 경과일수 계산 
# MAGIC       - 그 외의 차이, 평균, CPM 등등 PowerBI에서 생성이 가능한 관점/지표 등은 로직에 포함하지 않음
# MAGIC         . estimated_playback_based_cpm = SUM(estimated_youtube_ad_revenue)/SUM(estimated_monetized_playbacks)*1000
# MAGIC         . estimated_cpm=SUM(estimated_youtube_ad_revenue)/SUM(ad_impressions)*1000
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. 일자별 total수치 이상치 여부 확인 ==> 이상치 없음

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, video_id, video_id, program_id, count(*)
# MAGIC  FROM (SELECT date
# MAGIC             , channel_id
# MAGIC             , video_id
# MAGIC             , program_id
# MAGIC             , total_view_count
# MAGIC             , total_comment_count
# MAGIC             , total_like_count
# MAGIC             , total_dislike_count	
# MAGIC         FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_demographics_a1 
# MAGIC        GROUP BY date
# MAGIC             , channel_id
# MAGIC             , video_id
# MAGIC             , program_id
# MAGIC             , total_view_count
# MAGIC             , total_comment_count
# MAGIC             , total_like_count
# MAGIC             , total_dislike_count)
# MAGIC GROUP BY date, video_id, video_id, program_id
# MAGIC HAVING count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. 일자별 집계

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_sum_daily_count AS (
# MAGIC WITH daily_cnt_1 AS (SELECT date
# MAGIC             , channel_id
# MAGIC             , video_id
# MAGIC             , program_id
# MAGIC             , total_view_count
# MAGIC             , total_comment_count
# MAGIC             , total_like_count
# MAGIC             , total_dislike_count	
# MAGIC         FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_demographics_a1 
# MAGIC        GROUP BY date
# MAGIC             , channel_id
# MAGIC             , video_id
# MAGIC             , program_id
# MAGIC             , total_view_count
# MAGIC             , total_comment_count
# MAGIC             , total_like_count
# MAGIC             , total_dislike_count
# MAGIC )
# MAGIC , daily_cnt_2 AS (SELECT date
# MAGIC            , channel_id
# MAGIC            , video_id
# MAGIC            , program_id
# MAGIC            , total_view_count
# MAGIC            , LAG(total_view_count) OVER(PARTITION BY channel_id, video_id, program_id ORDER BY date)  AS total_view_count_pre
# MAGIC            , total_comment_count
# MAGIC            , LAG(total_comment_count) OVER(PARTITION BY channel_id, video_id, program_id ORDER BY date)  AS total_comment_count_pre
# MAGIC            , total_like_count
# MAGIC            , LAG(total_like_count) OVER(PARTITION BY channel_id, video_id, program_id ORDER BY date)  AS total_like_count_count_pre
# MAGIC            , total_dislike_count	     
# MAGIC            , LAG(total_dislike_count) OVER(PARTITION BY channel_id, video_id, program_id ORDER BY date)  AS total_dislike_count_pre	
# MAGIC         FROM daily_cnt_1
# MAGIC        ORDER BY video_id, date
# MAGIC   )
# MAGIC
# MAGIC   SELECT T01.date
# MAGIC          , T02.year
# MAGIC          , T02.month
# MAGIC          , T02.day
# MAGIC          , T02.weekday
# MAGIC          , T02.weekday_nm 
# MAGIC          , T01.channel_id
# MAGIC          , T03.channel_title         
# MAGIC          , T01.video_id
# MAGIC          , SUBSTRING(T03.video_title, 1, 20)       AS video_title
# MAGIC          , T03.video_publish_datetime_first         
# MAGIC          , HOUR(video_publish_datetime_first)      AS video_publish_hour
# MAGIC          , T01.program_id
# MAGIC          , T03.program_title         
# MAGIC          , T03.cnts_cd
# MAGIC          , T03.cnts_nm         
# MAGIC          , CAST(T01.date- CAST(TO_DATE(SUBSTRING(T03.video_publish_datetime_first, 1, 10), 'yyyy-MM-dd') AS DATE) AS bigint) AS streaming_days
# MAGIC          , total_view_count       
# MAGIC          , total_view_count_pre                              
# MAGIC          , total_view_count     - total_view_count_pre           AS increase_view_count
# MAGIC          , total_comment_count
# MAGIC          , total_comment_count_pre
# MAGIC          , total_comment_count  - total_comment_count_pre        AS increase_comment_count
# MAGIC          , total_like_count
# MAGIC          , total_like_count     - total_like_count_count_pre     AS increase_like_count_count
# MAGIC          , total_dislike_count	
# MAGIC          , total_dislike_count_pre     
# MAGIC          , total_dislike_count  - total_dislike_count_pre  	     AS increase_dislike_count
# MAGIC     FROM daily_cnt_2 T01
# MAGIC    INNER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_dim_date T02 
# MAGIC                 ON T01.date = T02.date 
# MAGIC    LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_video_master T03 
# MAGIC                 ON T01.channel_id  = T03.channel_id
# MAGIC                AND T01.video_id    = T03.video_id 
# MAGIC                AND T01.program_id  = T03.program_id
# MAGIC    WHERE 1 = 1
# MAGIC      AND total_view_count_pre          IS NOT NULL
# MAGIC      AND total_comment_count_pre       IS NOT NULL
# MAGIC      AND total_like_count_count_pre    IS NOT NULL
# MAGIC      AND total_dislike_count_pre       IS NOT NULL
# MAGIC      AND T03.video_title               IS NOT NULL
# MAGIC      AND T03.cnts_nm                   IS NOT NULL
# MAGIC -- ORDER BY T03.cnts_cd, T01.video_id, date     
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(date), count(*)
# MAGIC from mbc_poc_unity_catalog.mbc_poc_schema.z_sum_daily_count
# MAGIC group by month(date)
