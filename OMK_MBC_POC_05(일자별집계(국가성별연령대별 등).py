# Databricks notebook source
# MAGIC %md
# MAGIC #### view_percentage의 기준 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT T01.date, T01.video_id, sum(T01.total_view_count * T01.views_percentage / 100), max(T01.total_view_count)
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_demographics_a1 T01
# MAGIC WHERE video_id = 'ZqksBvfwqzE' and date = '2023-10-10'
# MAGIC group by T01.date, T01.video_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 이론적으로 특정 segment의 total view수는 일자별로 늘어나야 되는데 감소하는 일자 발생(비율로 계산할 때)
# MAGIC -  관점속성이 추정치로 계속 바뀌는 것으로 보임

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_total_view_by_dim AS (
# MAGIC SELECT date
# MAGIC      , program_id
# MAGIC      , channel_id
# MAGIC      , video_id
# MAGIC      , country_code
# MAGIC      , age_group
# MAGIC      , gender
# MAGIC      , CASE WHEN subscribed_status =  'subscribed' THEN 'Y' ELSE 'N' END  AS subscribed_yn    
# MAGIC      , SUM(total_view_count * views_percentage / 100)                     AS total_view_cnt_by_dimension
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_demographics_a1
# MAGIC group by  date
# MAGIC      , program_id
# MAGIC      , channel_id
# MAGIC      , video_id
# MAGIC      , country_code
# MAGIC      , age_group
# MAGIC      , gender
# MAGIC      , CASE WHEN subscribed_status =  'subscribed' THEN 'Y' ELSE 'N' END      
# MAGIC ) 
# MAGIC

# COMMAND ----------


CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.`z_일자_비디오_성별_연령대_구독여부별_View` AS 
(
SELECT T01.date                                              AS   `기준일YYYYMMDD` 
     , T02.Year                                              AS   `기준년도` 
     , T02.Month                                             AS   `기준월` 
     , T02.Day                                               AS   `기준일` 
     , T02.Weekday                                           AS   `요일_숫자_sortby` 
     , T02.Weekday_nm                                        AS   `요일` 
     , T01.program_id                                        AS   `프로그램ID` 
     , T04.program_title                                     AS   `프로그램명` 
     , T01.channel_id                                        AS   `채널ID` 
     , T04.channel_title                                     AS   `채널명` 
     , T04.cnts_cd                                           AS   `콘텐츠코드` 
     , T04.cnts_nm                                           AS   `콘텐츠명`     
     , T01.video_id                                          AS   `비디오ID` 
     , T04.video_title                                       AS   `비디오명` 
     , T04.video_publish_datetime_first                      AS   `비디오최초publish일시` 
     , CAST(TO_DATE(SUBSTRING(T04.video_publish_datetime_first, 1, 10), 'yyyy-MM-dd') AS DATE)      AS `비디오최초publish일지` 
     , HOUR(T04.video_publish_datetime_first)                                                       AS `비디오최초publish시간대` 
     , CAST(T01.date- CAST(TO_DATE(SUBSTRING(T04.video_publish_datetime_first, 1, 10), 'yyyy-MM-dd') AS DATE) AS bigint) AS `publish이후경과일수` 
     , T01.country_code                                       AS   `국가코드` 
     , T03.name                                               AS   `국가명` 
     , T03.region                                             AS   `국가대륙` 
     , T03.sub_region                                         AS   `국가대륙_sub_region` 
     , T03.intermediate_region                                AS   `국가대륙_intermediate_region`      
     , T01.age_group                                          AS   `연령대`    
     , T01.gender                                             AS   `성별` 
     , T01.subscribed_yn                                      AS   `구독여부`  
     , ROUND(T01.total_view_cnt_by_dimension, 0)              AS   `view수_누적당일`  
     , ROUND(LAG(T01.total_view_cnt_by_dimension) OVER(PARTITION BY T01.program_id, T01.channel_id, T01.video_id, T01.country_code, T01.age_group, T01.gender, T01.subscribed_yn     
                                                       ORDER BY T01.date), 0) 
                                                              AS   `view수_누적전일`  
     , ROUND(T01.total_view_cnt_by_dimension, 0)  -  round(LAG(T01.total_view_cnt_by_dimension) OVER(PARTITION BY T01.program_id, T01.channel_id, T01.video_id, T01.country_code, T01.age_group, T01.gender, T01.subscribed_yn     
                                                                                                 ORDER BY T01.date), 0) 
                                                              AS   `view수_전일대비증감수`  
  FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_total_view_by_dim T01
  LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_dim_date T02
       ON T01.date         = T02.date
  LEFT OUTER JOIN (SELECT `alpha-2` AS country_code, name, region, `sub-region` AS sub_region, `intermediate-region` AS intermediate_region    
                     FROM mbc_poc_unity_catalog.mbc_poc_schema.z_dim_country_continent x) T03
       ON T01.country_code = T03.country_code
  LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_video_master T04
       ON T01.channel_id   = T04.channel_id
      AND T01.program_id   = T04.program_id
      AND T01.video_id     = T04.video_id
)      
