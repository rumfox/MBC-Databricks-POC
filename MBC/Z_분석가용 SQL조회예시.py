# Databricks notebook source
# MAGIC %md 
# MAGIC ### 마트 조회 예시
# MAGIC     
# MAGIC - 1. 일자별 콘텐츠별 집계 조회 예시

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date                                   AS `일자`
# MAGIC       , Weekday_nm                            AS `요일`
# MAGIC       , cnts_nm                               AS `Youtube콘텐츠명`
# MAGIC       , CASE WHEN country_code = 'KR' THEN '1.국내' 
# MAGIC              WHEN country_code = 'ZZ' THEN '3.기타'       
# MAGIC              ELSE '2.해외' END                AS `국내외구분`
# MAGIC       , COUNT(distinct video_id)              AS `총시청비디오건수` 
# MAGIC       , ROUND(SUM(views)/COUNT(distinct video_id), 2)   AS `비디오별평균View수`       
# MAGIC       , SUM(views)                            AS `총View수` 
# MAGIC       , SUM(comments)                         AS `총댓글수`   
# MAGIC       , SUM(shares)                           AS `총공유수`   
# MAGIC       , ROUND(SUM(watch_time_minutes)/60, 0)  AS `총시청시간(시간)`   
# MAGIC       , ROUND(SUM(watch_time_minutes) / SUM(views), 2)  AS `평균시청시간(분)` 
# MAGIC       , ROUND(SUM(estimated_youtube_ad_revenue), 2)     AS `예상광고수입` 
# MAGIC       , SUM(estimated_monetized_playbacks)    As `예상playbacks수`  
# MAGIC       , SUM(ad_impressions)                   As `광고노출수`  
# MAGIC       , SUM(estimated_youtube_ad_revenue)/SUM(estimated_monetized_playbacks)*1000 AS `예상cpm(playbacks)`   
# MAGIC       , SUM(estimated_youtube_ad_revenue)/SUM(ad_impressions)*1000                AS `예상cpm`  
# MAGIC  FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country_cnt_rev
# MAGIC WHERE cnts_nm IS NOT NULL
# MAGIC GROUP BY date                             
# MAGIC       , Weekday_nm                        
# MAGIC       , cnts_cd                                 
# MAGIC       , cnts_nm                           
# MAGIC       , CASE WHEN country_code = 'KR' THEN '1.국내' 
# MAGIC              WHEN country_code = 'ZZ' THEN '3.기타'       
# MAGIC              ELSE '2.해외' END 
# MAGIC ORDER BY date DESC
# MAGIC       , cnts_nm    
# MAGIC       , CASE WHEN country_code = 'KR' THEN '1.국내' 
# MAGIC              WHEN country_code = 'ZZ' THEN '3.기타'       
# MAGIC              ELSE '2.해외' END 

# COMMAND ----------

# MAGIC %md 
# MAGIC - 2. 일자별 youtube 콘텐츠 인기순위 랭킹

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT date     
# MAGIC           , Weekday_nm
# MAGIC           , cnts_cd      
# MAGIC           , cnts_nm
# MAGIC           , COUNT(DISTINCT video_id)  AS video_cnt
# MAGIC           , SUM(views)     AS views
# MAGIC      FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country_cnt_rev
# MAGIC     WHERE cnts_cd IS NOT NULL
# MAGIC     GROUP BY date     
# MAGIC           , Weekday_nm
# MAGIC           , cnts_cd      
# MAGIC           , cnts_nm
# MAGIC )      
# MAGIC , base2 AS (
# MAGIC     SELECT date     
# MAGIC           , Weekday_nm
# MAGIC           , cnts_cd      
# MAGIC           , cnts_nm
# MAGIC           , video_cnt
# MAGIC           , views
# MAGIC           , ROW_NUMBER() OVER(PARTITION BY date ORDER BY views desc) AS rank
# MAGIC     FROM  base
# MAGIC    WHERE  cnts_nm IS NOT NULL
# MAGIC  )
# MAGIC
# MAGIC  SELECT date                                           AS `일자`
# MAGIC       , Weekday_nm                                     AS `요일`
# MAGIC       , rank                                           AS `순위`
# MAGIC       , cnts_cd        
# MAGIC       , cnts_nm                                        AS `Youtube콘텐츠명`
# MAGIC       , video_cnt                                      AS `video수`
# MAGIC       , views                                          AS `View수`
# MAGIC   FROM base2
# MAGIC   WHERE  rank <= 5
# MAGIC  ORDER BY date DESC
# MAGIC         , rank 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC - 3. 일자별 비디오 인기순위 랭킹

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT date     
# MAGIC           , Weekday_nm
# MAGIC           , video_id      
# MAGIC           , video_title
# MAGIC           , SUM(views)     AS views
# MAGIC      FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country_cnt_rev
# MAGIC     GROUP BY date                             
# MAGIC           , Weekday_nm                        
# MAGIC           , video_id
# MAGIC          , video_title
# MAGIC )      
# MAGIC , base2 AS (
# MAGIC     SELECT date     
# MAGIC           , Weekday_nm
# MAGIC           , video_id      
# MAGIC           , video_title
# MAGIC           , views
# MAGIC           , ROW_NUMBER() OVER(PARTITION BY date ORDER BY views desc) AS rank
# MAGIC     FROM  base
# MAGIC  )
# MAGIC
# MAGIC  SELECT date                                           AS `일자`
# MAGIC       , Weekday_nm                                     AS `요일`
# MAGIC       , rank                                           AS `순위`
# MAGIC       , video_title                                    AS `비디오명`
# MAGIC       , views                                          AS `View수`
# MAGIC   FROM base2
# MAGIC   WHERE  rank <= 10
# MAGIC  ORDER BY date DESC
# MAGIC         , rank
# MAGIC
