# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE table mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_total_cnt AS
# MAGIC SELECT video_id
# MAGIC     , date
# MAGIC     , MAX(total_view_count)          AS total_view_count
# MAGIC     , MAX(total_comment_count)        AS  total_comment_count  
# MAGIC     , MAX(total_like_count)           AS  total_like_count
# MAGIC     , MAX(total_dislike_count)        AS total_dislike_count
# MAGIC     , MAX(total_view_count_pre)       AS total_view_count_pre
# MAGIC     , MAX(total_comment_count_pre)    AS total_comment_count_pre
# MAGIC     , MAX(total_like_count - increase_like_count_count) AS total_like_count_pre
# MAGIC     , MAX(total_dislike_count_pre)    AS total_dislike_count_pre
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.z_sum_daily_count
# MAGIC GROUP BY video_id
# MAGIC     , date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.Z_report_by_daily_video_country AS (
# MAGIC SELECT T01.date
# MAGIC      , T02.Year
# MAGIC      , T02.Month
# MAGIC      , T02.Day
# MAGIC      , T02.Weekday
# MAGIC      , T02.Weekday_nm
# MAGIC      , T01.channel_id
# MAGIC      , T04.channel_title
# MAGIC      , T04.cnts_cd
# MAGIC      , t04.cnts_nm     
# MAGIC      , T04.regi_date           AS cnts_regi_date     
# MAGIC      , T04.updt_date           AS cnts_updt_date
# MAGIC      , T01.video_id
# MAGIC      , T04.video_title
# MAGIC      , T01.video_publish_datetime     
# MAGIC      , T04.video_publish_datetime_first     
# MAGIC      , HOUR(T04.video_publish_datetime_first)      AS video_publish_hour
# MAGIC      , CAST(T01.date- CAST(TO_DATE(SUBSTRING(T01.video_publish_datetime, 1, 10), 'yyyy-MM-dd') AS DATE) AS bigint) AS streaming_days     
# MAGIC      , T01.program_id
# MAGIC      , T04.program_title
# MAGIC      , T01.claimed_status
# MAGIC      , T01.uploader_type
# MAGIC      , T01.live_or_on_demand
# MAGIC      , T01.subscribed_status
# MAGIC      , T01.country_code
# MAGIC      , T03.name                  AS country_name
# MAGIC      , T03.region                AS country_region 
# MAGIC      , T03.sub_region            AS country_sub_region 
# MAGIC      , T03.intermediate_region   AS country_intermediate_region
# MAGIC      , views
# MAGIC      , comments
# MAGIC      , shares
# MAGIC      , watch_time_minutes
# MAGIC      , average_view_duration_seconds
# MAGIC      , average_view_duration_percentage
# MAGIC      , annotation_impressions
# MAGIC      , annotation_clickable_impressions
# MAGIC      , annotation_clicks
# MAGIC      , annotation_click_through_rate
# MAGIC      , annotation_closable_impressions
# MAGIC      , annotation_closes
# MAGIC      , annotation_close_rate
# MAGIC      , card_teaser_impressions
# MAGIC      , card_teaser_clicks
# MAGIC      , card_teaser_click_rate
# MAGIC      , card_impressions
# MAGIC      , card_clicks
# MAGIC      , card_click_rate
# MAGIC      , subscribers_gained
# MAGIC      , subscribers_lost
# MAGIC      , videos_added_to_playlists
# MAGIC      , videos_removed_from_playlists
# MAGIC      , likes, dislikes
# MAGIC      , red_views
# MAGIC      , red_watch_time_minutes
# MAGIC
# MAGIC      , T05.total_view_count
# MAGIC      , T05.total_comment_count
# MAGIC      , T05.total_like_count
# MAGIC      , T05.total_dislike_count
# MAGIC
# MAGIC      , T05.total_view_count_pre
# MAGIC      , T05.total_comment_count_pre
# MAGIC      , T05.total_like_count_pre
# MAGIC      , T05.total_dislike_count_pre	     
# MAGIC
# MAGIC      , T05.total_view_count        - T05.total_view_count_pre        AS increase_view_count
# MAGIC      , T05.total_comment_count     - T05.total_comment_count_pre     AS increase_comment_count 
# MAGIC      , T05.total_like_count        - T05.total_like_count_pre        AS increase_like_count
# MAGIC      , T05.total_dislike_count_pre - T05.total_dislike_count_pre 	AS increase_dislike_count_pre 
# MAGIC
# MAGIC
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3 T01
# MAGIC   LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_dim_date T02
# MAGIC        ON T01.date         = T02.date
# MAGIC   LEFT OUTER JOIN (SELECT `alpha-2` AS country_code, name, region, `sub-region` AS sub_region, `intermediate-region` AS intermediate_region    
# MAGIC                      FROM mbc_poc_unity_catalog.mbc_poc_schema.z_dim_country_continent x) T03
# MAGIC        ON T01.country_code = T03.country_code
# MAGIC   LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_video_master T04
# MAGIC        ON T01.channel_id   = T04.channel_id
# MAGIC       AND T01.program_id   = T04.program_id
# MAGIC       AND T01.video_id     = T04.video_id
# MAGIC    LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_total_cnt T05
# MAGIC        ON T01.date         = T05.date
# MAGIC       AND T01.video_id     = T05.video_id
# MAGIC )      ;
