# Databricks notebook source
# MAGIC %md
# MAGIC ### 다음지표는 sql 또는 시각화 tool에서 조회시 동적으로 생성되도록 해야 함
# MAGIC |지표|산식|
# MAGIC |----|----|
# MAGIC |estimated_playback_based_cpm|SUM(estimated_youtube_ad_revenue) / SUM(estimated_monetized_playbacks)*1000|
# MAGIC |estimated_cpm|SUM(estimated_youtube_ad_revenue) / SUM(ad_impressions)*1000|
# MAGIC |admanager_total_cpm_cpc_revenue||
# MAGIC |admanager_total_average_ecpm||
# MAGIC |admanager_total_ctr|total clicks, divided by total impressions, multiplied by 100|
# MAGIC |admanager_average_view_rate|if a user watches 20 seconds of a 30-second video, the video view rate percentage is calculated as 66.66.|
# MAGIC |admanager_average_view_time||
# MAGIC |admanager_completion_rate|Video completes / Video starts|
# MAGIC |admanager_total_error_rate|Total error count/(Total error count + Total impressions)|
# MAGIC |admanager_view_through_rate|Engaged view/Skip shown|
# MAGIC |average_view_duration_seconds||
# MAGIC |average_view_duration_percentage||
# MAGIC |annotation_click_through_rate|annotation_clicks / annotation_clickable_impressions|
# MAGIC |annotation_close_rate|annotation_closes / annotation_impressions|
# MAGIC |card_teaser_click_rate|card_teaser_clicks / card_teaser_impressions|
# MAGIC |card_click_rate|card_clicks/card_impressions|

# COMMAND ----------

# MAGIC %md
# MAGIC #### 개발자용 마트 생성 sql예시
# MAGIC - tuning필요
# MAGIC - 실제 구축시에는 전체 재적재가 아닌 일자별로 집계해서 insert를  배치로 하는workflow생성 필요

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 초기 적재용 
# MAGIC -- CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_cnt_revenue AS (
# MAGIC
# MAGIC -- 변경적재용
# MAGIC --DELETE FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_cnt_revenue 
# MAGIC -- WHERE date BETWEEN '2023-05-01' AND   '2023-05-31' 
# MAGIC
# MAGIC INSERT INTO mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_cnt_revenue 
# MAGIC WITH cnt AS (
# MAGIC SELECT date
# MAGIC      , channel_id
# MAGIC      , video_id
# MAGIC      , claimed_status
# MAGIC      , uploader_type
# MAGIC      , country_code
# MAGIC      , sum(COALESCE(views                           , 0)) AS  views                                    
# MAGIC      , sum(COALESCE(comments                        , 0)) AS  comments                                 
# MAGIC      , sum(COALESCE(shares                          , 0)) AS  shares                                   
# MAGIC      , sum(COALESCE(watch_time_minutes              , 0)) AS  watch_time_minutes                       
# MAGIC      , sum(COALESCE(annotation_impressions          , 0)) AS  annotation_impressions                   
# MAGIC      , sum(COALESCE(annotation_clickable_impressions, 0)) AS  annotation_clickable_impressions         
# MAGIC      , sum(COALESCE(annotation_clicks               , 0)) AS  annotation_clicks                        
# MAGIC      , sum(COALESCE(annotation_closable_impressions , 0)) AS  annotation_closable_impressions          
# MAGIC      , sum(COALESCE(annotation_closes               , 0)) AS  annotation_closes                        
# MAGIC      , sum(COALESCE(card_teaser_impressions         , 0)) AS  card_teaser_impressions                  
# MAGIC      , sum(COALESCE(card_teaser_clicks              , 0)) AS  card_teaser_clicks                       
# MAGIC      , sum(COALESCE(card_impressions                , 0)) AS  card_impressions                         
# MAGIC      , sum(COALESCE(card_clicks                     , 0)) AS  card_clicks                              
# MAGIC      , sum(COALESCE(subscribers_gained              , 0)) AS  subscribers_gained                       
# MAGIC      , sum(COALESCE(subscribers_lost                , 0)) AS  subscribers_lost                         
# MAGIC      , sum(COALESCE(videos_added_to_playlists       , 0)) AS  videos_added_to_playlists                
# MAGIC      , sum(COALESCE(videos_removed_from_playlists   , 0)) AS  videos_removed_from_playlists            
# MAGIC      , sum(COALESCE(likes                           , 0)) AS  likes                                    
# MAGIC      , sum(COALESCE(dislikes                        , 0)) AS  dislikes                                 
# MAGIC      , sum(COALESCE(red_views                       , 0)) AS  red_views                                
# MAGIC      , sum(COALESCE(red_watch_time_minutes          , 0)) AS  red_watch_time_minutes                   
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3 T01
# MAGIC -- 변경적재용
# MAGIC  WHERE date BETWEEN '2023-08-01' AND   '2023-08-31' 
# MAGIC  GROUP BY date
# MAGIC      , channel_id
# MAGIC      , video_id
# MAGIC      , claimed_status
# MAGIC      , uploader_type
# MAGIC      , country_code
# MAGIC )
# MAGIC , revenue AS (
# MAGIC SELECT date
# MAGIC      , channel_id
# MAGIC      , video_id
# MAGIC      , claimed_status
# MAGIC      , uploader_type
# MAGIC      , country_code
# MAGIC      , estimated_partner_revenue
# MAGIC      , estimated_partner_ad_revenue
# MAGIC      , estimated_partner_ad_auction_revenue
# MAGIC      , estimated_partner_ad_reserved_revenue
# MAGIC      , estimated_youtube_ad_revenue
# MAGIC      , estimated_monetized_playbacks
# MAGIC      , ad_impressions
# MAGIC --     , estimated_playback_based_cpm                        -- estimated_youtube_ad_revenue/estimated_monetized_playbacks*1000
# MAGIC --     , estimated_cpm                                       -- estimated_youtube_ad_revenue/ad_impressions*1000
# MAGIC      , estimated_partner_red_revenue
# MAGIC      , estimated_partner_transaction_revenue
# MAGIC      , admanager_total_impressions
# MAGIC      , admanager_total_clicks
# MAGIC      , admanager_total_cpm_cpc_revenue
# MAGIC      , admanager_total_average_ecpm
# MAGIC --     , admanager_total_ctr                                --total clicks, divided by total impressions, multiplied by 100
# MAGIC      , admanager_start
# MAGIC      , admanager_first_quartile
# MAGIC      , admanager_midpoint
# MAGIC      , admanager_third_quartile
# MAGIC      , admanager_complete
# MAGIC      , admanager_average_view_rate                      
# MAGIC --     , admanager_average_view_time                      -- Total video view time ∕ Video starts
# MAGIC --     , admanager_completion_rate                        -- Video completes / Video starts
# MAGIC      , admanager_total_error_count
# MAGIC --     , admanager_total_error_rate                       -- Total error count ∕ (Total error count + Total impressions)
# MAGIC      , admanager_video_length
# MAGIC      , admanager_skip_button_shown
# MAGIC      , admanager_engaged_view
# MAGIC --     , admanager_view_through_rate                      -- Engaged view ∕ Skip shown 
# MAGIC      , admanager_auto_plays
# MAGIC      , admanager_click_to_plays
# MAGIC      , admanager_creation_datetime
# MAGIC      , admanager_update_datetime
# MAGIC  FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_estimated_revenue_a1     
# MAGIC  WHERE date BETWEEN '2023-08-01' AND   '2023-08-31' 
# MAGIC )
# MAGIC
# MAGIC SELECT T01.*
# MAGIC      , T02.estimated_partner_revenue
# MAGIC      , T02.estimated_partner_ad_revenue
# MAGIC      , T02.estimated_partner_ad_auction_revenue
# MAGIC      , T02.estimated_partner_ad_reserved_revenue
# MAGIC      , T02.estimated_youtube_ad_revenue
# MAGIC      , T02.estimated_monetized_playbacks
# MAGIC      , T02.ad_impressions
# MAGIC      , T02.estimated_partner_red_revenue
# MAGIC      , T02.estimated_partner_transaction_revenue
# MAGIC      , T02.admanager_total_impressions
# MAGIC      , T02.admanager_total_clicks
# MAGIC      , T02.admanager_total_cpm_cpc_revenue
# MAGIC      , T02.admanager_total_average_ecpm
# MAGIC      , T02.admanager_start
# MAGIC      , T02.admanager_first_quartile
# MAGIC      , T02.admanager_midpoint
# MAGIC      , T02.admanager_third_quartile
# MAGIC      , T02.admanager_complete
# MAGIC      , T02.admanager_average_view_rate                      
# MAGIC      , T02.admanager_total_error_count
# MAGIC      , T02.admanager_video_length
# MAGIC      , T02.admanager_skip_button_shown
# MAGIC      , T02.admanager_engaged_view
# MAGIC      , T02.admanager_auto_plays
# MAGIC      , T02.admanager_click_to_plays
# MAGIC      , T02.admanager_creation_datetime
# MAGIC      , T02.admanager_update_datetime
# MAGIC   FROM cnt T01
# MAGIC   LEFT OUTER JOIN revenue T02 
# MAGIC                ON T01.date            = T02.date           
# MAGIC               AND T01.channel_id      = T02.channel_id     
# MAGIC               AND T01.video_id        = T02.video_id       
# MAGIC               AND T01.claimed_status  = T02.claimed_status 
# MAGIC               AND T01.uploader_type   = T02.uploader_type  
# MAGIC               AND T01.country_code    = T02.country_code   
# MAGIC -- )              

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 초기적재용
# MAGIC --CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.Z_report_by_daily_video_country_cnt_rev AS (
# MAGIC -- 변경적재용 
# MAGIC --DELETE FROM  mbc_poc_unity_catalog.mbc_poc_schema.Z_report_by_daily_video_country_cnt_rev
# MAGIC  --WHERE T01.date BETWEEN '2023-05-01' AND '2023-05-31'
# MAGIC
# MAGIC INSERT INTO mbc_poc_unity_catalog.mbc_poc_schema.Z_report_by_daily_video_country_cnt_rev
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
# MAGIC      , T04.video_publish_datetime_first     
# MAGIC      , CAST(TO_DATE(SUBSTRING(T04.video_publish_datetime_first, 1, 10), 'yyyy-MM-dd') AS DATE)            AS video_publish_date
# MAGIC      , HOUR(T04.video_publish_datetime_first)                                                       AS video_publish_hour
# MAGIC      , CAST(T01.date- CAST(TO_DATE(SUBSTRING(T04.video_publish_datetime_first, 1, 10), 'yyyy-MM-dd') AS DATE) AS bigint) AS streaming_days     
# MAGIC      , T04.program_id
# MAGIC      , T04.program_title
# MAGIC      , T01.claimed_status
# MAGIC      , T01.uploader_type
# MAGIC      , T01.country_code
# MAGIC      , T03.name                  AS country_name
# MAGIC      , T03.region                AS country_region 
# MAGIC      , T03.sub_region            AS country_sub_region 
# MAGIC      , T03.intermediate_region   AS country_intermediate_region
# MAGIC      , views
# MAGIC      , comments
# MAGIC      , shares
# MAGIC      , watch_time_minutes
# MAGIC      , annotation_impressions
# MAGIC      , annotation_clickable_impressions
# MAGIC      , annotation_clicks
# MAGIC      , annotation_closable_impressions
# MAGIC      , annotation_closes
# MAGIC      , card_teaser_impressions
# MAGIC      , card_teaser_clicks
# MAGIC      , card_impressions
# MAGIC      , card_clicks
# MAGIC      , subscribers_gained
# MAGIC      , subscribers_lost
# MAGIC      , videos_added_to_playlists
# MAGIC      , videos_removed_from_playlists
# MAGIC      , likes
# MAGIC      , dislikes
# MAGIC      , red_views
# MAGIC      , red_watch_time_minutes
# MAGIC
# MAGIC      , estimated_partner_revenue
# MAGIC      , estimated_partner_ad_revenue
# MAGIC      , estimated_partner_ad_auction_revenue
# MAGIC      , estimated_partner_ad_reserved_revenue
# MAGIC      , estimated_youtube_ad_revenue
# MAGIC      , estimated_monetized_playbacks
# MAGIC      , ad_impressions
# MAGIC      , estimated_partner_red_revenue
# MAGIC      , estimated_partner_transaction_revenue
# MAGIC      , admanager_total_impressions
# MAGIC      , admanager_total_clicks
# MAGIC      , admanager_total_cpm_cpc_revenue
# MAGIC      , admanager_total_average_ecpm
# MAGIC      , admanager_start
# MAGIC      , admanager_first_quartile
# MAGIC      , admanager_midpoint
# MAGIC      , admanager_third_quartile
# MAGIC      , admanager_complete
# MAGIC      , admanager_average_view_rate                      
# MAGIC      , admanager_total_error_count
# MAGIC      , admanager_video_length
# MAGIC      , admanager_skip_button_shown
# MAGIC      , admanager_engaged_view
# MAGIC      , admanager_auto_plays
# MAGIC      , admanager_click_to_plays
# MAGIC      , admanager_creation_datetime     
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
# MAGIC      , T05.total_dislike_count_pre - T05.total_dislike_count_pre	AS increase_dislike_count_pre 
# MAGIC
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_cnt_revenue T01
# MAGIC   LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_dim_date T02
# MAGIC        ON T01.date         = T02.date
# MAGIC   LEFT OUTER JOIN (SELECT `alpha-2` AS country_code, name, region, `sub-region` AS sub_region, `intermediate-region` AS intermediate_region    
# MAGIC                      FROM mbc_poc_unity_catalog.mbc_poc_schema.z_dim_country_continent x) T03
# MAGIC        ON T01.country_code = T03.country_code
# MAGIC   LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_video_master T04
# MAGIC        ON T01.channel_id   = T04.channel_id
# MAGIC       AND T01.video_id     = T04.video_id
# MAGIC    LEFT OUTER JOIN mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_total_cnt T05
# MAGIC        ON T01.date         = T05.date
# MAGIC       AND T01.video_id     = T05.video_id
# MAGIC -- 변경적재용      
# MAGIC WHERE T01.date BETWEEN '2023-08-01' AND '2023-08-31'
# MAGIC --)      ;
