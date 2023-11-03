# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_master AS (
# MAGIC      SELECT  T01.program_id
# MAGIC            , T01.program_title
# MAGIC            , T01.channel_id
# MAGIC            , T01.channel_title
# MAGIC            , T01.video_id
# MAGIC            , T01.video_title
# MAGIC            , T01.video_publish_datetime
# MAGIC       FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_demographics_a1 T01
# MAGIC      WHERE 1 = 1
# MAGIC --       AND 01.program_id IS NOT NULL
# MAGIC --       AND T01.channel_id IS NOT NULL
# MAGIC        AND T01.video_id   IS NOT NULL
# MAGIC        AND video_title    IS NOT NULL               
# MAGIC      GROUP BY  T01.program_id
# MAGIC            , T01.program_title
# MAGIC            , T01.channel_id
# MAGIC            , T01.channel_title
# MAGIC            , T01.video_id
# MAGIC            , T01.video_title
# MAGIC            , T01.video_publish_datetime
# MAGIC )           

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_video_master AS (
# MAGIC WITH video_mast AS (
# MAGIC      SELECT  T01.program_id
# MAGIC            , MAX(T01.program_title)                AS program_title
# MAGIC            , T01.channel_id
# MAGIC            , MAX(T01.channel_title)                AS channel_title
# MAGIC            , T01.video_id
# MAGIC            , MAX(T01.video_title)                  AS video_title
# MAGIC            , MIN(T01.video_publish_datetime)       AS video_publish_datetime_first
# MAGIC       FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_master T01
# MAGIC      WHERE 1 = 1
# MAGIC --       AND 01.program_id IS NOT NULL
# MAGIC --       AND T01.channel_id IS NOT NULL
# MAGIC        AND T01.video_id   IS NOT NULL
# MAGIC        AND video_title    IS NOT NULL               
# MAGIC      GROUP BY T01.program_id 
# MAGIC            , T01.channel_id     
# MAGIC            , T01.video_id
# MAGIC
# MAGIC /*      FROM (SELECT program_id, channel_id, video_id
# MAGIC            , video_title
# MAGIC            , MIN(video_publish_datetime) OVER(PARTITION BY video_id ) AS video_publish_datetime_first
# MAGIC            , ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY video_publish_datetime desc) AS rn
# MAGIC       FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_master) T01 
# MAGIC       LEFT OUTER JOIN (SELECT program_id, program_title
# MAGIC            , ROW_NUMBER() OVER(PARTITION BY program_id ORDER BY video_publish_datetime desc) AS rn
# MAGIC       FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_master) T02 ON T01.program_id = T02.program_id
# MAGIC       LEFT OUTER JOIN (SELECT channel_id, channel_title
# MAGIC            , ROW_NUMBER() OVER(PARTITION BY channel_id ORDER BY video_publish_datetime desc) AS rn
# MAGIC       FROM mbc_poc_unity_catalog.mbc_poc_schema.z_temp_video_master) T03 ON T01.channel_id = T03.channel_id
# MAGIC      WHERE T01.rn = 1 AND (T02.rn=1 or T02.rn IS NULL)  AND (T03.rn=1  or T03.rn IS NULL)
# MAGIC        AND T01.channel_id IS NOT NULL
# MAGIC        AND T01.video_id   IS NOT NULL
# MAGIC        AND video_title IS NOT NULL       */
# MAGIC      )
# MAGIC
# MAGIC SELECT T01.program_id
# MAGIC      , T01.program_title
# MAGIC      , T01.channel_id
# MAGIC      , T01.channel_title
# MAGIC      , T02.cnts_cd
# MAGIC      , T02.cnts_nm
# MAGIC      , T02.regi_date
# MAGIC      , T02.updt_date
# MAGIC      , T01.video_id
# MAGIC      , T01.video_title
# MAGIC      , T01.video_publish_datetime_first
# MAGIC   FROM video_mast T01
# MAGIC   LEFT OUTER JOIN (SELECT channel_id
# MAGIC                         , video_id
# MAGIC                         , MAX(cnts_cd)   AS cnts_cd
# MAGIC                         , MAX(cnts_nm)   AS cnts_nm
# MAGIC                         , MIN(regi_date) AS regi_date
# MAGIC                         , MAX(updt_date) AS updt_date
# MAGIC                      FROM mbc_poc_unity_catalog.mbc_poc_schema.mbc_youtube_video
# MAGIC                     WHERE 1 = 1 
# MAGIC                      AND cnts_nm IS NOT NULL 
# MAGIC                     GROUP BY channel_id
# MAGIC                         , video_id 
# MAGIC                   ) T02 
# MAGIC                ON T01.channel_id = T02.channel_id
# MAGIC               AND T01.video_id   = T02.video_id 
# MAGIC  WHERE 1 = 1 
# MAGIC )              

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mbc_poc_unity_catalog.mbc_poc_schema.z_video_master
# MAGIC

# COMMAND ----------


