# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC       datediff(MAX(DATE), MIN(DATE)) + 1 -  COUNT(DISTINCT DATE)    
# MAGIC     , MIN(DATE)
# MAGIC     , MAX(DATE)
# MAGIC     , datediff(MAX(DATE), MIN(DATE)) + 1
# MAGIC     , COUNT(DISTINCT DATE)
# MAGIC     , video_id
# MAGIC     , MAX(video_title)    
# MAGIC   FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3
# MAGIC  WHERE video_title IS NOT NULL
# MAGIC  GROUP BY video_id
# MAGIC HAVING  ((datediff(MAX(DATE), MIN(DATE)) + 1) -  COUNT(DISTINCT DATE)) > 0
# MAGIC  ORDER BY 1 DESC
