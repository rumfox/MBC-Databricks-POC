# Databricks notebook source
# MAGIC %md
# MAGIC ### 카탈로그 생성

# COMMAND ----------

# 카탈로그 생성하기
#유니티 카탈로그 생성
spark.sql("CREATE CATALOG IF NOT EXISTS mbc_poc_unity_catalog_agnes")

# COMMAND ----------

# 생성한 카탈로그 안으로 들어가기 
spark.sql("USE CATALOG mbc_poc_unity_catalog_agnes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 테이블생성

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW estimated_revenue_temp
# MAGIC (
# MAGIC date DATE,
# MAGIC channel_id STRING,
# MAGIC video_id STRING,
# MAGIC claimed_status STRING,
# MAGIC uploader_type STRING,
# MAGIC country_code STRING,
# MAGIC estimated_partner_revenue FLOAT,
# MAGIC estimated_partner_ad_revenue FLOAT,
# MAGIC estimated_partner_ad_auction_revenue FLOAT,
# MAGIC estimated_partner_ad_reserved_revenue FLOAT,
# MAGIC estimated_youtube_ad_revenue FLOAT,
# MAGIC estimated_monetized_playbacks FLOAT,
# MAGIC estimated_playback_based_cpm FLOAT,
# MAGIC ad_impressions FLOAT,
# MAGIC estimated_cpm FLOAT,
# MAGIC estimated_partner_red_revenue FLOAT,
# MAGIC estimated_partner_transaction_revenue FLOAT,
# MAGIC video_title STRING,
# MAGIC channel_title STRING,
# MAGIC program_id STRING,
# MAGIC program_title STRING,
# MAGIC matching_method STRING,
# MAGIC total_view_count FLOAT,
# MAGIC total_comment_count FLOAT,
# MAGIC total_like_count FLOAT,
# MAGIC total_dislike_count FLOAT,
# MAGIC video_publish_datetime DATE,
# MAGIC exchange_rates FLOAT,
# MAGIC admanager_video_title STRING,
# MAGIC admanager_admanager_video_id STRING,
# MAGIC admanager_channel_id STRING,
# MAGIC admanager_channel_title STRING,
# MAGIC admanager_admanager_channel_id STRING,
# MAGIC admanager_total_impressions FLOAT,
# MAGIC admanager_total_clicks FLOAT,
# MAGIC admanager_total_cpm_cpc_revenue FLOAT,
# MAGIC admanager_total_average_ecpm FLOAT,
# MAGIC admanager_total_ctr FLOAT,
# MAGIC admanager_start FLOAT,
# MAGIC admanager_first_quartile FLOAT,
# MAGIC admanager_midpoint FLOAT,
# MAGIC admanager_third_quartile FLOAT,
# MAGIC admanager_complete FLOAT,
# MAGIC admanager_average_view_rate FLOAT,
# MAGIC admanager_average_view_time FLOAT,
# MAGIC admanager_completion_rate FLOAT,
# MAGIC admanager_total_error_count FLOAT,
# MAGIC admanager_total_error_rate FLOAT,
# MAGIC admanager_video_length FLOAT,
# MAGIC admanager_skip_button_shown FLOAT,
# MAGIC admanager_engaged_view FLOAT,
# MAGIC admanager_view_through_rate FLOAT,
# MAGIC admanager_auto_plays FLOAT,
# MAGIC admanager_click_to_plays FLOAT,
# MAGIC admanager_creation_datetime DATE,
# MAGIC admanager_update_datetime DATE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "s3://mbcinfo/upload/content_owner_estimated_revenue_a1/",
# MAGIC   header = "true"
# MAGIC );

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE content_owner_estimated_revenue_a1 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (month)
# MAGIC COMMENT "content_owner_estimated_revenue_a1"
# MAGIC AS SELECT * , month(date) as month 
# MAGIC FROM estimated_revenue_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM content_owner_estimated_revenue_a1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED content_owner_estimated_revenue_a1;

# COMMAND ----------

### mbc_poc_catalog 로 이동
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# COMMAND ----------

spark.sql("USE SCHEMA mbc_poc_schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM content_owner_basic_a3

# COMMAND ----------

spark.sql("USE CATALOG mbc_poc_unity_catalog_agnes")

# COMMAND ----------

spark.sql("USE SCHEMA information_schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS information_schema;

# COMMAND ----------


