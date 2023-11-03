# Databricks notebook source
# MAGIC %md
# MAGIC ### 한글, 특수문자 등 테스트
# MAGIC - 테이블명 -- 특수문자숫자한글 다됨, {} ()도 다됨 (스페이스 안됨)
# MAGIC - 컬럼명   -- 특수문자일부, 숫자, 한글 됨(스페이스 안됨)
# MAGIC - 시각화 tool (Power BI) 테스트: 테이블&컬럼에 한글, 숫자, 언더바가 있는 있는 상관없으나 특수문자가 있는 경우 안됨
# MAGIC ==> 따라서 영문, 한글, 숫자, '_'까지만 사용할 것을 권장

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.`z_한글테스트` AS 
# MAGIC SELECT 
# MAGIC     1 AS `순번`,
# MAGIC     1000 AS `매출금액`
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.`z_한글테스트(상세)#1` AS 
# MAGIC SELECT 
# MAGIC     1 AS `순번`,
# MAGIC     1000 AS `매출금액#1`
# MAGIC """)

# COMMAND ----------


