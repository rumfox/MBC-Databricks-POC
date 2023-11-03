# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.z_dim_date AS (
# MAGIC select date
# MAGIC      , year(date)              AS Year
# MAGIC      , month(date)             AS Month
# MAGIC      , day(date)               AS Day
# MAGIC      , weekday(date)           AS Weekday          
# MAGIC     , date_format(date, 'E')   AS Weekday_nm         
# MAGIC from (select distinct date from mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3)
# MAGIC )
