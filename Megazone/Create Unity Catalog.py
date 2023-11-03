# Databricks notebook source
# MAGIC %md 
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Create a catalog

# COMMAND ----------

# Create a catalog.
spark.sql("CREATE CATALOG IF NOT EXISTS mbc_poc_unity_catalog")

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG mbc_poc_unity_catalog")

# COMMAND ----------

# Show all catalogs in the metastore.
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Grant create and use catalog permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE, USE CATALOG
  ON CATALOG mbc_poc_unity_catalog
  TO `mbcinfo@mbc.co.kr`""")

# COMMAND ----------

# Show grants on the quickstart catalog.
display(spark.sql("SHOW GRANT ON CATALOG mbc_poc_unity_catalog"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Create and manage schemas (databases)

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS mbc_poc_schema
  COMMENT 'A new Unity Catalog schema called mbc_poc_schema'""")

# COMMAND ----------

# Show schemas in the catalog that was set earlier.
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Describe the schema.
display(spark.sql("DESCRIBE SCHEMA EXTENDED mbc_poc_schema"))

# COMMAND ----------

# Grant create table, and use schema permissions for the schema to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE TABLE, USE SCHEMA
  ON SCHEMA mbc_poc_schema
  TO `mbcinfo@mbc.co.kr`""")
