# Databricks notebook source
# MAGIC %md
# MAGIC ### 좀더 쉬운 조회수 regression 시도하려 함

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_contry

# COMMAND ----------

# Databricks에서 SparkSession을 가져오기
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("mbc_ml_session").getOrCreate()

# Databricks 테이블을 데이터프레임으로 읽어오기
table_name = "mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_contry"  # 읽어올 테이블의 이름을 지정하세요
df = spark.table(table_name)

# 데이터프레임을 보거나 처리할 수 있습니다
df.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("correlation_matrix").getOrCreate()

# Load the DataFrame (assuming it is already created)


# Get column names for all numerical columns in the DataFrame
columns = df.column

# Remove any rows with null values
df = df.na.drop()

# Assemble the necessary columns into a single feature column
assembler = VectorAssembler(inputCols=columns, outputCol="features")
assembled_data = assembler.transform(df)

# Compute the correlation matrix
corr_matrix = Correlation.corr(assembled_data, "features")

# Extract the correlation values
correlation_matrix = corr_matrix.collect()[0]["pearson(features)"]

# Convert the correlation values to a DataFrame
correlation_df = spark.createDataFrame([(float(c),) for c in correlation_matrix], ["correlation"])

# Display the correlation DataFrame
correlation_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_contry

# COMMAND ----------


