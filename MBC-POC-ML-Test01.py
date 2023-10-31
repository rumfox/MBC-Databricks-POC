# Databricks notebook source
# MAGIC %md 
# MAGIC ### 10/19 회의에서 고객사의 needs를 파악하여 content_owner_basic_a3 table로  program_title를 예측해보고자 함
# MAGIC -> program_title Null값 제외 후 시도하려 했으나 skew가 너무 심해서 조정이 필요
# MAGIC -> count top 400 만 추림

# COMMAND ----------

# MAGIC %md  catalog.mbc_poc_schema.content_owner_basic_a3를 살펴보고 program_title 예측 진행
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3

# COMMAND ----------

# MAGIC %md  distinct 한 프로그램 타이틀 추출 

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT DISTINCT program_title
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3;

# COMMAND ----------

# MAGIC %md  프로그램 타이틀 중 null 갯수 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3
# MAGIC WHERE program_title IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3;

# COMMAND ----------

40558788/67413597

# COMMAND ----------

# 10/23 emma test
43060424/71516729

# COMMAND ----------

# MAGIC %md 
# MAGIC program_title 이 null 인 비율이 60%로 엄청 높음 -> null 값 drop 

# COMMAND ----------

# MAGIC %md table을 df로 가져와서 전처리 진행

# COMMAND ----------

# Databricks에서 SparkSession을 가져오기
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("mbc_session").getOrCreate()

# Databricks 테이블을 데이터프레임으로 읽어오기
table_name = "mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3"  # 읽어올 테이블의 이름을 지정하세요
df = spark.table(table_name)

# 데이터프레임을 보거나 처리할 수 있습니다
df.show()

# COMMAND ----------

row_count = df.count()
column_count = len(df.columns)
print("데이터프레임의 행 수:", row_count,"데이터프레임의 열 수:", column_count)

# COMMAND ----------

# MAGIC %md program title 이 null인 값 지우기 

# COMMAND ----------


dfdropnull  = df.filter(df["program_title"].isNotNull())

# COMMAND ----------

row_count = dfdropnull.count()
column_count = len(dfdropnull.columns)
print("데이터프레임의 행 수:", row_count,"데이터프레임의 열 수:", column_count)

# COMMAND ----------

column_names = dfdropnull.columns
print("데이터프레임의 컬럼 이름:", column_names)

# COMMAND ----------

value_counts = dfdropnull.groupBy("program_title").count()
value_counts.show()

# COMMAND ----------

# Spark 세션 생성
spark = SparkSession.builder.appName("undersampling").getOrCreate()

# 데이터프레임 생성 (data는 이미 생성되어 있다고 가정)
# "label" 컬럼이 클래스 레이블을 나타내는 컬럼입니다

# 클래스 불균형 확인
class_counts = dfdropnull.groupBy("program_title").count().orderBy("count", ascending=False)
class_counts.show()

# COMMAND ----------

# Spark 세션 생성
spark = SparkSession.builder.appName("undersampling").getOrCreate()

# 데이터프레임 생성 (data는 이미 생성되어 있다고 가정)
# "label" 컬럼이 클래스 레이블을 나타내는 컬럼입니다

# 클래스 불균형 확인
class_counts = dfdropnull.groupBy("program_title").count().orderBy("count", ascending=True)
class_counts.show()

# COMMAND ----------

# MAGIC %md program_title가 최저는 1개 최대는 3364386개로 차이가 너무 심함 -> 최대 상위 몇개 추출해서 해보려함 

# COMMAND ----------


dfdropnull.createOrReplaceTempView("dropnullfrom_content_owner_basic_a3")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dropnullfrom_content_owner_basic_a3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE drop_program_title_null_from_content_owner_basic_a3 
# MAGIC USING DELTA
# MAGIC COMMENT "drop_program_title_null_from_content_owner_basic_a3"
# MAGIC AS SELECT * 
# MAGIC FROM dropnullfrom_content_owner_basic_a3;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM drop_program_title_null_from_content_owner_basic_a3; 
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
value_counts_pd = class_counts.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(value_counts_pd["program_title"], value_counts_pd["count"])
plt.xlabel("Value")
plt.ylabel("Count")
plt.title("Value Counts of 'program_title'")
plt.xticks(rotation=45)
plt.show()



# COMMAND ----------

spark = SparkSession.builder.appName("sorted_columns").getOrCreate()

distinct_values = dfdropnull.select("program_title").distinct().orderBy("class_counts", ascending=True)

# 결과를 출력
distinct_values.show()


# COMMAND ----------

# MAGIC %md 상위 200개 추출 / 상위 400개 추출

# COMMAND ----------

# 상위 200개 추출
from pyspark.sql.functions import desc

top_200_program_title = dfdropnull.groupBy("program_title") \
    .count() \
    .orderBy(desc("count")) \
    .limit(200)

# COMMAND ----------

# 상위 400개 추출
from pyspark.sql.functions import desc

top_400_program_title = dfdropnull.groupBy("program_title") \
    .count() \
    .orderBy(desc("count")) \
    .limit(400)

# COMMAND ----------

# 상위 200개 추출
display(top_200_program_title.select("program_title"))

# COMMAND ----------

# 상위 400개 추출
display(top_400_program_title.select("program_title"))

# COMMAND ----------

# MAGIC %md 상위 200개 / 상위 400개 프로그램 타이틀을 가진 로우로만 새로운 테이블 생성

# COMMAND ----------

# 상위 200개
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("extract_matching_rows").getOrCreate()

# "dfdropnull" 및 "top_400_program_title" 데이터프레임을 생성 (이미 생성되어 있다고 가정)

# "program_title" 컬럼을 기준으로 두 데이터프레임을 조인
joined_df = dfdropnull.join(top_200_program_title, "program_title", "inner")

# 조인 결과 출력
joined_df.show()

# COMMAND ----------

# 상위 400개
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("extract_matching_rows").getOrCreate()

# "dfdropnull" 및 "top_400_program_title" 데이터프레임을 생성 (이미 생성되어 있다고 가정)

# "program_title" 컬럼을 기준으로 두 데이터프레임을 조인
joined_df = dfdropnull.join(top_400_program_title, "program_title", "inner")

# 조인 결과 출력
joined_df.show()

# COMMAND ----------

# "program_title" 컬럼의 고유한 값 추출
unique_program_titles = joined_df.select("program_title").distinct()

# 결과 출력
unique_program_titles.count()

# COMMAND ----------

#temp view 만들기 
joined_df.createOrReplaceTempView("program_titles_table_temp")

# COMMAND ----------

# MAGIC %md 테이블생성

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 상위 200개
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.top200_programtitle
# MAGIC COMMENT "top200_programtitle in youtube"
# MAGIC AS SELECT * FROM program_titles_table_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 상위 400개
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.top400_programtitle
# MAGIC COMMENT "top400_programtitle in youtube"
# MAGIC AS SELECT * FROM program_titles_table_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 상위 200개
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.top200_programtitle

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 상위 400개
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.top400_programtitle

# COMMAND ----------


