# Databricks notebook source
# MAGIC %md 
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 데이터 분석 
# MAGIC Data Source를 가져와서 Machine Learning을 통한 분석 및 데이터 시각화
# MAGIC <br><br/>
# MAGIC 1. 조회 성능
# MAGIC <br>1.1. 조회 성능 - 대용량 데이터 조회 성능
# MAGIC 2. Data Source
# MAGIC <br>2.1. RDB - 기존 DB 연계 기능
# MAGIC <br>2.2. 그 외 Data Source - athena, json 연계 기능
# MAGIC 3. Visualization
# MAGIC <br>3.1. 시각화 도구 제공 여부 - 데이터 조회 결과 시각화
# MAGIC 4. ML
# MAGIC <br>4.1. Machine Learning 기능 제공 (Forecasting/Inference)
# MAGIC <br>4.2. 모델 관리 기능 - MLFlow

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 조회 성능

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. 조회 성능 - 대용량 데이터 조회 성능
# MAGIC 대용량의 데이터가 있을 경우 튜닝을 통해 쿼리를 수행하여 조회 성능을 개선
# MAGIC <br>파티션 또는 zorder를 사용

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Data Source

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. RDB - 기존 DB 연계 기능

# COMMAND ----------

# DBTITLE 1,MBC Oracle DB 연결후 mbc_youtube_video 데이터를 가져와서 delta table 생성
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

jdbc_url = "jdbc:oracle:thin:@//203.238.227.108:1521/LOGDATA"
connection_properties = {
    "user"    : "mbceis",
    "password": "eismbc",
    "driver"  : "oracle.jdbc.OracleDriver"
}
query     = "(SELECT * FROM mbceis.mbc_youtube_video)"  
oracle_df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df        = oracle_df.withColumn("partition_dt", date_format(col("UPDT_DATE").cast("string"), "yyyyMM"))

# Define the Delta Lake table location
delta_table_path = "dbfs:/mbcinfo/oracle/tb_youtube_video/"

# Define the schema for the Delta table
delta_table_schema = StructType([
    StructField("VIDEO_ID", StringType(), nullable=True),
    StructField("CHANNEL_ID", StringType(), nullable=True),
    StructField("CNTS_CD", StringType(), nullable=True),
    StructField("CNTS_NM", StringType(), nullable=True),
    StructField("REGI_DATE", DateType(), nullable=True),
    StructField("UPDT_DATE", DateType(), nullable=True),
    StructField("VIDEO_TAGS", StringType(), nullable=True)
])

spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{delta_table_path}` USING delta")

s3_path = "s3a://mbcinfo/oracle/tb_youtube_video"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("UPDT_DATE").save(s3_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 그 외 Data Source - athena, json 연계 기능
# MAGIC s3에 담겨진 csv 파일 및 json 파일의 데이터를 읽어서
# MAGIC <br>CREATE EXTERNAL TABLE로 athena에 테이블 생성 가능
# MAGIC <br>
# MAGIC ##### 쿼리 example>
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS content_owner_basic_a3 (
# MAGIC <br>date STRING,
# MAGIC <br>channel_id STRING,
# MAGIC <br>video_id STRING,
# MAGIC <br>......
# MAGIC <br>) 
# MAGIC <br>ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
# MAGIC <br>WITH SERDEPROPERTIES (
# MAGIC <br>  'separatorChar' = ',',
# MAGIC <br>  'quoteChar' = '"',
# MAGIC <br>  'escapeChar' = '\\'
# MAGIC <br>)
# MAGIC <br>LOCATION 's3://mbcinfo/upload/content_owner_basic_a3/' 
# MAGIC <br>TBLPROPERTIES (
# MAGIC <br>   'skip.header.line.count'='1', 
# MAGIC <br>   'textinputformat.record.delimiter' = '\n', 
# MAGIC <br>   'input.regex' = '.*\\.csv$');

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. 시각화 도구 제공 여부 - 데이터 조회 결과 시각화
# MAGIC SQL 쿼리 결과를 대시보드로 visualization 하여 볼 수 있음

# COMMAND ----------

# MAGIC %md
# MAGIC ![Image Alt Text](https://github.com/seungheejo/test/blob/main/img/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA%202023-10-31%20%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB%2010.13.58.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) ML

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Machine Learning 기능 제공 (Forecasting/Inference)

# COMMAND ----------

# DBTITLE 1,content_owner_basic_a3를 살펴보고 program_title 예측
# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3;

# COMMAND ----------

# DBTITLE 1,distinct 한 프로그램 타이틀 추출
# MAGIC %sql
# MAGIC SELECT DISTINCT program_title
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3;

# COMMAND ----------

# DBTITLE 1,프로그램 타이틀 중 null 갯수
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3
# MAGIC WHERE program_title IS NULL;

# COMMAND ----------

# DBTITLE 1,전체 row 갯수
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3;

# COMMAND ----------

# DBTITLE 1,program_title 이 null 인 비율이 60%로 엄청 높음 -> null 값 drop
43060424/71516729

# COMMAND ----------

# DBTITLE 1,table을 df로 가져와서 전처리 진행
# Databricks에서 SparkSession을 가져오기
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("mbc_session").getOrCreate()

# Databricks 테이블을 데이터프레임으로 읽어오기
table_name = "mbc_poc_unity_catalog.mbc_poc_schema.content_owner_basic_a3"  # 읽어올 테이블의 이름을 지정하세요
df = spark.table(table_name)

# 데이터프레임을 보거나 처리할 수 있습니다
df.show()

row_count = df.count()
column_count = len(df.columns)
print("데이터프레임의 행 수:", row_count,"데이터프레임의 열 수:", column_count)

# COMMAND ----------

# DBTITLE 1,program title 이 null인 값 지우기
dfdropnull  = df.filter(df["program_title"].isNotNull())
row_count = dfdropnull.count()
column_count = len(dfdropnull.columns)
print("데이터프레임의 행 수:", row_count,"데이터프레임의 열 수:", column_count)

column_names = dfdropnull.columns
print("데이터프레임의 컬럼 이름:", column_names)

value_counts = dfdropnull.groupBy("program_title").count()
value_counts.show()

# COMMAND ----------

# DBTITLE 1,count 로 내림차순 정렬
# Spark 세션 생성
spark = SparkSession.builder.appName("undersampling").getOrCreate()

# 데이터프레임 생성 (data는 이미 생성되어 있다고 가정)
# "label" 컬럼이 클래스 레이블을 나타내는 컬럼입니다

# 클래스 불균형 확인
class_counts = dfdropnull.groupBy("program_title").count().orderBy("count", ascending=False)
class_counts.show()

# COMMAND ----------

# DBTITLE 1,count 로 오름차순 정렬
# Spark 세션 생성
spark = SparkSession.builder.appName("undersampling").getOrCreate()

# 데이터프레임 생성 (data는 이미 생성되어 있다고 가정)
# "label" 컬럼이 클래스 레이블을 나타내는 컬럼입니다

# 클래스 불균형 확인
class_counts = dfdropnull.groupBy("program_title").count().orderBy("count", ascending=True)
class_counts.show()

# COMMAND ----------

# DBTITLE 1,program_title 카운트의 최저, 최대의 차이가 너무 크기 때문에 temp view 부터 다시 생성
dfdropnull.createOrReplaceTempView("dropnullfrom_content_owner_basic_a3")

# COMMAND ----------

# DBTITLE 1,temp view 데이터로 delta table 생성
# MAGIC %sql
# MAGIC SELECT * FROM dropnullfrom_content_owner_basic_a3;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE drop_program_title_null_from_content_owner_basic_a3 
# MAGIC USING DELTA
# MAGIC COMMENT "drop_program_title_null_from_content_owner_basic_a3"
# MAGIC AS SELECT * 
# MAGIC FROM dropnullfrom_content_owner_basic_a3;
# MAGIC
# MAGIC SELECT * FROM drop_program_title_null_from_content_owner_basic_a3;

# COMMAND ----------

# DBTITLE 1,matlab plot 생성으로 visualization
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

# DBTITLE 1,결과 출력
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("sorted_columns").getOrCreate()
value_counts_pd = class_counts.toPandas()

# Sort distinct program_titles by frequency of occurrences
distinct_values = dfdropnull.select("program_title").distinct().orderBy("program_title", ascending=True)

# 결과를 출력
distinct_values.show()

# COMMAND ----------

# DBTITLE 1,최대 상위 200개, 400개 추출해서 진행
from pyspark.sql.functions import desc

# 상위 200개 추출
top_200_program_title = dfdropnull.groupBy("program_title") \
    .count() \
    .orderBy(desc("count")) \
    .limit(200)

display(top_200_program_title.select("program_title"))

# 상위 400개 추출
top_400_program_title = dfdropnull.groupBy("program_title") \
    .count() \
    .orderBy(desc("count")) \
    .limit(400)

display(top_400_program_title.select("program_title"))

# COMMAND ----------

# DBTITLE 1,상위 200개 프로그램 타이틀을 가진 데이터만 추출
# 상위 200개
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("extract_matching_rows").getOrCreate()

# "dfdropnull" 및 "top_400_program_title" 데이터프레임을 생성 (이미 생성되어 있다고 가정)

# "program_title" 컬럼을 기준으로 두 데이터프레임을 조인
joined_df = dfdropnull.join(top_200_program_title, "program_title", "inner")

# 조인 결과 출력
joined_df.show()

# "program_title" 컬럼의 고유한 값 추출
unique_program_titles = joined_df.select("program_title").distinct()

# 결과 출력
unique_program_titles.count()

#temp view 만들기 
joined_df.createOrReplaceTempView("program_titles_table_temp")

# COMMAND ----------

# DBTITLE 1,상위 200개 프로그램 테이블 생성
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.top200_programtitle
# MAGIC COMMENT "top200_programtitle in youtube"
# MAGIC AS SELECT * FROM program_titles_table_temp;
# MAGIC
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.top200_programtitle;

# COMMAND ----------

# DBTITLE 1,상위 400개 프로그램 타이틀을 가진 데이터만 추출
# 상위 400개
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("extract_matching_rows").getOrCreate()

# "dfdropnull" 및 "top_400_program_title" 데이터프레임을 생성 (이미 생성되어 있다고 가정)

# "program_title" 컬럼을 기준으로 두 데이터프레임을 조인
joined_df = dfdropnull.join(top_400_program_title, "program_title", "inner")

# 조인 결과 출력
joined_df.show()

# "program_title" 컬럼의 고유한 값 추출
unique_program_titles = joined_df.select("program_title").distinct()

# 결과 출력
unique_program_titles.count()

#temp view 만들기 
joined_df.createOrReplaceTempView("program_titles_table_temp")

# COMMAND ----------

# DBTITLE 1,상위 400개 프로그램 테이블 생성
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mbc_poc_unity_catalog.mbc_poc_schema.top400_programtitle
# MAGIC COMMENT "top400_programtitle in youtube"
# MAGIC AS SELECT * FROM program_titles_table_temp;
# MAGIC
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.top400_programtitle;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,좀더 쉬운 조회수 regression 시도를 위해 테이블 조회
# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country;

# COMMAND ----------

# DBTITLE 1,테이블 데이터 읽어오기
# Databricks에서 SparkSession을 가져오기
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("mbc_ml_session").getOrCreate()

# Databricks 테이블을 데이터프레임으로 읽어오기
table_name = "mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country"  # 읽어올 테이블의 이름을 지정하세요
df = spark.table(table_name)

# 데이터프레임을 보거나 처리할 수 있습니다
df.show()

# COMMAND ----------

# DBTITLE 1,데이터를 correlation 매트릭스로 생성후 dataframe으로 변환
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("correlation_matrix").getOrCreate()

# Load the DataFrame (assuming it is already created)


# Get column names for all numerical columns in the DataFrame
columns = df.columns

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

# DBTITLE 1,테이블 조회하여 확인
# MAGIC %sql
# MAGIC SELECT * FROM mbc_poc_unity_catalog.mbc_poc_schema.z_report_by_daily_video_country;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 모델 관리 기능 - MLFlow

# COMMAND ----------

# DBTITLE 1,필요한 라이브러리를 가져옴
import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt
 
from numpy import savetxt
 
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# DBTITLE 1,scikit-learn에서 데이터세트를 가져오고 학습 및 테스트 데이터세트 생성
db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

# DBTITLE 1,mlflow.sklearn.autolog() 를 사용하여 랜덤 포레스트 모델과 로그 매개변수, 매트릭 및 모델 생성
# Enable autolog()
# mlflow.sklearn.autolog() requires mlflow 1.11.0 or above.
mlflow.sklearn.autolog()
 
# With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
with mlflow.start_run():
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  
  # Use the model to make predictions on the test dataset.
  predictions = rf.predict(X_test)
