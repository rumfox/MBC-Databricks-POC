# Databricks notebook source
# MAGIC %md 
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 데이터 엔지니어링 
# MAGIC 테이블을 생성해보고 SQL 쿼리를 통해 데이터를 핸들링하여 파이프라인을 생성해 스케쥴링 또는 특정 이벤트 발생시의 데이터 활용
# MAGIC <br><br/>
# MAGIC 1. Table Format
# MAGIC <br>1.1. SQL Query - AWS S3에서 데이터를 가져와서 Delta table 생성
# MAGIC <br>1.2. Schema Evolution - 필드 추가/삭제, 필드 타입 변경
# MAGIC <br>1.3. Time Travel - 데이터 변경이력 관리
# MAGIC <br>1.4. Optimize - 데이터 조회 성능 최적화
# MAGIC <br>1.5. Upsert - 데이터 입력/수정/삭제
# MAGIC <br>1.6. Partitioning - 대용량 데이터 파티셔닝
# MAGIC <br>1.7. Metadata 관리 - Unity Catalog를 활용한 관리
# MAGIC 2. Pipeline (ETL) - 오라클 디비에서 불러와서 json으로 적재한것, 기존에 가져올때부터 데이터 조작을 해서 가져오는것 
# MAGIC <br>2.1. Schedule, Trigger 기반 - 사전 정의된 스케쥴 수행 및 특정 이벤트 발생시 데이터 활용
# MAGIC <br>2.2. Streaming - Delta Live Tables로 실시간 데이터 활용
# MAGIC 3. Pipeline (ELT) - csv 파일 데이터를 불러오고 나서 조작 하는것
# MAGIC <br>3.1. Data Transformation - ELT 구성

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Table Format

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. SQL Query - AWS S3에서 데이터를 가져와서 Delta table 생성

# COMMAND ----------

# DBTITLE 1,S3 경로에 적재되어있는 csv 파일 리스트 조회
# MAGIC %sql
# MAGIC select * from csv.`s3://mbcinfo/upload/content_owner_demographics_a1/`;

# COMMAND ----------

# DBTITLE 1,delta table을 생성할 상위 카탈로그와 스키마 지정
# MAGIC %sql
# MAGIC USE mbc_poc_unity_catalog.mbc_poc_schema;

# COMMAND ----------

# DBTITLE 1,S3 경로에 적재되어있는 csv 파일로 temp view 생성
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW content_owner_demographics_a1_temp_view(
# MAGIC   date DATE,
# MAGIC   channel_id STRING,
# MAGIC   video_id STRING,
# MAGIC   claimed_status STRING,
# MAGIC   uploader_type STRING,
# MAGIC   live_or_on_demand STRING,
# MAGIC   subscribed_status STRING,
# MAGIC   country_code STRING,
# MAGIC   age_group STRING,
# MAGIC   gender STRING,
# MAGIC   views_percentage DOUBLE,
# MAGIC   video_title STRING,
# MAGIC   channel_title STRING,
# MAGIC   program_id STRING,
# MAGIC   program_title STRING,
# MAGIC   matching_method STRING,
# MAGIC   total_view_count INT,
# MAGIC   total_comment_count INT,
# MAGIC   total_like_count INT,
# MAGIC   total_dislike_count INT,
# MAGIC   video_publish_datetime TIMESTAMP 
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "s3://mbcinfo/upload/content_owner_demographics_a1/", header "true");

# COMMAND ----------

# DBTITLE 1,temp view 데이터 조회
# MAGIC %sql
# MAGIC SELECT * FROM content_owner_demographics_a1_temp_view;

# COMMAND ----------

# DBTITLE 1,temp view 정보 조회
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED content_owner_demographics_a1_temp_view;

# COMMAND ----------

# DBTITLE 1,temp view 로 delta table 생성
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE content_owner_demographics_a1
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (partition_dt) -- 인덱싱이 되어서 데이터 조회 성능이 더 좋아짐
# MAGIC COMMENT "content_owner_demographics_a1"
# MAGIC AS SELECT *, date_format(date, 'yyyyMM') as partition_dt, current_timestamp() as record_insert_dt FROM content_owner_demographics_a1_temp_view;

# COMMAND ----------

# DBTITLE 1,delta table 이 잘 생성되었는지 조회
# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Schema Evolution - 필드 추가/삭제, 필드 타입 변경

# COMMAND ----------

# DBTITLE 1,필드 추가
# MAGIC %sql
# MAGIC ALTER TABLE content_owner_demographics_a1 ADD COLUMN field_test int;

# COMMAND ----------

# DBTITLE 1,필드 타입 변경
from pyspark.sql.functions import col

spark.read.table("content_owner_demographics_a1").withColumn("field_test", col("field_test").cast("string")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("content_owner_demographics_a1")

# COMMAND ----------

# DBTITLE 1,필드 삭제
# MAGIC %sql
# MAGIC
# MAGIC -- 테이블의 프로토콜 버전이 drop 하기에 충분치 않으므로 업그레이드
# MAGIC ALTER TABLE content_owner_demographics_a1 SET TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = '2',
# MAGIC     'delta.minWriterVersion' = '5',
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE content_owner_demographics_a1 DROP COLUMN field_test;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Time Travel - 데이터 변경이력 관리

# COMMAND ----------

# DBTITLE 1,테이블 history 조회
# MAGIC %sql
# MAGIC DESCRIBE HISTORY content_owner_demographics_a1;

# COMMAND ----------

# DBTITLE 1,과거 버전의 데이터 조회
# MAGIC %sql
# MAGIC SELECT * FROM content_owner_demographics_a1 VERSION AS OF 2;

# COMMAND ----------

# DBTITLE 1,과거 버전으로 돌아가기
# MAGIC %sql
# MAGIC RESTORE TABLE content_owner_demographics_a1 TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Optimize - 데이터 조회 성능 최적화

# COMMAND ----------

# MAGIC %md
# MAGIC **`OPTIMIZE`** 명령어는 기존의 데이터 파일내의 레코드들을 합쳐서 새로 최적의 사이즈로 파일을 만들고 기존의 작은 파일들을 읽기 성능이 좋은 큰 파일들로 대체 
# MAGIC <br>이 때 옵션값으로 하나 이상의 필드를 지정해서 **`ZORDER`** 인덱싱 수행 가능
# MAGIC <br>Z-Ordering은 관련 정보를 동일한 파일 집합에 배치해서 읽어야 하는 데이터의 양을 줄여 쿼리 성능을 향상 시키는 기술 
# MAGIC <br>쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 `ZORDER BY`를 사용

# COMMAND ----------

# DBTITLE 1,opimize zorder 실행
# MAGIC %sql
# MAGIC OPTIMIZE content_owner_demographics_a1 ZORDER BY channel_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Upsert - 데이터 입력 insert/수정 update/삭제 delete

# COMMAND ----------

# DBTITLE 1,사용할 카탈로그와 스키마 지정
# MAGIC %sql
# MAGIC USE mbc_poc_unity_catalog.mbc_poc_schema;

# COMMAND ----------

# DBTITLE 1,데이터를 조작할 테이블 조회
# MAGIC %sql
# MAGIC SELECT * FROM content_owner_demographics_a1;

# COMMAND ----------

# DBTITLE 1,update 할 데이터를 조건을 지정하여 조회
# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 
# MAGIC where date = "2023-09-01" 
# MAGIC and channel_id = "UC70u2e8gK14R7QfFs7ehNDw" 
# MAGIC and video_id = "07vb0mFXoW8"
# MAGIC and claimed_status = "claimed"
# MAGIC and uploader_type = "self"
# MAGIC and live_or_on_demand = "on_demand"
# MAGIC and subscribed_status = "not_subscribed"
# MAGIC and country_code = "KE"
# MAGIC and age_group = "AGE_35_44"
# MAGIC and gender = "FEMALE"
# MAGIC and views_percentage = 0.030000750018750466
# MAGIC and video_title = "[C.C.] Visiting home after a long time, lounging while watching TV. Feeling so happy! #SHINEE #KEY"
# MAGIC and channel_title = "MBC WORLD"
# MAGIC and total_view_count = 223996
# MAGIC and total_comment_count = 99
# MAGIC and total_like_count = 5702
# MAGIC and total_dislike_count = 42
# MAGIC and video_publish_datetime = "2023-08-28T05:00:00.000+0000";

# COMMAND ----------

# DBTITLE 1,delete 할 데이터를 조건을 지정하여 조회
# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 
# MAGIC where date = "2023-09-01" 
# MAGIC and channel_id = "UCG5bAssl2H0wjLG4BEv5ScQ" 
# MAGIC and video_id = "WPmrx-O_Oi8"
# MAGIC and claimed_status = "claimed"
# MAGIC and uploader_type = "self"
# MAGIC and live_or_on_demand = "on_demand"
# MAGIC and subscribed_status = "subscribed" 
# MAGIC and country_code = "AU" 
# MAGIC and age_group = "AGE_65_"
# MAGIC and gender = "FEMALE"
# MAGIC and views_percentage = 0.7142857142857143
# MAGIC and video_title = "[어디서 타는 냄새 안나요?] 불새 Phoenix 세훈에게 노골적으로 반감을 드러내는 정민"
# MAGIC and channel_title = "옛드 : MBC 레전드 드라마"
# MAGIC and total_view_count = 52130
# MAGIC and total_comment_count = 10
# MAGIC and total_like_count = 100
# MAGIC and total_dislike_count = 9
# MAGIC and video_publish_datetime = "2012-07-09T10:40:24.000+0000";

# COMMAND ----------

# DBTITLE 1,insert 할 데이터를 조건을 지정하여 조회
# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 
# MAGIC where date = "2023-10-01"
# MAGIC and channel_id = "UC7lb15P-Hux7A5gBuhCxtuQ"
# MAGIC and video_id = "-1M-dNL5KU0"
# MAGIC and claimed_status = "claimed"
# MAGIC and uploader_type = "self"
# MAGIC and live_or_on_demand = "on_demand"
# MAGIC and subscribed_status = "subscribed"
# MAGIC and country_code = "AO"
# MAGIC and age_group = "AGE_55_64"
# MAGIC and gender = "FEMALE"
# MAGIC and views_percentage = 0.000464246087566097
# MAGIC and video_title = "[연인 10회 예고] ＂당신... 이젠 내가 가져야겠어＂, MBC 230902 방송"
# MAGIC and channel_title = "MBCdrama"
# MAGIC and program_id = "M01_T60433G"
# MAGIC and program_title = null
# MAGIC and matching_method = "SMR"
# MAGIC and total_view_count = 422565
# MAGIC and total_comment_count = 1251
# MAGIC and total_like_count = 4453
# MAGIC and total_dislike_count = 32
# MAGIC and video_publish_datetime = "2023-09-01T14:38:12.000+0000";

# COMMAND ----------

# DBTITLE 1,update, delete, insert 할 데이터로 temp view 생성 및 조회
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW user_data_upsert_values(date,	channel_id,	video_id,	claimed_status,	uploader_type,	live_or_on_demand,	subscribed_status,	country_code,	age_group,	gender,	views_percentage,	video_title,	channel_title,	program_id,	program_title,	matching_method,	total_view_count,	total_comment_count,	total_like_count,	total_dislike_count,	video_publish_datetime, type) AS VALUES
# MAGIC   ("2023-10-01", "UC7lb15P-Hux7A5gBuhCxtuQ", "-1M-dNL5KU0", "claimed",	"self",	"on_demand",	"not_subscribed",	"KE",	"AGE_35_44",	"FEMALE",	0.030000750018750466,	"[C.C.] Visiting home after a long time, lounging while watching TV. Feeling so happy! #SHINEE #KEY",	"MBC WORLD",	null,	null,	null,	223996,	99,	5702,	42,	"2023-08-28T05:00:00.000+0000",	"update"),
# MAGIC   ("2023-09-01", "UCG5bAssl2H0wjLG4BEv5ScQ", "WPmrx-O_Oi8", "claimed",	"self",	"on_demand",	"subscribed",	"AU",	"AGE_65_",	"FEMALE",	0.714285714285714,	"[어디서 타는 냄새 안나요?] 불새 Phoenix 세훈에게 노골적으로 반감을 드러내는 정민",	"옛드 : MBC 레전드 드라마",	null,	null,	null,	52130,	10,	100,	9,	"2012-07-09T10:40:24.000+0000","delete"),
# MAGIC   ("2023-10-01", "UC7lb15P-Hux7A5gBuhCxtuQ", "-1M-dNL5KU0", "claimed",	"self",	"on_demand",	"subscribed",	"AO",	"AGE_55_64",	"FEMALE",	0.000464246087566097,	"[연인 10회 예고] ＂당신... 이젠 내가 가져야겠어＂, MBC 230902 방송",	"MBCdrama",	"M01_T60433G",	null,	"SMR",	422565,	1251,	4453,	32,	"2023-09-01T14:38:12.000+0000",	"insert");
# MAGIC   
# MAGIC SELECT * FROM user_data_upsert_values;

# COMMAND ----------

# DBTITLE 1,temp view 데이터를 update, delete, insert 타입별로 테이블에 merge
# MAGIC %sql
# MAGIC MERGE INTO content_owner_demographics_a1 b
# MAGIC USING user_data_upsert_values u
# MAGIC ON b.channel_id=u.channel_id
# MAGIC AND b.video_id=u.video_id
# MAGIC AND b.video_title=u.video_title
# MAGIC AND b.country_code=u.country_code
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Partitioning - 대용량 데이터 파티셔닝

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1번에서 Delta table 생성시에 파티셔닝 수행함
# MAGIC
# MAGIC <br>오라클 rdb 에서도 유사한 개념
# MAGIC <br>CREATE OR REPLACE TABLE content_owner_demographics_a1
# MAGIC <br>USING DELTA
# MAGIC <br>_**PARTITIONED BY (partition_dt)**_
# MAGIC <br>COMMENT "content_owner_demographics_a1"
# MAGIC <br>AS SELECT *, date_format(date, 'yyyyMM') as partition_dt, current_timestamp() as record_insert_dt FROM content_owner_demographics_a1_temp_view;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Metadata 관리 - Unity Catalog를 활용한 관리

# COMMAND ----------

# DBTITLE 1,unity catalog 생성
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS mbc_poc_unity_catalog;

# COMMAND ----------

# DBTITLE 1,생성한 카탈로그 사용
# MAGIC %sql
# MAGIC USE CATALOG mbc_poc_unity_catalog;

# COMMAND ----------

# DBTITLE 1,카탈로그 하위 테이블 정보 조회시 pipeline_internal.catalogType: "UNITY_CATALOG" 확인 가능
# MAGIC %sql
# MAGIC desc detail mbc_poc_unity_catalog.mbc_poc_schema.mbc_youtube_video;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Pipeline (ETL)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Schedule, Trigger 기반 - 사전 정의된 스케쥴 수행 및 특정 이벤트 발생시 데이터 활용
# MAGIC Workflows 메뉴에서 생성한 Job을 클릭하면 우측에서 Job 설정값을 볼 수 있음
# MAGIC <br>Schedules & Triggers 에서 Edit Trigger를 클릭하여 특정 스케쥴과 트리거 설정 가능 

# COMMAND ----------

# MAGIC %md
# MAGIC ![Image Alt Text](https://github.com/seungheejo/test/blob/main/img/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA%202023-10-31%20%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB%209.44.35.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Streaming - Delta Live Tables로 실시간 데이터 활용
# MAGIC Workflows 메뉴에서 생성한 Delta Live Tables를 클릭하면 파이프라인 화면을 볼 수 있음
# MAGIC <br>화면 하위에서 데이터 활용 로그를 확인 가능하고 권한 셋팅 및 Development 또는 Production으로 변경 가능

# COMMAND ----------

# MAGIC %md
# MAGIC ![Image Alt Text](https://github.com/seungheejo/test/blob/main/img/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA%202023-10-31%20%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB%209.47.39.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Pipeline (ELT)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data Transformation - ELT 구성

# COMMAND ----------

# DBTITLE 1,데이터 추출 -> 데이터 로드 -> 데이터 변환
from pyspark.sql.functions import col

# Read data from CSV into a DataFrame
df_extracted = spark.read.csv("s3://mbcinfo/upload/content_owner_basic_a3/", header=True, inferSchema=True)

# Example transformation: Add a new column
df_transformed = df_extracted.withColumn("total_comment_count_twice", col("total_comment_count") * 2)

# Register DataFrame as a temporary view
df_extracted.createOrReplaceTempView("my_temp_view")

# Use SQL expressions for transformations
df_transformed_sql = spark.sql("""
  SELECT *,
         old_column * 2 AS new_column
  FROM my_temp_view
""")
