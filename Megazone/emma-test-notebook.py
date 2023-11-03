# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.ls("dbfs:/pipelines/17730c34-3671-45d1-a4fd-9517bbf50c26/system/events/_delta_log/")

# COMMAND ----------

dbutils.fs.ls("s3a://databricks-workspace-stack-107c6-metastore-bucket/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from csv.`s3://mbcinfo/upload/content_owner_basic_a3/`;
# MAGIC select * from csv.`s3://mbcinfo/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE mbc_poc_unity_catalog.mbc_poc_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tb_video_id_word;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW content_owner_basic_a3_temp_view(
# MAGIC 	date DATE,
# MAGIC 	channel_id STRING,
# MAGIC 	video_id STRING,
# MAGIC 	claimed_status STRING,
# MAGIC 	uploader_type STRING,
# MAGIC 	live_or_on_demand STRING,
# MAGIC 	subscribed_status STRING,
# MAGIC 	country_code STRING,
# MAGIC 	views INT,
# MAGIC 	comments INT,
# MAGIC 	shares INT,
# MAGIC 	watch_time_minutes DOUBLE,
# MAGIC 	average_view_duration_seconds DOUBLE,
# MAGIC 	average_view_duration_percentage DOUBLE,
# MAGIC 	annotation_impressions INT,
# MAGIC 	annotation_clickable_impressions INT,
# MAGIC 	annotation_clicks INT,
# MAGIC 	annotation_click_through_rate DOUBLE,
# MAGIC 	annotation_closable_impressions INT,
# MAGIC 	annotation_closes INT,
# MAGIC 	annotation_close_rate DOUBLE,
# MAGIC 	card_teaser_impressions INT,
# MAGIC 	card_teaser_clicks INT,
# MAGIC 	card_teaser_click_rate DOUBLE,
# MAGIC 	card_impressions INT,
# MAGIC 	card_clicks INT,
# MAGIC 	card_click_rate DOUBLE,
# MAGIC 	subscribers_gained INT,
# MAGIC 	subscribers_lost INT,
# MAGIC 	videos_added_to_playlists INT,
# MAGIC 	videos_removed_from_playlists INT,
# MAGIC 	likes INT,
# MAGIC 	dislikes INT,
# MAGIC 	red_views INT,
# MAGIC 	red_watch_time_minutes DOUBLE,
# MAGIC 	video_title STRING,
# MAGIC 	channel_title STRING,
# MAGIC 	program_id STRING,
# MAGIC 	program_title STRING,
# MAGIC 	matching_method STRING,
# MAGIC 	total_view_count INT,
# MAGIC 	total_comment_count INT,
# MAGIC 	total_like_count INT,
# MAGIC 	total_dislike_count INT,
# MAGIC 	video_publish_datetime TIMESTAMP
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "s3://mbcinfo/upload/content_owner_basic_a3/", header "true");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM content_owner_basic_a3_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED content_owner_basic_a3_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE content_owner_basic_a3
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (partition_dt)
# MAGIC COMMENT "content_owner_basic_a3"
# MAGIC AS SELECT *, date_format(date, 'yyyyMM') as partition_dt, current_timestamp() as record_insert_dt FROM content_owner_basic_a3_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from content_owner_basic_a3 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE mbc_poc_unity_catalog.mbc_poc_schema;
# MAGIC SELECT * FROM mbc_youtube_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# ---------------------------------------------------
# JDBC --> Delta Table Overwrite Every Day 
# ---------------------------------------------------
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
#s3_path = "s3://databricks-workspace-stack-107c6-metastore-bucket/c0eb9d77-839a-4cdc-82d9-c7fc2465465b/tables/fe66778f-bbbe-4bbf-8d9b-d8c5c5bf1ef0"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("UPDT_DATE").save(s3_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN mbc_poc_unity_catalog.mbc_poc_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED mbc_poc_unity_catalog.mbc_poc_schema.mbc_youtube_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mbc_poc_unity_catalog.mbc_poc_schema.mbc_youtube_video limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mbc_youtube_video
# MAGIC where 1=1
# MAGIC  --and  partition_dt = '';
# MAGIC
# MAGIC  limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT partition_dt
# MAGIC FROM content_owner_demographics_a1;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE 를 사용한 Upsert 수행 
# MAGIC
# MAGIC Databricks에서는 MERGE문을 사용해서 Upsert- 데이터의 Update,Insert 및 기타 데이터 조작을 하나의 명령어로 수행합니다.  
# MAGIC 아래의 예제는 변경사항을 기록하는 CDC(Change Data Capture) 로그데이터를 updates라는 임시뷰로 생성합니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE mbc_poc_unity_catalog.mbc_poc_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 where date = "2023-09-01" and channel_id = "UC7lb15P-Hux7A5gBuhCxtuQ" and video_id = "-1M-dNL5KU0" and country_code = "KE" and subscribed_status = "not_subscribed";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 where date = "2023-09-01" and channel_id = "UC7lb15P-Hux7A5gBuhCxtuQ" and video_id = "-1M-dNL5KU0" and country_code = "AU" and subscribed_status = "not_subscribed";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from content_owner_demographics_a1 where date = "2023-09-01" and country_code = "AU" and subscribed_status = "subscribed";

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW user_data_upsert_values(date,	channel_id,	video_id,	claimed_status,	uploader_type,	live_or_on_demand,	subscribed_status,	country_code,	age_group,	gender,	views_percentage,	video_title,	channel_title,	program_id,	program_title,	matching_method,	total_view_count,	total_comment_count,	total_like_count,	total_dislike_count,	video_publish_datetime, month, type) AS VALUES
# MAGIC   ("2023-10-01", "UC7lb15P-Hux7A5gBuhCxtuQ", "-1M-dNL5KU0", "claimed",	"self",	"on_demand",	"subscribed",	"KE",	"AGE_55_64",	"FEMALE",	0.000464246087566097,	"[연인 10회 예고] ＂당신... 이젠 내가 가져야겠어＂, MBC 230902 방송",	"MBCdrama",	"M01_T60433G",	null,	"SMR",	422565,	1251,	4453,	32,	"2023-09-01T14:38:12.000+0000",	9,"update"),
# MAGIC   ("2023-09-01", "UCG5bAssl2H0wjLG4BEv5ScQ", "WPmrx-O_Oi8", "claimed",	"self",	"on_demand",	"subscribed",	"AU",	"AGE_65_",	"FEMALE",	0.714285714285714,	"[어디서 타는 냄새 안나요?] 불새 Phoenix 세훈에게 노골적으로 반감을 드러내는 정민",	"옛드 : MBC 레전드 드라마",	null,	null,	null,	52130,	10,	100,	9,	"2012-07-09T10:40:24.000+0000",	9,"delete"),
# MAGIC   ("2023-10-01", "UC7lb15P-Hux7A5gBuhCxtuQ", "-1M-dNL5KU0", "claimed",	"self",	"on_demand",	"subscribed",	"AO",	"AGE_55_64",	"FEMALE",	0.000464246087566097,	"[연인 10회 예고] ＂당신... 이젠 내가 가져야겠어＂, MBC 230902 방송",	"MBCdrama",	"M01_T60433G",	null,	"SMR",	422565,	1251,	4453,	32,	"2023-09-01T14:38:12.000+0000",	9,"insert");
# MAGIC   
# MAGIC SELECT * FROM user_data_upsert_values;

# COMMAND ----------

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

# MAGIC %sql
# MAGIC SELECT * FROM content_owner_basic_a3;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Table History를 사용한 Time Travel 기능 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history content_owner_basic_a3;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Data file에 대한 최적화 
# MAGIC
# MAGIC 이런저런 작업을 하다 보면 필연적으로 굉장히 작은 데이터 파일들이 많이 생성되게 됩니다.  
# MAGIC 성능 향상을 위해서 이런 파일들에 대한 최적화하는 방법과 불필요한 파일들을 정리하는 명령어들에 대해서 알아봅시다. 

# COMMAND ----------

# MAGIC %md
# MAGIC **`OPTIMIZE`** 명령어는 기존의 데이터 파일내의 레코드들을 합쳐서 새로 최적의 사이즈로 파일을 만들고 기존의 작은 파일들을 읽기 성능이 좋은 큰 파일들로 대체합니다.  
# MAGIC 이 떄 옵션값으로 하나 이상의 필드를 지정해서 **`ZORDER`** 인덱싱을 수행할 수 있습니다.  
# MAGIC Z-Ordering은 관련 정보를 동일한 파일 집합에 배치해서 읽어야 하는 데이터의 양을 줄여 쿼리 성능을 향상 시키는 기술입니다. 쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 `ZORDER BY`를 사용합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL content_owner_basic_a3;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE content_owner_basic_a3 ZORDER BY channel_id;
