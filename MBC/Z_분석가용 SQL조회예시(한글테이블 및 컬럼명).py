# Databricks notebook source
# MAGIC %md 
# MAGIC #### 한글 테이블/컬럼명 조회 예시
# MAGIC - ' 가 아닌  `를 사용해야 함 (아래 예시에서 복사해서 사용할 것)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  `기준일YYYYMMDD`
# MAGIC         , T01.*
# MAGIC FROM mbc_poc_unity_catalog.mbc_poc_schema.`z_일자_비디오_성별_연령대_구독여부별_View` T01

# COMMAND ----------

# MAGIC %md
# MAGIC #### 일자별 비디오별 전체뷰수 대비, 구독상태view수 및 비율 조회
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  `기준일YYYYMMDD`
# MAGIC         , SUBSTRING(`콘텐츠명`, 1, 20)                                  AS `콘텐츠명`
# MAGIC         , SUBSTRING(`비디오명`, 1, 30)                                  AS `비디오명`
# MAGIC         , ROUND(SUM(CASE WHEN `구독여부` = 'Y' THEN `view수_누적당일` ELSE 0 END) / SUM(`view수_누적당일`)*100, 2) || '%'  AS `구독상태view비율`
# MAGIC         , SUM(`view수_누적당일`)                                           AS `view수`
# MAGIC         , SUM(CASE WHEN `구독여부` = 'Y' THEN `view수_누적당일` ELSE 0 END) AS `구독상태view수`
# MAGIC  FROM mbc_poc_unity_catalog.mbc_poc_schema.`z_일자_비디오_성별_연령대_구독여부별_View` T01
# MAGIC WHERE  `비디오명` IS NOT NULL
# MAGIC GROUP BY `기준일YYYYMMDD`
# MAGIC         , SUBSTRING(`콘텐츠명`, 1, 20)
# MAGIC        , `비디오ID`
# MAGIC        , SUBSTRING(`비디오명`, 1, 30) 
# MAGIC ORDER BY 1 DESC
# MAGIC        , 3       
