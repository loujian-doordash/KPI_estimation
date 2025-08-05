# Databricks notebook source
from functools import reduce
import os
import re
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import types as T
# Snowflake options
DB_SCOPE = 'duocheng-scope'
user = dbutils.secrets.get(scope=DB_SCOPE, key="sfusername")
password = dbutils.secrets.get(scope=DB_SCOPE, key="sfpassword")

options = dict(
    sfurl="doordash.snowflakecomputing.com/",
    sfaccount="DOORDASH",
    sfuser=user,
    sfpassword=password,
    sfdatabase="proddb",
    sfschema="duocheng",
    sfwarehouse="ADHOC_ETL",
)

def load_data_from_snowflake(query, options=options) -> DataFrame:
    df = spark.read.format("snowflake").options(**options).option("query", query).load()
    df = df.select([F.col(x).alias(x.lower()) for x in df.columns])
    return df


def write_data_to_snowflake(
    df: DataFrame, table_name, mode="overwrite", options=options
):
    df.write.format("snowflake").options(**options).option("dbtable", table_name).mode(
        mode
    ).save()


def cast_data_type(df: DataFrame, dtypes: Optional[list] = None) -> DataFrame:
    def _check(name: str, col_type: str) -> DataFrame:
        if "decimal" in col_type:
            [_, scale] = re.findall(r"\d+", col_type)
            if scale == "0":
                return F.col(name).cast("int")
            else:
                return F.col(name).cast("double")
        else:
            return F.col(name)

    return df.select([_check(name, col_type) for name, col_type in df.dtypes])

# COMMAND ----------

month = "07"
day = "22"
date = month + day
date_with_dash = month + "-" + day

# COMMAND ----------

query = f'''
select 
TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD HH24:MI') AS CLIPPED_IMPRESSION_TIMESTAMP,
  CARD_POSITION,
  COALESCE(DD_SESSION_ID,'null') AS DD_SESSION_ID,
  COALESCE(TO_VARCHAR(STORE_ID), 'null') AS STORE_ID,
  COALESCE(L1_CATEGORY_ID, 'null') AS L1_CATEGORY_ID,
  COALESCE(L2_CATEGORY_ID, 'null') AS L2_CATEGORY_ID,
  BUSINESS_MERCHANT_SUPPLIED_ID AS UNHASHED_BMS_ID,
  PARSE_JSON(other_properties):ranking_event_id::STRING AS NV_RANKING_ID
FROM Proddb.public.fact_item_card_view_dedup
WHERE IS_SPONSORED = FALSE AND FEATURE = 'category' 
  AND DATE(RECEIVED_AT) = '2025-{date_with_dash}' 
  AND CURRENCY = 'USD'
'''

# COMMAND ----------

df_NV_impression = load_data_from_snowflake(query)
write_data_to_snowflake(df_NV_impression, f"proddb.duocheng.nv_impression_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: load sibyl log with pCTR

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat_ws, expr, map_keys,array_contains

# TODO: select NV_RANKING_ID: col("NV_RANKING_ID").alias('NV_RANKING_ID')
# TODO: Q: do we still need to select multiple models and then union?

df = spark.read.format("org.apache.iceberg.spark.source.IcebergSource") \
    .load("iceberg.iguazu_server_events_production.sps_nv_consumer") \
    .filter((col("iguazu_partition_date") == f"2025-{date_with_dash}") &  
            (col("predictor_name") == "dsml-nv-store_item_ranker_ctr_mtml") &
            (col("model_id") == "mtml_item_ranker_v2_ctr_1742750442") &
            (array_contains(map_keys(col("features")), "page_source")) &
            (array_contains(map_keys(col("features")), "ranking_event_id")) & 
            (col("features")["page_source"] == "RETAIL_CATEGORY_PAGE"))\
    .select(col("features")["dd_session_id"].alias("DD_SESSION_ID"),
            col("features")["store_id"].alias("STORE_ID"),
            col("features")["l1_category_id"].alias("L1_CATEGORY_ID"),
            col("features")["l2_category_id"].alias("L2_CATEGORY_ID"),
            col("prediction_result").alias("NV_PCTR"),
            col("features")["unhashed_business_merchant_supplied_id"].alias("UNHASHED_BMS_ID"),
            col("features")["ranking_event_id"].alias("NV_RANKING_ID"))

#df = df.drop("iguazu_sent_at", "_kafka_timestamp")
df.count()
write_data_to_snowflake(df, f"proddb.duocheng.nv_non_v2c_sibyl_log_{date}")

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat_ws, expr, map_keys,array_contains

df = spark.read.format("org.apache.iceberg.spark.source.IcebergSource") \
    .load("iceberg.iguazu_server_events_production.sps_nv_consumer") \
    .filter((col("iguazu_partition_date") == f"2025-{date_with_dash}") &  
            (col("predictor_name") == "dsml-nv-store_item_ranker_v2c_ctr_mtml") &
            (col("model_id") == "mtml_v2c_ctr_1747176394") &
            (array_contains(map_keys(col("features")), "page_source")) &
            (array_contains(map_keys(col("features")), "ranking_event_id")) & 
            (col("features")["page_source"] == "RETAIL_CATEGORY_PAGE"))\
    .select(col("features")["dd_session_id"].alias("DD_SESSION_ID"),
            col("features")["store_id"].alias("STORE_ID"),
            col("features")["l1_category_id"].alias("L1_CATEGORY_ID"),
            col("features")["l2_category_id"].alias("L2_CATEGORY_ID"),
            col("prediction_result").alias("NV_PCTR"),
            col("features")["unhashed_business_merchant_supplied_id"].alias("UNHASHED_BMS_ID"),
            col("features")["ranking_event_id"].alias("NV_RANKING_ID"))

#df = df.drop("iguazu_sent_at", "_kafka_timestamp")
df.count()
write_data_to_snowflake(df, f"proddb.duocheng.nv_v2c_sibyl_log_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2.5: union the log and generate fake Ranking ID

# COMMAND ----------

query = f'''
SELECT 
  COALESCE(DD_SESSION_ID, 'null') AS DD_SESSION_ID, 
  COALESCE(STORE_ID, 'null') AS STORE_ID, 
  COALESCE(L1_CATEGORY_ID, 'null') AS L1_CATEGORY_ID, 
  COALESCE(L2_CATEGORY_ID, 'null') AS L2_CATEGORY_ID,
  NV_PCTR,
  UNHASHED_BMS_ID,
  NV_RANKING_ID
FROM proddb.duocheng.nv_v2c_sibyl_log_{date}

UNION ALL

SELECT 
  COALESCE(DD_SESSION_ID, 'null') AS DD_SESSION_ID, 
  COALESCE(STORE_ID, 'null') AS STORE_ID, 
  COALESCE(L1_CATEGORY_ID, 'null') AS L1_CATEGORY_ID, 
  COALESCE(L2_CATEGORY_ID, 'null') AS L2_CATEGORY_ID,
  NV_PCTR,
  UNHASHED_BMS_ID,
  NV_RANKING_ID
FROM proddb.duocheng.nv_non_v2c_sibyl_log_{date};
'''


# COMMAND ----------

df_NV_union_log = load_data_from_snowflake(query)
write_data_to_snowflake(df_NV_union_log, f"proddb.duocheng.nv_union_sibyl_log_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 3: join sibyl log and nv impression

# COMMAND ----------

query = f'''
SELECT
  nv_imp.NV_RANKING_ID AS NV_RANKING_ID,
  nv_imp.CARD_POSITION AS CARD_POSITION,
  nv_imp.CLIPPED_IMPRESSION_TIMESTAMP AS CLIPPED_IMPRESSION_TIMESTAMP,
  nv_imp.DD_SESSION_ID AS DD_SESSION_ID,
  nv_imp.STORE_ID AS STORE_ID,
  nv_imp.L1_CATEGORY_ID AS L1_CATEGORY_ID,
  nv_imp.L2_CATEGORY_ID AS L2_CATEGORY_ID,
  nv_log.NV_PCTR AS NV_PCTR
FROM proddb.duocheng.nv_impression_{date} AS nv_imp
JOIN proddb.duocheng.nv_union_sibyl_log_{date} AS nv_log
  ON nv_imp.UNHASHED_BMS_ID = nv_log.UNHASHED_BMS_ID
     AND nv_imp.NV_RANKING_ID = nv_log.NV_RANKING_ID
'''

# COMMAND ----------

df_NV_joined_table = load_data_from_snowflake(query)
write_data_to_snowflake(df_NV_joined_table, f"proddb.duocheng.nv_joined_table_{date}")

# COMMAND ----------

query = f'''
SELECT
  NV_RANKING_ID,
  MIN(DD_SESSION_ID) AS DD_SESSION_ID_MIN,
  MIN(STORE_ID) AS STORE_ID_MIN,
  MIN(L1_CATEGORY_ID) AS L1_CATEGORY_ID_MIN,
  MIN(L2_CATEGORY_ID) AS L2_CATEGORY_ID_MIN,
  MIN(CLIPPED_IMPRESSION_TIMESTAMP) AS CLIPPED_IMPRESSION_TIMESTAMP_MIN,
  ARRAY_AGG(NV_PCTR) WITHIN GROUP (ORDER BY CARD_POSITION) AS NV_PCTR_LIST,
  ARRAY_AGG(CARD_POSITION) WITHIN GROUP (ORDER BY CARD_POSITION) AS CARD_POSITION_LIST
FROM proddb.duocheng.nv_joined_table_{date}
GROUP BY NV_RANKING_ID
'''

# COMMAND ----------

df_NV_grouped_table = load_data_from_snowflake(query)
write_data_to_snowflake(df_NV_grouped_table, f"proddb.duocheng.nv_grouped_table_{date}")

# COMMAND ----------

query = f'''
WITH valid_keys AS (
  SELECT
    DD_SESSION_ID_MIN,
    CLIPPED_IMPRESSION_TIMESTAMP_MIN,
    STORE_ID_MIN,
    L1_CATEGORY_ID_MIN,
    L2_CATEGORY_ID_MIN
  FROM proddb.duocheng.nv_grouped_table_{date}
  GROUP BY
    DD_SESSION_ID_MIN,
    CLIPPED_IMPRESSION_TIMESTAMP_MIN,
    STORE_ID_MIN,
    L1_CATEGORY_ID_MIN,
    L2_CATEGORY_ID_MIN
  HAVING COUNT(*) = 1
)
SELECT nv_table.*
FROM proddb.duocheng.nv_grouped_table_{date} nv_table
JOIN valid_keys vk
  ON nv_table.DD_SESSION_ID_MIN = vk.DD_SESSION_ID_MIN
 AND nv_table.CLIPPED_IMPRESSION_TIMESTAMP_MIN = vk.CLIPPED_IMPRESSION_TIMESTAMP_MIN
 AND nv_table.STORE_ID_MIN = vk.STORE_ID_MIN
 AND nv_table.L1_CATEGORY_ID_MIN = vk.L1_CATEGORY_ID_MIN
 AND nv_table.L2_CATEGORY_ID_MIN = vk.L2_CATEGORY_ID_MIN
'''

# COMMAND ----------

df_NV_final_table = load_data_from_snowflake(query)
write_data_to_snowflake(df_NV_final_table, f"proddb.duocheng.nv_final_table_{date}")