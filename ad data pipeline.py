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
day = "23"
date = month + day
date_with_dash = month + "-" + day

# COMMAND ----------

# select ad impressions from impression table
# Filters: select data only from 1. certain dates, 2. category surface, 3. Ad items and 4. USD currecncy
# clip timestep to minute level
# use COALESCE(xxx,'null') to avoid NULL = NULL in future steps
# TODO: 1. select users from only the treatment group;


query = f'''
SELECT
  TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD HH24:MI') AS CLIPPED_IMPRESSION_TIMESTAMP,
  CARD_POSITION,
  COALESCE(DD_SESSION_ID,'null') AS DD_SESSION_ID,
  COALESCE(TO_VARCHAR(STORE_ID), 'null') AS STORE_ID,
  COALESCE(L1_CATEGORY_ID, 'null') AS L1_CATEGORY_ID,
  COALESCE(L2_CATEGORY_ID, 'null') AS L2_CATEGORY_ID,
  ID AS IMPRESSION_EVENT_ID,
  DATE(RECEIVED_AT) AS RECEIVED_AT
FROM Proddb.public.fact_item_card_view_dedup
WHERE IS_SPONSORED = TRUE AND FEATURE = 'category' 
  AND DATE(RECEIVED_AT) = '2025-{date_with_dash}' 
  AND CURRENCY = 'USD'
'''


# COMMAND ----------

df_ad_impression = load_data_from_snowflake(query)
write_data_to_snowflake(df_ad_impression, f"proddb.duocheng.ad_impression_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: Join the processed impression table and the ads funnel table

# COMMAND ----------

# Join ad impression table with ad funnel table using IMPRESSION_EVENT_ID
query = f'''
SELECT 
  ad_funnel.AD_REQUEST_ID,
  ad_funnel.AD_QUALITY_SCORE,
  ad_funnel.EXPECTED_VALUE,
  ad_funnel.TRUE_BID,
  ad_funnel.BUSINESS_MERCHANT_SUPPLIED_ID AS UNHASHED_BMS_ID,
  PARSE_JSON(ad_funnel.PRICING_METADATA):"adRank"::INTEGER AS AD_RANK,
  ad_imp.*
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL ad_funnel
LEFT JOIN proddb.duocheng.ad_impression_{date} ad_imp
  ON ad_imp.IMPRESSION_EVENT_ID = ad_funnel.IMPRESSION_EVENT_ID
  WHERE DATE(ad_funnel.AUCTION_OCCURRED_AT) = '2025-{date_with_dash}'
  AND (PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L1' OR PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L2')
'''

# COMMAND ----------

df_ad_joined_table = load_data_from_snowflake(query)
write_data_to_snowflake(df_ad_joined_table, f"proddb.duocheng.ad_left_joined_table_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 3

# COMMAND ----------

query = f'''
SELECT
  AD_REQUEST_ID,
  MIN(DD_SESSION_ID) AS DD_SESSION_ID_MIN,
  MIN(STORE_ID) AS STORE_ID_MIN,
  MIN(L1_CATEGORY_ID) AS L1_CATEGORY_ID_MIN,
  MIN(L2_CATEGORY_ID) AS L2_CATEGORY_ID_MIN,
  MIN(CLIPPED_IMPRESSION_TIMESTAMP) AS CLIPPED_IMPRESSION_TIMESTAMP_MIN,
  ARRAY_AGG(AD_QUALITY_SCORE) 
    WITHIN GROUP (
      ORDER BY 
        CASE WHEN CARD_POSITION IS NULL THEN 1 ELSE 0 END,
        CARD_POSITION,
        CASE WHEN CARD_POSITION IS NULL THEN -EXPECTED_VALUE END
    ) AS AD_QUALITY_SCORE_LIST,
  ARRAY_AGG(UNHASHED_BMS_ID) 
    WITHIN GROUP (
      ORDER BY 
        CASE WHEN CARD_POSITION IS NULL THEN 1 ELSE 0 END,
        CARD_POSITION,
        CASE WHEN CARD_POSITION IS NULL THEN -EXPECTED_VALUE END
    ) AS UNHASHED_BMS_ID_LIST,    
  ARRAY_AGG(EXPECTED_VALUE) 
    WITHIN GROUP (
      ORDER BY 
        CASE WHEN CARD_POSITION IS NULL THEN 1 ELSE 0 END,
        CARD_POSITION,
        CASE WHEN CARD_POSITION IS NULL THEN -EXPECTED_VALUE END
    ) AS EXPECTED_VALUE_LIST,
  ARRAY_AGG(TRUE_BID) 
    WITHIN GROUP (
      ORDER BY 
        CASE WHEN CARD_POSITION IS NULL THEN 1 ELSE 0 END,
        CARD_POSITION,
        CASE WHEN CARD_POSITION IS NULL THEN -EXPECTED_VALUE END
    ) AS TRUE_BID_LIST,    
  ARRAY_AGG(COALESCE(CARD_POSITION, 99999)) 
    WITHIN GROUP (
      ORDER BY 
        CASE WHEN CARD_POSITION IS NULL THEN 1 ELSE 0 END,
        CARD_POSITION,
        CASE WHEN CARD_POSITION IS NULL THEN -EXPECTED_VALUE END
    ) AS CARD_POSITION_LIST

FROM proddb.duocheng.ad_left_joined_table_{date}
GROUP BY AD_REQUEST_ID;
'''

# COMMAND ----------

df_ad_grouped_table = load_data_from_snowflake(query)
write_data_to_snowflake(df_ad_grouped_table, f"proddb.duocheng.ad_grouped_left_joined_table_{date}")

# COMMAND ----------

# MAGIC %md
# MAGIC step 4: remove dup

# COMMAND ----------

query = f'''
WITH valid_keys AS (
  SELECT
    DD_SESSION_ID_MIN,
    CLIPPED_IMPRESSION_TIMESTAMP_MIN,
    STORE_ID_MIN,
    L1_CATEGORY_ID_MIN,
    L2_CATEGORY_ID_MIN
  FROM proddb.duocheng.AD_GROUPED_LEFT_JOINED_TABLE_{date}
  GROUP BY
    DD_SESSION_ID_MIN,
    CLIPPED_IMPRESSION_TIMESTAMP_MIN,
    STORE_ID_MIN,
    L1_CATEGORY_ID_MIN,
    L2_CATEGORY_ID_MIN
  HAVING COUNT(*) = 1
)
SELECT ad_table.*
FROM proddb.duocheng.AD_GROUPED_LEFT_JOINED_TABLE_{date} ad_table
JOIN valid_keys vk
  ON ad_table.DD_SESSION_ID_MIN = vk.DD_SESSION_ID_MIN
 AND ad_table.CLIPPED_IMPRESSION_TIMESTAMP_MIN = vk.CLIPPED_IMPRESSION_TIMESTAMP_MIN
 AND ad_table.STORE_ID_MIN = vk.STORE_ID_MIN
 AND ad_table.L1_CATEGORY_ID_MIN = vk.L1_CATEGORY_ID_MIN
 AND ad_table.L2_CATEGORY_ID_MIN = vk.L2_CATEGORY_ID_MIN
'''

# COMMAND ----------

df_ad_grouped_table_rm_dup = load_data_from_snowflake(query)
write_data_to_snowflake(df_ad_grouped_table_rm_dup, f"proddb.duocheng.AD_GROUPED_LEFT_JOINED_TABLE_{date}_RM_DUP")