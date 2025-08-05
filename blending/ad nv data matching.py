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
day = "10"
date = month + day
date_with_dash = month + "-" + day

# COMMAND ----------

start_day = 10
end_day = 22

# COMMAND ----------

for day in range(start_day, end_day + 1):
  date = "07" + str(day)
  query = f'''
  SELECT
    ad.AD_QUALITY_SCORE_LIST,
    ad.EXPECTED_VALUE_LIST AS AD_EXPECTED_VALUE_LIST,
    ad.CARD_POSITION_LIST AS AD_CARD_POSITION_LIST,
    nv.NV_PCTR_LIST,
    nv.CARD_POSITION_LIST as NV_CARD_POSITION_LIST,
    ad.DD_SESSION_ID_MIN,
    ad.STORE_ID_MIN,
    ad.AD_REQUEST_ID,
    nv.NV_RANKING_ID,
    ad.L1_CATEGORY_ID_MIN,
    ad.L2_CATEGORY_ID_MIN,
    ad.UNHASHED_BMS_ID_LIST AS AD_UNHASHED_BMS_ID_LIST,
    nv.UNHASHED_BMS_ID_LIST AS NV_UNHASHED_BMS_ID_LIST,
  FROM proddb.duocheng.AD_GROUPED_LEFT_JOINED_TABLE_{date}_RM_DUP AS ad
  JOIN proddb.duocheng.nv_final_table_{date} AS nv
    ON ad.DD_SESSION_ID_MIN = nv.DD_SESSION_ID_MIN
  AND ad.CLIPPED_IMPRESSION_TIMESTAMP_MIN = nv.CLIPPED_IMPRESSION_TIMESTAMP_MIN
  AND ad.STORE_ID_MIN = nv.STORE_ID_MIN
  AND ad.L1_CATEGORY_ID_MIN = nv.L1_CATEGORY_ID_MIN
  AND ad.L2_CATEGORY_ID_MIN = nv.L2_CATEGORY_ID_MIN'''
  df_final_blending_input = load_data_from_snowflake(query)
  write_data_to_snowflake(df_final_blending_input, f"proddb.duocheng.final_blending_input_{date}")
