# Databricks notebook source
from functools import reduce
import os
import re
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import types as T

import ast
import random
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, FloatType
from pyspark.sql.functions import udf
import matplotlib.pyplot as plt

from pyspark.sql.functions import expr, lit, avg, expr, posexplode, col, monotonically_increasing_id, sort_array, collect_list, size, explode, sum as _sum
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

# blending config
K = 12
ALPHA = 20
BETA = ALPHA
MAX_ADS_BLOCK_SIZE = 3
MIN_NV_BLOCK_SIZE = 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

query = """SELECT * FROM proddb.duocheng.FINAL_BLENDING_LONG_INPUT_UPTO_0721"""
df_raw = load_data_from_snowflake(query)

# COMMAND ----------

columns_to_cast = [
    ("ad_card_position_list", ArrayType(IntegerType()), "ad_sorted_card_position_array"),
    ("nv_card_position_list", ArrayType(IntegerType()), "nv_sorted_card_position_array"),
    ("ad_quality_score_list", ArrayType(FloatType()), "ad_quality_score_array"),
    ("ad_expected_value_list", ArrayType(FloatType()), "ad_expected_value_array"),
    ("nv_pctr_list", ArrayType(FloatType()), "nv_pctr_array"),
    ("ad_unhashed_bms_id_list", ArrayType(StringType()), "ad_unhashed_bms_id_array"),
    ("nv_unhashed_bms_id_list", ArrayType(StringType()), "nv_unhashed_bms_id_array"),
]

for col, arr_type, new_col in columns_to_cast:
    df_raw = df_raw.withColumn(
        new_col,
        F.from_json(F.regexp_replace(F.col(col), r"\n|\s", ""), arr_type)
    )

# COMMAND ----------

df_unified = df_raw.withColumns({

    # Step 1: zip and rename struct fields
    "zipped_struct": expr("""
        transform(
            arrays_zip(
                concat(ad_sorted_card_position_array, nv_sorted_card_position_array),
                concat(ad_quality_score_array, nv_pctr_array),
                concat(ad_expected_value_array, array_repeat(0.0, size(nv_sorted_card_position_array)))
            ),
            x -> named_struct('position', x['0'], 'engagement', x['1'], 'revenue', x['2'])
        )
    """)

}).withColumns({

    # Step 2: sort the array of struct by pos (default behavior of sort_array)
    "sorted_struct": expr("sort_array(zipped_struct)")

}).withColumns({

    # Step 3: extract signals from sorted struct
    "card_position_array": expr("transform(sorted_struct, x -> x.position)"),
    "expected_engagement_array": expr("transform(sorted_struct, x -> x.engagement)"),
    "expected_revenue_array": expr("transform(sorted_struct, x -> x.revenue)")

}).withColumns({

    # Step 4
    # Discounted sum of top-K engagement: sum(x / log2(i + 2))
    "sum_enga_util_topk": expr(f"""
        aggregate(
            zip_with(
                slice(expected_engagement_array, 1, {K}),
                sequence(0, {K - 1}),
                (x, i) -> x / log2(i + 2)
            ),
            0D,
            (acc, val) -> acc + val
        )
    """),

    # Discounted sum of top-K revenue: sum(x / log2(i + 2))
    "sum_rev_util_topk": expr(f"""
        aggregate(
            zip_with(
                slice(expected_revenue_array, 1, {K}),
                sequence(0, {K - 1}),
                (x, i) -> x / log2(i + 2)
            ),
            0D,
            (acc, val) -> acc + val
        )
    """)
})

# COMMAND ----------

# MAGIC %md
# MAGIC # Blending

# COMMAND ----------

def merge_sort_blending_dedup(
  ads_expected_engagement_utility,
  ads_expected_revenue_utility,
  nv_expected_engagement_utility,
  alpha,
  beta,
  ads_sorted_card_position,
  nv_sorted_card_position,
  max_ads_block_size,
  min_nv_block_size,
  ads_bms_id,
  nv_bms_id,
):
  if not beta:
    beta = alpha
  i = j = 0
  merged = []

  consecutive_ads_count = 0
  consecutive_nv_count = 0
  must_insert_nv = False
  placed_item_id = set()

  def insert_nv_to_merged(j):
    merged.append({
      "source": "nv",
      "revenue_signal": 0.0,
      "engagement_signal": nv_expected_engagement_utility[j],
      "index": j,
      "original_card_position": nv_sorted_card_position[j]  
    })
    placed_item_id.add(nv_bms_id[j])

  def insert_ads_to_merged(i):
    merged.append({
      "source": "ads",
      "revenue_signal": ads_expected_revenue_utility[i],
      "engagement_signal": ads_expected_engagement_utility[i],
      "index": i,
      "original_card_position": ads_sorted_card_position[i]
    })
    placed_item_id.add(ads_bms_id[i])

  def compare_ads_to_nv(i, j):
    return ads_expected_revenue_utility[i] + alpha * ads_expected_engagement_utility[i] > beta * nv_expected_engagement_utility[j]

  while i < len(ads_expected_engagement_utility) or j < len(nv_expected_engagement_utility):
    # de-duplication
    if i < len(ads_expected_engagement_utility) and (ads_bms_id[i] in placed_item_id):
        i += 1
        continue 
    if j < len(nv_expected_engagement_utility) and (nv_bms_id[j] in placed_item_id):
        j += 1
        continue

    while must_insert_nv:
      if j < len(nv_expected_engagement_utility):
        insert_nv_to_merged(j)
        j += 1
        consecutive_nv_count += 1

        if consecutive_nv_count >= min_nv_block_size:
          must_insert_nv = False
          consecutive_nv_count = 0
      else:
        must_insert_nv = False
        consecutive_nv_count = 0

    if i < len(ads_expected_engagement_utility) and j < len(nv_expected_engagement_utility):
      if consecutive_ads_count >= max_ads_block_size:
        must_insert_nv = True
        consecutive_ads_count = 0
        continue

      if compare_ads_to_nv(i, j):
        insert_ads_to_merged(i)
        i += 1
        consecutive_ads_count += 1
      else:
        insert_nv_to_merged(j)
        j += 1
        consecutive_nv_count += 1

        if consecutive_ads_count:
          must_insert_nv = True
          consecutive_ads_count = 0
    

    elif i < len(ads_expected_engagement_utility):
      if consecutive_ads_count >= max_ads_block_size:
        break

      insert_ads_to_merged(i)
      i += 1
      consecutive_ads_count += 1

    elif j < len(nv_expected_engagement_utility):
      
      j += 1
      
  return merged

# COMMAND ----------

result_schema = ArrayType(StructType([
    StructField("source", StringType()),
    StructField("revenue_signal", FloatType()),
    StructField("engagement_signal", FloatType()),
    StructField("index", IntegerType()),
    StructField("original_card_position", IntegerType())
]))

merge_sort_blending_udf = udf(merge_sort_blending_dedup, result_schema)

# COMMAND ----------

df_merged = df_unified.withColumn(
  "merged_result",
  merge_sort_blending_udf(
    df_unified["ad_quality_score_array"],
    df_unified["ad_expected_value_array"],    
    df_unified["nv_pctr_array"],
    lit(ALPHA),
    lit(BETA),
    df_unified["ad_sorted_card_position_array"],
    df_unified["nv_sorted_card_position_array"],
    lit(MAX_ADS_BLOCK_SIZE),
    lit(MIN_NV_BLOCK_SIZE),
    df_unified["ad_unhashed_bms_id_array"],
    df_unified["nv_unhashed_bms_id_array"],
  )
)

# COMMAND ----------

df_merged_count = df_merged.count()
display(df_merged_count)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis
# MAGIC
# MAGIC 1. Discounted expected number of clicks (namely, weighted sum of pCTR)
# MAGIC 2. Discounted expected (ads) revenue (namely, weighted sum of adjusted_bid * pCTR)
# MAGIC 3. Ads density (# ads among top-12 and top-2 positions)
# MAGIC 4. Distribution of ads slots
# MAGIC

# COMMAND ----------

def ads_load_topK(df, k, fixed=False):
    if fixed:
        return df.withColumn(
            "ads_load_topk",
            expr(f"size(filter(slice(sorted_struct, 1, {k}), x -> x.revenue > 0))")
        )
    else:
        return df.withColumn(
            f"ads_load_topk_blended",
            expr(f"size(filter(slice(merged_result, 1, {k}), x -> x.revenue_signal > 0))")
        )

# COMMAND ----------

def sum_engagement_util_topK(df, k):
  return df.withColumn(
    "sum_engagement_util_topk_blended",
    expr(f"""
        aggregate(
            zip_with(
                slice(merged_result, 1, {k}),
                sequence(0, {k - 1}),
                (x, i) -> x.engagement_signal / log2(i + 2)
            ),
            0D,
            (acc, v) -> acc + v
        )""")
  )
    
def sum_revenue_util_topK(df, k):
  return df.withColumn(
    "sum_revenue_util_topk_blended",
    expr(f"""
        aggregate(
            zip_with(
                slice(merged_result, 1, {k}),
                sequence(0, {k - 1}),
                (x, i) -> x.revenue_signal / log2(i + 2)
            ),
            0D,
            (acc, v) -> acc + v
        )""")
  )
    

# COMMAND ----------

def calculate_metric_pack(df, k):
  df = ads_load_topK(df, k, fixed=False)
  df = ads_load_topK(df, k, fixed=True)
  df = sum_engagement_util_topK(df, k)
  df = sum_revenue_util_topK(df, k)
  
  metrics = [
    ("ads_load_topk", "ads_load_topk_blended"),
    ("sum_enga_util_topk", f"sum_engagement_util_topk_blended"),
    ("sum_rev_util_topk", f"sum_revenue_util_topk_blended")
  ]
  
  stats = []
  for fixed_col, nonfixed_col in metrics:
    for col in [fixed_col, nonfixed_col]:
      stats.append(
        df.selectExpr(
          f"'{col}' as metric",
          f"mean({col}) as mean",
          f"percentile_approx({col}, 0.5) as median",
          f"percentile_approx({col}, array(0.25, 0.50, 0.75)) as quantiles"
        )
      )
  result = stats[0]
  for s in stats[1:]:
    result = result.unionByName(s)
  
  display(result)
  return df
df_final = calculate_metric_pack(df_merged, K)

# COMMAND ----------

def plot_ad_slot_index_distribution_util_blending(df, k):
  exploded = df.select(posexplode("merged_result").alias("item_index", "item"))

  ads_index_topK = exploded.filter(
    (F.col("item.revenue_signal") != 0) & (F.col("item_index") < k)
  )

  index_histogram = ads_index_topK.groupBy("item_index").count().orderBy("item_index")
  index_histogram_pd = index_histogram.toPandas()

  # to Pandas  
  all_index = pd.DataFrame({"item_index": list(range(k))})
  index_histogram_pd = all_index.merge(index_histogram_pd, how="left", on="item_index").fillna(0)
  index_histogram_pd["count"] = index_histogram_pd["count"].astype(int)

  # Plotting
  plt.figure(figsize=(8, 4))
  plt.bar(index_histogram_pd["item_index"], index_histogram_pd["count"], width=0.7)

  plt.xlabel("Ad Position (Slot Index 0–" + str(k-1) + ")")
  plt.ylabel("Ad Count")
  plt.title("Histogram of Ad Positions in Top "+str(k)+" Slots (Utility-based Blending)")
  plt.xticks(range(k))
  plt.grid(axis='y')
  plt.tight_layout()
  plt.show()

def plot_ad_slot_index_distribution_fixed_slot(df, k):
  exploded = df.select(posexplode("sorted_struct").alias("item_index", "item"))

  ads_index_topK = exploded.filter(
    (F.col("item.revenue") != 0) & (F.col("item_index") < k)
  )

  index_histogram = ads_index_topK.groupBy("item_index").count().orderBy("item_index")
  index_histogram_pd = index_histogram.toPandas()

  # to Pandas  
  all_index = pd.DataFrame({"item_index": list(range(k))})
  index_histogram_pd = all_index.merge(index_histogram_pd, how="left", on="item_index").fillna(0)
  index_histogram_pd["count"] = index_histogram_pd["count"].astype(int)

  # Plotting
  plt.figure(figsize=(8, 4))
  plt.bar(index_histogram_pd["item_index"], index_histogram_pd["count"], width=0.7)

  plt.xlabel("Ad Position (Slot Index 0–" + str(k-1) + ")")
  plt.ylabel("Ad Count")
  plt.title("Histogram of Ad Positions in Top "+str(k)+" Slots (Fixed-slot Blending)")
  plt.xticks(range(k))
  plt.grid(axis='y')
  plt.tight_layout()
  plt.show()


# COMMAND ----------

plot_ad_slot_index_distribution_util_blending(df_merged, K)

# COMMAND ----------

plot_ad_slot_index_distribution_fixed_slot(df_merged, K)

# COMMAND ----------

#write_data_to_snowflake(df_final, f"proddb.duocheng.blending_result_{ALPHA}_{MAX_ADS_BLOCK_SIZE}_{MIN_NV_BLOCK_SIZE}")