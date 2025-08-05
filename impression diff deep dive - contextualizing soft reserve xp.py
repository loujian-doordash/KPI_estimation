# Databricks notebook source
import typing
import pytz
import pandas as pd
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import re
from typing import Optional

import pyspark.sql.functions as F
from fabricator_core.connectors.context_io import load_from_context
from fabricator_core.connectors.snowflake import load_data_spark
from fabricator_core.core.contexts.dataset_context import DatasetContext
from fabricator_core.core.etl.dataset import DatasetUpload
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from fabricator_core.connectors.snowflake import set_scope
import snowflake.connector as sf
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
USERNAME = email.split('@')[0]

user = dbutils.secrets.get(scope=f"{USERNAME}-scope", key="sf-username")
password = dbutils.secrets.get(scope=f"{USERNAME}-scope", key="sf-password")
sf_session_options = {
    "sfurl": "doordash.snowflakecomputing.com/",
    "sfaccount": "DOORDASH",
    "sfuser": user,
    "sfpassword": password,
    "sfdatabase": "proddb",
    "sfschema": USERNAME.replace('.', '').upper(),
    "sfwarehouse": "AD_ANALYTICS_SERVICE",
    'sfrole': USERNAME.replace('.', '').upper(),
    "sfautocommit": True
}

params = dict(
    user=user,
    password=password,
    account="DOORDASH",
    database="PRODDB",
    warehouse="AD_ANALYTICS_SERVICE",
    role=user.replace('.',''),
    schema=USERNAME.replace('.', '').upper(),
)

# COMMAND ----------

print(USERNAME.replace('.', '').upper())

# COMMAND ----------

exposure_query = """
        SELECT
        bucket_key::VARCHAR AS bucket_key_str,
        experiment_group,
        FIRST_EXPOSURE_TIME
        FROM METRICS_REPO.PUBLIC.contextualization_pricing_stage2_exposures
        WHERE experiment_group IN ('exp_2_control', 'exp_2_treatment')
    """

exposure_df = load_data_spark(
        exposure_query,
        sf_session_options=sf_session_options
)
# Register as temp view for SQL access
exposure_df.createOrReplaceTempView("experiment_exposures")

# Display sample of exposure data
display(exposure_df.limit(5))

# COMMAND ----------

# Define date range
start_date = "2025-04-16"
end_date = "2025-04-28"

# Load auction data for the date range using Snowflake connector
# Split the query by day to avoid timeout issues
from datetime import datetime, timedelta
from pyspark.sql import functions as F  # Ensure F is imported in this cell

# Parse start and end dates
start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

# Create a list to store dataframes for each day
daily_auction_dfs = []

# Process one day at a time to avoid timeouts
current_date = start_date_obj
while current_date <= end_date_obj:
    current_date_str = current_date.strftime("%Y-%m-%d")
    print(f"Fetching data for {current_date_str}...")
    
    # Query for just this single day
    daily_query = f"""
        SELECT
            AUCTION_ID,
            CONSUMER_ID::VARCHAR AS consumer_id_str,
            AUCTION_OCCURRED_AT,
            '{current_date_str}'::DATE AS auction_date
        FROM fact_ads_item_request_funnel
        WHERE AUCTION_OCCURRED_AT::DATE = '{current_date_str}'
        AND TRUE_BID > 0
    """
    daily_df = load_data_spark(
        daily_query,
        sf_session_options=sf_session_options
    )
    
    # Store the day's data
    daily_auction_dfs.append(daily_df)
    
    # Move to next day
    current_date += timedelta(days=1)

# COMMAND ----------

# Union all the daily dataframes together
if daily_auction_dfs:
    auctions_df = daily_auction_dfs[0]
    for df in daily_auction_dfs[1:]:
        auctions_df = auctions_df.union(df)
    
    # Register as temp view
    auctions_df.createOrReplaceTempView("filtered_auctions")
    
    # Display sample and approximate count
    approx_count = auctions_df.agg(F.approx_count_distinct("AUCTION_ID").alias("approx_count")).collect()[0][0]
    print(f"Approximate total auctions in date range: {approx_count:,}")
    display(auctions_df.limit(5))
else:
    print("No data was retrieved for the specified date range.")
    auctions_df = spark.createDataFrame([], schema=["AUCTION_ID", "consumer_id_str", "AUCTION_OCCURRED_AT", "auction_date"])
    auctions_df.createOrReplaceTempView("filtered_auctions")

# COMMAND ----------

# Join datasets using PySpark DataFrame API
joined_data = auctions_df.join(
    exposure_df,
    (auctions_df["consumer_id_str"] == exposure_df["bucket_key_str"]) & 
    (auctions_df["AUCTION_OCCURRED_AT"] > exposure_df["FIRST_EXPOSURE_TIME"]),
    "inner"
).select(
    exposure_df["experiment_group"],
    auctions_df["auction_date"],
    auctions_df["AUCTION_ID"]
)

# Register as temp view for SQL access
joined_data.createOrReplaceTempView("joined_experiment_data")

# Display sample of joined data
print("Sample of joined experiment data:")
display(joined_data.limit(5))

# COMMAND ----------

# Method 1: Using PySpark DataFrame API for daily analysis
daily_counts_df = joined_data.groupBy("experiment_group", "auction_date") \
    .agg(F.countDistinct("AUCTION_ID").alias("request_count")) \
    .orderBy("auction_date", "experiment_group")

# Display the results
print("Daily request counts by experiment group:")
display(daily_counts_df)

# Convert to pandas for easier visualization and analysis
daily_counts_pd = daily_counts_df.toPandas()

# Save to CSV for potential further analysis
daily_counts_pd.to_csv("/dbfs/FileStore/experiment_daily_counts.csv", index=False)
print("Saved daily counts to /dbfs/FileStore/experiment_daily_counts.csv")

# COMMAND ----------

# Create pivot table for easier comparison
pivot_df = daily_counts_pd.pivot(index='auction_date', columns='experiment_group', values='request_count')

# Fill any missing values with 0
pivot_df = pivot_df.fillna(0)

# Plot the data using seaborn for better visualization
plt.figure(figsize=(14, 7))

# Plot 1: Bar chart showing daily counts
ax = pivot_df.plot(kind='bar', figsize=(14, 7))
plt.title('Daily Request Counts by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Requests', fontsize=14)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.legend(title='Experiment Group', fontsize=12)
display(plt.gcf())

# Plot 2: Line chart showing trends over time
plt.figure(figsize=(14, 7))
sns.lineplot(data=pivot_df, markers=True, dashes=False)
plt.title('Daily Request Trends by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Requests', fontsize=14)
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
display(plt.gcf())

# COMMAND ----------

# Calculate ratios where both values exist
if 'exp_2_control' in pivot_df.columns and 'exp_2_treatment' in pivot_df.columns:
    # Calculate treatment/control ratio
    pivot_df['treatment_control_ratio'] = pivot_df['exp_2_treatment'] / pivot_df['exp_2_control']
    
    # Calculate percent difference
    pivot_df['percent_difference'] = (pivot_df['exp_2_treatment'] - pivot_df['exp_2_control']) / pivot_df['exp_2_control'] * 100
    
    # Plot ratios with confidence interval using seaborn
    plt.figure(figsize=(14, 7))
    ax = sns.lineplot(data=pivot_df['treatment_control_ratio'], marker='o', linewidth=2)
    plt.title('Daily Ratio of Treatment vs Control Requests', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Ratio (Treatment/Control)', fontsize=14)
    plt.axhline(y=1, color='r', linestyle='--', alpha=0.5, label='Equal Ratio Reference')  # Reference line at ratio=1
    
    # Add mean line
    mean_ratio = pivot_df['treatment_control_ratio'].mean()
    plt.axhline(y=mean_ratio, color='g', linestyle='-', alpha=0.5, label=f'Mean Ratio: {mean_ratio:.2f}')
    
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    display(plt.gcf())
    
    # Plot percent difference
    plt.figure(figsize=(14, 7))
    sns.barplot(x=pivot_df.index, y='percent_difference', data=pivot_df)
    plt.title('Daily Percent Difference (Treatment vs Control)', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Percent Difference (%)', fontsize=14)
    plt.axhline(y=0, color='r', linestyle='--', alpha=0.5)  # Reference line at 0%
    plt.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aggregate Totals for Overall Comparison

# COMMAND ----------

# Get overall totals using PySpark DataFrame API
overall_counts_df = joined_data.groupBy("experiment_group") \
    .agg(
        F.countDistinct("AUCTION_ID").alias("total_requests"),
        F.countDistinct("auction_date").alias("days_with_data")
    )

# Display the aggregated results
print("Overall experiment metrics:")
display(overall_counts_df)

# Convert to pandas for additional analysis
overall_pd = overall_counts_df.toPandas()

# Calculate additional metrics
if len(overall_pd) > 0:
    overall_pd['avg_requests_per_day'] = overall_pd['total_requests'] / overall_pd['days_with_data']
    
    # Create bar chart for total requests
    plt.figure(figsize=(10, 6))
    sns.barplot(x='experiment_group', y='total_requests', data=overall_pd)
    plt.title('Total Requests by Experiment Group', fontsize=16)
    plt.xlabel('Experiment Group', fontsize=14)
    plt.ylabel('Total Requests', fontsize=14)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    display(plt.gcf())
    
    # Create bar chart for average requests per day
    plt.figure(figsize=(10, 6))
    sns.barplot(x='experiment_group', y='avg_requests_per_day', data=overall_pd)
    plt.title('Average Daily Requests by Experiment Group', fontsize=16)
    plt.xlabel('Experiment Group', fontsize=14)
    plt.ylabel('Avg. Requests per Day', fontsize=14)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    display(plt.gcf())
    
    # Display table with metrics
    display(overall_pd)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## The following is for impression deep dive

# COMMAND ----------

# Define date range
start_date = "2025-04-16"
end_date = "2025-04-28"
# Load experiment exposure data
exposures_query = """
SELECT 
    try_to_number(bucket_key) as consumer_id, 
    experiment_group, 
    first_exposure_time
FROM METRICS_REPO.PUBLIC.CONTEXTUALIZATION_PRICING_STAGE2_EXPOSURES
GROUP BY ALL
"""

exposures_df = load_data_spark(
    exposures_query,
    sf_session_options=sf_session_options
)

# Display exposure data summary
print(f"Loaded {exposures_df.count()} exposure records")
display(exposures_df.groupBy("experiment_group").count().orderBy("experiment_group"))

# Load retail stores data
stores_query = """
SELECT *
FROM edw.cng.dimension_new_vertical_store_tags
WHERE ux = 'Retail'
"""

stores_df = load_data_spark(
    stores_query,
    sf_session_options=sf_session_options
)

# Display stores data summary
print(f"Loaded {stores_df.count()} retail stores")

# Parse start and end dates
start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

# Create a list to store dataframes for each day
daily_imps_dfs = []

# Process one day at a time to avoid timeouts
current_date = start_date_obj
while current_date <= end_date_obj:
    current_date_str = current_date.strftime("%Y-%m-%d")
    print(f"Fetching impressions data for {current_date_str}...")
    
    # Query for just this single day
    daily_query = f"""
        SELECT
            id,
            consumer_id,
            store_id,
            event_timestamp,
            dd_device_id,
            platform,
            item_id,
            card_position,
            feature_detailed,
            feature,
            search_term_clean,
            autocomplete_term,
            item_collection_id,
            l1_category_id,
            l2_category_id,
            container_id,
            ad_auction_id,
            ad_group_id,
            is_sponsored,
            collection_type,
            '{current_date_str}'::DATE AS event_date
        FROM proddb.public.fact_item_card_view_dedup
        WHERE event_date = '{current_date_str}'
    """
    
    daily_df = load_data_spark(
        daily_query,
        sf_session_options=sf_session_options
    )
    
    # Store the day's data
    daily_imps_dfs.append(daily_df)
    
    # Move to next day
    current_date += timedelta(days=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join and Process Data

# COMMAND ----------

# Union all daily impression dataframes
imps_df = daily_imps_dfs[0]
for df in daily_imps_dfs[1:]:
    imps_df = imps_df.union(df)

# Join with stores to filter for retail stores only
retail_imps_df = imps_df.join(
    stores_df,
    on="store_id",
    how="inner"
)

# Join with exposures to get experiment group assignment
# Only include impressions before exposure
joined_df = retail_imps_df.join(
    exposures_df,
    on="consumer_id",
    how="inner"
)

# Apply filtering condition for pre-exposure events
filtered_df = joined_df.filter(
    joined_df.event_timestamp >= joined_df.first_exposure_time
)

# COMMAND ----------

# Calculate daily metrics by experiment group
daily_metrics_df = filtered_df.groupBy("experiment_group", "event_date") \
    .agg(
        F.count("*").alias("all_imps"),
        F.sum(
            F.when(
                (F.col("is_sponsored") == True) & 
                ((F.col("collection_type").isNull()) | (F.col("collection_type") != "sponsored_brand")),
                1
            ).otherwise(0)
        ).alias("sp_imps")
    ) \
    .orderBy("event_date", "experiment_group")

# Display the daily metrics
print("Daily Impressions Metrics by Experiment Group:")
display(daily_metrics_df)

# COMMAND ----------

# Convert to pandas for easier visualization
daily_metrics_pd = daily_metrics_df.toPandas()

# Create pivot tables for visualization
all_imps_pivot = daily_metrics_pd.pivot(index='event_date', columns='experiment_group', values='all_imps')
all_imps_pivot = all_imps_pivot.fillna(0)

sp_imps_pivot = daily_metrics_pd.pivot(index='event_date', columns='experiment_group', values='sp_imps')
sp_imps_pivot = sp_imps_pivot.fillna(0)

# Plot 1: Bar chart showing daily counts of all impressions
plt.figure(figsize=(14, 7))
ax = all_imps_pivot.plot(kind='bar', figsize=(14, 7))
plt.title('Daily All Impressions by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Impressions', fontsize=14)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.legend(title='Experiment Group', fontsize=12)
display(plt.gcf())

# Plot 2: Line chart showing trends over time for all impressions
plt.figure(figsize=(14, 7))
sns.lineplot(data=all_imps_pivot, markers=True, dashes=False)
plt.title('Daily All Impressions Trends by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Impressions', fontsize=14)
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
display(plt.gcf())

# Plot 3: Bar chart showing daily counts of sponsored impressions
plt.figure(figsize=(14, 7))
ax = sp_imps_pivot.plot(kind='bar', figsize=(14, 7))
plt.title('Daily Sponsored Impressions by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Sponsored Impressions', fontsize=14)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.legend(title='Experiment Group', fontsize=12)
display(plt.gcf())

# Plot 4: Line chart showing trends over time for sponsored impressions
plt.figure(figsize=(14, 7))
sns.lineplot(data=sp_imps_pivot, markers=True, dashes=False)
plt.title('Daily Sponsored Impressions Trends by Experiment Group', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Number of Sponsored Impressions', fontsize=14)
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Calculate and Visualize Ratios

# COMMAND ----------

# Calculate ratios and differences for all impressions
# Identify control and treatment columns
control_col = None
treatment_col = None

# Identify control and treatment columns based on column names
for col in all_imps_pivot.columns:
    if 'control' in str(col).lower():
        control_col = col
    elif 'treatment' in str(col).lower():
        treatment_col = col

# If we have both control and treatment columns, calculate metrics
if control_col and treatment_col:
    # For all impressions
    all_imps_pivot['treatment_control_ratio'] = all_imps_pivot[treatment_col] / all_imps_pivot[control_col]
    all_imps_pivot['percent_difference'] = (all_imps_pivot[treatment_col] - all_imps_pivot[control_col]) / all_imps_pivot[control_col] * 100
    
    # Plot ratios with confidence interval for all impressions
    plt.figure(figsize=(14, 7))
    ax = sns.lineplot(data=all_imps_pivot['treatment_control_ratio'], marker='o', linewidth=2)
    plt.title('Daily Ratio of Treatment vs Control - All Impressions', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Ratio (Treatment/Control)', fontsize=14)
    plt.axhline(y=1, color='r', linestyle='--', alpha=0.5, label='Equal Ratio Reference')
    
    # Add mean line
    mean_ratio = all_imps_pivot['treatment_control_ratio'].mean()
    plt.axhline(y=mean_ratio, color='g', linestyle='-', alpha=0.5, label=f'Mean Ratio: {mean_ratio:.2f}')
    
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    display(plt.gcf())
    
    # Plot percent difference for all impressions
    plt.figure(figsize=(14, 7))
    sns.barplot(x=all_imps_pivot.index, y='percent_difference', data=all_imps_pivot)
    plt.title('Daily Percent Difference - All Impressions (Treatment vs Control)', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Percent Difference (%)', fontsize=14)
    plt.axhline(y=0, color='r', linestyle='--', alpha=0.5)
    plt.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    display(plt.gcf())
    
    # For sponsored impressions
    sp_imps_pivot['treatment_control_ratio'] = sp_imps_pivot[treatment_col] / sp_imps_pivot[control_col]
    sp_imps_pivot['percent_difference'] = (sp_imps_pivot[treatment_col] - sp_imps_pivot[control_col]) / sp_imps_pivot[control_col] * 100
    
    # Plot ratios with confidence interval for sponsored impressions
    plt.figure(figsize=(14, 7))
    ax = sns.lineplot(data=sp_imps_pivot['treatment_control_ratio'], marker='o', linewidth=2)
    plt.title('Daily Ratio of Treatment vs Control - Sponsored Impressions', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Ratio (Treatment/Control)', fontsize=14)
    plt.axhline(y=1, color='r', linestyle='--', alpha=0.5, label='Equal Ratio Reference')
    
    # Add mean line
    mean_ratio = sp_imps_pivot['treatment_control_ratio'].mean()
    plt.axhline(y=mean_ratio, color='g', linestyle='-', alpha=0.5, label=f'Mean Ratio: {mean_ratio:.2f}')
    
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    display(plt.gcf())
    
    # Plot percent difference for sponsored impressions
    plt.figure(figsize=(14, 7))
    sns.barplot(x=sp_imps_pivot.index, y='percent_difference', data=sp_imps_pivot)
    plt.title('Daily Percent Difference - Sponsored Impressions (Treatment vs Control)', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Percent Difference (%)', fontsize=14)
    plt.axhline(y=0, color='r', linestyle='--', alpha=0.5)
    plt.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    display(plt.gcf())

# COMMAND ----------

# Get overall totals using PySpark DataFrame API
overall_counts_df = daily_metrics_df.groupBy("experiment_group") \
    .agg(
        F.sum("all_imps").alias("total_all_imps"),
        F.sum("sp_imps").alias("total_sp_imps"),
        F.countDistinct("event_date").alias("days_with_data")
    )

# Add derived metrics
overall_counts_df = overall_counts_df.withColumn(
    "sp_imps_ratio", F.col("total_sp_imps") / F.col("total_all_imps") * 100
)
overall_counts_df = overall_counts_df.withColumn(
    "avg_all_imps_per_day", F.col("total_all_imps") / F.col("days_with_data")
)
overall_counts_df = overall_counts_df.withColumn(
    "avg_sp_imps_per_day", F.col("total_sp_imps") / F.col("days_with_data")
)

# Display the aggregated results
print("Overall experiment metrics:")
display(overall_counts_df)

# Convert to pandas for additional analysis
overall_pd = overall_counts_df.toPandas()

# Create bar chart for total all impressions
plt.figure(figsize=(10, 6))
sns.barplot(x='experiment_group', y='total_all_imps', data=overall_pd)
plt.title('Total All Impressions by Experiment Group', fontsize=16)
plt.xlabel('Experiment Group', fontsize=14)
plt.ylabel('Total Impressions', fontsize=14)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
display(plt.gcf())

# Create bar chart for total sponsored impressions
plt.figure(figsize=(10, 6))
sns.barplot(x='experiment_group', y='total_sp_imps', data=overall_pd)
plt.title('Total Sponsored Impressions by Experiment Group', fontsize=16)
plt.xlabel('Experiment Group', fontsize=14)
plt.ylabel('Total Sponsored Impressions', fontsize=14)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
display(plt.gcf())

# Create bar chart for average impressions per day
plt.figure(figsize=(10, 6))
sns.barplot(x='experiment_group', y='avg_all_imps_per_day', data=overall_pd)
plt.title('Average Daily Impressions by Experiment Group', fontsize=16)
plt.xlabel('Experiment Group', fontsize=14)
plt.ylabel('Avg. Impressions per Day', fontsize=14)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
display(plt.gcf())

# Create bar chart for sponsored impressions ratio
plt.figure(figsize=(10, 6))
sns.barplot(x='experiment_group', y='sp_imps_ratio', data=overall_pd)
plt.title('Sponsored Impressions Ratio (% of All Impressions)', fontsize=16)
plt.xlabel('Experiment Group', fontsize=14)
plt.ylabel('Sponsored Ratio (%)', fontsize=14)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
display(plt.gcf())


# COMMAND ----------

# Create a summary table for easy comparison
summary_df = spark.createDataFrame(overall_pd)
display(summary_df)

# Calculate percentage differences between groups (assuming two groups)
if len(overall_pd) >= 2:
    # Identify control and treatment rows
    control_row = None
    treatment_row = None
    
    for _, row in overall_pd.iterrows():
        if 'control' in str(row['experiment_group']).lower():
            control_row = row
        elif 'treatment' in str(row['experiment_group']).lower():
            treatment_row = row
    
    if control_row is not None and treatment_row is not None:
        # Calculate differences
        all_imps_diff = (treatment_row['total_all_imps'] - control_row['total_all_imps']) / control_row['total_all_imps'] * 100
        sp_imps_diff = (treatment_row['total_sp_imps'] - control_row['total_sp_imps']) / control_row['total_sp_imps'] * 100
        ratio_diff = treatment_row['sp_imps_ratio'] - control_row['sp_imps_ratio']
        
        # Create a difference summary DataFrame
        diff_data = {
            'Metric': ['All Impressions', 'Sponsored Impressions', 'Sponsored Ratio (pp)'],
            'Percent Difference': [all_imps_diff, sp_imps_diff, ratio_diff],
            'Direction': ['Higher in Treatment' if all_imps_diff > 0 else 'Lower in Treatment',
                         'Higher in Treatment' if sp_imps_diff > 0 else 'Lower in Treatment',
                         'Higher in Treatment' if ratio_diff > 0 else 'Lower in Treatment']
        }
        diff_df = spark.createDataFrame(pd.DataFrame(diff_data))
        
        # Display findings
        print(f"Overall Differences (Treatment vs Control):")
        display(diff_df)
        
        # Create a visualization of the differences
        plt.figure(figsize=(10, 6))
        bars = plt.bar(['All Impressions', 'Sponsored Impressions'], 
                       [all_imps_diff, sp_imps_diff],
                       color=['blue', 'orange'])
        
        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + (1 if height > 0 else -3),
                    f'{height:.2f}%',
                    ha='center', va='bottom' if height > 0 else 'top', fontsize=12)
            
        plt.title('Percent Difference Between Treatment and Control', fontsize=16)
        plt.ylabel('Percent Difference (%)', fontsize=14)
        plt.axhline(y=0, color='r', linestyle='--', alpha=0.5)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        display(plt.gcf())

# COMMAND ----------

