# Snowflake Query Analysis for Bid Event Migration

This folder contains SQL queries to help analyze and compare different auction/bid event tables in Snowflake.

## Query Files

### 1. `01_check_table_schemas.sql`
**Purpose**: Understand the structure and schema of both tables
- Get detailed schema information for `bid_event_ice` and `FACT_ADS_ITEM_REQUEST_FUNNEL`
- Compare basic table statistics (row counts, date ranges)
- Identify available fields and data types

**Run this first** to understand what fields are available in each table.

### 2. `02_field_mapping_analysis.sql`  
**Purpose**: Map fields between tables and understand data transformations
- Examine fields currently used in the blending algorithm
- Find equivalent fields in `bid_event_ice`
- Compare data freshness and volume between tables

**Run this second** to understand how to map your current logic to the raw bid event table.

### 3. `03_join_relationship_analysis.sql`
**Purpose**: Understand how tables relate and if they can be substituted
- Check if `impression_event_id` exists in both tables
- Compare record counts and coverage
- Validate data consistency between tables

**Run this third** to determine if `bid_event_ice` can replace `FACT_ADS_ITEM_REQUEST_FUNNEL`.

### 4. `04_new_auction_candidates_check.sql`
**Purpose**: Check readiness of the new auction candidates table
- Verify if `datalake.ads.ads_auction_candidates_event` is accessible
- Compare schema with existing tables
- Assess migration timeline and data availability

**Run this last** to prepare for the eventual migration to the new table.

## Usage Instructions

1. **Run queries in order** (01 → 02 → 03 → 04)
2. **Update field names** based on actual schemas discovered in step 01
3. **Adjust date ranges** to match your analysis needs
4. **Document findings** to plan your migration strategy

## Expected Outcomes

After running these queries, you should understand:
- Which table provides the data you need for blending
- How to map fields between different table versions  
- Timeline for migrating to the new auction candidates table
- Any data quality or availability issues

## Notes

- Some queries may fail if tables are not accessible - this is expected
- Field names are placeholders and need to be updated based on actual schemas
- Date field names may vary between tables
- The new auction candidates table may not be available yet (per deprecation timeline)

## Migration Timeline Reference

- **Current**: `bid_event_ice` and `FACT_ADS_ITEM_REQUEST_FUNNEL` are active
- **Target**: Aug 15, 2025 - New auction candidates table available in Snowflake  
- **Deadline**: Sep 5, 2025 - Old tables deprecated