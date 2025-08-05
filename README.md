# KPI Estimation for Ad and Non-Video Content Blending

This project implements a data pipeline and blending algorithm for optimizing content ranking and KPI estimation on category pages, specifically comparing ad-sponsored content with non-video organic content.

## Overview

The project consists of four main components that work together to process, match, and blend ad and non-video (NV) content for optimal user experience and business outcomes.

## Components

### 1. Ad Data Pipeline (`ad data pipeline.py`)
Extracts and processes sponsored/ad impression data:
- Pulls ad impressions from category surfaces (L1/L2 categories)
- Joins impression data with ad funnel metrics (quality scores, expected values, bids)
- Groups data by ad request ID and aggregates metrics into arrays
- Removes duplicates based on session, timestamp, store, and category combinations

### 2. Non-Video Data Pipeline (`nv data pipeline.py`)
Processes organic (non-sponsored) content impression data:
- Extracts non-sponsored impressions from category surfaces
- Joins with ML prediction logs (pCTR - predicted click-through rates)
- Combines data from multiple CTR models (v2c and non-v2c)
- Groups and deduplicates data similar to ad pipeline

### 3. Data Matching (`ad nv data matching.py`)
Joins the processed ad and NV datasets:
- Matches ad and NV data on session ID, timestamp, store, and category
- Creates unified dataset for blending analysis
- Processes multiple days of data

### 4. Blending Algorithm & Analysis (`Blending algorithm & analysis (dedup).py`)
Core algorithm that optimizes content ranking:
- **Utility-based blending**: Combines revenue signals (ads) and engagement signals
- **Deduplication**: Ensures no duplicate items in final ranking
- **Block constraints**: Max 3 consecutive ads, min 2 consecutive NV items
- **Discounted utility calculation**: Uses position-based discounting (DCG-style)
- **Analysis & Visualization**: Compares different blending strategies

## Key Features

- **Revenue Optimization**: Balances ad revenue with user engagement
- **User Experience**: Ensures organic content isn't overwhelmed by ads
- **KPI Measurement**: Compares different blending strategies on key metrics
- **Deduplication**: Advanced logic to prevent duplicate content
- **Performance Analysis**: Comprehensive metrics and visualizations

## Technology Stack

- **PySpark**: For large-scale data processing
- **Snowflake**: Data warehouse for storage and querying
- **Databricks**: Development and execution environment
- **Pandas**: Data analysis and manipulation
- **Matplotlib**: Data visualization

## Configuration

Key blending parameters:
- `K = 12`: Top-K positions to analyze
- `ALPHA = 20`: Weight for ad engagement signals
- `BETA = 20`: Weight for NV engagement signals
- `MAX_ADS_BLOCK_SIZE = 3`: Maximum consecutive ads
- `MIN_NV_BLOCK_SIZE = 2`: Minimum consecutive NV items

## Usage

1. Run the ad data pipeline to process sponsored content
2. Run the NV data pipeline to process organic content
3. Execute data matching to join ad and NV datasets
4. Run the blending algorithm for optimization and analysis

## Business Impact

This framework enables:
- A/B testing of different content blending strategies
- Revenue optimization while maintaining user engagement
- Data-driven decisions for content ranking algorithms
- Performance measurement and KPI tracking

## Output

The system generates:
- Optimized content rankings
- Performance metrics and comparisons
- Visualization of ad position distributions
- Statistical analysis of blending effectiveness