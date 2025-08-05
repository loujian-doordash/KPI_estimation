-- Schema Comparison: bid_event_ice vs FACT_ADS_ITEM_REQUEST_FUNNEL
-- Run these queries to understand the structure and available fields

-- ===============================================
-- 1. Check bid_event_ice table schema
-- ===============================================
DESCRIBE TABLE iguazu.server_events_production.bid_event_ice;

-- Alternative way to see schema with sample data types
SELECT *
FROM iguazu.server_events_production.bid_event_ice
LIMIT 1;

-- ===============================================
-- 2. Check FACT_ADS_ITEM_REQUEST_FUNNEL table schema  
-- ===============================================
DESCRIBE TABLE proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL;

-- Alternative way to see schema with sample data types
SELECT *
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
LIMIT 1;

-- ===============================================
-- 3. Compare key fields side by side
-- ===============================================
-- bid_event_ice key fields (update based on actual schema)
SELECT 
    'bid_event_ice' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT impression_event_id) as unique_impression_events,
    MIN(iguazu_partition_date) as min_date,
    MAX(iguazu_partition_date) as max_date
FROM iguazu.server_events_production.bid_event_ice
WHERE iguazu_partition_date >= CURRENT_DATE - 7;

-- FACT_ADS_ITEM_REQUEST_FUNNEL key fields
SELECT 
    'FACT_ADS_ITEM_REQUEST_FUNNEL' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT IMPRESSION_EVENT_ID) as unique_impression_events,
    MIN(DATE(AUCTION_OCCURRED_AT)) as min_date,
    MAX(DATE(AUCTION_OCCURRED_AT)) as max_date
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
WHERE DATE(AUCTION_OCCURRED_AT) >= CURRENT_DATE - 7;