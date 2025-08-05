-- Field Mapping Analysis: Understanding what fields map between tables
-- This helps understand the relationship and data transformations

-- ===============================================
-- 1. Key fields used in current blending algorithm
-- ===============================================
-- Current fields from FACT_ADS_ITEM_REQUEST_FUNNEL used in blending:
SELECT 
    AD_REQUEST_ID,
    AD_QUALITY_SCORE,
    EXPECTED_VALUE,
    TRUE_BID,
    BUSINESS_MERCHANT_SUPPLIED_ID,
    IMPRESSION_EVENT_ID,
    AUCTION_OCCURRED_AT,
    PLACEMENT,
    PARSE_JSON(PRICING_METADATA):"adRank"::INTEGER AS AD_RANK
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
WHERE DATE(AUCTION_OCCURRED_AT) = '2025-07-23'  -- Use recent date
    AND (PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L1' 
         OR PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L2')
LIMIT 10;

-- ===============================================
-- 2. Equivalent fields in bid_event_ice (adjust field names as needed)
-- ===============================================
-- Note: Field names might be different - update based on DESCRIBE results
SELECT 
    -- ad_request_id,           -- Check actual field name
    -- quality_score,           -- Check actual field name  
    -- expected_value,          -- Check actual field name
    -- bid_amount,              -- Check actual field name
    -- merchant_id,             -- Check actual field name
    -- impression_event_id,     -- Check actual field name
    -- auction_timestamp,       -- Check actual field name
    -- placement_type,          -- Check actual field name
    *
FROM iguazu.server_events_production.bid_event_ice
WHERE iguazu_partition_date = '2025-07-23'  -- Use recent date
LIMIT 10;

-- ===============================================
-- 3. Data freshness and volume comparison
-- ===============================================
-- Check latest data availability
SELECT 
    'bid_event_ice' as source,
    MAX(iguazu_partition_date) as latest_date,
    COUNT(*) as records_today
FROM iguazu.server_events_production.bid_event_ice
WHERE iguazu_partition_date = CURRENT_DATE

UNION ALL

SELECT 
    'request_funnel' as source,
    MAX(DATE(AUCTION_OCCURRED_AT)) as latest_date,
    COUNT(*) as records_today
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
WHERE DATE(AUCTION_OCCURRED_AT) = CURRENT_DATE;