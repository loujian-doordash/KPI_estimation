-- Join Relationship Analysis: Understanding how tables relate
-- This helps determine if we can replace FACT_ADS_ITEM_REQUEST_FUNNEL with bid_event_ice

-- ===============================================
-- 1. Check if impression_event_id exists in both tables
-- ===============================================
-- Sample impression event IDs from request funnel
WITH sample_impression_ids AS (
    SELECT DISTINCT IMPRESSION_EVENT_ID
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE DATE(AUCTION_OCCURRED_AT) = '2025-07-23'
    LIMIT 100
)
SELECT 
    s.IMPRESSION_EVENT_ID,
    CASE WHEN b.impression_event_id IS NOT NULL THEN 'Found' ELSE 'Missing' END as in_bid_event
FROM sample_impression_ids s
LEFT JOIN iguazu.server_events_production.bid_event_ice b
    ON s.IMPRESSION_EVENT_ID = b.impression_event_id  -- adjust field name as needed
    AND b.iguazu_partition_date = '2025-07-23'
ORDER BY in_bid_event, s.IMPRESSION_EVENT_ID;

-- ===============================================
-- 2. Compare record counts for same time period
-- ===============================================
SELECT 
    'FACT_ADS_ITEM_REQUEST_FUNNEL' as table_name,
    DATE(AUCTION_OCCURRED_AT) as date,
    COUNT(*) as record_count,
    COUNT(DISTINCT IMPRESSION_EVENT_ID) as unique_impressions,
    COUNT(DISTINCT AD_REQUEST_ID) as unique_requests
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
WHERE DATE(AUCTION_OCCURRED_AT) BETWEEN '2025-07-20' AND '2025-07-23'
    AND (PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L1' 
         OR PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L2')
GROUP BY DATE(AUCTION_OCCURRED_AT)

UNION ALL

SELECT 
    'bid_event_ice' as table_name,
    iguazu_partition_date as date,
    COUNT(*) as record_count,
    COUNT(DISTINCT impression_event_id) as unique_impressions,  -- adjust field name
    COUNT(DISTINCT request_id) as unique_requests  -- adjust field name
FROM iguazu.server_events_production.bid_event_ice
WHERE iguazu_partition_date BETWEEN '2025-07-20' AND '2025-07-23'
    -- Add placement filter if available in bid_event_ice
GROUP BY iguazu_partition_date
ORDER BY table_name, date;

-- ===============================================
-- 3. Check data overlap and consistency
-- ===============================================
-- Sample overlap check for specific impression events
SELECT 
    rf.IMPRESSION_EVENT_ID,
    rf.AD_QUALITY_SCORE as rf_quality_score,
    rf.EXPECTED_VALUE as rf_expected_value,
    rf.TRUE_BID as rf_true_bid,
    -- be.quality_score as be_quality_score,  -- adjust field names
    -- be.expected_value as be_expected_value,
    -- be.bid_amount as be_bid_amount,
    'comparison_needed' as note
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
-- JOIN iguazu.server_events_production.bid_event_ice be
--     ON rf.IMPRESSION_EVENT_ID = be.impression_event_id
--     AND DATE(rf.AUCTION_OCCURRED_AT) = be.iguazu_partition_date
WHERE DATE(rf.AUCTION_OCCURRED_AT) = '2025-07-23'
    AND (rf.PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L1' 
         OR rf.PLACEMENT = 'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L2')
LIMIT 10;