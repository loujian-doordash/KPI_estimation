-- Check New Auction Candidates Table Availability
-- This checks if the new table is ready for migration

-- ===============================================
-- 1. Check if new auction candidates table exists and is accessible
-- ===============================================
-- Try to access the new table mentioned in deprecation doc
SELECT 'Checking new auction candidates table...' as status;

-- Check if table exists (might fail if not available yet)
SELECT COUNT(*) as record_count
FROM datalake.ads.ads_auction_candidates_event
WHERE partition_date = CURRENT_DATE - 1  -- adjust date field name as needed
LIMIT 1;

-- Alternative: Check if Snowflake version exists
-- Note: Document mentions there's a different Snowflake table with CANDIDATES column
SELECT COUNT(*) as record_count  
FROM iguazu.server_events_production.auction_candidates_event_ice
WHERE iguazu_partition_date = CURRENT_DATE - 1
LIMIT 1;

-- ===============================================
-- 2. If accessible, compare schema with current tables
-- ===============================================
DESCRIBE TABLE datalake.ads.ads_auction_candidates_event;

-- Sample data from new table
SELECT *
FROM datalake.ads.ads_auction_candidates_event
WHERE partition_date = CURRENT_DATE - 1  -- adjust date field name
LIMIT 5;

-- ===============================================
-- 3. Migration readiness assessment
-- ===============================================
-- Check if key fields for blending algorithm are available
SELECT 
    'ads_auction_candidates_event' as table_name,
    -- ad_request_id,           -- adjust field names based on schema
    -- quality_score,
    -- expected_value, 
    -- bid_amount,
    -- merchant_id,
    -- impression_event_id,
    -- auction_timestamp,
    COUNT(*) as sample_count
FROM datalake.ads.ads_auction_candidates_event
WHERE partition_date = CURRENT_DATE - 1
-- AND placement_type IN ('PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L1', 
--                        'PLACEMENT_TYPE_SPONSORED_PRODUCTS_CATEGORY_L2')
GROUP BY 1;  -- adjust GROUP BY based on actual SELECT fields

-- ===============================================
-- 4. Timeline check - when was this table last updated?
-- ===============================================
SELECT 
    MAX(partition_date) as latest_data_date,  -- adjust field name
    COUNT(DISTINCT partition_date) as days_available,
    MIN(partition_date) as earliest_data_date
FROM datalake.ads.ads_auction_candidates_event;