#!/usr/bin/env python3
"""
Investigate the relationship between bid_event_ice and impression table (fact_item_card_view_dedup)
Understand how auction events connect to impression events
"""
import snowflake.connector
import os
import getpass

def get_connection():
    # Get password from environment or prompt user
    password = os.getenv('SNOWFLAKE_PASSWORD')
    if not password:
        password = getpass.getpass("Enter Snowflake password: ")
    
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'DOORDASH'),
        user=os.getenv('SNOWFLAKE_USER', 'JIAN.LOU'),
        password=password,
        warehouse='AD_ANALYTICS_SERVICE',
        database='PRODDB',
        schema='JIANLOU',
        role='JIANLOU'
    )

def run_query(query, description=""):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print(f"🔍 {description}")
        print(f"{'='*60}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✅ Query successful! Returned {len(results)} rows")
        
        if results:
            print(f"\n{' | '.join(columns)}")
            print("-" * min(100, len(' | '.join(columns))))
            
            for row in results[:10]:  # Show max 10 rows
                row_str = ' | '.join([str(val)[:20] if val is not None else 'NULL' for val in row])
                print(row_str)
            
            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

if __name__ == "__main__":
    print("🔍 INVESTIGATING bid_event_ice vs IMPRESSION TABLE RELATIONSHIP")
    
    # 1. Check impression table structure and volume
    print("\n📊 STEP 1: Impression Table Overview")
    
    impression_overview_query = """
    SELECT 
        COUNT(*) as total_impressions,
        COUNT(DISTINCT ID) as unique_impression_ids,
        COUNT(DISTINCT STORE_ID) as unique_stores,
        COUNT(DISTINCT DD_SESSION_ID) as unique_sessions,
        COUNT(CASE WHEN IS_SPONSORED = TRUE THEN 1 END) as sponsored_impressions,
        COUNT(CASE WHEN IS_SPONSORED = FALSE THEN 1 END) as organic_impressions
    FROM proddb.public.fact_item_card_view_dedup
    WHERE DATE(RECEIVED_AT) = CURRENT_DATE - 1
        AND FEATURE = 'category'
        AND CURRENCY = 'USD';
    """
    run_query(impression_overview_query, "Impression table overview (yesterday)")
    
    # 2. Sample impression data structure
    print("\n📋 STEP 2: Sample Impression Data Structure")
    
    impression_sample_query = """
    SELECT 
        ID as IMPRESSION_EVENT_ID,
        STORE_ID,
        DD_SESSION_ID,
        IS_SPONSORED,
        CARD_POSITION,
        RECEIVED_AT,
        L1_CATEGORY_ID,
        L2_CATEGORY_ID
    FROM proddb.public.fact_item_card_view_dedup
    WHERE DATE(RECEIVED_AT) = CURRENT_DATE - 1
        AND FEATURE = 'category'
        AND CURRENCY = 'USD'
        AND IS_SPONSORED = TRUE
    ORDER BY RECEIVED_AT DESC
    LIMIT 5;
    """
    run_query(impression_sample_query, "Sample sponsored impressions")
    
    # 3. Check if bid_event_ice has impression linking fields
    print("\n🔗 STEP 3: Does bid_event_ice have impression linking?")
    
    bid_event_fields_query = """
    SELECT 
        AUCTION_ID,
        STORE_ID,
        OCCURRED_AT,
        BID_PRICE_UNIT_AMOUNT,
        BID_STATUS
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 5;
    """
    run_query(bid_event_fields_query, "bid_event_ice sample (checking for impression fields)")
    
    # 4. Try to find connection via STORE_ID and timestamps
    print("\n⏰ STEP 4: Can we connect via STORE_ID + Timestamp?")
    
    store_timestamp_connection_query = """
    SELECT 
        be.AUCTION_ID,
        be.STORE_ID as bid_store_id,
        imp.STORE_ID as imp_store_id,
        be.OCCURRED_AT as bid_time,
        imp.RECEIVED_AT as impression_time,
        DATEDIFF('second', be.OCCURRED_AT, imp.RECEIVED_AT) as time_diff_seconds,
        imp.ID as IMPRESSION_EVENT_ID,
        imp.IS_SPONSORED
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.fact_item_card_view_dedup imp
        ON be.STORE_ID = imp.STORE_ID
        AND ABS(DATEDIFF('second', be.OCCURRED_AT, imp.RECEIVED_AT)) <= 60  -- Within 1 minute
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND imp.DATE(RECEIVED_AT) = CURRENT_DATE - 1
        AND imp.IS_SPONSORED = TRUE
        AND imp.FEATURE = 'category'
        AND imp.CURRENCY = 'USD'
    LIMIT 10;
    """
    run_query(store_timestamp_connection_query, "Potential store+time connections")
    
    # 5. Check how request_funnel connects to impressions
    print("\n🔗 STEP 5: How does request_funnel connect to impressions?")
    
    funnel_impression_connection_query = """
    SELECT 
        rf.AUCTION_ID,
        rf.IMPRESSION_EVENT_ID as funnel_impression_id,
        imp.ID as actual_impression_id,
        rf.STORE_ID as funnel_store,
        imp.STORE_ID as imp_store,
        rf.AUCTION_OCCURRED_AT,
        imp.RECEIVED_AT,
        CASE WHEN rf.IMPRESSION_EVENT_ID = imp.ID THEN 'MATCH' ELSE 'NO_MATCH' END as id_match
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
    LEFT JOIN proddb.public.fact_item_card_view_dedup imp
        ON rf.IMPRESSION_EVENT_ID = imp.ID
    WHERE rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.IMPRESSION_EVENT_ID IS NOT NULL
    LIMIT 10;
    """
    run_query(funnel_impression_connection_query, "request_funnel to impression connections")
    
    # 6. Check if we can trace bid_event → request_funnel → impression
    print("\n🔄 STEP 6: Can we trace: bid_event → request_funnel → impression?")
    
    full_chain_query = """
    SELECT 
        be.AUCTION_ID,
        be.BID_PRICE_UNIT_AMOUNT,
        be.OCCURRED_AT as bid_time,
        rf.IMPRESSION_EVENT_ID,
        rf.TRUE_BID,
        rf.AD_QUALITY_SCORE,
        imp.RECEIVED_AT as impression_time,
        imp.CARD_POSITION,
        imp.IS_SPONSORED
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    LEFT JOIN proddb.public.fact_item_card_view_dedup imp
        ON rf.IMPRESSION_EVENT_ID = imp.ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.IMPRESSION_EVENT_ID IS NOT NULL
    LIMIT 10;
    """
    run_query(full_chain_query, "Full chain: bid_event → request_funnel → impression")
    
    # 7. Summary statistics
    print("\n📊 STEP 7: Connection Summary Statistics")
    
    connection_stats_query = """
    SELECT 
        'bid_events_with_funnel_match' as metric,
        COUNT(*) as count
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'funnel_with_impression_id' as metric,
        COUNT(*) as count
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
    WHERE rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.IMPRESSION_EVENT_ID IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'funnel_with_valid_impression' as metric,
        COUNT(*) as count
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
    JOIN proddb.public.fact_item_card_view_dedup imp
        ON rf.IMPRESSION_EVENT_ID = imp.ID
    WHERE rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1;
    """
    run_query(connection_stats_query, "Connection statistics")
    
    print(f"\n{'='*60}")
    print("🎯 KEY INSIGHTS:")
    print("1. bid_event_ice → request_funnel: Connected via AUCTION_ID")
    print("2. request_funnel → impression: Connected via IMPRESSION_EVENT_ID") 
    print("3. bid_event_ice → impression: INDIRECT connection through request_funnel")
    print("4. Direct bid_event → impression: Would need store+timestamp matching")
    print("5. For blending: Use request_funnel as the bridge between auction and impression data")
    print(f"{'='*60}")