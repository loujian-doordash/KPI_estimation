#!/usr/bin/env python3
"""
Simple comparison between bid_event_ice and FACT_ADS_ITEM_REQUEST_FUNNEL
Focus on understanding key differences and how to use bid_event_ice effectively
"""
import snowflake.connector
import pandas as pd
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

def run_simple_query(query, description=""):
    """Run a simple query with limited results"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print(f"\n{'='*50}")
        print(f"🔍 {description}")
        print(f"{'='*50}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✅ Query successful! Returned {len(results)} rows")
        
        # Print results in a nice format
        if results:
            # Print header
            print(f"\n{' | '.join(columns)}")
            print("-" * (len(' | '.join(columns))))
            
            # Print first few rows
            for i, row in enumerate(results[:10]):  # Show max 10 rows
                row_str = ' | '.join([str(val)[:30] if val is not None else 'NULL' for val in row])
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
    print("🎯 SIMPLE TABLE COMPARISON: bid_event_ice vs FACT_ADS_ITEM_REQUEST_FUNNEL")
    
    # 1. Quick row count comparison (recent data only)
    print("\n📊 STEP 1: Quick Row Count Comparison (Yesterday)")
    
    bid_count_query = """
    SELECT COUNT(*) as bid_event_count
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 1;
    """
    run_simple_query(bid_count_query, "bid_event_ice row count (yesterday)")
    
    funnel_count_query = """
    SELECT COUNT(*) as funnel_count
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 1;
    """
    run_simple_query(funnel_count_query, "FACT_ADS_ITEM_REQUEST_FUNNEL row count (yesterday)")
    
    # 2. Sample data from bid_event_ice - key fields
    print("\n📋 STEP 2: Sample Data from bid_event_ice (Key Fields)")
    
    bid_sample_query = """
    SELECT 
        AUCTION_ID,
        STORE_ID,
        BID_PRICE_UNIT_AMOUNT,
        OCCURRED_AT,
        BID_STATUS,
        ELIMINATION_REASON
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    ORDER BY OCCURRED_AT DESC
    LIMIT 5;
    """
    run_simple_query(bid_sample_query, "bid_event_ice sample (key fields)")
    
    # 3. Sample data from FACT_ADS_ITEM_REQUEST_FUNNEL - key fields used in blending
    print("\n📋 STEP 3: Sample Data from FACT_ADS_ITEM_REQUEST_FUNNEL (Blending Fields)")
    
    funnel_sample_query = """
    SELECT 
        AUCTION_ID,
        AD_REQUEST_ID,
        IMPRESSION_EVENT_ID,
        AD_QUALITY_SCORE,
        EXPECTED_VALUE,
        TRUE_BID,
        BUSINESS_MERCHANT_SUPPLIED_ID
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    ORDER BY AUCTION_OCCURRED_AT DESC
    LIMIT 5;
    """
    run_simple_query(funnel_sample_query, "FACT_ADS_ITEM_REQUEST_FUNNEL sample (blending fields)")
    
    # 4. Check for common AUCTION_IDs
    print("\n🔗 STEP 4: Do These Tables Share AUCTION_IDs?")
    
    auction_overlap_query = """
    SELECT 
        'bid_event_ice' as source,
        COUNT(DISTINCT AUCTION_ID) as unique_auction_ids
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'request_funnel' as source,
        COUNT(DISTINCT AUCTION_ID) as unique_auction_ids
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1;
    """
    run_simple_query(auction_overlap_query, "Unique AUCTION_IDs in each table")
    
    # 5. Check what's unique in each table
    print("\n🆔 STEP 5: What Makes Each Table Unique?")
    
    bid_unique_query = """
    SELECT 
        'bid_event_ice' as table_name,
        COUNT(*) as total_rows,
        COUNT(DISTINCT AUCTION_ID) as unique_auctions,
        COUNT(DISTINCT STORE_ID) as unique_stores,
        COUNT(DISTINCT BID_STATUS) as unique_bid_statuses
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1;
    """
    run_simple_query(bid_unique_query, "bid_event_ice uniqueness metrics")
    
    funnel_unique_query = """
    SELECT 
        'request_funnel' as table_name,
        COUNT(*) as total_rows,
        COUNT(DISTINCT AUCTION_ID) as unique_auctions,
        COUNT(DISTINCT AD_REQUEST_ID) as unique_ad_requests,
        COUNT(DISTINCT IMPRESSION_EVENT_ID) as unique_impressions
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1;
    """
    run_simple_query(funnel_unique_query, "request_funnel uniqueness metrics")
    
    # 6. What data is available in bid_event_ice for blending?
    print("\n🎯 STEP 6: What Blending-Relevant Data is in bid_event_ice?")
    
    bid_blending_query = """
    SELECT 
        BID_STATUS,
        COUNT(*) as count,
        AVG(BID_PRICE_UNIT_AMOUNT) as avg_bid_price,
        COUNT(DISTINCT STORE_ID) as unique_stores
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND BID_PRICE_UNIT_AMOUNT IS NOT NULL
    GROUP BY BID_STATUS
    ORDER BY count DESC;
    """
    run_simple_query(bid_blending_query, "bid_event_ice data for blending analysis")
    
    print(f"\n{'='*60}")
    print("🎯 ANALYSIS SUMMARY:")
    print("✅ Both tables have AUCTION_ID - potential join key")
    print("✅ bid_event_ice has raw bid data (BID_PRICE_UNIT_AMOUNT, BID_STATUS)")
    print("✅ request_funnel has processed blending data (AD_QUALITY_SCORE, EXPECTED_VALUE)")
    print("💡 Next: Determine if bid_event_ice can replace request_funnel for blending")
    print(f"{'='*60}")