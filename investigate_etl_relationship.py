#!/usr/bin/env python3
"""
Investigate the relationship between bid_event_ice and FACT_ADS_ITEM_REQUEST_FUNNEL
Look for patterns that suggest how one is derived from the other
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
            print("-" * min(80, len(' | '.join(columns))))
            
            for row in results:
                row_str = ' | '.join([str(val)[:20] if val is not None else 'NULL' for val in row])
                print(row_str)
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

if __name__ == "__main__":
    print("🔍 INVESTIGATING ETL RELATIONSHIP BETWEEN TABLES")
    
    # 1. Compare timestamps - are they the same auctions processed differently?
    print("\n⏰ STEP 1: Timestamp Comparison for Same Auctions")
    
    timestamp_query = """
    SELECT 
        be.AUCTION_ID,
        be.OCCURRED_AT as bid_event_time,
        rf.AUCTION_OCCURRED_AT as request_funnel_time,
        DATEDIFF('second', be.OCCURRED_AT, rf.AUCTION_OCCURRED_AT) as time_diff_seconds
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 10;
    """
    run_query(timestamp_query, "Timestamp comparison for shared auctions")
    
    # 2. Check if bid amounts match
    print("\n💰 STEP 2: Bid Amount Relationships")
    
    bid_comparison_query = """
    SELECT 
        be.AUCTION_ID,
        be.BID_PRICE_UNIT_AMOUNT as raw_bid_price,
        rf.TRUE_BID as processed_true_bid,
        rf.EXPECTED_VALUE as expected_value,
        ROUND(rf.TRUE_BID / be.BID_PRICE_UNIT_AMOUNT, 2) as bid_ratio
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND be.BID_PRICE_UNIT_AMOUNT > 0
        AND rf.TRUE_BID > 0
    LIMIT 10;
    """
    run_query(bid_comparison_query, "Bid amount relationships")
    
    # 3. Check aggregation patterns - multiple bid_events per request_funnel row?
    print("\n📊 STEP 3: Aggregation Patterns")
    
    aggregation_query = """
    SELECT 
        rf.AUCTION_ID,
        rf.AD_REQUEST_ID,
        COUNT(*) as bid_events_count,
        COUNT(DISTINCT be.BID_PRICE_UNIT_AMOUNT) as unique_bid_amounts,
        AVG(be.BID_PRICE_UNIT_AMOUNT) as avg_bid_price,
        MAX(rf.TRUE_BID) as funnel_true_bid
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
    JOIN iguazu.server_events_production.bid_event_ice be
        ON rf.AUCTION_ID = be.AUCTION_ID
    WHERE rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND be.OCCURRED_AT::DATE = CURRENT_DATE - 1
    GROUP BY rf.AUCTION_ID, rf.AD_REQUEST_ID, rf.TRUE_BID
    HAVING COUNT(*) > 1
    LIMIT 10;
    """
    run_query(aggregation_query, "Auctions with multiple bid events")
    
    # 4. Check what data is unique to request_funnel
    print("\n🆔 STEP 4: What's Added in Request Funnel?")
    
    unique_fields_query = """
    SELECT 
        COUNT(DISTINCT AD_REQUEST_ID) as unique_ad_requests,
        COUNT(DISTINCT IMPRESSION_EVENT_ID) as unique_impressions,
        COUNT(DISTINCT BUSINESS_MERCHANT_SUPPLIED_ID) as unique_merchants,
        COUNT(DISTINCT PLACEMENT) as unique_placements,
        COUNT(*) as total_rows
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1;
    """
    run_query(unique_fields_query, "Unique identifiers in request_funnel")
    
    # 5. Sample the additional fields that aren't in bid_event_ice
    print("\n📋 STEP 5: Sample Enhanced Data in Request Funnel")
    
    enhanced_data_query = """
    SELECT 
        AUCTION_ID,
        AD_REQUEST_ID,
        IMPRESSION_EVENT_ID,
        ROUND(AD_QUALITY_SCORE, 4) as quality_score,
        ROUND(EXPECTED_VALUE, 2) as expected_val,
        PLACEMENT,
        BUSINESS_MERCHANT_SUPPLIED_ID
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND AD_QUALITY_SCORE IS NOT NULL
        AND EXPECTED_VALUE IS NOT NULL
    LIMIT 5;
    """
    run_query(enhanced_data_query, "Enhanced data fields in request_funnel")
    
    print(f"\n{'='*60}")
    print("🎯 INVESTIGATION SUMMARY:")
    print("1. Check if timestamps match (same auction events)")
    print("2. See how bid amounts are transformed")
    print("3. Understand aggregation (multiple bids → single funnel row)")
    print("4. Identify what ML/business logic is added")
    print("5. Use findings to inform Slack questions to Ads DE team")
    print(f"{'='*60}")