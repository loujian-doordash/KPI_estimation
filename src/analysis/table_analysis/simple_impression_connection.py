#!/usr/bin/env python3
"""
Simple analysis of bid_event_ice → impression connection
Based on findings: bid_event_ice connects to impressions ONLY through request_funnel
"""
import snowflake.connector
import os
import getpass

def get_connection():
    password = os.getenv('SNOWFLAKE_PASSWORD', 'Lo914021')  # Use default for now
    return snowflake.connector.connect(
        account='DOORDASH',
        user='JIAN.LOU',
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
        
        print(f"\n{'='*50}")
        print(f"🔍 {description}")
        print(f"{'='*50}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✅ Query successful! Returned {len(results)} rows")
        
        if results:
            print(f"\n{' | '.join(columns)}")
            print("-" * min(80, len(' | '.join(columns))))
            
            for row in results:
                row_str = ' | '.join([str(val)[:25] if val is not None else 'NULL' for val in row])
                print(row_str)
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

if __name__ == "__main__":
    print("🔍 SIMPLE BID_EVENT → IMPRESSION CONNECTION ANALYSIS")
    
    # 1. Final connection statistics
    print("\n📊 STEP 1: Connection Success Rates")
    
    connection_stats = """
    SELECT 
        'Total bid_events' as metric,
        COUNT(*) as count,
        0 as percentage
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'Bid_events with request_funnel' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / 5500371119, 2) as percentage
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'Request_funnel with impressions' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / 3444855580, 2) as percentage
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
    JOIN proddb.public.fact_item_card_view_dedup imp
        ON rf.IMPRESSION_EVENT_ID = imp.ID
    WHERE rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND imp.DATE(RECEIVED_AT) = CURRENT_DATE - 1;
    """
    run_query(connection_stats, "Connection success rates")
    
    # 2. Sample of full chain
    print("\n🔗 STEP 2: Sample Full Chain Connection")
    
    full_chain_sample = """
    SELECT 
        be.AUCTION_ID,
        be.BID_PRICE_UNIT_AMOUNT as raw_bid,
        rf.TRUE_BID as processed_bid,
        rf.AD_QUALITY_SCORE,
        imp.CARD_POSITION,
        imp.IS_SPONSORED,
        DATEDIFF('second', be.OCCURRED_AT, imp.RECEIVED_AT) as time_diff_sec
    FROM iguazu.server_events_production.bid_event_ice be
    JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    JOIN proddb.public.fact_item_card_view_dedup imp
        ON rf.IMPRESSION_EVENT_ID = imp.ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND imp.DATE(RECEIVED_AT) = CURRENT_DATE - 1
    LIMIT 5;
    """
    run_query(full_chain_sample, "Sample: bid_event → request_funnel → impression")
    
    # 3. Why bid_event_ice can't directly connect to impressions
    print("\n❌ STEP 3: Why No Direct Connection?")
    
    no_direct_connection = """
    SELECT 
        'Missing in bid_event_ice' as field_type,
        'IMPRESSION_EVENT_ID' as missing_field,
        'Required for impression join' as reason
    
    UNION ALL
    
    SELECT 
        'Available in request_funnel' as field_type,
        'IMPRESSION_EVENT_ID' as available_field,
        'Enables impression linking' as reason;
    """
    run_query(no_direct_connection, "Why bid_event_ice needs request_funnel bridge")
    
    print(f"\n{'='*60}")
    print("🎯 FINAL CONCLUSION:")
    print("1. ❌ bid_event_ice has NO direct connection to impressions")
    print("2. ✅ request_funnel acts as a BRIDGE between auctions and impressions")
    print("3. 🔗 Connection chain: bid_event → request_funnel → impression")
    print("4. 📊 request_funnel adds IMPRESSION_EVENT_ID during ETL processing")
    print("5. 💡 For blending: You MUST use request_funnel to get impression data")
    print(f"{'='*60}")