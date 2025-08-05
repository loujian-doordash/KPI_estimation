#!/usr/bin/env python3
"""
Check what type of auctions are missing from request_funnel but exist in bid_event_ice
This helps determine if bid_event_ice adds value for blending
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
        
        print(f"\n{'='*50}")
        print(f"🔍 {description}")
        print(f"{'='*50}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        print(f"✅ Query successful! Returned {len(results)} rows")
        
        if results:
            print(f"\n{' | '.join(columns)}")
            print("-" * (len(' | '.join(columns))))
            
            for row in results:
                row_str = ' | '.join([str(val)[:30] if val is not None else 'NULL' for val in row])
                print(row_str)
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

if __name__ == "__main__":
    print("🔍 INVESTIGATING MISSING AUCTIONS")
    
    # Check what bid statuses exist in bid_event_ice
    print("\n📊 STEP 1: What Bid Statuses Exist in bid_event_ice?")
    
    bid_status_query = """
    SELECT 
        BID_STATUS,
        COUNT(*) as count,
        COUNT(DISTINCT AUCTION_ID) as unique_auctions,
        ROUND(AVG(BID_PRICE_UNIT_AMOUNT), 2) as avg_bid_price
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    GROUP BY BID_STATUS
    ORDER BY count DESC;
    """
    run_query(bid_status_query, "Bid statuses in bid_event_ice")
    
    # Check if missing auctions have different characteristics
    print("\n🔗 STEP 2: Sample of Auctions NOT in request_funnel")
    
    missing_auctions_query = """
    SELECT 
        be.AUCTION_ID,
        be.BID_STATUS,
        be.BID_PRICE_UNIT_AMOUNT,
        be.ELIMINATION_REASON,
        be.STORE_ID
    FROM iguazu.server_events_production.bid_event_ice be
    LEFT JOIN proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL rf
        ON be.AUCTION_ID = rf.AUCTION_ID
    WHERE be.OCCURRED_AT::DATE = CURRENT_DATE - 1
        AND rf.AUCTION_ID IS NULL
    LIMIT 10;
    """
    run_query(missing_auctions_query, "Sample auctions missing from request_funnel")
    
    # Check if request_funnel filters by placement type
    print("\n🎯 STEP 3: What Placements are in request_funnel?")
    
    placement_query = """
    SELECT 
        PLACEMENT,
        COUNT(*) as count,
        COUNT(DISTINCT AUCTION_ID) as unique_auctions
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    GROUP BY PLACEMENT
    ORDER BY count DESC;
    """
    run_query(placement_query, "Placements in request_funnel")
    
    print(f"\n{'='*60}")
    print("🎯 KEY INSIGHTS:")
    print("1. request_funnel likely filters for specific placements (category pages)")
    print("2. Missing auctions might be from other placements (search, homepage, etc.)")
    print("3. For blending algorithm, request_funnel is probably more relevant")
    print("4. bid_event_ice is useful for broader auction analysis")
    print(f"{'='*60}")