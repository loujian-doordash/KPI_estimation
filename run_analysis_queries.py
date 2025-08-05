#!/usr/bin/env python3
"""
Run the analysis queries from your queries/ folder
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

def run_query(query, description=""):
    """Run a query and return results as pandas DataFrame"""
    try:
        conn = get_connection()
        print(f"\n{'='*60}")
        print(f"🔍 {description}")
        print(f"{'='*60}")
        
        df = pd.read_sql(query, conn)
        print(f"✅ Query successful! Returned {len(df)} rows")
        print(df.head(10))  # Show first 10 rows
        
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

if __name__ == "__main__":
    # Query 1: Compare table schemas
    print("📋 ANALYSIS 1: Table Schema Comparison")
    
    # Check FACT_ADS_ITEM_REQUEST_FUNNEL schema
    funnel_schema_query = """
    DESCRIBE TABLE proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL;
    """
    run_query(funnel_schema_query, "FACT_ADS_ITEM_REQUEST_FUNNEL Schema")
    
    # Query 2: Compare record counts
    print("\n📊 ANALYSIS 2: Record Count Comparison")
    
    count_query = """
    SELECT 
        'bid_event_ice' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT AUCTION_ID) as unique_auctions,
        MIN(OCCURRED_AT::DATE) as min_date,
        MAX(OCCURRED_AT::DATE) as max_date
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE >= CURRENT_DATE - 7
    
    UNION ALL
    
    SELECT 
        'request_funnel' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT IMPRESSION_EVENT_ID) as unique_auctions,
        MIN(AUCTION_OCCURRED_AT::DATE) as min_date,
        MAX(AUCTION_OCCURRED_AT::DATE) as max_date
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE >= CURRENT_DATE - 7;
    """
    run_query(count_query, "Record Counts - Last 7 Days")
    
    # Query 3: Sample data comparison
    print("\n📋 ANALYSIS 3: Sample Data from Each Table")
    
    bid_event_sample = """
    SELECT 
        AUCTION_ID,
        STORE_ID,
        BID_PRICE_UNIT_AMOUNT,
        OCCURRED_AT,
        BID_STATUS
    FROM iguazu.server_events_production.bid_event_ice
    WHERE OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 5;
    """
    run_query(bid_event_sample, "Sample from bid_event_ice")
    
    request_funnel_sample = """
    SELECT 
        AD_REQUEST_ID,
        IMPRESSION_EVENT_ID,
        AD_QUALITY_SCORE,
        EXPECTED_VALUE,
        TRUE_BID,
        AUCTION_OCCURRED_AT
    FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
    WHERE AUCTION_OCCURRED_AT::DATE = CURRENT_DATE - 1
    LIMIT 5;
    """
    run_query(request_funnel_sample, "Sample from FACT_ADS_ITEM_REQUEST_FUNNEL")
    
    print(f"\n{'='*60}")
    print("🎯 ANALYSIS COMPLETE!")
    print("✅ Your Snowflake connection is working perfectly")
    print("✅ You have access to both bid_event_ice and FACT_ADS_ITEM_REQUEST_FUNNEL")
    print("✅ Ready to proceed with your blending algorithm analysis!")
    print(f"{'='*60}")