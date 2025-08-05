#!/usr/bin/env python3
"""
Compare fields available in impression table vs request_funnel table
to understand why both are needed for blending algorithm
"""

import snowflake.connector
import pandas as pd

# Snowflake connection
conn = snowflake.connector.connect(
    account='DOORDASH',
    user='JIAN.LOU',
    password='Lo914021',
    warehouse='AD_ANALYTICS_SERVICE',
    database='PRODDB',
    schema='JIANLOU',
    role='JIANLOU'
)

cursor = conn.cursor()

def run_query(query, description):
    """Execute query and return results"""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print('='*60)
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        if results:
            print(f"✅ Query successful! Returned {len(results)} rows")
            df = pd.DataFrame(results, columns=columns)
            print(df.to_string(index=False, max_colwidth=50))
        else:
            print("✅ Query successful! No results returned")
            
        return results, columns
        
    except Exception as e:
        print(f"❌ Query failed: {str(e)}")
        return None, None

print("🎯 IMPRESSION TABLE vs REQUEST_FUNNEL FIELD COMPARISON")
print("="*60)

# 1. Get impression table schema
impression_schema_query = """
DESCRIBE TABLE proddb.public.fact_item_card_view_dedup
"""

print("\n📊 STEP 1: Impression Table Fields")
imp_results, imp_cols = run_query(impression_schema_query, "Impression table schema")

# 2. Get request_funnel schema  
funnel_schema_query = """
DESCRIBE TABLE proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
"""

print("\n📊 STEP 2: Request Funnel Fields")
funnel_results, funnel_cols = run_query(funnel_schema_query, "Request funnel schema")

# 3. Sample data from impression table (key fields used in pipeline)
impression_sample_query = """
SELECT 
    RECEIVED_AT,
    CARD_POSITION,
    DD_SESSION_ID,
    STORE_ID,
    L1_CATEGORY_ID,
    L2_CATEGORY_ID,
    ID as IMPRESSION_EVENT_ID,
    IS_SPONSORED,
    FEATURE,
    CURRENCY,
    ITEM_ID,
    SUBTOTAL_UNIT_AMOUNT
FROM proddb.public.fact_item_card_view_dedup 
WHERE IS_SPONSORED = TRUE 
  AND DATE(RECEIVED_AT) = '2025-08-04'
  AND CURRENCY = 'USD'
LIMIT 5
"""

print("\n📊 STEP 3: Impression Table Sample Data")
run_query(impression_sample_query, "Key impression fields used in pipeline")

# 4. Sample data from request_funnel
funnel_sample_query = """
SELECT 
    IMPRESSION_EVENT_ID,
    AD_REQUEST_ID,
    AD_QUALITY_SCORE,
    EXPECTED_VALUE,
    TRUE_BID,
    BUSINESS_MERCHANT_SUPPLIED_ID,
    PRICING_METADATA,
    AUCTION_OCCURRED_AT,
    PLACEMENT
FROM proddb.public.FACT_ADS_ITEM_REQUEST_FUNNEL
WHERE DATE(AUCTION_OCCURRED_AT) = '2025-08-04'
LIMIT 5
"""

print("\n📊 STEP 4: Request Funnel Sample Data")
run_query(funnel_sample_query, "Key request funnel fields")

# 5. What's unique to each table?
unique_to_impression = """
SELECT 'UNIQUE TO IMPRESSION TABLE' as TABLE_TYPE, 'User Experience & Context' as CATEGORY, 
       'CARD_POSITION, DD_SESSION_ID, RECEIVED_AT, ITEM_ID, SUBTOTAL_UNIT_AMOUNT' as KEY_FIELDS
UNION ALL
SELECT 'UNIQUE TO REQUEST_FUNNEL' as TABLE_TYPE, 'Auction & Business Logic' as CATEGORY,
       'AD_QUALITY_SCORE, EXPECTED_VALUE, TRUE_BID, PRICING_METADATA, AUCTION_OCCURRED_AT' as KEY_FIELDS
"""

print("\n📊 STEP 5: What Each Table Provides")
run_query(unique_to_impression, "Unique value proposition of each table")

# 6. Why both are needed for blending
why_both_needed = """
SELECT 'IMPRESSION TABLE' as SOURCE, 'WHERE ads appear on page' as PURPOSE, 'CARD_POSITION' as EXAMPLE_FIELD
UNION ALL
SELECT 'IMPRESSION TABLE', 'User session context', 'DD_SESSION_ID'
UNION ALL  
SELECT 'IMPRESSION TABLE', 'User timing/behavior', 'RECEIVED_AT'
UNION ALL
SELECT 'IMPRESSION TABLE', 'Item pricing context', 'SUBTOTAL_UNIT_AMOUNT'
UNION ALL
SELECT 'REQUEST_FUNNEL', 'Auction competitiveness', 'AD_QUALITY_SCORE'
UNION ALL
SELECT 'REQUEST_FUNNEL', 'Bidding strategy', 'TRUE_BID, EXPECTED_VALUE'
UNION ALL
SELECT 'REQUEST_FUNNEL', 'Ad ranking logic', 'PRICING_METADATA (adRank)'
"""

print("\n📊 STEP 6: Why Both Tables Are Essential")
run_query(why_both_needed, "Complementary data for blending algorithm")

conn.close()

print("\n" + "="*60)
print("🎯 SUMMARY: Why You Need BOTH Tables")
print("="*60)
print("✅ IMPRESSION TABLE provides:")
print("   • User experience data (card position, session, timing)")
print("   • Item context (pricing, categories)")
print("   • User behavior patterns")
print()
print("✅ REQUEST_FUNNEL provides:")
print("   • Auction competitiveness metrics")
print("   • Bidding strategy data")
print("   • Business logic (quality scores, expected values)")
print()
print("🔗 TOGETHER they enable:")
print("   • Understanding WHERE ads appear AND WHY")
print("   • Correlating auction decisions with user experience")
print("   • Optimizing blending based on both auction AND user context")
print("="*60)