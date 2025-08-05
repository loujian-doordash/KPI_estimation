# bid_event_ice → Impression Table Relationship Analysis

## 🎯 **Key Finding: NO DIRECT CONNECTION**

**bid_event_ice cannot directly connect to impression table (`fact_item_card_view_dedup`)**

## 📊 **Data Volume Overview**

| Table | Volume (Yesterday) | Key Fields |
|-------|-------------------|------------|
| **bid_event_ice** | 5.5B rows | `AUCTION_ID`, `STORE_ID`, `OCCURRED_AT` |
| **fact_item_card_view_dedup** | 58.3M impressions | `ID` (impression_event_id), `STORE_ID`, `RECEIVED_AT` |
| **Sponsored impressions** | 4.9M (8.5%) | Subset relevant to ads |
| **Organic impressions** | 53.4M (91.5%) | Non-sponsored content |

## 🔗 **Connection Analysis**

### **❌ Direct Connection: IMPOSSIBLE**
- **bid_event_ice** has: `AUCTION_ID`, `STORE_ID`, `OCCURRED_AT`
- **Impression table** has: `ID` (impression_event_id), `STORE_ID`, `RECEIVED_AT`
- **Missing link**: No `IMPRESSION_EVENT_ID` in bid_event_ice

### **✅ Indirect Connection: Through request_funnel**
```
bid_event_ice 
    ↓ (via AUCTION_ID)
FACT_ADS_ITEM_REQUEST_FUNNEL 
    ↓ (via IMPRESSION_EVENT_ID)  
fact_item_card_view_dedup
```

## 🔄 **Data Flow Pattern**

### **Step 1: Auction to Request Funnel**
- **Join Key**: `AUCTION_ID`
- **Success Rate**: ~63% (3.4B/5.5B bid events make it to request_funnel)
- **Data Enhancement**: Adds `IMPRESSION_EVENT_ID`, `AD_QUALITY_SCORE`, `EXPECTED_VALUE`

### **Step 2: Request Funnel to Impression**
- **Join Key**: `IMPRESSION_EVENT_ID` 
- **Success Rate**: ~100% (perfect matches observed)
- **Time Alignment**: Auction and impression times match within seconds
- **Store Validation**: Store IDs match perfectly

## 🎯 **From Your Blending Reference Code**

### **Current Implementation Pattern:**
```sql
-- Step 1: Get impressions
SELECT ID AS IMPRESSION_EVENT_ID, STORE_ID, CARD_POSITION...
FROM fact_item_card_view_dedup
WHERE IS_SPONSORED = TRUE AND FEATURE = 'category'

-- Step 2: Join with request_funnel
SELECT ad_funnel.*, ad_imp.*
FROM FACT_ADS_ITEM_REQUEST_FUNNEL ad_funnel
LEFT JOIN ad_impression_table ad_imp
  ON ad_imp.IMPRESSION_EVENT_ID = ad_funnel.IMPRESSION_EVENT_ID
```

## 🚨 **Critical Implications for Your Analysis**

### **Why You CANNOT Replace request_funnel with bid_event_ice:**

1. **❌ Missing Impression Link**: bid_event_ice has no `IMPRESSION_EVENT_ID`
2. **❌ No Card Position**: Can't determine where ads appeared on page
3. **❌ No Impression Timing**: Can't correlate auction wins with actual impressions
4. **❌ Missing Blending Context**: No connection to user impression experience

### **What request_funnel Provides That bid_event_ice Cannot:**

| Feature | request_funnel | bid_event_ice | Impact |
|---------|---------------|---------------|---------|
| **Impression Linking** | ✅ IMPRESSION_EVENT_ID | ❌ Missing | CRITICAL for blending |
| **Card Position** | ✅ Via impression join | ❌ No connection | Need for ranking analysis |
| **User Context** | ✅ Session/impression data | ❌ Auction-only | Essential for UX optimization |
| **Timing Correlation** | ✅ Auction → Impression | ❌ Auction only | Required for performance measurement |

## 💡 **Business Logic Understanding**

### **Why This Architecture Makes Sense:**
1. **Auction Events**: Raw bid events in `bid_event_ice`
2. **ETL Processing**: Links auctions to impressions during request_funnel creation
3. **Impression Tracking**: Separate system tracks what users actually see
4. **Data Integration**: request_funnel bridges auction and impression systems

### **ETL Process (Inferred):**
```
bid_event_ice (Raw Auctions)
    ↓ (Business rules + ML processing)
    ↓ (Link with impression system)
    ↓ (Add impression_event_id)
FACT_ADS_ITEM_REQUEST_FUNNEL (Processed + Linked)
    ↓ (Your blending algorithm)
Optimized Rankings
```

## 🎯 **Recommendations for Your Blending Work**

### **✅ DO:**
- **Continue using FACT_ADS_ITEM_REQUEST_FUNNEL** as your primary data source
- **Use request_funnel's IMPRESSION_EVENT_ID** to join with impression data
- **Leverage the existing impression pipeline** in your blending reference code
- **Plan migration to ads_auction_candidates_event** (Sep 5 deadline)

### **❌ DON'T:**
- **Try to replace request_funnel with bid_event_ice** for blending
- **Attempt direct bid_event_ice → impression joins** (will fail)
- **Build custom impression linking logic** (reinventing the wheel)

## 🔮 **Future Migration Considerations**

When migrating to `ads_auction_candidates_event`:
- **Verify it includes IMPRESSION_EVENT_ID** (should be 1:1 with request_funnel)
- **Test impression linking continues to work**
- **Ensure blending algorithm compatibility**

## 📊 **Summary Statistics**

- **Total impression volume**: 58.3M/day
- **Sponsored impressions**: 4.9M/day (your target audience)
- **Auction-to-impression success rate**: ~63% through request_funnel
- **Impression matching accuracy**: 100% (perfect ID matches)
- **Time correlation**: Auction and impression within seconds

## 🎯 **Bottom Line**

**For your blending algorithm, the impression connection is ONLY possible through FACT_ADS_ITEM_REQUEST_FUNNEL.** The request_funnel table serves as the essential bridge between auction events and user impression experiences, making it indispensable for any ranking optimization work.