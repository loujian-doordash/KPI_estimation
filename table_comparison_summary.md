# Table Comparison Summary: FACT_ADS_ITEM_REQUEST_FUNNEL vs bid_event_ice

## 📊 **Key Findings Overview**

| Metric | bid_event_ice | FACT_ADS_ITEM_REQUEST_FUNNEL | Difference |
|--------|---------------|------------------------------|------------|
| **Total Rows (Yesterday)** | 5.5B | 3.4B | +60% more data |
| **Unique Auctions** | 216M | 155M | +61M auctions |
| **Coverage** | 100% (all auctions) | 72% (filtered subset) | 28% missing |
| **Data Type** | Raw events | Processed/enriched | Transformed |
| **Unique Stores** | 427K | ~155K (estimated) | More comprehensive |

## 🔍 **Data Structure Differences**

### **bid_event_ice (Raw Auction Events)**
✅ **Strengths:**
- **Complete auction data**: All 216M auctions from yesterday
- **Raw bid information**: `BID_PRICE_UNIT_AMOUNT`, `BID_STATUS`, `ELIMINATION_REASON`
- **Comprehensive coverage**: Includes all placements and auction types
- **Real-time data**: Direct from auction system
- **Audit trail**: Shows all bid attempts and outcomes

❌ **Limitations:**
- **No ML-computed metrics**: Missing `AD_QUALITY_SCORE`, `EXPECTED_VALUE`
- **No impression linking**: No `IMPRESSION_EVENT_ID` for joining with impressions
- **Raw format**: Requires additional processing for blending algorithms
- **Only bid status = WINNER**: All records show `BID_STATUS_WINNER` (may need investigation)

### **FACT_ADS_ITEM_REQUEST_FUNNEL (Processed for Blending)**
✅ **Strengths:**
- **Blending-ready metrics**: Pre-calculated `AD_QUALITY_SCORE`, `EXPECTED_VALUE`, `TRUE_BID`
- **Impression integration**: Has `IMPRESSION_EVENT_ID` for joining with impression data
- **Business logic applied**: Processed according to established algorithms
- **Optimized for analytics**: Structured for blending and ranking use cases
- **Additional identifiers**: `AD_REQUEST_ID`, `BUSINESS_MERCHANT_SUPPLIED_ID`

❌ **Limitations:**
- **Subset of data**: Missing 61M auctions (28% of total)
- **Black box processing**: Unclear how metrics are calculated
- **Dependency risk**: Built on top of tables being deprecated
- **Less granular**: May aggregate multiple bid events

## 🔗 **Relationship Analysis**

### **Shared Data:**
- **Common AUCTION_IDs**: 155M auctions appear in both tables
- **Timestamp alignment**: `OCCURRED_AT` ≈ `AUCTION_OCCURRED_AT` (0 second difference)
- **Same store coverage**: Overlapping `STORE_ID` values

### **Data Transformation Patterns:**
From our investigation (partial results):
- **Timestamps match exactly**: Same auction events, processed differently
- **Bid amount transformation**: `BID_PRICE_UNIT_AMOUNT` → `TRUE_BID` (with business logic)
- **Enhanced with ML**: Added `AD_QUALITY_SCORE` and `EXPECTED_VALUE`

## 🎯 **Missing Auction Analysis**

**61M auctions in `bid_event_ice` but NOT in `request_funnel`**

**Likely reasons for exclusion:**
1. **Placement filtering**: Only category page auctions included in request_funnel
2. **Status filtering**: May exclude certain bid statuses or elimination reasons
3. **Quality thresholds**: Auctions below certain quality scores filtered out
4. **Business rules**: Specific criteria for blending eligibility

## 💼 **Business Implications**

### **For Blending Algorithm Use:**

#### **Use FACT_ADS_ITEM_REQUEST_FUNNEL when:**
- ✅ **Current blending algorithm**: Ready-to-use metrics
- ✅ **Category page focus**: Filtered for relevant placements
- ✅ **Impression joining**: Need to link with impression data
- ✅ **Established pipeline**: Proven data quality and business logic

#### **Use bid_event_ice when:**
- 🔍 **Comprehensive analysis**: Need all auction data
- 📊 **Custom metrics**: Want to build your own quality scores
- 🚀 **Future migration**: Preparing for new auction candidates table
- 🔧 **Deep investigation**: Understanding auction dynamics

## 🚨 **Migration Considerations**

### **September 5, 2025 Deadline:**
- **Current approach**: Continue using `request_funnel` (stable)
- **Future state**: Migrate to `ads_auction_candidates_event` (1:1 replacement)
- **Risk mitigation**: `request_funnel` likely to be updated with new data source

### **Recommended Strategy:**
1. **Short-term**: Keep using `FACT_ADS_ITEM_REQUEST_FUNNEL` for blending
2. **Medium-term**: Plan migration to new auction candidates table
3. **Long-term**: Consider `bid_event_ice` for broader auction analysis

## 🔧 **ETL Relationship (Hypothesis)**

Based on our analysis, the likely data flow is:
```
bid_event_ice (Raw Events)
    ↓ (Filter by placement/status)
    ↓ (Apply business rules)
    ↓ (Calculate ML metrics)
    ↓ (Join with impression data)
FACT_ADS_ITEM_REQUEST_FUNNEL (Processed)
```

## 📋 **Next Steps**

1. **Slack Investigation**: Use `slack_questions.md` to understand ETL process
2. **Ads DE Team**: Contact Plaban Dash, Douglas Martins, Sina Roushan
3. **Migration Planning**: Prepare for auction candidates table transition
4. **Algorithm Validation**: Ensure blending algorithm works with new data source

## 🎯 **Conclusion**

- **For current blending work**: Stick with `FACT_ADS_ITEM_REQUEST_FUNNEL`
- **For future development**: Understand the ETL process and prepare for migration
- **For comprehensive analysis**: Use `bid_event_ice` as supplementary data source
- **Key insight**: Tables are complementary, not competing - use the right tool for the right job