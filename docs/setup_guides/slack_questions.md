# Questions for Slack - FACT_ADS_ITEM_REQUEST_FUNNEL Investigation

## 🎯 **Main Question:**
How is `FACT_ADS_ITEM_REQUEST_FUNNEL` generated from `bid_event_ice` or other source tables?

## 📊 **Specific Questions to Ask:**

### **Data Lineage:**
1. **What ETL pipeline creates `FACT_ADS_ITEM_REQUEST_FUNNEL`?**
2. **Is `bid_event_ice` the source table for `request_funnel`?**
3. **What other tables are joined/used in the ETL?**

### **Data Processing:**
4. **How are `AD_QUALITY_SCORE` and `EXPECTED_VALUE` calculated?**
5. **What ML models/algorithms compute these scores?**
6. **Why does `request_funnel` have fewer auctions than `bid_event_ice`?**

### **Filtering Logic:**
7. **What filters are applied when creating `request_funnel`?**
   - Specific placements only? (category pages vs search/homepage)
   - Certain bid statuses only? 
   - Geographic/currency filters?

### **Business Logic:**
8. **What business rules determine which auctions make it into `request_funnel`?**
9. **Is `request_funnel` specifically designed for blending algorithms?**
10. **How often is `request_funnel` updated vs `bid_event_ice`?**

## 🔧 **Technical Questions:**

### **Schema & Joins:**
11. **How does `IMPRESSION_EVENT_ID` get added to `request_funnel`?**
12. **What table provides the impression linking?**
13. **Is there a specific schema/data model diagram?**

### **Migration Planning:**
14. **With bid_event deprecation (Sep 5), how will `request_funnel` be updated?**
15. **Will it use the new `ads_auction_candidates_event` table?**
16. **Any planned changes to the ETL process?**

## 👥 **Who to Ask:**

Based on the deprecation document, contact:
- **Ads DE Team**: Plaban Dash, Douglas Martins, Sina Roushan
- **Ads Econ Team**: Yuchen Yang, Yongjin Xiao, Ze Ma, Adrian Rivera Cardoso

## 🎯 **Why This Matters:**

Understanding this relationship helps determine:
- Whether to stick with `request_funnel` for blending
- How to adapt when underlying tables change
- Whether `bid_event_ice` can be a suitable replacement
- Migration strategy for September deadline

## 📈 **Our Findings to Share:**

- `bid_event_ice`: 5.5B rows, 216M auctions
- `request_funnel`: 3.4B rows, 155M auctions (72% coverage)
- ~61M auctions exist in `bid_event_ice` but not in `request_funnel`
- `request_funnel` has processed ML metrics ready for blending
- Both tables share common AUCTION_IDs for joining