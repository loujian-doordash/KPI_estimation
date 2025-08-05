# 📊 Data Analysis for KPI Estimation

This folder contains analysis of auction data to understand relationships and inform KPI estimation model development.

## 🎯 **Purpose for KPI Estimation:**
Understanding data relationships is crucial for building accurate KPI estimation models for auction simulators. This analysis helps identify:
- Key features for KPI prediction models
- Data quality and availability
- Relationships between auction inputs and performance outcomes

## 📁 **Analysis Results:**

### **Table Relationship Analysis:**
- `impression_vs_funnel_summary.md` - Why both tables are needed for KPI modeling
- `bid_event_impression_relationship_summary.md` - Auction data flow analysis
- `table_comparison_summary.md` - Schema comparison for feature engineering

## 🎯 **Key Insights for KPI Models:**

### **Data Flow for KPI Estimation:**
1. **Auction Events** → `bid_event_ice` (raw auction data)
2. **Processed Metrics** → `FACT_ADS_ITEM_REQUEST_FUNNEL` (quality scores, expected values)
3. **User Outcomes** → Impression tables (actual user experience)

### **Potential KPI Features:**
- **Auction Inputs**: Bid amounts, quality scores, expected values
- **Context**: Store, category, time, user session
- **Outcomes**: Win rates, positions, user engagement

### **Data Quality Considerations:**
- `FACT_ADS_ITEM_REQUEST_FUNNEL` is the key bridge between auction decisions and outcomes
- Real-time data availability for simulation
- Missing data patterns and handling strategies

## 🔄 **Next Steps for KPI Modeling:**
1. Define specific KPIs to estimate (win rate, revenue, engagement)
2. Design feature engineering pipeline
3. Create training datasets with historical auction outcomes
4. Develop baseline prediction models