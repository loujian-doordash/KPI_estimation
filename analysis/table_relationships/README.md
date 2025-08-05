# 📊 Table Relationship Analysis

Current analysis work understanding auction data relationships to inform KPI estimation model development.

## 🎯 **Purpose for KPI Estimation:**
Understanding how auction data flows between tables is crucial for:
- Identifying features for KPI prediction models
- Ensuring data quality and completeness
- Designing efficient data pipelines for model training

## 📁 **Analysis Scripts:**

### **Schema and Relationship Analysis:**
- `compare_table_fields.py` - Compare field availability across tables
- `simple_table_comparison.py` - Quick table comparison utilities
- `bid_event_impressions_analysis.py` - Analyze bid event to impression relationships
- `simple_impression_connection.py` - Simplified connection analysis

## 🔍 **Key Findings for KPI Models:**

### **Data Flow Pattern:**
```
Auction Events (bid_event_ice) 
    ↓
Processed Metrics (FACT_ADS_ITEM_REQUEST_FUNNEL)
    ↓  
User Outcomes (impression tables)
```

### **Feature Availability:**
- **Auction Inputs**: Available in `bid_event_ice` and `FACT_ADS_ITEM_REQUEST_FUNNEL`
- **Performance Metrics**: Quality scores, expected values in request funnel
- **User Outcomes**: Position, engagement, timing in impression tables
- **Business Context**: Store, category, session information

### **Data Quality Insights:**
1. **`FACT_ADS_ITEM_REQUEST_FUNNEL`** is the key linking table
2. **Real-time availability** varies between tables
3. **Coverage differences** between bid events and impression data
4. **Deduplication** requirements for accurate KPI calculation

## 🎯 **Implications for KPI Modeling:**

### **Feature Engineering Pipeline:**
- Use request funnel as primary feature source
- Augment with impression outcome data
- Handle missing data patterns appropriately

### **Training Data Creation:**
- Join pattern: `bid_event → request_funnel → impression`
- Filter for complete data chains
- Account for temporal delays between auction and impression

### **Model Architecture:**
- Multi-stage prediction (auction → outcome)
- Handle sparse features appropriately
- Account for position and context effects

## 🔄 **Next Steps:**
1. Complete relationship mapping
2. Design feature extraction pipeline
3. Create training dataset specification
4. Begin model feature engineering