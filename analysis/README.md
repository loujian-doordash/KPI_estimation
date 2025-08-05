# 📊 Current Analysis Phase

This folder contains ongoing exploratory data analysis to understand auction data and inform KPI estimation model development.

## 🎯 **Current Status: Data Understanding Phase**

We are currently in the **data analysis and exploration phase**, building understanding of auction data relationships before developing KPI estimation models.

## 📁 **Analysis Areas:**

### **📋 `table_relationships/`**
**Current Work**: Understanding data flow between auction tables
- Scripts analyzing connections between `bid_event_ice`, `FACT_ADS_ITEM_REQUEST_FUNNEL`, and impression tables
- Schema comparison and field mapping
- Data quality assessment

**For KPI Modeling**: Identifies key features and data sources for prediction models

### **🎯 `auction_behavior/`**
**Current Work**: Understanding auction dynamics and patterns
- A/B testing analysis scripts
- Auction performance pattern analysis
- User behavior in auction contexts

**For KPI Modeling**: Informs model features and target variable definitions

### **📈 `kpi_exploration/`**
**Future Work**: Initial KPI metric exploration and definition
- KPI calculation methodology
- Historical KPI trend analysis
- KPI correlation analysis

## 🔄 **Analysis → KPI Modeling Pipeline:**

### **Phase 1: Data Understanding (Current)**
- ✅ Table relationship mapping
- ✅ Data flow analysis  
- ✅ Schema comparison
- 🔄 Auction behavior patterns

### **Phase 2: KPI Definition (Next)**
- Define specific KPIs to estimate
- Calculate historical KPI values
- Identify prediction targets

### **Phase 3: Feature Engineering**
- Extract features from auction data
- Create training datasets
- Design feature pipelines

### **Phase 4: Model Development**
- Build prediction models
- Validate on historical data
- Integrate with simulator

## 📊 **Key Findings So Far:**

1. **`FACT_ADS_ITEM_REQUEST_FUNNEL`** is the key table linking auction decisions to outcomes
2. **Real-time vs batch data** considerations for simulation
3. **Multiple objective optimization** patterns from blending algorithms
4. **Position effects** and user experience factors

## 🎯 **Next Steps:**
1. Complete auction behavior analysis
2. Define specific KPIs for estimation
3. Begin feature engineering pipeline development
4. Design model architecture for KPI prediction