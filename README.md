# 🎯 KPI Estimation Project

**A comprehensive analysis and optimization system for content blending algorithms in ad/organic content ranking.**

## 📋 **Project Overview**

This project analyzes and optimizes utility-based blending algorithms that mix sponsored ads and organic (non-video) content to maximize both user engagement and revenue while maintaining good user experience.

### **🎯 Key Objectives:**
- Understand data relationships between impression tables, request funnels, and bid events
- Analyze and optimize utility-based blending algorithms
- Evaluate performance of different content mixing strategies
- Prepare for migration from legacy `bid_event` tables to new `ads_auction_candidates_event`

## 🏗️ **Project Structure**

```
KPI_estimation/
├── 📖 docs/                    # All project documentation
│   ├── table_analysis/         # Table relationship analysis
│   ├── algorithm_analysis/     # Blending algorithm documentation
│   ├── setup_guides/          # Configuration and setup guides
│   └── experiments/           # Research documentation
│
├── 🐍 src/                     # Source code (modular structure)
│   ├── data_pipelines/        # ETL processing pipelines
│   ├── algorithms/            # Core blending algorithms
│   ├── analysis/              # Research and analysis scripts
│   │   ├── table_analysis/    # Table relationship analysis
│   │   └── experiments/       # A/B testing and experiments
│   ├── utils/                 # Shared utilities
│   └── config/               # Configuration management
│
├── 🗃️ sql/                     # SQL queries and database scripts
│   ├── schema_analysis/       # Table schema and relationship queries
│   ├── auction_analysis/      # Auction and bidding queries
│   ├── testing/              # Connection tests
│   └── etl_analysis/         # ETL pipeline queries
│
├── 🧪 tests/                   # Unit and integration tests
├── 🔧 scripts/                 # Utility and setup scripts
├── 📓 notebooks/               # Jupyter notebooks (if any)
├── 📊 data/                    # Local data files
└── 📚 reference/               # Reference materials
    └── original_blending/      # Original blending code (reference only)
```

## 🚀 **Quick Start**

### **1. Understanding the System**
Start with the documentation:
```bash
# Read project overview
cat docs/README.md

# Understand data relationships
cat docs/table_analysis/impression_vs_funnel_summary.md

# Learn about the blending algorithm
cat docs/algorithm_analysis/blending_algorithm_analysis.md
```

### **2. Setting Up Environment**
```bash
# Install dependencies
pip install snowflake-connector-python pandas numpy matplotlib pyspark

# Configure Snowflake connection (see setup guides)
cat docs/setup_guides/README_MCP_SNOWFLAKE_SERVER.md
```

### **3. Running Analysis**
```bash
# Table relationship analysis
python -m src.analysis.table_analysis.compare_table_fields

# Blending algorithm analysis (see reference/original_blending/)
```

## 🎯 **Key Findings**

### **📊 Data Architecture:**
- **`FACT_ADS_ITEM_REQUEST_FUNNEL`**: Essential bridge between auction decisions and user impressions
- **`fact_item_card_view_dedup`**: User experience data (positions, sessions, timing)
- **`bid_event_ice`**: Raw auction data (no direct impression link)

### **🔄 Blending Algorithm:**
- **Method**: Utility-based merge-sort with business constraints
- **Optimization**: Multi-objective (revenue + engagement)
- **Constraints**: Max consecutive ads, minimum NV content blocks
- **Performance**: DCG-style position-discounted utility calculation

## 📚 **Documentation Index**

| Topic | Location | Description |
|-------|----------|-------------|
| **Data Relationships** | `docs/table_analysis/` | Table schemas, joins, data flow |
| **Blending Algorithm** | `docs/algorithm_analysis/` | Algorithm mechanics and optimization |
| **Setup Guides** | `docs/setup_guides/` | Environment and tool configuration |
| **SQL Queries** | `sql/` | Database analysis and testing queries |
| **Source Code** | `src/` | Modular Python codebase |
| **Reference Materials** | `reference/` | Original implementations and historical context |

## 🛠️ **Development Workflow**

### **For Analysis:**
1. Check existing documentation in `docs/`
2. Use SQL queries from `sql/` for data exploration
3. Create analysis scripts in `src/analysis/`
4. Document findings in `docs/`

### **For Algorithm Development:**
1. Reference original code in `reference/original_blending/`
2. Create clean implementations in `src/algorithms/`
3. Add unit tests in `tests/`
4. Document in `docs/algorithm_analysis/`

### **For Data Pipeline Work:**
1. Analyze existing pipelines in `reference/original_blending/`
2. Create modular versions in `src/data_pipelines/`
3. Use SQL queries from `sql/` for validation
4. Document data flow in `docs/table_analysis/`

## 🎯 **Next Steps**

1. **Migration Planning**: Prepare for `ads_auction_candidates_event` transition
2. **Algorithm Optimization**: Test different parameter configurations
3. **Performance Monitoring**: Implement KPI tracking and alerting
4. **A/B Testing**: Design experiments for algorithm improvements

## 🤝 **Contributing**

- Follow the modular structure when adding new code
- Document all analysis and findings
- Create unit tests for new functionality
- Use clear, descriptive naming conventions

---

**Project Contact**: DoorDash Ads Team  
**Last Updated**: January 2025