# 📁 KPI Estimation Project Reorganization Plan

## 🎯 **Current Issues:**
1. **Root folder cluttered** with many analysis scripts and documentation files
2. **Mixed file types** - Python scripts, SQL queries, and documentation all in root
3. **Unclear purpose** of some files without proper grouping
4. **Inconsistent naming** - some files have spaces, others use underscores
5. **No clear separation** between different types of analysis

## 🏗️ **Proposed New Structure:**

```
KPI_estimation/
├── README.md                           # Main project overview
├── .gitignore
│
├── docs/                               # 📖 All documentation
│   ├── README.md                       # Documentation index
│   ├── table_analysis/                 # Table relationship analysis docs
│   │   ├── impression_vs_funnel_summary.md
│   │   ├── bid_event_impression_relationship_summary.md
│   │   ├── table_comparison_summary.md
│   │   └── README.md
│   ├── algorithm_analysis/             # Algorithm documentation
│   │   ├── blending_algorithm_analysis.md
│   │   └── README.md
│   ├── setup_guides/                   # Setup and configuration guides
│   │   ├── README_MCP_SNOWFLAKE_SERVER.md
│   │   ├── slack_questions.md
│   │   └── README.md
│   └── experiments/                    # Experimental analysis docs
│       └── README.md
│
├── src/                               # 🐍 Source code and scripts
│   ├── data_pipelines/                # Data processing pipelines
│   │   ├── __init__.py
│   │   ├── ad_data_pipeline.py
│   │   ├── nv_data_pipeline.py
│   │   ├── ad_nv_data_matching.py
│   │   └── README.md
│   ├── algorithms/                    # Core algorithms
│   │   ├── __init__.py
│   │   ├── blending_algorithm.py
│   │   └── README.md
│   ├── analysis/                      # Analysis and research scripts
│   │   ├── __init__.py
│   │   ├── table_analysis/
│   │   │   ├── __init__.py
│   │   │   ├── compare_table_fields.py
│   │   │   ├── simple_table_comparison.py
│   │   │   ├── bid_event_impressions_analysis.py
│   │   │   ├── simple_impression_connection.py
│   │   │   └── README.md
│   │   ├── experiments/
│   │   │   ├── __init__.py
│   │   │   ├── impression_diff_deep_dive.py
│   │   │   ├── check_missing_auctions.py
│   │   │   └── README.md
│   │   └── README.md
│   ├── utils/                         # Utility functions and helpers
│   │   ├── __init__.py
│   │   ├── snowflake_connection.py
│   │   ├── data_processing.py
│   │   └── README.md
│   └── config/                        # Configuration files
│       ├── __init__.py
│       ├── snowflake_config.py
│       └── README.md
│
├── sql/                               # 🗃️ SQL queries and database scripts
│   ├── README.md                      # SQL documentation index
│   ├── schema_analysis/               # Table schema and relationship queries
│   │   ├── 01_check_table_schemas.sql
│   │   ├── 02_field_mapping_analysis.sql
│   │   ├── 03_join_relationship_analysis.sql
│   │   └── README.md
│   ├── auction_analysis/              # Auction and bidding queries
│   │   ├── 04_new_auction_candidates_check.sql
│   │   └── README.md
│   ├── testing/                       # Test queries and connections
│   │   ├── simple_test.sql
│   │   ├── test_snowflake_connection.py
│   │   └── README.md
│   └── etl_analysis/                  # ETL and data pipeline queries
│       └── README.md
│
├── tests/                             # 🧪 Test files
│   ├── __init__.py
│   ├── test_snowflake_direct.py
│   ├── test_data_pipelines.py
│   ├── test_algorithms.py
│   └── README.md
│
├── scripts/                           # 🔧 Utility and setup scripts
│   ├── setup_environment.py
│   ├── run_analysis_queries.py
│   ├── investigate_etl_relationship.py
│   └── README.md
│
├── notebooks/                         # 📓 Jupyter notebooks (if any)
│   └── README.md
│
├── data/                              # 📊 Local data files (if any)
│   ├── samples/
│   ├── outputs/
│   └── README.md
│
└── reference/                         # 📚 Reference materials
    ├── original_blending/             # Original blending code (moved from blending/)
    │   ├── README.md                  # Note: Reference materials only
    │   ├── blending_algorithm_analysis_dedup.py
    │   ├── ad_data_pipeline.py
    │   ├── nv_data_pipeline.py
    │   └── ad_nv_data_matching.py
    └── README.md
```

## 🔄 **Migration Benefits:**

### **1. Clear Separation of Concerns:**
- **`docs/`**: All documentation in one place
- **`src/`**: Clean, modular source code
- **`sql/`**: All SQL queries organized by purpose
- **`tests/`**: Proper testing structure
- **`reference/`**: Historical/reference materials

### **2. Professional Structure:**
- **Python package structure** with `__init__.py` files
- **Proper module organization** for imports
- **Clear naming conventions** (snake_case, no spaces)
- **Logical grouping** by functionality

### **3. Scalability:**
- **Easy to add new analysis** types
- **Modular code** for reusability
- **Clear documentation** structure
- **Proper testing** framework

### **4. Team Collaboration:**
- **Clear project structure** for new team members
- **Organized documentation** easy to navigate
- **Separated concerns** reduce conflicts
- **Professional appearance** for stakeholders

## 🚀 **Implementation Steps:**

1. **Create new folder structure**
2. **Move files to appropriate locations**
3. **Rename files for consistency**
4. **Create README files for each section**
5. **Update import statements**
6. **Create proper Python modules**
7. **Update main README with new structure**
8. **Test all functionality still works**

## 📝 **File Mapping:**

### **Current → New Location:**
- `blending_algorithm_analysis.md` → `docs/algorithm_analysis/`
- `compare_table_fields.py` → `src/analysis/table_analysis/`
- `queries/*.sql` → `sql/schema_analysis/` or `sql/auction_analysis/`
- `blending/*.py` → `reference/original_blending/` (reference) + `src/` (cleaned versions)
- Analysis scripts → `src/analysis/`
- Test scripts → `tests/`
- Setup scripts → `scripts/`

This reorganization will make your project much more professional, maintainable, and easier to understand for both you and any future collaborators!