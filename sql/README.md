# 🗃️ SQL Queries

This folder contains all SQL queries and database scripts, organized by purpose.

## 📁 **Folder Structure:**

### **📋 `schema_analysis/`**
SQL queries for analyzing table schemas and relationships:
- `01_check_table_schemas.sql` - Compare table schemas and field types
- `02_field_mapping_analysis.sql` - Analyze field mappings between tables
- `03_join_relationship_analysis.sql` - Understand table join relationships

### **🎯 `auction_analysis/`**
SQL queries for auction and bidding analysis:
- `04_new_auction_candidates_check.sql` - Check new auction candidates table readiness

### **🧪 `testing/`**
SQL queries for testing connections and basic functionality:
- `simple_test.sql` - Basic connectivity tests
- `test_snowflake_connection.py` - Python script for testing Snowflake connections

### **🔄 `etl_analysis/`**
SQL queries for analyzing ETL processes and data pipelines.

## 🚀 **Usage:**

### **Running SQL Files:**
1. **Via Snowflake UI**: Copy and paste queries into Snowflake worksheets
2. **Via Python scripts**: Use the connection utilities in `src/utils/`
3. **Via MCP server**: Use the configured Snowflake MCP server

### **File Naming Convention:**
- Use numbered prefixes for sequential analysis (01_, 02_, etc.)
- Use descriptive names that explain the query purpose
- Group related queries in the same subfolder

## 📊 **Query Organization:**

- **Schema queries** - Understanding table structures
- **Analysis queries** - Data exploration and insights
- **Testing queries** - Validation and connectivity
- **ETL queries** - Pipeline analysis and monitoring

## 🔗 **Integration:**

These SQL files are designed to work with:
- Snowflake data warehouse
- Python analysis scripts in `src/`
- Documentation in `docs/table_analysis/`