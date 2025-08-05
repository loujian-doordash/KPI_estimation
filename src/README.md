# 🐍 Source Code

This folder contains all the source code for the KPI estimation project, organized in a modular structure.

## 📁 **Module Structure:**

### **🔄 `data_pipelines/`**
ETL processing pipelines:
- Data extraction from Snowflake tables
- Processing ad and non-video content data
- Data preparation for blending algorithms

### **🎯 `algorithms/`**
Core blending algorithms:
- Utility-based merge-sort blending
- Multi-objective optimization functions
- Algorithm configuration and parameters

### **📊 `analysis/`**
Research and analysis scripts:
- **`table_analysis/`** - Scripts for analyzing table relationships
- **`experiments/`** - Experimental analysis and A/B testing

### **🔧 `utils/`**
Shared utilities and helpers:
- Database connection utilities
- Data processing functions
- Common helper functions

### **⚙️ `config/`**
Configuration management:
- Database connection settings
- Algorithm parameters
- Environment configurations

## 🚀 **Getting Started:**

1. **Import modules** using the package structure:
   ```python
   from src.utils import snowflake_connection
   from src.algorithms import blending_algorithm
   ```

2. **Run analysis scripts** from the project root:
   ```bash
   python -m src.analysis.table_analysis.compare_table_fields
   ```

3. **Follow naming conventions**:
   - Use snake_case for files and functions
   - Use clear, descriptive names
   - Include docstrings for all modules

## 📋 **Code Standards:**

- Add docstrings to all functions and classes
- Use type hints where appropriate
- Follow PEP 8 style guidelines
- Include unit tests for new functionality