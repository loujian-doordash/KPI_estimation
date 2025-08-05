# 📚 Blending Algorithms Reference

This folder contains reference blending algorithms that provide insights for developing KPI estimation models.

## 🎯 **Purpose for KPI Estimation:**

The blending algorithms serve as **reference material** for understanding:
- Multi-objective optimization patterns
- Utility function design
- Position and context effects
- Business constraint handling

These insights inform the architecture and approach for KPI estimation models.

## 📁 **Reference Materials:**

### **Core Blending Algorithm:**
- `Blending algorithm & analysis (dedup).py` - Utility-based blending algorithm
- `ad data pipeline.py` - Ad data processing pipeline
- `nv data pipeline.py` - Non-video content processing pipeline
- `ad nv data matching.py` - Data matching and preparation

## 🔍 **Key Insights for KPI Models:**

### **1. Multi-Objective Optimization:**
```python
# From blending algorithm - useful for KPI prediction
ad_utility = expected_revenue + ALPHA * engagement_score
```
**KPI Modeling Insight**: Shows how to combine multiple objectives (revenue, engagement) in prediction models

### **2. Position Effects:**
```python
# DCG-style utility calculation
utility = signal_value / log2(position + 2)
```
**KPI Modeling Insight**: Position has exponential impact on performance - important feature for KPI estimation

### **3. Business Constraints:**
- Maximum consecutive ads (user experience)
- Minimum content diversity requirements
- Deduplication logic

**KPI Modeling Insight**: Real-world constraints must be incorporated into KPI prediction models

### **4. Utility Functions:**
- Revenue utility: `expected_value / log2(position + 2)`
- Engagement utility: `quality_score / log2(position + 2)`

**KPI Modeling Insight**: Shows how to design utility functions that capture business value

## 🎯 **Application to KPI Estimation:**

### **Feature Engineering:**
- Use position discounting patterns
- Incorporate multi-objective thinking
- Consider business constraints in features

### **Model Architecture:**
- Multi-task learning for related KPIs
- Utility-based loss functions
- Constraint-aware prediction

### **Evaluation:**
- Business-aligned metrics
- Position-aware evaluation
- Multi-objective performance assessment

## ⚠️ **Important Note:**
These algorithms are **reference material only**. The actual development of KPI estimation models will be done in the `src/models/` folder following proper software engineering practices.

## 🔄 **Usage Pattern:**
1. **Study** reference algorithms for insights
2. **Extract** relevant patterns and principles
3. **Implement** clean KPI estimation models in `src/`
4. **Document** insights and decisions in `docs/kpi_modeling/`