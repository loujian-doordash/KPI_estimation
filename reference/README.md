# 📚 Reference Materials

This folder contains reference materials and historical code from the project's development.

## 📁 **Contents:**

### **📂 `original_blending/`**
**Purpose**: Reference copy of the original blending algorithm and data pipelines

**Note**: These files are for reference only. The active, clean versions of this code should be developed in the `src/` folder following proper software engineering practices.

**Contents**:
- Original Databricks notebook with blending algorithm
- Legacy data pipeline scripts
- Original documentation and analysis

## 🎯 **Usage Guidelines:**

### **✅ Use this folder for:**
- Understanding the original implementation
- Extracting logic for new clean implementations
- Historical context and algorithm evolution
- Comparing old vs new approaches

### **❌ Don't use this folder for:**
- Active development (use `src/` instead)
- Production code (create clean versions in `src/`)
- New features (develop in appropriate `src/` modules)

## 🔄 **Migration Strategy:**

When working with reference materials:

1. **Extract the logic** from reference files
2. **Create clean implementations** in `src/` modules
3. **Add proper documentation** and type hints
4. **Write unit tests** for new implementations
5. **Keep reference files** for historical context

## 📝 **Documentation:**

- Reference materials should remain as-is for historical accuracy
- New implementations should be documented in `docs/`
- Changes and improvements should be tracked via git commits