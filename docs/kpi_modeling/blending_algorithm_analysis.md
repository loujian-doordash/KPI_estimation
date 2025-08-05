# 🎯 Blending Algorithm Analysis

## 📋 **Overview**
This is a **utility-based content blending algorithm** that optimally mixes sponsored ads and non-video (NV) content to maximize both user engagement and revenue.

## ⚙️ **Algorithm Configuration**
```python
K = 12                    # Top-K positions to optimize
ALPHA = 20               # Weight for ad engagement utility
BETA = ALPHA (20)        # Weight for NV engagement utility
MAX_ADS_BLOCK_SIZE = 3   # Maximum consecutive ads allowed
MIN_NV_BLOCK_SIZE = 2    # Minimum consecutive NV content required
```

## 🔄 **How the Algorithm Works**

### **Step 1: Data Preparation & Unification**
```python
# Input: Two separate ranked lists
- Ad items: [quality_scores, expected_values, card_positions, bms_ids]
- NV items: [pctr_scores, card_positions, bms_ids]

# Unification: Combine into single utility-ranked list
zipped_struct = arrays_zip(
    positions: concat(ad_positions, nv_positions),
    engagement: concat(ad_quality_scores, nv_pctr),
    revenue: concat(ad_expected_values, nv_zeros)  # NV has 0 revenue
)
```

### **Step 2: Utility Calculation (Discounted DCG)**
```python
# For each item in top-K positions:
utility = signal_value / log2(position + 2)

# Total utilities:
engagement_utility = Σ(engagement_score / log2(i + 2))  # i = position
revenue_utility = Σ(expected_value / log2(i + 2))
```

### **Step 3: Merge-Sort Blending with Business Constraints**

#### **Core Blending Logic:**
```python
def compare_ads_to_nv(ad_item, nv_item):
    ad_utility = ad_expected_revenue + ALPHA * ad_engagement
    nv_utility = BETA * nv_engagement
    return ad_utility > nv_utility  # True = place ad, False = place NV
```

#### **Business Constraints:**
1. **Deduplication**: Same `bms_id` can't appear twice
2. **Max ads block**: No more than 3 consecutive ads
3. **Min NV block**: Must have at least 2 consecutive NV items after ads
4. **Forced NV insertion**: After max ads block, must insert NV content

### **Step 4: Merge Algorithm Flow**
```python
while items_remaining:
    # Deduplication check
    if current_item.bms_id in placed_items:
        skip_item()
        continue
    
    # Force NV insertion if needed
    if must_insert_nv:
        insert_nv_items(min_nv_block_size)
        reset_counters()
    
    # Compare utilities and place best item
    if ad_utility > nv_utility:
        if consecutive_ads < max_ads_block_size:
            place_ad()
            consecutive_ads += 1
        else:
            must_insert_nv = True  # Force NV block
    else:
        place_nv()
        consecutive_ads = 0  # Reset ad counter
```

## 📊 **Algorithm Outputs**

### **Blended Result Structure:**
```python
merged_result = [
    {
        "source": "ads" | "nv",
        "revenue_signal": float,      # Expected revenue (0 for NV)
        "engagement_signal": float,   # Quality score or pCTR
        "index": int,                # Original position in source list
        "original_card_position": int # Original card position
    }
]
```

### **Performance Metrics:**
1. **Ads Load Top-K**: Number of ads in top-K positions
2. **Sum Engagement Utility**: Discounted sum of engagement scores
3. **Sum Revenue Utility**: Discounted sum of expected revenue
4. **Ad Position Distribution**: Histogram of where ads appear

## 🎯 **Key Algorithm Features**

### **1. Utility-Based Ranking**
- **Revenue optimization**: `expected_value / log2(position + 2)`
- **Engagement optimization**: `quality_score / log2(position + 2)`
- **Position discount**: Earlier positions weighted more heavily

### **2. Multi-Objective Optimization**
```python
# Ad utility = Revenue component + Engagement component
ad_utility = expected_revenue + ALPHA * engagement_score
nv_utility = BETA * engagement_score
```

### **3. Business Logic Constraints**
- **User experience**: Prevent ad spam with max consecutive ads
- **Content diversity**: Ensure minimum NV content blocks
- **Deduplication**: Avoid showing same item multiple times

### **4. Flexible Configuration**
- **ALPHA/BETA**: Control revenue vs engagement tradeoff
- **Block sizes**: Adjust ad density and content diversity
- **K**: Change optimization window size

## 📈 **Performance Analysis**

### **Comparison Metrics:**
- **Fixed-slot blending**: Original position-based ranking
- **Utility-based blending**: Algorithm output
- **Metrics compared**: Ads load, engagement utility, revenue utility

### **Visualizations:**
1. **Ad Position Histograms**: Where ads actually appear
2. **Utility Distributions**: Performance metric comparisons
3. **Statistical Analysis**: Mean, median, quantiles

## 💡 **Algorithm Strengths**

1. **Mathematically principled**: Uses DCG-style utility functions
2. **Business-aware**: Incorporates real-world constraints
3. **Flexible**: Configurable parameters for different strategies
4. **Deduplication**: Prevents showing duplicate items
5. **Multi-objective**: Balances revenue and engagement

## 🔧 **Key Parameters Impact**

- **Higher ALPHA**: More weight on ad engagement → better user experience
- **Higher MAX_ADS_BLOCK_SIZE**: More consecutive ads → higher revenue potential
- **Higher MIN_NV_BLOCK_SIZE**: More diverse content → better user experience
- **Higher K**: Optimize over more positions → broader impact

## 📊 **Input Data Requirements**

From your data pipeline analysis, the algorithm needs:
- **Ad data**: Quality scores, expected values, positions (from request_funnel)
- **NV data**: pCTR scores, positions
- **User context**: Session IDs, card positions (from impression table)
- **Deduplication**: Business merchant IDs for both ad and NV content

This algorithm effectively balances revenue optimization with user experience while respecting business constraints and content diversity requirements.