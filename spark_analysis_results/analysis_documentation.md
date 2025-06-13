
# Assignment Day 21 - Data Analysis with PySpark
**Analysis Date:** 2025-06-13 14:35:22

## Summary

This analysis examines airline customer behavior using PySpark to process and analyze large datasets containing customer demographics, flight activity, and loyalty program information.

### Key Findings:
1. **Average Flights per Customer per Year:** 30.40 flights
2. **Customer Segmentation:** Analysis across 3 loyalty card tiers
3. **Education Impact:** Clear correlation between education level and travel behavior
4. **Temporal Trends:** Seasonal and yearly patterns identified in flight activity

## Dataset Overview

### Data Sources:
- **Calendar Dataset:** 2,557 records with date hierarchies
- **Flight Activity Dataset:** 389,065 flight records after cleaning
- **Customer Loyalty Dataset:** 16,737 unique customer profiles

### Data Quality:
- Missing values handled through median imputation and business logic
- Duplicate records identified and aggregated appropriately
- Data types validated and optimized for analysis

## Analysis Methodology

### 1. Data Preprocessing
- **Missing Value Treatment:** Filled missing points_accumulated values with 0.0
- **Outlier Detection:** Salary values imputed with median ($73,262.00)
- **Duplicate Handling:** Aggregated duplicate flight records by customer-month

### 2. Feature Engineering
- Created customer lifetime value metrics
- Calculated flight frequency categories
- Developed customer value segments based on salary
- Generated points utilization rates

### 3. SQL Analysis
Four main business questions addressed:
- Average customer flight frequency
- Loyalty program effectiveness
- Education level impact on travel behavior
- Temporal trend analysis

### 4. Visualization
- Monthly flight trends over time
- Loyalty points distribution by card type
- Education level analysis across multiple dimensions

## Business Insights

### Customer Segmentation
- **High-value customers** show consistent flight patterns
- **Education level** correlates with travel frequency and spending
- **Loyalty program** effectiveness varies significantly by card tier

### Temporal Patterns
- **Seasonal variations** in flight activity identified
- **Year-over-year growth** patterns analyzed
- **Monthly customer engagement** tracked

## Technical Implementation

### Technologies Used:
- **Apache Spark** for distributed data processing
- **PySpark SQL** for complex analytical queries
- **Python ecosystem** (pandas, matplotlib, seaborn) for visualization
- **Multiple export formats** (CSV, Parquet, JSON) for downstream use

### Performance Optimizations:
- Adaptive query execution enabled
- Partition coalescing for optimal file sizes
- Kryo serialization for improved performance
- Arrow-based columnar processing

## Output Files Generated

### CSV Exports:
- `customer_summary.csv` - Customer-level aggregated metrics
- `monthly_trends.csv` - Time-series analysis data
- `loyalty_card_performance.csv` - Loyalty program effectiveness metrics

### Parquet Files:
- `customer_flight_complete.parquet` - Complete joined dataset
- `customer_flight_enhanced.parquet` - Enhanced with calculated columns

### Visualizations:
- `monthly_trends.png` - Time series analysis charts
- `loyalty_points.png` - Loyalty program analysis
- `education_analysis.png` - Education level impact analysis

## Recommendations

### Business Strategy:
1. **Focus on high-value segments** showing strong engagement patterns
2. **Optimize loyalty program** based on card-tier performance analysis
3. **Leverage seasonal patterns** for capacity planning and marketing
4. **Education-based targeting** for personalized marketing campaigns

## Data Governance

### Privacy and Security:
- Customer data handled according to privacy regulations
- No personally identifiable information exposed in analysis
- Aggregated results only shared in business reports

### Data Lineage:
- Complete audit trail of data transformations
- Reproducible analysis pipeline
- Version control for analytical code

---
**Analysis completed on:** 2025-06-13 at 14:35:27
**Total processing time:** Approximately 10-15 minutes
