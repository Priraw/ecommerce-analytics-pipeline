# E-Commerce Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Contributions Welcome](https://img.shields.io/badge/Contributions-Welcome-brightgreen.svg)](CONTRIBUTING.md)

# E-Commerce Analytics Data Pipeline

**Status:** 🚀 Live & Deployed
## Dashboard Preview

📊 [View Full Dashboard (PDF)](https://github.com/Priraw/ecommerce-analytics-pipeline/blob/f50ae616a633b10c28cdaa711e91783d87d6ea31/ecommerce-analytics-dashboard.pdf)

### Page Previews:
![Executive Summary](screenshots-dashboard/page1_executive.png)
![Revenue Analysis](screenshots-dashboard/page2_revenue.png)

## Overview
End-to-end data pipeline processing e-commerce transactions, from raw data to analytics dashboard.

## Architecture

https://www.figma.com/design/QTEDsvcPt41HmJTUIkXsW5/Untitled?node-id=7-110&t=KkRBBiq7I1GhkIzf-1

- **Database:** PostgreSQL (star schema)
- **ETL:** Python (pandas, psycopg2)
- **Visualization:** Power BI
- **Data Quality:** 75.1% retention after cleaning

[See full documentation →](docs/architecture.md)

### Data Flow
1. Extract: Raw CSV → Python pandas
2. Transform: Validation, cleaning, feature engineering
3. Load: PostgreSQL star schema
4. Optimize: Indexes + materialized views
5. Visualize: Power BI dashboard

## Tech Stack
- **Data Processing:** Python (pandas, NumPy)
- **Database:** PostgreSQL
- **Analytics:** Power BI
- **Deployment:** [Details TBD]
- **Version Control:** Git

## 📊 Results

✅ **406,829 transactions** processed  
✅ **4,372 customers** analyzed  
✅ **3,684 products** cataloged  
✅ **$9.7M revenue** tracked  
✅ **87% query performance** improvement (15s → 2s)  


