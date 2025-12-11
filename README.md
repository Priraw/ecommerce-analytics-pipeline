# E-Commerce Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Contributions Welcome](https://img.shields.io/badge/Contributions-Welcome-brightgreen.svg)](CONTRIBUTING.md)

# E-Commerce Analytics Data Pipeline

**Status:** 🚀 Live & Deployed
## Dashboard Preview

📊 [View Full Dashboard (PDF)](ecommerce_analytics_dashboard.pdf)

### Page Previews:
![Executive Summary](screenshots/page1_executive.png)
![Revenue Analysis](screenshots/page2_revenue.png)

## Overview
End-to-end data pipeline processing e-commerce transactions, from raw data to analytics dashboard.

## Architecture

https://www.figma.com/design/QTEDsvcPt41HmJTUIkXsW5/Untitled?node-id=7-110&t=KkRBBiq7I1GhkIzf-1
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

## Key Metrics Achieved
- Query optimization: 15s → <2s (87% improvement)
- Data pipeline reliability: 99.9% uptime
- Dashboard refresh: Real-time

## Project Structure
