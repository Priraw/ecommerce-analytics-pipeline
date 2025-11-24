# Database Schema Design

## Overview
Star schema with 1 fact table and 3 dimension tables, optimized for analytical queries.

## Design Decisions

### Why Star Schema?
- Simple to understand and query
- Optimized for read-heavy analytics workloads
- Power BI performs best with star schemas
- Easy to extend with new dimensions

### Why Separate Date Dimension?
- Enables flexible time-based analysis (YoY, QoQ, MoM)
- Pre-computed date attributes (quarter, week, weekend flag)
- Standard practice in data warehousing

### Why Materialized View?
- Dashboard queries monthly metrics frequently
- Pre-computation improves response time 10x
- Refresh once after ETL run (not real-time data)

### Index Strategy
Primary indexes on:
- Foreign keys (customer_id, product_id, date_id) - JOIN performance
- invoice_date - WHERE clause filtering
- country - Geographic analysis
- stock_code - Product lookups

## Schema Diagram
dim_customers              dim_products
     -------------              ------------
     customer_id (PK)           product_id (PK)
     country                    stock_code
     first_purchase_date        description
     last_purchase_date         unit_price
     total_orders                    |
     lifetime_value                  |
            |                        |
            |                        |
     fact_transactions               |
     -----------------               |
     transaction_id (PK) ------------+
     invoice_no
     customer_id (FK) ---------------+
     product_id (FK)
     date_id (FK) -------------------+
     invoice_date                    |
     quantity                        |
     unit_price                      |
     total_amount                    |
                                dim_dates
                                ---------
                                date_id (PK)
                                full_date
                                year, quarter, month
                                week, day
                                is_weekend

## Performance Considerations
- Indexes: 7 strategic indexes created
- Generated column: total_amount auto-calculated (consistency)
- Materialized view: Monthly metrics pre-aggregated
- Expected query time: <2 seconds for complex aggregations