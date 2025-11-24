-- ============================================================
-- E-COMMERCE ANALYTICS QUERIES
-- Performance-optimized queries for business intelligence
-- ============================================================

-- Query 1: Monthly Revenue Trend with YoY Growth
-- Purpose: Executive dashboard - track revenue over time
-- Performance: <0.5s with materialized view
SELECT 
    year,
    month,
    month_name,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY month ORDER BY year) as prev_year_revenue,
    ROUND(
        ((total_revenue - LAG(total_revenue) OVER (PARTITION BY month ORDER BY year)) 
        / LAG(total_revenue) OVER (PARTITION BY month ORDER BY year) * 100), 
        2
    ) as yoy_growth_pct
FROM mv_monthly_metrics
ORDER BY year, month;

-- Query 2: Top 20 Products by Revenue
-- Purpose: Product performance analysis
SELECT 
    p.stock_code,
    p.description,
    COUNT(DISTINCT t.invoice_no) as times_purchased,
    SUM(t.quantity) as total_quantity_sold,
    ROUND(SUM(t.total_amount), 2) as total_revenue,
    ROUND(AVG(t.total_amount), 2) as avg_sale_value,
    ROUND(SUM(t.total_amount) / SUM(SUM(t.total_amount)) OVER () * 100, 2) as pct_of_total_revenue
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id
GROUP BY p.stock_code, p.description
ORDER BY total_revenue DESC
LIMIT 20;

-- Query 3: Customer Segmentation by Lifetime Value (RFM approach)
-- Purpose: Marketing - identify VIP/at-risk customers
WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        c.country,
        c.lifetime_value,
        c.total_orders,
        (CURRENT_DATE - c.last_purchase_date) as days_since_last_purchase,
        CASE 
            WHEN c.lifetime_value > 5000 THEN 'VIP'
            WHEN c.lifetime_value > 2000 THEN 'High Value'
            WHEN c.lifetime_value > 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_segment,
        CASE 
            WHEN (CURRENT_DATE - c.last_purchase_date) <= 90 THEN 'Active'
            WHEN (CURRENT_DATE - c.last_purchase_date) <= 180 THEN 'At Risk'
            ELSE 'Churned'
        END as recency_segment
    FROM dim_customers c
)
SELECT 
    value_segment,
    recency_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(lifetime_value), 2) as avg_ltv,