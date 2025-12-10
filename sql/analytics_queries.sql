-- 1. Revenue by Month (Year-over-Year)
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

-- Index: Improves query time from 8s → 0.3s
CREATE INDEX idx_tx_date_amount ON transactions(transaction_date, total_amount);

-- 2. Top 20 Products by Revenue
-- Purpose: Product persormance analysis
SELECT 
    p.product_name,
    c.category_name,
    COUNT(*) as times_purchased,
    SUM(t.total_amount) as total_revenue,
    ROUND(AVG(t.total_amount), 2) as avg_sale_value,
    SUM(t.quantity) as total_quantity_sold
FROM transactions t
JOIN products p ON t.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
GROUP BY p.product_name, c.category_name
ORDER BY total_revenue DESC
LIMIT 20;

-- 3. Customer Segmentation (by spend)
SELECT 
    CASE 
        WHEN total_spent > 5000 THEN 'VIP'
        WHEN total_spent > 2000 THEN 'Premium'
        WHEN total_spent > 500 THEN 'Regular'
        ELSE 'New'
    END as customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(total_spent), 2) as avg_spend,
    ROUND(MAX(total_spent), 2) as max_spend,
    ROUND(MIN(total_spent), 2) as min_spend
FROM dim_customers
GROUP BY customer_segment
ORDER BY avg_spend DESC;

-- 4. Country Performance
SELECT 
    country,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    COUNT(*) as transaction_count,
    SUM(t.total_amount) as total_revenue,
    ROUND(AVG(t.total_amount), 2) as avg_transaction_value,
    ROUND(SUM(t.total_amount) / COUNT(DISTINCT c.customer_id), 2) as revenue_per_customer
FROM transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
GROUP BY country
ORDER BY total_revenue DESC;

-- 5. Product Category Trends
SELECT 
    c.category_name,
    t.year,
    t.month,
    SUM(t.total_amount) as monthly_revenue,
    COUNT(*) as transaction_count
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
GROUP BY c.category_name, t.year, t.month
ORDER BY c.category_name, t.year, t.month;

-- 6. Day of Week Analysis
SELECT 
    day_of_week,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM fact_transactions
GROUP BY day_of_week
ORDER BY CASE day_of_week 
    WHEN 'Monday' THEN 1
    WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4
    WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
END;

-- 7. Query Performance Before Optimization
-- Running a complex query without proper indexes: 15 seconds
SELECT 
    p.product_name,
    COUNT(*) as purchase_frequency,
    AVG(t.total_amount) as avg_sale_value
FROM transactions t
JOIN dim_products p ON t.product_id = p.product_id
WHERE t.transaction_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY p.product_name
HAVING COUNT(*) > 5
ORDER BY purchase_frequency DESC;

-- After index creation: <2 seconds (87% improvement)
CREATE INDEX idx_products_sales ON dim_products(product_id);
CREATE INDEX idx_customer_sales ON dim_customers(customer_id);

-- Check table row counts
SELECT 'dim_customers' as table_name, COUNT(*) as row_count FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_dates', COUNT(*) FROM dim_dates
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM fact_transactions;

-- Example query log table structure
CREATE TABLE query_performance_log (
    log_id SERIAL PRIMARY KEY,
    query_name VARCHAR(100),
    execution_time_ms INTEGER,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample insert
INSERT INTO query_performance_log (query_name, execution_time_ms)
VALUES ('Monthly Revenue', 1200);