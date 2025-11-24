-- Drop tables if exist (for clean runs)
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;

-- Dimension: Customers
CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    country VARCHAR(50),
    first_purchase_date DATE,
    last_purchase_date DATE,
    total_orders INTEGER DEFAULT 0,
    lifetime_value DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Products
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date (for time-based analysis)
CREATE TABLE dim_dates (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Transactions
CREATE TABLE fact_transactions (
    transaction_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    date_id INTEGER REFERENCES dim_dates(date_id),
    invoice_date TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for query performance
CREATE INDEX idx_transactions_customer ON fact_transactions(customer_id);
CREATE INDEX idx_transactions_product ON fact_transactions(product_id);
CREATE INDEX idx_transactions_date ON fact_transactions(date_id);
CREATE INDEX idx_transactions_invoice_date ON fact_transactions(invoice_date);
CREATE INDEX idx_customers_country ON dim_customers(country);
CREATE INDEX idx_products_stock_code ON dim_products(stock_code);

-- Materialized view: Monthly metrics (pre-computed for dashboard)
CREATE MATERIALIZED VIEW mv_monthly_metrics AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT t.customer_id) as unique_customers,
    COUNT(DISTINCT t.invoice_no) as total_orders,
    COUNT(*) as total_items,
    SUM(t.total_amount) as total_revenue,
    AVG(t.total_amount) as avg_transaction_value,
    SUM(t.quantity) as total_quantity
FROM fact_transactions t
JOIN dim_dates d ON t.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Create index on materialized view
CREATE INDEX idx_mv_monthly_year_month ON mv_monthly_metrics(year, month);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_monthly_metrics()
RETURNS void AS $
BEGIN
    REFRESH MATERIALIZED VIEW mv_monthly_metrics;
END;
$ LANGUAGE plpgsql;

COMMENT ON TABLE dim_customers IS 'Customer dimension with aggregated metrics';
COMMENT ON TABLE dim_products IS 'Product catalog dimension';
COMMENT ON TABLE dim_dates IS 'Date dimension for time-based analysis';
COMMENT ON TABLE fact_transactions IS 'Transaction fact table (grain: one row per product per invoice)';
COMMENT ON MATERIALIZED VIEW mv_monthly_metrics IS 'Pre-aggregated monthly metrics for dashboard performance';