# Query Optimization Deep Dive

## Problem Statement
Initial queries on 500K row dataset were taking 8-15 seconds. Unacceptable for dashboard experience.

## Root Cause Analysis

### Query 1: Product Revenue Analysis
```sql
SELECT 
    p.product_name,
    COUNT(*) as purchase_frequency,
    AVG(t.total_amount) as avg_sale_value
FROM transactions t
JOIN products p ON t.product_id = p.product_id
WHERE t.transaction_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY p.product_name
HAVING COUNT(*) > 5
ORDER BY purchase_frequency DESC;
```

**Original execution time:** 14.2 seconds
**Execution plan:** Full table scan on transactions table

### Solution: Strategic Indexing
```sql
-- Create index on join columns
CREATE INDEX idx_product_id ON transactions(product_id);

-- Create index on frequently filtered date column
CREATE INDEX idx_transaction_date ON transactions(transaction_date);

-- Create covering index for this specific query
CREATE INDEX idx_product_date_amount ON transactions(product_id, transaction_date, total_amount);
```

**New execution time:** 1.8 seconds
**Improvement:** 87% faster

## Indexing Strategy

### Index 1: Foreign Key Columns
```sql
CREATE INDEX idx_customer_id ON transactions(customer_id);
CREATE INDEX idx_product_id ON transactions(product_id);
```
**Benefit:** Speeds up JOIN operations

### Index 2: Date Range Queries
```sql
CREATE INDEX idx_transaction_date ON transactions(transaction_date);
```
**Benefit:** WHERE clauses with date filtering

### Index 3: Aggregation Columns
```sql
CREATE INDEX idx_tx_date_amount ON transactions(transaction_date, total_amount);
```
**Benefit:** GROUP BY and SUM operations

### Index 4: Dimension Lookups
```sql
CREATE INDEX idx_customer_country ON customers(country);
CREATE INDEX idx_product_category ON products(category_id);
```
**Benefit:** Dimension table filters

## Performance Benchmarks

| Query Type | Before | After | Improvement |
|-----------|--------|-------|------------|
| Simple aggregation | 3.5s | 0.4s | 88% |
| Multi-table join | 8.2s | 1.1s | 87% |
| Date range filter | 6.1s | 0.8s | 87% |
| Full dataset scan | 12.4s | 2.0s | 84% |

## Lessons for Production

1. **Don't optimize prematurely** - Identify actual bottlenecks
2. **Use EXPLAIN ANALYZE** - Understand execution plans
3. **Create indexes on JOIN columns** - Usually the biggest win
4. **Monitor index usage** - Remove unused indexes
5. **Analyze trade-offs** - Indexes speed reads but slow writes

## Monitoring Query Performance
```sql
-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Find missing indexes
SELECT 
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan
FROM pg_stat_user_tables
WHERE seq_scan > 1000
ORDER BY seq_tup_read DESC;
```

## Conclusion

Smart indexing transformed query performance from unacceptable (14s) to excellent (<2s). Key insight: Understand your access patterns before indexing.