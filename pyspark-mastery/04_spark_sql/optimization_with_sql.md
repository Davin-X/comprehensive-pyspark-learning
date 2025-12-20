# 🚀 SQL Query Optimization: Performance Tuning Techniques

## 🎯 Overview

**Spark SQL optimization** focuses on leveraging the Catalyst optimizer and Tungsten execution engine to improve query performance. Understanding optimization techniques is crucial for writing efficient SQL queries in production environments.

## 🔍 Understanding the Catalyst Optimizer

### Query Optimization Pipeline

```
SQL Query → Parser → Analyzer → Optimizer → Physical Plan → Execution
    ↓         ↓         ↓          ↓            ↓            ↓
Parsing → Semantic Analysis → Logical Opt. → Physical Opt. → Tungsten → Results
```

**Catalyst performs:**
- **Syntax validation** and parsing
- **Semantic analysis** with catalog lookup
- **Logical optimization** (predicate pushdown, constant folding)
- **Physical planning** (join strategies, data exchange)
- **Cost-based optimization** for execution planning

## 🎯 Logical Optimizations

### 1. **Predicate Pushdown**

**Push filters as early as possible in the query plan**

```sql
-- ❌ Bad: Filter applied after join
SELECT c.name, o.total
FROM customers c
JOIN (
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id
WHERE c.region = 'US';

-- ✅ Good: Filter pushed down to source
SELECT c.name, o.total
FROM customers c
JOIN (
    SELECT customer_id, SUM(amount) as total
    FROM orders
    WHERE customer_id IN (SELECT customer_id FROM customers WHERE region = 'US')
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id
WHERE c.region = 'US';
```

**Benefits:**
- Reduces data shuffled across network
- Minimizes I/O operations
- Improves overall query performance

### 2. **Projection Pushdown**

**Select only required columns early in processing**

```sql
-- ❌ Bad: Select all columns, then filter
SELECT name, salary
FROM (
    SELECT e.*, d.department_name, d.location
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id
) full_data
WHERE salary > 50000;

-- ✅ Good: Project only needed columns
SELECT e.name, e.salary
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 50000;
```

### 3. **Constant Folding**

**Pre-compute constant expressions**

```sql
-- Catalyst automatically optimizes these:
SELECT salary * 1.1 as bonus FROM employees;  -- Constant multiplication
SELECT '2023-01-01' as start_date FROM orders; -- Constant strings
SELECT 100 + 200 as total FROM products;     -- Constant arithmetic
```

### 4. **Subquery Optimization**

**Use appropriate subquery patterns**

```sql
-- ✅ Efficient: IN subquery with small result set
SELECT name, salary
FROM employees
WHERE dept_id IN (
    SELECT dept_id FROM departments WHERE location = 'NY'
);

-- ✅ Efficient: EXISTS for existence checks
SELECT dept_name
FROM departments d
WHERE EXISTS (
    SELECT 1 FROM employees e
    WHERE e.dept_id = d.dept_id AND e.salary > 100000
);

-- ❌ Inefficient: NOT IN with NULL values
SELECT name FROM employees
WHERE dept_id NOT IN (SELECT dept_id FROM departments WHERE budget < 10000);
-- NULL values make this inefficient
```

## 🔗 Join Optimizations

### 1. **Join Reordering**

**Catalyst automatically reorders joins for optimal performance**

```sql
-- Catalyst may reorder this for better performance:
SELECT *
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id;

-- Optimized execution might be:
-- 1. Filter smallest tables first
-- 2. Join on selective conditions
-- 3. Minimize intermediate result sizes
```

### 2. **Broadcast Join Hints**

**Force broadcast joins for small tables**

```sql
-- Force broadcast join
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table l
JOIN small_table s ON l.key = s.key;

-- Multiple broadcast hints
SELECT /*+ BROADCAST(t1), BROADCAST(t2) */ *
FROM large_table l
JOIN small_table t1 ON l.key1 = t1.key
JOIN medium_table t2 ON l.key2 = t2.key;
```

### 3. **Join Type Selection**

**Choose optimal join strategies**

```sql
-- Broadcast join (small table)
SELECT /*+ BROADCAST(departments) */ *
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

-- Sort-merge join (large sorted tables)
SELECT /*+ MERGE(employees, orders) */ *
FROM employees e
JOIN orders o ON e.emp_id = o.sales_rep_id
WHERE e.hire_date >= '2020-01-01';

-- Shuffle hash join (default for large tables)
SELECT /*+ SHUFFLE_HASH(employees) */ *
FROM employees e
JOIN salaries s ON e.emp_id = s.emp_id;
```

## 📊 Aggregation Optimizations

### 1. **Partial Aggregation**

**Aggregate early to reduce data volume**

```sql
-- ✅ Good: Aggregate before join
SELECT d.dept_name, dept_stats.avg_salary
FROM departments d
JOIN (
    SELECT dept_id, AVG(salary) as avg_salary, COUNT(*) as emp_count
    FROM employees
    GROUP BY dept_id
    HAVING COUNT(*) > 5  -- Filter early
) dept_stats ON d.dept_id = dept_stats.dept_id;

-- ❌ Bad: Join then aggregate (moves more data)
SELECT d.dept_name, AVG(e.salary) as avg_salary
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
GROUP BY d.dept_name;
```

### 2. **Distinct Optimization**

**Use appropriate distinct patterns**

```sql
-- ✅ Efficient: DISTINCT on indexed columns
SELECT DISTINCT dept_id, location
FROM employees
WHERE salary > 50000;

-- ✅ Efficient: GROUP BY for aggregations
SELECT dept_id, COUNT(*), AVG(salary)
FROM employees
GROUP BY dept_id;

-- ❌ Inefficient: DISTINCT on large result sets
SELECT DISTINCT *
FROM orders
WHERE order_date >= '2020-01-01';  -- May return millions of rows
```

## 🗂️ Partitioning and Bucketing

### 1. **Partition Pruning**

**Leverage partitioned tables for query optimization**

```sql
-- Partitioned table automatically prunes irrelevant partitions
SELECT *
FROM sales_partitioned
WHERE sale_date >= '2023-01-01' AND sale_date < '2023-02-01';
-- Only reads January 2023 partition

-- Multiple partition columns
SELECT SUM(amount)
FROM sales_partitioned
WHERE year = 2023 AND month = 1 AND region = 'US';
-- Reads only specific partition
```

### 2. **Bucketing for Joins**

**Pre-shuffle data to optimize join performance**

```sql
-- Create bucketed tables
CREATE TABLE employees_bucketed (
    emp_id INT,
    name STRING,
    dept_id INT,
    salary DOUBLE
)
CLUSTERED BY (dept_id) INTO 8 BUCKETS;

-- Join on bucketed column (no shuffle needed)
SELECT e.name, d.dept_name
FROM employees_bucketed e
JOIN departments_bucketed d ON e.dept_id = d.dept_id;
-- Data co-located, minimal shuffling
```

## ⚡ Execution Optimizations

### 1. **Adaptive Query Execution**

**Dynamically optimize query execution**

```sql
-- Enable adaptive query execution
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;

-- AQE automatically handles:
-- - Dynamic partition coalescing
-- - Skew join optimization
-- - Join strategy switching
```

### 2. **Cost-Based Optimization**

**Leverage statistics for optimal plans**

```sql
-- Collect table statistics
ANALYZE TABLE employees COMPUTE STATISTICS;
ANALYZE TABLE employees COMPUTE STATISTICS FOR COLUMNS dept_id, salary;

-- CBO uses statistics for:
-- - Join order optimization
-- - Filter selectivity estimation
-- - Broadcast join decisions
```

### 3. **Query Result Caching**

**Cache expensive query results**

```sql
-- Cache query results
CREATE TEMP VIEW expensive_calculation AS
SELECT /*+ CACHE */ 
    dept_id,
    AVG(salary) as avg_salary,
    STDDEV(salary) as salary_stddev
FROM employees
GROUP BY dept_id;

-- Subsequent queries on cached data are faster
SELECT * FROM expensive_calculation WHERE avg_salary > 60000;
```

## 🎯 Query Tuning Techniques

### 1. **EXPLAIN Plan Analysis**

**Always analyze query execution plans**

```sql
-- Get detailed execution plan
EXPLAIN EXTENDED SELECT *
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 50000;

-- Look for:
-- - BroadcastHashJoin (good for small tables)
-- - SortMergeJoin (good for large tables)
-- - Exchange (shuffle) operations
-- - Filter pushdown indicators
```

### 2. **Predicate Optimization**

**Optimize WHERE clause conditions**

```sql
-- ✅ Good: SARGable predicates
SELECT * FROM employees
WHERE salary > 50000
  AND hire_date >= '2020-01-01'
  AND dept_id IN (1, 2, 3);

-- ❌ Bad: Non-SARGable predicates
SELECT * FROM employees
WHERE YEAR(hire_date) = 2023;  -- Function on column prevents index use

-- ✅ Better: SARGable equivalent
SELECT * FROM employees
WHERE hire_date >= '2023-01-01' AND hire_date < '2024-01-01';
```

### 3. **Subquery vs Join Performance**

**Choose appropriate data access patterns**

```sql
-- Test different approaches for same result
-- Approach 1: Subquery
SELECT dept_name
FROM departments
WHERE dept_id IN (
    SELECT dept_id FROM employees
    WHERE salary > (SELECT AVG(salary) FROM employees)
);

-- Approach 2: Join
SELECT DISTINCT d.dept_name
FROM departments d
JOIN employees e ON d.dept_id = e.dept_id
JOIN (SELECT AVG(salary) as avg_sal FROM employees) stats ON e.salary > stats.avg_sal;

-- Approach 3: CTE
WITH avg_salary AS (SELECT AVG(salary) as avg_sal FROM employees)
SELECT DISTINCT d.dept_name
FROM departments d
JOIN employees e ON d.dept_id = e.dept_id
JOIN avg_salary a ON e.salary > a.avg_sal;
```

## 📈 Advanced Optimization Patterns

### 1. **Query Decomposition**

**Break complex queries into optimized parts**

```sql
-- Complex query decomposition
WITH filtered_employees AS (
    SELECT /*+ CACHE */ emp_id, dept_id, salary
    FROM employees
    WHERE salary > 50000 AND hire_date >= '2020-01-01'
),
department_stats AS (
    SELECT /*+ CACHE */ dept_id, COUNT(*) as emp_count, AVG(salary) as avg_salary
    FROM filtered_employees
    GROUP BY dept_id
    HAVING COUNT(*) >= 5
)
SELECT d.dept_name, ds.avg_salary, ds.emp_count
FROM departments d
JOIN department_stats ds ON d.dept_id = ds.dept_id
ORDER BY ds.avg_salary DESC;
```

### 2. **Materialized View Pattern**

**Pre-compute expensive aggregations**

```sql
-- Create materialized view equivalent
CREATE TEMP VIEW monthly_sales_summary AS
SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') as month,
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_order_value,
    MAX(amount) as largest_order
FROM orders
WHERE order_date >= '2023-01-01'
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM'), customer_id;

-- Query materialized view (fast)
SELECT * FROM monthly_sales_summary
WHERE month = '2023-01' AND total_amount > 1000
ORDER BY total_amount DESC;
```

### 3. **Progressive Filtering**

**Apply filters in optimal order**

```sql
-- Progressive filtering pattern
SELECT /*+ CACHE */ customer_id, order_count, total_amount
FROM (
    SELECT customer_id,
           COUNT(*) as order_count,
           SUM(amount) as total_amount,
           MAX(order_date) as last_order_date
    FROM orders
    WHERE order_date >= '2023-01-01'  -- Filter early (partition pruning)
      AND amount > 0                  -- Remove invalid data
      AND customer_id IS NOT NULL     -- Remove NULLs
    GROUP BY customer_id
    HAVING COUNT(*) >= 3              -- Aggregate filter
) customer_orders
WHERE total_amount > 500              -- Post-aggregation filter
ORDER BY total_amount DESC;
```

## 🔧 Configuration Tuning

### Spark SQL Configurations

```sql
-- Memory settings
SET spark.sql.shuffle.partitions = 200;  -- Increase for large datasets
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 128MB;

-- Broadcast settings
SET spark.sql.autoBroadcastJoinThreshold = 10485760;  -- 10MB default
SET spark.sql.broadcastTimeout = 300;  -- 5 minutes timeout

-- CBO settings
SET spark.sql.cbo.enabled = true;
SET spark.sql.cbo.joinReorder.enabled = true;
SET spark.sql.cbo.starSchemaDetection = true;

-- Cache settings
SET spark.sql.inMemoryColumnarStorage.compressed = true;
SET spark.sql.inMemoryColumnarStorage.batchSize = 10000;
```

### Hardware-Aware Tuning

```sql
-- Match parallelism to cluster resources
SET spark.sql.shuffle.partitions = ${spark.executor.instances * spark.executor.cores * 2};

-- Memory-aware broadcast threshold
SET spark.sql.autoBroadcastJoinThreshold = ${spark.executor.memory * 0.3};
```

## 📊 Monitoring and Debugging

### Query Performance Metrics

```python
# Monitor query execution
def monitor_query_performance(query, description):
    """Monitor and report query performance metrics"""
    
    start_time = time.time()
    result = spark.sql(query)
    
    # Force execution
    row_count = result.count()
    execution_time = time.time() - start_time
    
    # Get query plan
    plan = result.explain(mode="formatted")
    
    # Analyze plan for optimization opportunities
    if "Exchange" in plan:
        print(f"⚠️  {description}: Shuffle detected - consider broadcast joins")
    if "SortMergeJoin" in plan:
        print(f"ℹ️  {description}: Using sort-merge join")
    if "BroadcastHashJoin" in plan:
        print(f"✅ {description}: Using broadcast join")
    
    print(f"⏱️  {description}: {execution_time:.3f}s, {row_count:,} rows")
    
    return result

# Usage
result = monitor_query_performance("""
    SELECT d.dept_name, COUNT(*) as emp_count, AVG(e.salary) as avg_salary
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id
    GROUP BY d.dept_name
""", "Department analysis")
```

## 🎯 Common Optimization Mistakes

### 1. **Ignoring Statistics**

```sql
-- ❌ Bad: No table statistics
SELECT * FROM large_table WHERE date_column > '2023-01-01';
-- CBO can't optimize without statistics

-- ✅ Good: Analyze tables first
ANALYZE TABLE large_table COMPUTE STATISTICS;
ANALYZE TABLE large_table COMPUTE STATISTICS FOR COLUMNS date_column;
SELECT * FROM large_table WHERE date_column > '2023-01-01';
```

### 2. **Overusing DISTINCT**

```sql
-- ❌ Bad: DISTINCT on large result sets
SELECT DISTINCT customer_id, product_id, order_date
FROM orders WHERE order_date >= '2020-01-01';

-- ✅ Good: Use GROUP BY for aggregations
SELECT customer_id, product_id, MAX(order_date) as latest_order
FROM orders
WHERE order_date >= '2020-01-01'
GROUP BY customer_id, product_id;
```

### 3. **Cartesian Products**

```sql
-- ❌ Bad: Missing join conditions
SELECT e.name, d.dept_name
FROM employees e, departments d;
-- Creates Cartesian product!

-- ✅ Good: Explicit join conditions
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;
```

## 🎯 Performance Checklist

### Pre-Query Optimization
- [ ] **Analyze table statistics**: `ANALYZE TABLE table COMPUTE STATISTICS`
- [ ] **Check partitioning**: Ensure partition pruning works
- [ ] **Review indexes**: Use appropriate bucketing/partitioning
- [ ] **Set configurations**: Tune memory and parallelism settings

### Query-Level Optimization
- [ ] **EXPLAIN plan**: Review execution strategy
- [ ] **Filter early**: Push predicates down
- [ ] **Select minimally**: Project only needed columns
- [ ] **Join wisely**: Use appropriate join types and hints

### Post-Query Analysis
- [ ] **Monitor execution**: Check task durations and data movement
- [ ] **Cache results**: For repeated expensive operations
- [ ] **Materialize views**: For complex reusable calculations
- [ ] **Tune configurations**: Based on observed performance

## 🎯 Interview Questions

### Common Interview Questions:
1. **How does the Catalyst optimizer work?**
2. **What is predicate pushdown?**
3. **How do you optimize join performance in Spark SQL?**
4. **What's the difference between broadcast and sort-merge joins?**
5. **How do you analyze a query execution plan?**

### Answers:
- **Catalyst**: Multi-stage optimizer (parsing → analysis → logical opt → physical opt)
- **Predicate pushdown**: Moving filters as close to data source as possible
- **Join optimization**: Use broadcast for small tables, sort-merge for large tables
- **Broadcast vs Sort-merge**: Broadcast sends small table to all executors, sort-merge shuffles both
- **EXPLAIN**: `EXPLAIN EXTENDED query` shows detailed execution plan with costs

## 📚 Summary

### Optimization Hierarchy:

1. **Logical Level**:
   - Predicate pushdown
   - Projection pushdown
   - Constant folding
   - Subquery optimization

2. **Physical Level**:
   - Join strategy selection
   - Partitioning optimization
   - Data exchange minimization

3. **Execution Level**:
   - Memory management
   - Parallelism tuning
   - Caching strategies

### Key Performance Indicators:
- **Query execution time**: Should be predictable and monitored
- **Data shuffling**: Minimize network traffic
- **Memory usage**: Avoid spills and OOM errors
- **CPU utilization**: Balance across cluster

### Best Practices:
- **Always EXPLAIN** complex queries
- **Collect statistics** on tables regularly
- **Use appropriate join hints** when needed
- **Monitor and tune** based on actual performance
- **Cache intermediate results** for complex workflows

**Optimization is an iterative process - measure, analyze, and improve!**
