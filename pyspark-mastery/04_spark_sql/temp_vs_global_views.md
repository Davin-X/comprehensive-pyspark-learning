# 🔍 Temporary vs Global Views: View Management in Spark SQL

## 🎯 Overview

**Views** in Spark SQL are virtual tables created from SQL queries. They provide reusable access to complex query results without storing the data physically. Understanding the difference between temporary and global views is crucial for managing data access and session persistence in Spark applications.

## 📊 View Types in Spark SQL

### Temporary Views (`createOrReplaceTempView`)

**Temporary views** are session-scoped and only accessible within the current SparkSession.

```sql
-- Create temporary view
CREATE TEMP VIEW temp_employees AS
SELECT * FROM employees WHERE salary > 50000;

-- Or using DataFrame API
df.createOrReplaceTempView("temp_employees")
```

**Characteristics:**
- ✅ **Session-scoped**: Available only in current SparkSession
- ✅ **Automatic cleanup**: Disappears when session ends
- ✅ **Lightweight**: No additional storage overhead
- ✅ **Thread-safe**: Each session has its own views
- ❌ **Not shareable**: Cannot be accessed by other sessions

### Global Views (`createOrReplaceGlobalTempView`)

**Global temporary views** are database-scoped and accessible across all sessions in the Spark application.

```sql
-- Create global temporary view
CREATE GLOBAL TEMP VIEW global_employees AS
SELECT * FROM employees WHERE department = 'Engineering';

-- Or using DataFrame API
df.createOrReplaceGlobalTempView("global_employees")
```

**Characteristics:**
- ✅ **Application-scoped**: Available across all SparkSessions
- ✅ **Persistent**: Survives session restarts
- ✅ **Shareable**: Accessible by all sessions in the application
- ❌ **Special access**: Requires `global_temp.` prefix
- ❌ **Application lifetime**: Only exists while application runs

## 🔄 View Lifecycle Management

### Creating Views

```python
# Method 1: From DataFrame
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceGlobalTempView("departments")

# Method 2: Using SQL DDL
spark.sql("CREATE TEMP VIEW high_earners AS SELECT * FROM employees WHERE salary > 70000")
spark.sql("CREATE GLOBAL TEMP VIEW dept_stats AS SELECT department, COUNT(*) as count FROM employees GROUP BY department")

# Method 3: Using SQL with existing tables
spark.sql("""
    CREATE TEMP VIEW employee_summary AS
    SELECT 
        e.name,
        e.department,
        e.salary,
        d.location
    FROM employees e
    LEFT JOIN departments d ON e.department = d.dept_name
""")
```

### Accessing Views

```python
# Temporary views - direct access
temp_result = spark.sql("SELECT * FROM employees WHERE salary > 60000")

# Global temporary views - require global_temp prefix
global_result = spark.sql("SELECT * FROM global_temp.departments")

# Mixed access
mixed_query = spark.sql("""
    SELECT e.name, d.location
    FROM employees e
    JOIN global_temp.departments d ON e.department = d.dept_name
""")
```

### View Management Operations

```python
# Check available views
spark.sql("SHOW TABLES").show()
spark.sql("SHOW TABLES IN global_temp").show()

# Describe view structure
spark.sql("DESCRIBE employees").show()
spark.sql("DESCRIBE global_temp.departments").show()

# Drop views
spark.sql("DROP VIEW IF EXISTS employees")
spark.sql("DROP VIEW IF EXISTS global_temp.departments")

# Refresh view (if underlying data changed)
spark.sql("REFRESH TABLE employees")  # For managed tables
# Views automatically reflect DataFrame changes
```

## 🎯 When to Use Each View Type

### Use Temporary Views When:

1. **Single-session operations**
   ```python
   # Data analysis in a single notebook/session
   sales_df.createOrReplaceTempView("sales")
   result = spark.sql("SELECT region, SUM(amount) FROM sales GROUP BY region")
   ```

2. **Session-specific transformations**
   ```python
   # User-specific data filtering
   user_data = full_dataset.filter(col("user_id") == current_user)
   user_data.createOrReplaceTempView("my_data")
   ```

3. **Temporary aggregations**
   ```python
   # Intermediate calculations
   spark.sql("""
       CREATE TEMP VIEW daily_stats AS
       SELECT date, SUM(revenue) as daily_revenue
       FROM transactions
       GROUP BY date
   """)
   ```

4. **Testing and development**
   ```python
   # Quick queries during development
   test_df.createOrReplaceTempView("test_data")
   spark.sql("SELECT COUNT(*) FROM test_data").show()
   ```

### Use Global Views When:

1. **Cross-session data sharing**
   ```python
   # Shared reference data across sessions
   countries_df.createOrReplaceGlobalTempView("countries")
   
   # Session 1
   spark.sql("SELECT * FROM global_temp.countries WHERE region = 'Asia'")
   
   # Session 2 (different notebook/session)
   spark.sql("SELECT COUNT(*) FROM global_temp.countries")
   ```

2. **Application-wide configurations**
   ```python
   # Application settings available to all sessions
   config_df.createOrReplaceGlobalTempView("app_config")
   ```

3. **Shared business logic**
   ```python
   # Common business rules used across the application
   spark.sql("""
       CREATE GLOBAL TEMP VIEW business_rules AS
       SELECT customer_tier, discount_rate, credit_limit
       FROM customer_segments
   """)
   ```

4. **Microservice data sharing**
   ```python
   # Data shared between microservices in same application
   shared_data.createOrReplaceGlobalTempView("service_data")
   ```

## 🔧 Advanced View Techniques

### Dynamic View Creation

```python
def create_dynamic_view(df, view_name, filters=None, global_temp=False):
    """Create views dynamically with optional filters"""
    
    temp_df = df
    if filters:
        for col_name, condition in filters.items():
            if isinstance(condition, str):
                temp_df = temp_df.filter(f"{col_name} == '{condition}'")
            else:
                temp_df = temp_df.filter(col(col_name) == condition)
    
    if global_temp:
        temp_df.createOrReplaceGlobalTempView(view_name)
    else:
        temp_df.createOrReplaceTempView(view_name)
    
    return view_name

# Usage
create_dynamic_view(sales_df, "us_sales", {"country": "USA"})
create_dynamic_view(customers_df, "vip_customers", {"tier": "VIP"}, global_temp=True)
```

### View Dependencies and Caching

```python
# Create dependent views
spark.sql("""
    CREATE TEMP VIEW raw_sales AS
    SELECT * FROM sales WHERE amount > 0
""")

spark.sql("""
    CREATE TEMP VIEW sales_summary AS
    SELECT region, SUM(amount) as total_sales
    FROM raw_sales
    GROUP BY region
""")

# Cache expensive views for performance
spark.sql("CACHE TABLE sales_summary")  # Cache the view result

# Check cached tables
spark.sql("SHOW TABLES").filter("isCached = true").show()
```

### View Metadata and Statistics

```python
# Get view information
def get_view_info(view_name, is_global=False):
    """Get comprehensive view information"""
    
    if is_global:
        full_name = f"global_temp.{view_name}"
    else:
        full_name = view_name
    
    # Basic info
    describe = spark.sql(f"DESCRIBE {full_name}")
    print(f"=== View: {full_name} ===")
    describe.show()
    
    # Row count
    try:
        count = spark.sql(f"SELECT COUNT(*) as row_count FROM {full_name}").collect()[0][0]
        print(f"Row count: {count:,}")
    except:
        print("Could not get row count")
    
    # Check if cached
    cache_info = spark.sql("SHOW TABLES").filter(f"tableName = '{view_name}'")
    if cache_info.count() > 0:
        is_cached = cache_info.select("isCached").collect()[0][0]
        print(f"Cached: {is_cached}")
    
    return describe

# Usage
get_view_info("employees")
get_view_info("departments", is_global=True)
```

## ⚡ Performance Considerations

### View Performance Patterns

```python
import time

# Test view performance
def benchmark_view(view_name, query, is_global=False):
    """Benchmark view query performance"""
    
    if is_global:
        full_query = query.replace("FROM table", f"FROM global_temp.{view_name}")
    else:
        full_query = query.replace("FROM table", f"FROM {view_name}")
    
    start_time = time.time()
    result = spark.sql(full_query)
    count = result.count()
    execution_time = time.time() - start_time
    
    print(f"Query: {full_query}")
    print(".3f")
    print(f"Rows returned: {count}")
    
    return result, execution_time

# Compare performance
query = "SELECT department, COUNT(*) FROM table GROUP BY department"

# Temporary view
sales_df.createOrReplaceTempView("temp_sales")
temp_result, temp_time = benchmark_view("temp_sales", query)

# Global view
sales_df.createOrReplaceGlobalTempView("global_sales")
global_result, global_time = benchmark_view("global_sales", query, is_global=True)

print(f"\\nPerformance difference: {abs(temp_time - global_time):.3f} seconds")
```

### Caching Strategies

```python
# Strategic view caching
def optimize_view_caching():
    """Implement intelligent view caching strategy"""
    
    # Cache frequently accessed reference data
    spark.sql("CREATE GLOBAL TEMP VIEW countries AS SELECT * FROM country_table")
    spark.sql("CACHE TABLE global_temp.countries")
    
    # Cache expensive aggregations
    spark.sql("""
        CREATE TEMP VIEW monthly_revenue AS
        SELECT 
            YEAR(order_date) as year,
            MONTH(order_date) as month,
            SUM(amount) as revenue
        FROM orders
        GROUP BY YEAR(order_date), MONTH(order_date)
    """)
    spark.sql("CACHE TABLE monthly_revenue")
    
    # Don't cache frequently changing data
    # Don't cache large transactional data
    
    print("Optimized caching strategy applied")
    spark.sql("SHOW TABLES").filter("isCached = true").show()

optimize_view_caching()
```

## 🚨 Common Mistakes and Solutions

### Mistake 1: Wrong View Scope

```python
# ❌ Wrong: Using temp view when global is needed
df.createOrReplaceTempView("shared_data")
# Other sessions can't access this view

# ✅ Correct: Use global view for sharing
df.createOrReplaceGlobalTempView("shared_data")
# Now accessible via global_temp.shared_data
```

### Mistake 2: Forgetting Global Prefix

```python
# ❌ Wrong: Accessing global view without prefix
spark.sql("SELECT * FROM departments")  # Fails if it's global

# ✅ Correct: Use global_temp prefix
spark.sql("SELECT * FROM global_temp.departments")
```

### Mistake 3: View Name Conflicts

```python
# ❌ Wrong: Same name for temp and global views
df.createOrReplaceTempView("employees")
df.createOrReplaceGlobalTempView("employees")  # Overwrites temp view

# ✅ Correct: Use descriptive names
df.createOrReplaceTempView("session_employees")
df.createOrReplaceGlobalTempView("global_employees")
```

### Mistake 4: Not Refreshing Stale Views

```python
# ❌ Wrong: View doesn't reflect DataFrame changes
original_df.createOrReplaceTempView("data")
modified_df = original_df.filter(col("active") == true)
# View still shows original data

# ✅ Correct: Recreate view or use latest DataFrame
modified_df.createOrReplaceTempView("data")
```

## 🎯 Best Practices

### 1. **Choose Appropriate View Type**

```python
# Rule of thumb:
# - Use TEMP views for session-specific work
# - Use GLOBAL views for application-wide data sharing
# - Consider data size, access patterns, and lifetime
```

### 2. **Naming Conventions**

```python
# Consistent naming patterns
# temp_views: temp_table_name
# global_views: global_table_name
# cached_views: cached_table_name

spark.sql("CREATE TEMP VIEW temp_daily_sales AS SELECT * FROM sales WHERE date = CURRENT_DATE")
spark.sql("CREATE GLOBAL TEMP VIEW global_products AS SELECT * FROM product_catalog")
```

### 3. **View Lifecycle Management**

```python
# Clean up views when done
def cleanup_views():
    """Clean up temporary views"""
    temp_views = ["temp_sales", "temp_customers", "temp_orders"]
    for view in temp_views:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
    
    global_views = ["global_config", "global_ref_data"]
    for view in global_views:
        spark.sql(f"DROP VIEW IF EXISTS global_temp.{view}")
    
    print("Views cleaned up")

# Use in try/finally blocks
try:
    # Your Spark SQL operations
    pass
finally:
    cleanup_views()
```

### 4. **Performance Monitoring**

```python
# Monitor view usage
def monitor_view_performance():
    """Monitor view query performance"""
    
    # Enable query execution logging
    spark.sparkContext.setLogLevel("INFO")
    
    # Check execution plans
    query = "SELECT * FROM employees WHERE salary > 50000"
    spark.sql(query).explain(mode="formatted")
    
    # Monitor cache hit ratios
    cached_tables = spark.sql("SHOW TABLES").filter("isCached = true")
    print("Currently cached views:")
    cached_tables.show()

monitor_view_performance()
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What's the difference between temporary and global views?**
2. **When should you use global views?**
3. **How do you access global temporary views?**
4. **What happens to views when a Spark session ends?**

### Answers:
- **Temp vs Global**: Temp views are session-scoped, global views are application-scoped
- **Use global when**: Sharing data across multiple sessions in same application
- **Access global**: Use `global_temp.view_name` syntax
- **Session end**: Temp views disappear, global views persist until application ends

## 📚 Summary

### View Type Comparison:

| Feature | Temporary View | Global Temporary View |
|---------|----------------|----------------------|
| **Scope** | Single session | All sessions in app |
| **Lifetime** | Session duration | Application lifetime |
| **Access** | Direct name | `global_temp.name` |
| **Performance** | Same | Same |
| **Use Case** | Session work | Shared data |
| **Cleanup** | Auto on session end | Manual or app end |

### Key Takeaways:
- **Temporary views** for session-specific operations
- **Global views** for cross-session data sharing
- **Choose based on data lifecycle and access patterns**
- **Monitor performance and clean up when done**
- **Use appropriate naming conventions**

### Performance Tips:
- Cache frequently accessed views
- Use appropriate view types for your use case
- Monitor view usage and performance
- Clean up unused views to free memory

**Views are powerful tools for organizing and sharing data access patterns in Spark SQL applications!**
