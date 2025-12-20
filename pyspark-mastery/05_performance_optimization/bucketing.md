# 🪣 Bucketing: Advanced Data Organization

## 🎯 Overview

**Bucketing** is an advanced data organization technique that pre-shuffles data and co-locates related records for optimal join performance. Unlike partitioning (which organizes data by directory structure), bucketing physically organizes data within files for efficient querying.

## 🔍 Understanding Bucketing

### Bucketing vs Partitioning

| Feature | Partitioning | Bucketing |
|---------|-------------|-----------|
| **Organization** | Directory structure | File content organization |
| **Shuffle Reduction** | Limited (range queries) | Significant (joins on bucketed columns) |
| **Storage Overhead** | Minimal | Pre-computed hash distribution |
| **Query Optimization** | Predicate pushdown | Join co-location |
| **File Count** | Many small files | Fewer larger files |
| **Use Case** | Time-based filtering | Join optimization |

**Bucketing pre-organizes data for joins - no shuffle needed!**

## 🎯 Creating Bucketed Tables

### Basic Bucketing

```python
# Create bucketed table with DataFrame API
df.write \\
    .mode("overwrite") \\
    .bucketBy(16, "user_id") \\  # 16 buckets by user_id
    .sortBy("user_id") \\        # Optional sorting within buckets
    .saveAsTable("bucketed_users")

# Create bucketed table with SQL
spark.sql("""
    CREATE TABLE bucketed_orders (
        order_id INT,
        user_id INT,
        product_id INT,
        amount DOUBLE,
        order_date DATE
    )
    CLUSTERED BY (user_id) INTO 32 BUCKETS  -- 32 buckets
    STORED AS PARQUET
""")
```

### Advanced Bucketing Options

```python
# Multiple bucket columns
df.write \\
    .bucketBy(64, "country", "category") \\  # Composite bucketing
    .sortBy("country", "category") \\
    .saveAsTable("products_bucketed")

# Bucketed with partitioning (hybrid approach)
df.write \\
    .partitionBy("year", "month") \\        # Partition by time
    .bucketBy(16, "user_id") \\            # Bucket within partitions
    .sortBy("user_id", "order_date") \\
    .saveAsTable("time_user_bucketed")

# SQL with multiple bucket columns
spark.sql("""
    CREATE TABLE sales_bucketed (
        sale_id INT,
        user_id INT,
        product_id INT,
        store_id INT,
        amount DOUBLE
    )
    CLUSTERED BY (user_id, store_id) INTO 128 BUCKETS
    STORED AS PARQUET
""")
```

## ⚡ Join Optimization with Bucketing

### Bucketed Join Performance

```python
# Create bucketed tables for optimal joins
users_df = spark.read.parquet("users/")
orders_df = spark.read.parquet("orders/")

# Bucket both tables on join key
users_df.write \\
    .bucketBy(32, "user_id") \\
    .sortBy("user_id") \\
    .saveAsTable("users_bucketed")

orders_df.write \\
    .bucketBy(32, "user_id") \\  # Same bucket count
    .sortBy("user_id") \\
    .saveAsTable("orders_bucketed")

# Join on bucketed columns (no shuffle!)
joined_df = spark.sql("""
    SELECT u.name, u.email, o.order_id, o.amount, o.order_date
    FROM users_bucketed u
    INNER JOIN orders_bucketed o ON u.user_id = o.user_id
""")

print("Bucketed join - data co-located, minimal shuffling!")
joined_df.explain(mode="formatted")
```

### Bucket Count Optimization

```python
def optimize_bucket_count(df, join_key, target_bucket_size_mb=256):
    """Calculate optimal bucket count for join performance"""
    
    # Estimate data size per join key group
    key_groups = df.groupBy(join_key).count()
    avg_group_size = key_groups.agg({"count": "avg"}).collect()[0][0]
    
    # Estimate total data size
    sample_size = df.limit(1000).rdd.map(lambda row: len(str(row))).sum()
    estimated_row_size = sample_size / 1000
    total_rows = df.count()
    total_size_mb = (total_rows * estimated_row_size) / (1024 * 1024)
    
    # Calculate optimal buckets
    # Aim for target_bucket_size_mb per bucket
    optimal_buckets = max(1, int(total_size_mb / target_bucket_size_mb))
    
    # Don't exceed reasonable limits
    optimal_buckets = min(optimal_buckets, 1024)  # Max 1024 buckets
    
    # Ensure it's a power of 2 for better distribution
    import math
    optimal_buckets = 2 ** int(math.log2(optimal_buckets))
    
    print(f"Total data: {total_size_mb:.0f}MB")
    print(f"Average group size: {avg_group_size:.0f} rows")
    print(f"Optimal buckets: {optimal_buckets}")
    
    return optimal_buckets

# Usage
bucket_count = optimize_bucket_count(sales_df, "user_id", 128)
print(f"Use {bucket_count} buckets for user_id joins")
```

## 📊 Bucketing for Aggregation Optimization

### Pre-aggregated Buckets

```python
# Bucket for aggregation optimization
sales_df.write \\
    .bucketBy(64, "product_id") \\
    .sortBy("product_id") \\
    .saveAsTable("sales_by_product")

# Aggregation on bucketed column (fast!)
product_totals = spark.sql("""
    SELECT product_id, 
           COUNT(*) as order_count,
           SUM(amount) as total_amount,
           AVG(amount) as avg_amount,
           MAX(amount) as max_amount
    FROM sales_by_product
    GROUP BY product_id
""")

# Much faster than non-bucketed aggregation
print("Bucketed aggregation - data pre-grouped for fast processing!")
```

### Bucketed Window Functions

```python
# Bucketed table for window functions
employees_df.write \\
    .bucketBy(16, "department") \\
    .sortBy("department", "salary") \\
    .saveAsTable("employees_bucketed")

# Window functions on bucketed data
department_ranks = spark.sql("""
    SELECT name, department, salary,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_dense_rank,
           SUM(salary) OVER (PARTITION BY department) as dept_total_salary
    FROM employees_bucketed
""")

print("Window functions optimized by bucketing and sorting")
```

## 🔄 Dynamic Bucketing

### Adaptive Bucketing

```python
def create_adaptive_buckets(df, bucket_columns, min_buckets=8, max_buckets=512):
    """Create bucketed table with adaptive bucket count"""
    
    # Analyze data distribution
    total_rows = df.count()
    distinct_keys = df.select(bucket_columns).distinct().count()
    
    # Calculate buckets based on data characteristics
    if distinct_keys < 1000:
        # Few distinct keys - fewer buckets
        bucket_count = min(min_buckets, distinct_keys)
    elif distinct_keys > 100000:
        # Many distinct keys - more buckets
        bucket_count = max_buckets
    else:
        # Medium case - scale with data
        bucket_count = min(max_buckets, max(min_buckets, total_rows // 10000))
    
    # Ensure power of 2
    import math
    bucket_count = 2 ** int(math.log2(bucket_count))
    
    print(f"Adaptive bucketing: {bucket_count} buckets for {distinct_keys} distinct keys")
    
    # Create bucketed table
    df.write \\
        .mode("overwrite") \\
        .bucketBy(bucket_count, *bucket_columns) \\
        .saveAsTable(f"adaptive_{'_'.join(bucket_columns)}")
    
    return bucket_count

# Usage
buckets = create_adaptive_buckets(customer_df, ["country", "segment"])
```

## 🎯 Bucket Maintenance

### Bucket Statistics and Monitoring

```python
def analyze_bucket_distribution(table_name):
    """Analyze bucket distribution and effectiveness"""
    
    print(f"=== Bucket Analysis: {table_name} ===")
    
    # Get table details
    table_df = spark.table(table_name)
    
    # Check bucket columns
    bucket_info = spark.sql(f"DESC DETAIL {table_name}").collect()
    if bucket_info:
        bucket_cols = bucket_info[0].bucketColumnNames
        num_buckets = bucket_info[0].numBuckets
        print(f"Bucket columns: {bucket_cols}")
        print(f"Number of buckets: {num_buckets}")
    
    # Analyze data distribution across buckets
    if bucket_cols:
        bucket_dist = table_df.groupBy(*bucket_cols).count() \\
            .withColumn("bucket_id", F.spark_partition_id()) \\
            .groupBy("bucket_id") \\
            .agg(F.sum("count").alias("rows_in_bucket")) \\
            .orderBy("bucket_id")
        
        print("
Bucket distribution:")
        bucket_dist.show()
        
        # Calculate skew
        stats = bucket_dist.agg(
            F.avg("rows_in_bucket").alias("avg_rows"),
            F.max("rows_in_bucket").alias("max_rows"),
            F.min("rows_in_bucket").alias("min_rows")
        ).collect()[0]
        
        skew_ratio = stats.max_rows / stats.avg_rows if stats.avg_rows > 0 else 0
        print(f"\\nBucket skew ratio: {skew_ratio:.2f}x")
        
        if skew_ratio > 2:
            print("⚠️  HIGH BUCKET SKEW - consider different bucket key or count")
        else:
            print("✅ Good bucket distribution")
    
    return bucket_dist if 'bucket_dist' in locals() else None

# Usage
bucket_analysis = analyze_bucket_distribution("bucketed_orders")
```

### Bucket Optimization

```python
# Optimize existing bucketed table
spark.sql("ANALYZE TABLE bucketed_orders COMPUTE STATISTICS")

# Check for small files (bucket optimization opportunity)
small_files = spark.sql("""
    SELECT file_path, file_size
    FROM (
        SELECT input_file_name() as file_path,
               sum(length(concat(*))) as file_size
        FROM bucketed_orders
        GROUP BY input_file_name()
    )
    WHERE file_size < 134217728  -- 128MB
""")

if small_files.count() > 0:
    print("Found small files - consider bucket optimization")
    
    # Optimize buckets (requires Delta Lake or similar)
    # spark.sql("OPTIMIZE bucketed_orders")
else:
    print("Bucket files are appropriately sized")
```

## 📈 Advanced Bucketing Patterns

### Multi-Level Bucketing

```python
# Create multi-level bucketed tables for complex queries

# Level 1: Coarse bucketing for general queries
sales_df.write \\
    .bucketBy(32, "country") \\
    .saveAsTable("sales_country_buckets")

# Level 2: Fine bucketing for detailed analysis
sales_df.write \\
    .bucketBy(128, "country", "product_category") \\
    .saveAsTable("sales_detailed_buckets")

# Level 3: Composite bucketing for specific use cases
sales_df.write \\
    .bucketBy(256, "user_id", "product_id") \\
    .saveAsTable("sales_user_product_buckets")

# Choose appropriate level based on query
def choose_bucket_level(query_type):
    """Choose appropriate bucketed table for query type"""
    
    if query_type == "country_analytics":
        return "sales_country_buckets"
    elif query_type == "category_analysis":
        return "sales_detailed_buckets"
    elif query_type == "user_product_recs":
        return "sales_user_product_buckets"
    else:
        return "sales_raw"  # Fallback to non-bucketed

# Usage
table = choose_bucket_level("category_analysis")
results = spark.sql(f"SELECT * FROM {table} WHERE product_category = 'Electronics'")
```

### Bucketed Data Pipeline

```python
def create_bucketed_data_pipeline(raw_df, bucket_configs):
    """Create bucketed tables for different access patterns"""
    
    bucketed_tables = {}
    
    for config in bucket_configs:
        table_name = config["table_name"]
        bucket_cols = config["bucket_columns"]
        num_buckets = config["num_buckets"]
        partition_cols = config.get("partition_columns", [])
        
        print(f"Creating bucketed table: {table_name}")
        
        # Apply bucketing
        writer = raw_df.write.mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.bucketBy(num_buckets, *bucket_cols) \\
            .sortBy(*bucket_cols) \\
            .saveAsTable(table_name)
        
        bucketed_tables[table_name] = {
            "bucket_columns": bucket_cols,
            "num_buckets": num_buckets,
            "partition_columns": partition_cols
        }
    
    return bucketed_tables

# Define bucket configurations
bucket_configs = [
    {
        "table_name": "users_bucketed",
        "bucket_columns": ["user_id"],
        "num_buckets": 64
    },
    {
        "table_name": "products_bucketed",
        "bucket_columns": ["category", "brand"],
        "num_buckets": 32
    },
    {
        "table_name": "orders_time_bucketed",
        "bucket_columns": ["user_id"],
        "num_buckets": 128,
        "partition_columns": ["year", "month"]
    }
]

# Create bucketed pipeline
bucketed_tables = create_bucketed_data_pipeline(sales_df, bucket_configs)
print("Bucketed data pipeline created:", list(bucketed_tables.keys()))
```

## ⚡ Bucketing Performance Comparison

### Benchmarking Bucketed vs Non-Bucketed

```python
import time

def benchmark_join_performance(left_table, right_table, join_key, description):
    """Benchmark join performance for bucketed vs non-bucketed tables"""
    
    print(f"=== {description} ===")
    
    # Join query
    query = f"""
        SELECT l.*, r.*
        FROM {left_table} l
        INNER JOIN {right_table} r ON l.{join_key} = r.{join_key}
    """
    
    # Execute join
    start_time = time.time()
    result = spark.sql(query)
    count = result.count()
    execution_time = time.time() - start_time
    
    print(f"Result count: {count:,} rows")
    print(f"Execution time: {execution_time:.3f} seconds")
    
    # Analyze execution plan
    plan = result.explain(mode="formatted")
    
    if "Exchange" in str(plan):
        print("⚠️  Shuffle detected - not using bucketing effectively")
    else:
        print("✅ No shuffle - bucketing working!")
    
    return execution_time, count

# Compare bucketed vs non-bucketed joins
print("Comparing bucketed vs non-bucketed join performance:")

# Non-bucketed join
time1, count1 = benchmark_join_performance(
    "orders", "users", "user_id", "Non-bucketed Join"
)

# Bucketed join
time2, count2 = benchmark_join_performance(
    "orders_bucketed", "users_bucketed", "user_id", "Bucketed Join"
)

# Performance analysis
if time1 > 0 and time2 > 0:
    speedup = time1 / time2
    if speedup > 1:
        print(f"\\n🎯 Bucketed join is {speedup:.1f}x faster!")
    else:
        print(f"\\n⚠️  Bucketed join is {1/speedup:.1f}x slower (check bucket configuration)")

print(f"Results identical: {count1 == count2}")
```

## 🚨 Common Bucketing Mistakes

### Mistake 1: Wrong Bucket Count

```python
# ❌ Bad: Too few buckets (poor parallelism)
df.write.bucketBy(4, "user_id").saveAsTable("users")  # Only 4 buckets

# ❌ Bad: Too many buckets (overhead)
df.write.bucketBy(2048, "user_id").saveAsTable("users")  # Excessive buckets

# ✅ Good: Balanced bucket count
df.write.bucketBy(64, "user_id").saveAsTable("users")  # Good balance
```

### Mistake 2: Incompatible Bucket Counts

```python
# ❌ Bad: Different bucket counts for join
left_df.write.bucketBy(32, "key").saveAsTable("left_table")
right_df.write.bucketBy(64, "key").saveAsTable("right_table")  # Different count!

# Join will still shuffle - bucketing ineffective

# ✅ Good: Same bucket count
left_df.write.bucketBy(64, "key").saveAsTable("left_table")
right_df.write.bucketBy(64, "key").saveAsTable("right_table")
```

### Mistake 3: Not Bucketing Join Keys

```python
# ❌ Bad: Bucketing on wrong column
df.write.bucketBy(64, "timestamp").saveAsTable("events")  # Bucketing on time
# But joining on user_id - still shuffles!

# ✅ Good: Bucket on join key
df.write.bucketBy(64, "user_id").saveAsTable("events")  # Bucket on join key
```

### Mistake 4: Ignoring Sort Order

```python
# ❌ Bad: Bucketed but not sorted
df.write.bucketBy(64, "user_id").saveAsTable("users")  # No sorting

# Window functions and aggregations slower

# ✅ Good: Bucketed and sorted
df.write \\
    .bucketBy(64, "user_id") \\
    .sortBy("user_id") \\  # Sort within buckets
    .saveAsTable("users")
```

## 🎯 When to Use Bucketing

### Bucketing Use Cases

```python
# Use bucketing when:

# 1. Frequent joins on specific columns
frequent_join_keys = ["user_id", "customer_id", "product_id"]

# 2. Large tables with repeated aggregations
aggregation_keys = ["department", "category", "region"]

# 3. Window functions on grouped data
window_function_keys = ["department", "team", "location"]

# 4. High-cardinality join keys
high_cardinality_keys = ["transaction_id", "session_id", "event_id"]

# 5. Tables larger than memory
large_tables_mb = 10000  # Tables > 10GB benefit from bucketing

def should_bucket(table_size_mb, join_frequency, key_cardinality):
    """Determine if table should be bucketed"""
    
    # High join frequency
    if join_frequency > 10:  # Joins per day
        return True, "High join frequency"
    
    # Large table with good cardinality
    if table_size_mb > 5000 and key_cardinality > 10000:
        return True, "Large table, good cardinality"
    
    # Window functions on large data
    if table_size_mb > 1000:
        return True, "Large table for window functions"
    
    return False, "Not necessary"

# Usage
bucket, reason = should_bucket(8000, 15, 50000)
print(f"Should bucket: {bucket} - {reason}")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What is bucketing in Spark and how is it different from partitioning?**
2. **When should you use bucketing?**
3. **How does bucketing optimize joins?**
4. **What's the optimal number of buckets?**
5. **How do you create bucketed tables?**

### Answers:
- **Bucketing vs Partitioning**: Bucketing organizes data within files for join optimization, partitioning creates directory structure for filtering
- **When to use**: Frequent joins on large tables, high-cardinality keys, window functions
- **Join optimization**: Co-locates data by join key, eliminates shuffle for bucketed joins
- **Optimal buckets**: 2^x number (32, 64, 128), balance parallelism vs overhead
- **Creation**: `CLUSTERED BY (column) INTO n BUCKETS` in SQL or `bucketBy(n, column)` in DataFrame

## 📚 Summary

### Bucketing Strategy:

1. **Identify join patterns** - Which columns are frequently joined?
2. **Choose bucket count** - Power of 2, balance parallelism vs overhead
3. **Select bucket keys** - High-cardinality columns for good distribution
4. **Consider sorting** - Sort within buckets for additional optimizations
5. **Monitor performance** - Verify shuffle elimination in execution plans

### Performance Impact:
- **Join speed**: 5-20x faster for bucketed joins (no shuffle)
- **Aggregation speed**: 2-5x faster for bucketed aggregations
- **Storage overhead**: Minimal (just metadata)
- **Query flexibility**: Works with all Spark operations

### Best Practices:
- **Same bucket count** for joining tables
- **Power of 2 buckets** for optimal distribution
- **High-cardinality keys** for even data distribution
- **Sort within buckets** for window functions
- **Monitor skew** and adjust bucket count as needed

**Bucketing is the secret weapon for high-performance Spark joins!**

---

**🎉 You now master bucketing for optimal data organization!**
