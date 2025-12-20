# 📊 Data Partitioning: Optimizing Data Distribution

## 🎯 Overview

**Partitioning** is the process of dividing large datasets into smaller, manageable chunks that can be processed in parallel. Proper partitioning is crucial for Spark performance as it determines how data is distributed across the cluster and affects all operations including joins, aggregations, and shuffles.

## 🔍 Understanding Partitioning in Spark

### What is Partitioning?

**Partitioning** splits data into smaller pieces called partitions that can be processed independently on different nodes in the cluster.

```python
# Default partitioning (based on data size and cores)
df = spark.read.csv("large_file.csv")  # Creates ~num_cores partitions

# Custom partitioning
df.repartition(100)  # Explicitly set to 100 partitions
df.repartition("column")  # Partition by column value
df.repartition(50, "column")  # 50 partitions, distributed by column
```

### Why Partitioning Matters

1. **Parallelism**: More partitions = more parallel tasks
2. **Data Locality**: Related data on same node reduces shuffling
3. **Memory Management**: Smaller partitions fit in memory
4. **Join Performance**: Co-located data avoids expensive shuffles
5. **Skew Handling**: Proper distribution prevents bottlenecks

## 🎯 Types of Partitioning

### 1. Hash Partitioning (Default)

**Distributes data based on hash of partition key**

```python
# Hash partitioning by column
df.repartition("user_id")  # Hash of user_id determines partition

# Multiple columns
df.repartition(["country", "state"])  # Composite hash partitioning

# Benefits:
# - Even distribution for high-cardinality keys
# - Good for general-purpose operations
# - Predictable data placement
```

### 2. Range Partitioning

**Distributes data based on value ranges**

```python
# Range partitioning (requires sorting)
from pyspark.sql.functions import col

# Partition by date ranges
df.orderBy("date").repartitionByRange(10, "date")

# Partition by numeric ranges
df.orderBy("amount").repartitionByRange(20, col("amount"))

# Benefits:
# - Excellent for range queries
# - Better compression for sorted data
# - Efficient for time-series operations
```

### 3. Custom Partitioning

**User-defined partitioning logic**

```python
from pyspark import RDD

class CustomPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def __call__(self, key):
        # Custom logic: partition by first letter
        if isinstance(key, str):
            return ord(key[0].lower()) % self.num_partitions
        return hash(key) % self.num_partitions

# Apply custom partitioning
custom_partitioner = CustomPartitioner(26)  # 26 partitions for A-Z
rdd.partitionBy(26, custom_partitioner)
```

## 📈 Partition Size Optimization

### Finding Optimal Partition Size

**Rule of thumb**: Each partition should be 100MB - 1GB

```python
def analyze_partition_sizes(df, name="DataFrame"):
    """Analyze partition sizes and distribution"""
    
    partition_info = df.rdd.mapPartitionsWithIndex(
        lambda idx, iter: [(idx, sum(len(str(row)) for row in iter))]
    ).collect()
    
    sizes = [size for _, size in partition_info]
    
    print(f"=== {name} Partition Analysis ===")
    print(f"Total partitions: {len(sizes)}")
    print(f"Average size: {sum(sizes)/len(sizes)/1024/1024:.1f} MB")
    print(f"Min size: {min(sizes)/1024/1024:.1f} MB")
    print(f"Max size: {max(sizes)/1024/1024:.1f} MB")
    print(f"Std deviation: {pd.Series(sizes).std()/1024/1024:.1f} MB")
    
    # Check for skew
    avg_size = sum(sizes) / len(sizes)
    max_size = max(sizes)
    skew_ratio = max_size / avg_size if avg_size > 0 else float('inf')
    
    if skew_ratio > 3:
        print(f"⚠️  HIGH SKEW DETECTED: {skew_ratio:.1f}x ratio")
        print("Consider repartitioning or salting")
    else:
        print("✅ Good partition distribution"
    
    return partition_info

# Usage
partition_info = analyze_partition_sizes(large_df, "Sales Data")
```

### Adjusting Partition Count

```python
# Too few partitions (under-utilizes cluster)
df = spark.read.parquet("large_dataset")  # Only 8 partitions on 100-node cluster

# Too many partitions (overhead)
df.repartition(10000)  # Excessive task scheduling overhead

# Optimal partitioning
total_cores = spark.sparkContext.defaultParallelism
target_size_mb = 256  # Target 256MB per partition

# Calculate optimal partitions
df_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
df_size_mb = df_size_bytes / 1024 / 1024
optimal_partitions = max(int(df_size_mb / target_size_mb), total_cores)

# Repartition optimally
df_optimal = df.repartition(optimal_partitions)
print(f"Repartitioned from {df.rdd.getNumPartitions()} to {optimal_partitions} partitions")
```

## 🔄 Repartitioning Strategies

### When to Repartition

```python
# Scenario 1: Too few partitions after filter
large_df = spark.read.parquet("1TB_dataset")
filtered_df = large_df.filter("country = 'US'")  # Much smaller but same partitions
optimized_df = filtered_df.repartition(50)  # Redistribute to fewer partitions

# Scenario 2: Data skew causing slow tasks
skewed_df = skewed_df.repartition("user_id")  # Redistribute by high-cardinality column

# Scenario 3: Prepare for join on specific column
left_df = left_df.repartition("join_key")
right_df = right_df.repartition("join_key")  # Same partitioning for co-location

# Scenario 4: Control output file count
df.write.parquet("output/", mode="overwrite")  # Uses current partitioning
df.repartition(10).write.parquet("output/", mode="overwrite")  # Exactly 10 files
```

### Coalesce vs Repartition

```python
# coalesce() - reduce partitions without full shuffle (efficient)
df_1000_partitions = df.repartition(1000)  # Creates 1000 partitions
df_100_partitions = df_1000_partitions.coalesce(100)  # Efficiently reduces to 100

# repartition() - full redistribution (expensive but flexible)
df_balanced = df.repartition(50, "important_column")  # Balance by column + count

# When to use each:
# - coalesce(): Reduce partitions after filtering (no shuffle if reducing)
# - repartition(): Increase partitions or balance by column (full shuffle)
```

## 🎯 Join Partitioning

### Optimizing Joins with Partitioning

```python
# Strategy 1: Pre-partition for joins
orders_df = orders_df.repartition("customer_id")
customers_df = customers_df.repartition("customer_id")

# Join on co-partitioned data (no shuffle!)
joined_df = orders_df.join(customers_df, "customer_id")

# Strategy 2: Broadcast small tables
small_df = spark.read.parquet("small_lookup")
large_df = spark.read.parquet("large_fact_table")

# Broadcast join (sends small_df to all executors)
broadcast_join = large_df.join(
    F.broadcast(small_df), 
    "lookup_key"
)

# Strategy 3: Salting for skewed joins
def salt_dataframe(df, key_column, num_salts=10):
    """Add salt to distribute hot keys"""
    return df.withColumn("salt", F.floor(F.rand() * num_salts)) \\
             .withColumn("salted_key", F.concat(F.col(key_column), F.lit("_"), F.col("salt")))

# Salt both sides
salted_large = salt_dataframe(large_skewed_df, "hot_key")
salted_small = salt_dataframe(small_skewed_df, "hot_key")

# Join on salted keys (evenly distributed)
balanced_join = salted_large.join(salted_small, "salted_key")
```

## 📊 Aggregation Partitioning

### Optimizing GroupBy Operations

```python
# Problem: Skewed aggregation
sales_df = spark.read.parquet("sales_data")
# user_id distribution: most users have few sales, few users have millions

# Solution 1: Repartition before aggregation
balanced_sales = sales_df.repartition("user_id")  # Distribute evenly
user_totals = balanced_sales.groupBy("user_id").sum("amount")

# Solution 2: Use salting for skewed keys
def salted_aggregation(df, group_col, agg_col, num_salts=10):
    """Salt keys for balanced aggregation"""
    salted_df = df.withColumn("salt", F.floor(F.rand() * num_salts)) \\
                  .withColumn("salted_key", F.concat(F.col(group_col), F.lit("_"), F.col("salt")))
    
    # Aggregate on salted keys
    salted_agg = salted_df.groupBy("salted_key").agg(F.sum(agg_col).alias("total"))
    
    # Remove salt from results
    return salted_agg.withColumn(group_col, F.split("salted_key", "_")[0]) \\
                     .groupBy(group_col).sum("total")

# Usage
balanced_totals = salted_aggregation(sales_df, "user_id", "amount")
```

## 🗂️ File Partitioning

### Optimizing File Layout

```python
# Write partitioned files for efficient reads
sales_df.write \\
    .partitionBy("year", "month") \\
    .parquet("sales_partitioned/")

# Benefits:
# - Partition pruning: only read relevant data
# - Optimized for time-based queries
# - Better compression and file sizes

# Query with partition pruning
january_sales = spark.read.parquet("sales_partitioned/") \\
    .filter("year = 2023 AND month = 1")  # Only reads January partition

# Multiple partition columns
sales_df.write \\
    .partitionBy("country", "category", "year") \\
    .parquet("sales_multi_partitioned/")

# Query multiple partitions
us_electronics_2023 = spark.read.parquet("sales_multi_partitioned/") \\
    .filter("country = 'US' AND category = 'electronics' AND year = 2023")
```

## ⚡ Dynamic Partitioning

### Adaptive Partitioning

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Spark automatically adjusts partitioning based on:
# - Data size per partition
# - Available cluster resources
# - Query complexity

# Manual dynamic partitioning based on data size
def dynamic_partition(df, target_partition_size_mb=256):
    """Dynamically partition based on data size"""
    
    # Estimate total size
    total_size = df.rdd.map(lambda row: len(str(row))).sum()
    total_size_mb = total_size / 1024 / 1024
    
    # Calculate optimal partitions
    optimal_partitions = max(int(total_size_mb / target_partition_size_mb), 
                           spark.sparkContext.defaultParallelism)
    
    print(f"Data size: {total_size_mb:.0f}MB, using {optimal_partitions} partitions")
    
    return df.repartition(optimal_partitions)

# Usage
optimized_df = dynamic_partition(large_df)
```

## 📈 Monitoring Partition Performance

### Partition Health Checks

```python
def monitor_partition_health(df, operation_name="DataFrame"):
    """Comprehensive partition health monitoring"""
    
    print(f"=== Partition Health Check: {operation_name} ===")
    
    # Basic partition info
    num_partitions = df.rdd.getNumPartitions()
    print(f"Number of partitions: {num_partitions}")
    
    # Size distribution
    partition_sizes = df.rdd.mapPartitionsWithIndex(
        lambda idx, iter: [(idx, sum(1 for _ in iter))]
    ).collect()
    
    sizes = [size for _, size in partition_sizes]
    
    if sizes:
        print(f"Partition sizes - Min: {min(sizes):,}, Max: {max(sizes):,}, Avg: {sum(sizes)/len(sizes):,.0f}")
        
        # Check for empty partitions
        empty_partitions = sum(1 for size in sizes if size == 0)
        if empty_partitions > 0:
            print(f"⚠️  {empty_partitions} empty partitions detected")
        
        # Check for skew
        avg_size = sum(sizes) / len(sizes)
        max_size = max(sizes)
        skew_ratio = max_size / avg_size if avg_size > 0 else 0
        
        if skew_ratio > 5:
            print(f"🚨 HIGH SKEW: {skew_ratio:.1f}x ratio (max/avg)")
        elif skew_ratio > 2:
            print("⚠️  MODERATE SKEW detected")
        else:
            print("✅ Good partition balance")
    
    # Memory estimation
    sample_row = df.limit(1).collect()
    if sample_row:
        row_size_bytes = len(str(sample_row[0]))
        estimated_memory_mb = (sum(sizes) * row_size_bytes) / 1024 / 1024
        print(f"Estimated memory usage: {estimated_memory_mb:.0f}MB")
    
    return partition_sizes

# Usage
health_check = monitor_partition_health(processed_df, "Processed Sales Data")
```

## 🎯 Best Practices

### Partitioning Guidelines

1. **Right-size partitions**: 100MB - 1GB per partition
2. **Balance data distribution**: Avoid skew > 3x
3. **Consider access patterns**: Partition by frequently filtered columns
4. **Minimize shuffling**: Co-locate data for joins
5. **Monitor and adjust**: Use adaptive execution when possible

### Common Patterns

```python
# Pattern 1: ETL Pipeline Partitioning
def etl_pipeline_partitioning(raw_df):
    """ETL with optimal partitioning at each step"""
    
    # Stage 1: Initial processing (fewer partitions for I/O)
    stage1 = raw_df.repartition(50) \\
                   .filter("status = 'active'") \\
                   .withColumn("processed_date", F.current_date())
    
    # Stage 2: Aggregation (balance for grouping)
    stage2 = stage1.repartition("category") \\
                   .groupBy("category") \\
                   .agg(F.sum("amount").alias("total"))
    
    # Stage 3: Output (control file count)
    stage2.repartition(20).write.parquet("output/")
    
    return stage2

# Pattern 2: Join Optimization
def optimize_joins(left_df, right_df, join_key):
    """Optimize joins with proper partitioning"""
    
    # Check sizes
    left_count = left_df.count()
    right_count = right_df.count()
    
    if right_count < 100000:  # Small table
        # Use broadcast join
        result = left_df.join(F.broadcast(right_df), join_key)
    else:
        # Pre-partition for join
        left_partitioned = left_df.repartition(join_key)
        right_partitioned = right_df.repartition(join_key)
        result = left_partitioned.join(right_partitioned, join_key)
    
    return result

# Pattern 3: Time-series Partitioning
def time_series_partitioning(df, time_column="timestamp"):
    """Optimize for time-series queries"""
    
    # Add partitioning columns
    partitioned_df = df.withColumn("year", F.year(time_column)) \\
                       .withColumn("month", F.month(time_column)) \\
                       .withColumn("day", F.dayofmonth(time_column))
    
    # Write with time-based partitioning
    partitioned_df.write \\
        .partitionBy("year", "month", "day") \\
        .parquet("time_series_data/")
    
    return partitioned_df
```

## 🚨 Common Partitioning Mistakes

### Mistake 1: Too Many Partitions

```python
# ❌ Bad: Excessive partitions
df.repartition(10000)  # Too much overhead

# ✅ Better: Reasonable partition count
cores = spark.sparkContext.defaultParallelism
df.repartition(max(cores * 2, 100))  # 2-3x cores, max 100
```

### Mistake 2: Ignoring Data Skew

```python
# ❌ Bad: Blind repartitioning
skewed_df.repartition(100)  # Still skewed!

# ✅ Better: Repartition on high-cardinality column
skewed_df.repartition("unique_id")  # Balances data
```

### Mistake 3: Not Considering Join Keys

```python
# ❌ Bad: Different partitioning for join
left_df.repartition("col_A")
right_df.repartition("col_B")  # Different key = shuffle

# ✅ Better: Same partitioning for join keys
left_df.repartition("join_key")
right_df.repartition("join_key")  # Co-located = fast join
```

### Mistake 4: Over-partitioning Small Datasets

```python
# ❌ Bad: Small dataset with many partitions
small_df = spark.read.csv("small_file.csv")  # 10MB file
small_df.repartition(1000)  # Unnecessary overhead

# ✅ Better: Use coalesce for small datasets
small_df.coalesce(4)  # Reduce to reasonable number
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What is partitioning in Spark and why is it important?**
2. **What's the difference between repartition() and coalesce()?**
3. **How do you handle data skew in partitioning?**
4. **When should you repartition data?**
5. **How does partitioning affect join performance?**

### Answers:
- **Partitioning**: Divides data into chunks for parallel processing
- **repartition() vs coalesce()**: repartition() always shuffles, coalesce() avoids shuffle when reducing partitions
- **Handle skew**: Use salting, repartition on high-cardinality columns, or broadcast joins
- **When to repartition**: After filtering (too few partitions), before joins (co-location), for output control
- **Join performance**: Co-partitioned data avoids shuffles, improving join speed 10-100x

## 📚 Summary

### Partitioning Strategy:

1. **Assess data characteristics**: Size, distribution, access patterns
2. **Choose partitioning strategy**: Hash, range, or custom
3. **Optimize partition count**: 100MB-1GB per partition
4. **Balance data distribution**: Avoid skew > 3x
5. **Consider operation types**: Joins, aggregations, file I/O
6. **Monitor and adjust**: Use adaptive execution when possible

### Key Metrics to Monitor:
- **Partition count**: Should match cluster parallelism
- **Partition sizes**: Should be balanced (low variance)
- **Skew ratio**: Max partition / avg partition (< 3.0 ideal)
- **Shuffle data**: Minimize network traffic
- **Task duration**: Should be even across stages

### Performance Impact:
- **Good partitioning**: 5-10x performance improvement
- **Bad partitioning**: 10x performance degradation
- **Optimal partitions**: 2-3x number of cores
- **Memory per partition**: < 1GB for stability

**Proper partitioning is the foundation of high-performance Spark applications!**
