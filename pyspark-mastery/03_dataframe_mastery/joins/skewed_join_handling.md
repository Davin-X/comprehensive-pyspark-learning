# ⚖️ Handling Skewed Joins: Dealing with Data Imbalance

## 🎯 Overview

**Data skew** is one of the most common performance killers in distributed systems. When join keys are unevenly distributed, some partitions become overloaded while others remain underutilized, causing slow performance and potential failures.

## 🔍 Understanding Data Skew

### What is Data Skew?

**Data skew** occurs when data is unevenly distributed across partitions, causing some tasks to process significantly more data than others.

```python
# Example of skewed data distribution
Partition 0: 10,000 records  (fast)
Partition 1: 10,000 records  (fast)
Partition 2: 10,000 records  (fast)
Partition 3: 5,000,000 records (very slow - SKEW!)

# Result: Total job time determined by slowest partition
# 4 partitions should take ~4 minutes
# But actually takes ~50 minutes due to skew!
```

### Skew in Joins

**Join skew** happens when join keys have uneven distribution:

```python
# Customer orders (fact table)
orders = [
    ("customer_1", 100), ("customer_1", 200), ("customer_1", 300),  # Many orders
    ("customer_2", 150),                                            # Few orders
    ("customer_3", 25000), ("customer_3", 30000), ...              # Many orders
]

# Customer info (dimension table)
customers = [
    ("customer_1", "VIP"),    # Popular customer
    ("customer_2", "Regular"),
    ("customer_3", "VIP"),    # Popular customer
]

# Result: customer_1 and customer_3 partitions get massive data
# customer_2 partition gets tiny data
# Join performance suffers dramatically
```

## 🚨 Detecting Data Skew

### Symptoms of Skew

1. **Long-running tasks**: Some tasks take much longer than others
2. **OOM errors**: Memory exhaustion on certain executors
3. **Slow joins**: Performance degrades with data size
4. **Uneven CPU usage**: Some cores idle, others overloaded

### Detection Methods

#### 1. Monitor Task Duration in Spark UI

Access `http://localhost:4040` and check:
- **Task duration variance**: Look for tasks 10-100x slower than average
- **Data processed per task**: Check input/output sizes
- **Shuffle read/write**: Identify partitions with excessive data movement

#### 2. Analyze Partition Sizes

```python
# Check partition distribution
partition_sizes = df.select(F.spark_partition_id().alias(\"partition\")) \\
    .groupBy(\"partition\") \\
    .count() \\
    .orderBy(\"count\", ascending=False)

partition_sizes.show()

# Calculate skew metrics
total_rows = partition_sizes.agg(F.sum(\"count\")).collect()[0][0]
avg_per_partition = total_rows / partition_sizes.count()
max_partition = partition_sizes.collect()[0][1]  # Largest partition

skew_ratio = max_partition / avg_per_partition
print(f"Skew ratio: {skew_ratio:.2f}x")
print(f"Acceptable: < 2.0x")
print(f"Problematic: > 5.0x")
```

#### 3. Analyze Join Key Distribution

```python
# Check join key frequency
key_distribution = df.groupBy(\"join_key\") \\
    .count() \\
    .orderBy(\"count\", ascending=False) \\
    .limit(20)

key_distribution.show()

# Identify hot keys (most frequent)
hot_keys = key_distribution.filter(\"count > 1000000\")  # Adjust threshold
print(f\"Hot keys found: {hot_keys.count()}\")
```

## 🛠️ Skew Handling Strategies

### Strategy 1: Salting (Hash Redistribution)

**Add salt to join keys to distribute hot keys across partitions.**

```python
# Original skewed join
skewed_join = large_df.join(small_df, \"hot_key\", \"inner\")

# Salting solution
num_salts = 10  # Adjust based on skew severity

# Add salt to both DataFrames
salted_large = large_df.withColumn(
    \"salt\", F.floor(F.rand() * num_salts)
).withColumn(
    \"salted_key\", F.concat(F.col(\"hot_key\"), F.lit(\"_\"), F.col(\"salt\"))
)

salted_small = small_df.crossJoin(
    spark.range(0, num_salts).toDF(\"salt\")
).withColumn(
    \"salted_key\", F.concat(F.col(\"hot_key\"), F.lit(\"_\"), F.col(\"salt\"))
)

# Join on salted keys (evenly distributed)
balanced_join = salted_large.join(salted_small, \"salted_key\", \"inner\")

# Remove salt from results
final_result = balanced_join.drop(\"salt\", \"salted_key\")
```

**Pros**: Simple, effective for most skew cases
**Cons**: Increases data size, requires post-processing

### Strategy 2: Broadcast Join for Small Tables

**When one table is small enough, broadcast it entirely.**

```python
# Check if small table qualifies for broadcast
small_table_size = small_df.count() * len(small_df.columns) * 50  # Rough bytes
threshold = int(spark.conf.get(\"spark.sql.autoBroadcastJoinThreshold\"))

if small_table_size < threshold:
    # Use broadcast join
    optimized_join = large_df.join(
        F.broadcast(small_df), 
        \"join_key\", 
        \"inner\"
    )
else:
    print(\"Table too large for broadcast, consider salting\")
```

**Pros**: Eliminates shuffle entirely, very fast
**Cons**: Limited by broadcast threshold (default 10MB)

### Strategy 3: Pre-aggregation

**Aggregate before joining to reduce data volume.**

```python
# Instead of joining detailed records
detailed_join = fact_df.join(dim_df, \"key\", \"inner\")

# Pre-aggregate fact table
aggregated_fact = fact_df.groupBy(\"key\").agg(
    F.sum(\"amount\").alias(\"total_amount\"),
    F.count(\"*\").alias(\"transaction_count\"),
    F.avg(\"amount\").alias(\"avg_amount\")
)

# Join aggregated data (much smaller)
efficient_join = aggregated_fact.join(dim_df, \"key\", \"inner\")
```

**Pros**: Reduces data volume significantly
**Cons**: Loses granularity, may not suit all use cases

### Strategy 4: Custom Partitioning

**Use custom partitioner to distribute hot keys.**

```python
from pyspark import RDD

class CustomPartitioner:
    def __init__(self, num_partitions, hot_keys):
        self.num_partitions = num_partitions
        self.hot_keys = set(hot_keys)
    
    def __call__(self, key):
        if key in self.hot_keys:
            # Distribute hot keys across multiple partitions
            return hash(key) % (self.num_partitions // 2) + (self.num_partitions // 2)
        else:
            # Normal keys go to first half
            return hash(key) % (self.num_partitions // 2)

# Apply custom partitioning
hot_keys_list = [\"customer_1\", \"customer_3\"]  # Identify manually or automatically
partitioner = CustomPartitioner(16, hot_keys_list)

# Repartition with custom logic
balanced_rdd = df.rdd.partitionBy(16, partitioner).toDF()
```

**Pros**: Precise control over distribution
**Cons**: Complex to implement, requires knowing hot keys

### Strategy 5: Isolate and Handle Separately

**Process hot keys and normal keys separately.**

```python
# Identify hot keys (keys with high frequency)
key_counts = df.groupBy(\"join_key\").count()
hot_keys_df = key_counts.filter(\"count > 100000\").select(\"join_key\")
normal_keys_df = key_counts.filter(\"count <= 100000\").select(\"join_key\")

# Process hot keys separately
hot_data_large = large_df.join(hot_keys_df, \"join_key\", \"inner\")
hot_data_small = small_df.join(hot_keys_df, \"join_key\", \"inner\")

# Use broadcast for hot keys (since they're few)
hot_result = hot_data_large.join(
    F.broadcast(hot_data_small), \"join_key\", \"inner\"
)

# Process normal keys with regular join
normal_data_large = large_df.join(normal_keys_df, \"join_key\", \"inner\")
normal_data_small = small_df.join(normal_keys_df, \"join_key\", \"inner\")
normal_result = normal_data_large.join(normal_data_small, \"join_key\", \"inner\")

# Combine results
final_result = hot_result.union(normal_result)
```

**Pros**: Optimal processing for each data segment
**Cons**: Complex implementation, requires analysis

## ⚡ Adaptive Query Execution

### Spark 3.0+ Automatic Skew Handling

```python
# Enable adaptive query execution
spark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")
spark.conf.set(\"spark.sql.adaptive.coalescePartitions.enabled\", \"true\")
spark.conf.set(\"spark.sql.adaptive.skewJoin.enabled\", \"true\")

# Configure skew detection
spark.conf.set(\"spark.sql.adaptive.skewJoin.skewedPartitionFactor\", \"10\")
spark.conf.set(\"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes\", \"256MB\")

# Spark automatically detects and handles skew
auto_handled_join = large_df.join(small_df, \"key\", \"inner\")
```

**Spark automatically:**
- Detects skewed partitions
- Splits large partitions into smaller ones
- Rebalances data distribution
- Optimizes join performance

## 📊 Performance Monitoring

### Skew Detection Queries

```python
# Comprehensive skew analysis
def analyze_skew(df, partition_col=None):
    \"\"\"Analyze data skew in DataFrame\"\"\"
    
    # Partition-level analysis
    partition_stats = df.withColumn(\"partition_id\", F.spark_partition_id()) \\
        .groupBy(\"partition_id\") \\
        .count() \\
        .orderBy(\"count\", ascending=False)
    
    # Calculate metrics
    stats = partition_stats.agg(
        F.count(\"partition_id\").alias(\"total_partitions\"),
        F.sum(\"count\").alias(\"total_rows\"),
        F.avg(\"count\").alias(\"avg_rows_per_partition\"),
        F.max(\"count\").alias(\"max_rows_in_partition\"),
        F.min(\"count\").alias(\"min_rows_in_partition\")
    ).collect()[0]
    
    skew_ratio = stats.max_rows_in_partition / stats.avg_rows_per_partition
    
    print(f\"=== SKEW ANALYSIS ===\")
    print(f\"Total partitions: {stats.total_partitions}\")
    print(f\"Total rows: {stats.total_rows:,}\")
    print(f\"Skew ratio: {skew_ratio:.2f}x\")
    print(f\"Max partition size: {stats.max_rows_in_partition:,}\")
    print(f\"Min partition size: {stats.min_rows_in_partition:,}\")
    
    # Key-level analysis if column specified
    if partition_col:
        key_skew = df.groupBy(partition_col).count().orderBy(\"count\", ascending=False).limit(10)
        print(f\"\\nTop 10 {partition_col} frequencies:\")
        key_skew.show()
    
    # Recommendations
    if skew_ratio > 5:
        print(\"\\n🚨 HIGH SKEW DETECTED! Consider:\")
        print(\"- Salting join keys\")
        print(\"- Using broadcast joins\")
        print(\"- Pre-aggregating data\")
        print(\"- Custom partitioning\")
    elif skew_ratio > 2:
        print(\"\\n⚠️ MODERATE SKEW DETECTED. Monitor performance.\")
    else:
        print(\"\\n✅ LOW SKEW. Data well-distributed.\")
    
    return skew_ratio

# Usage
skew_ratio = analyze_skew(orders_df, \"customer_id\")
```

## 🎯 Best Practices

### 1. **Prevention First**

```python
# Design data models to avoid skew
# Use surrogate keys instead of natural keys
# Implement data partitioning strategies at source
# Regularly monitor and rebalance data
```

### 2. **Monitoring and Alerting**

```python
# Set up automated skew detection
def monitor_join_performance(join_df, threshold_minutes=10):
    \"\"\"Monitor join performance and alert on skew\"\"\"
    start_time = time.time()
    
    try:
        result_count = join_df.count()
        execution_time = time.time() - start_time
        
        if execution_time > threshold_minutes * 60:
            alert_team(f\"Slow join detected: {execution_time:.1f} seconds\")
            
        return result_count, execution_time
        
    except Exception as e:
        alert_team(f\"Join failed: {str(e)}\")
        raise

# Usage
count, duration = monitor_join_performance(large_join)
```

### 3. **Progressive Optimization**

```python
def optimize_join(large_df, small_df, join_key):
    \"\"\"Apply progressive join optimizations\"\"\"
    
    # Step 1: Try broadcast join first
    small_size = small_df.count() * len(small_df.columns) * 50
    threshold = int(spark.conf.get(\"spark.sql.autoBroadcastJoinThreshold\"))
    
    if small_size < threshold:
        print(\"Using broadcast join\")
        return large_df.join(F.broadcast(small_df), join_key, \"inner\")
    
    # Step 2: Check for skew
    key_dist = large_df.groupBy(join_key).count()
    max_count = key_dist.agg(F.max(\"count\")).collect()[0][0]
    total_rows = large_df.count()
    
    if max_count > total_rows * 0.1:  # 10% of data in one key
        print(\"High skew detected, using salting\")
        return salted_join(large_df, small_df, join_key)
    
    # Step 3: Regular optimized join
    print(\"Using optimized shuffle join\")
    return large_df.join(small_df, join_key, \"inner\")

# Usage
result = optimize_join(fact_table, dim_table, \"customer_id\")
```

## 🚨 Common Anti-Patterns

### 1. **Ignoring Skew Until Production**

```python
# ❌ Bad: Test with uniform data, deploy to skewed production
# Result: Production failures, slow performance
```

### 2. **Over-Engineering Simple Cases**

```python
# ❌ Bad: Use salting for perfectly balanced data
# Result: Unnecessary complexity and overhead
```

### 3. **Not Monitoring After Fixes**

```python
# ❌ Bad: Implement skew fix, never check if it worked
# Result: False sense of security
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What is data skew in Spark joins?**
2. **How do you detect data skew?**
3. **What are strategies to handle join skew?**
4. **When should you use salting vs broadcast joins?**
5. **How does adaptive query execution help with skew?**

### Answers:
- **Data skew**: Uneven data distribution causing some tasks to be much slower
- **Detection**: Monitor task durations in Spark UI, analyze partition sizes
- **Strategies**: Salting, broadcast joins, pre-aggregation, custom partitioning
- **Salting vs Broadcast**: Salting for large tables with hot keys, broadcast for small lookup tables
- **AQE**: Automatically detects and handles skew by splitting large partitions

## 📚 Summary

### Skew Handling Hierarchy:

1. **Prevention**: Design to avoid skew (best)
2. **Broadcast**: For small tables (< 10MB)
3. **Salting**: Redistribute hot keys (most common)
4. **Pre-aggregation**: Reduce data before joining
5. **Custom partitioning**: Precise control
6. **AQE**: Automatic handling in Spark 3.0+

### Key Metrics to Monitor:
- **Skew ratio**: Max partition / avg partition (> 2.0 is concerning)
- **Task duration variance**: Should be < 5x between fastest/slowest
- **Memory usage**: Watch for OOM on skewed partitions
- **Shuffle data**: Excessive data movement indicates skew

### Remember:
**Data skew is inevitable in real-world data - plan for it!**

---

**🎯 Mastering skew handling separates good Spark developers from great ones!**
