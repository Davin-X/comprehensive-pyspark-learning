# ⚖️ Handling Data Skew: Balancing Uneven Workloads

## 🎯 Overview

**Data skew** is one of the most common performance killers in distributed systems. When data is unevenly distributed across partitions, some tasks process significantly more data than others, causing slow performance, memory issues, and job failures. Understanding and handling skew is crucial for production Spark applications.

## 🔍 Understanding Data Skew

### What is Data Skew?

**Data skew** occurs when data distribution is uneven across partitions, causing some tasks to be much slower than others.

```python
# Normal distribution (good)
Partition 0: 10,000 records (2 minutes)
Partition 1: 10,000 records (2 minutes)
Partition 2: 10,000 records (2 minutes)
Partition 3: 10,000 records (2 minutes)
Total time: 2 minutes

# Skewed distribution (bad)
Partition 0: 10,000 records (2 minutes)
Partition 1: 10,000 records (2 minutes)
Partition 2: 10,000 records (2 minutes)
Partition 3: 5,000,000 records (50 minutes) ← SKEW!
Total time: 50 minutes (25x slower!)
```

### Types of Skew

1. **Data Skew**: Uneven data distribution
2. **Processing Skew**: Uneven computation complexity
3. **Network Skew**: Uneven data transfer
4. **Memory Skew**: Uneven memory usage

**Data skew is the most common and impactful type!**

## 🚨 Detecting Data Skew

### Symptoms of Skew

```python
# Check for skew indicators
def detect_skew_symptoms(df, operation_name="DataFrame"):
    """Detect common skew symptoms"""
    
    print(f"=== Skew Detection: {operation_name} ===")
    
    # 1. Partition size analysis
    partition_sizes = df.rdd.mapPartitionsWithIndex(
        lambda idx, iter: [(idx, sum(1 for _ in iter))]
    ).collect()
    
    sizes = [size for _, size in partition_sizes]
    if sizes:
        avg_size = sum(sizes) / len(sizes)
        max_size = max(sizes)
        min_size = min(sizes)
        skew_ratio = max_size / avg_size if avg_size > 0 else 0
        
        print(f"Partition sizes: Min={min_size:,}, Max={max_size:,}, Avg={avg_size:,.0f}")
        print(f"Skew ratio: {skew_ratio:.2f}x")
        
        if skew_ratio > 5:
            print("🚨 CRITICAL SKEW DETECTED")
        elif skew_ratio > 2:
            print("⚠️  MODERATE SKEW DETECTED")
        else:
            print("✅ Balanced distribution")
    
    # 2. Stage duration analysis (from Spark UI)
    print("\\nCheck Spark UI for:")
    print("- Task duration variance (>5x difference)")
    print("- Data processed per task")
    print("- GC time per executor")
    print("- Shuffle read/write imbalance")
    
    return skew_ratio if 'skew_ratio' in locals() else None

# Usage
skew_ratio = detect_skew_symptoms(large_df, "Sales Data")
```

### Key Distribution Analysis

```python
def analyze_key_distribution(df, key_column, operation="analysis"):
    """Analyze distribution of key values"""
    
    print(f"=== Key Distribution Analysis: {key_column} ===")
    
    # Count occurrences per key
    key_counts = df.groupBy(key_column).count().orderBy("count", ascending=False)
    
    # Basic statistics
    stats = key_counts.agg(
        F.count(key_column).alias("distinct_keys"),
        F.sum("count").alias("total_rows"),
        F.avg("count").alias("avg_count_per_key"),
        F.max("count").alias("max_count_per_key"),
        F.min("count").alias("min_count_per_key")
    ).collect()[0]
    
    print(f"Distinct keys: {stats.distinct_keys:,}")
    print(f"Total rows: {stats.total_rows:,}")
    print(f"Average rows per key: {stats.avg_count_per_key:.1f}")
    print(f"Maximum rows per key: {stats.max_count_per_key:,}")
    print(f"Minimum rows per key: {stats.min_count_per_key}")
    
    # Identify hot keys (potential skew)
    hot_keys_threshold = stats.total_rows * 0.01  # 1% of total data
    hot_keys = key_counts.filter(f"count > {hot_keys_threshold}")
    
    print(f"\\nHot keys (>1% of data): {hot_keys.count()}")
    if hot_keys.count() > 0:
        print("Top hot keys:")
        hot_keys.show(5)
        
        # Skew impact assessment
        hot_keys_total = hot_keys.agg(F.sum("count")).collect()[0][0]
        skew_percentage = (hot_keys_total / stats.total_rows) * 100
        
        print(f"Data in hot keys: {hot_keys_total:,} rows ({skew_percentage:.1f}%)")
        
        if skew_percentage > 10:
            print("🚨 SEVERE SKEW: Hot keys contain >10% of data")
        elif skew_percentage > 5:
            print("⚠️  MODERATE SKEW: Hot keys contain 5-10% of data")
    
    return key_counts, stats

# Usage
key_dist, stats = analyze_key_distribution(orders_df, "customer_id")
```

## 🛠️ Skew Handling Strategies

### Strategy 1: Salting (Hash Redistribution)

**Add salt to distribute hot keys across partitions**

```python
def salt_dataframe(df, key_column, num_salts=10):
    """Add salt to distribute skewed keys"""
    return df.withColumn("salt", F.floor(F.rand() * num_salts)) \\
             .withColumn("salted_key", F.concat(F.col(key_column), F.lit("_"), F.col("salt")))

def unsalt_dataframe(df, original_key_col):
    """Remove salt from joined results"""
    return df.withColumn(original_key_col, 
                        F.split("salted_key", "_")[0]) \\
             .drop("salt", "salted_key")

# Example: Handle skewed customer data
print("=== Salting Strategy ===")

# Analyze original skew
original_dist, _ = analyze_key_distribution(orders_df, "customer_id")

# Apply salting
num_salts = 20  # Distribute hot keys across 20 partitions
salted_orders = salt_dataframe(orders_df, "customer_id", num_salts)

# Create expanded customer table for joining
customers_expanded = customers_df.crossJoin(
    spark.range(0, num_salts).toDF("salt")
).withColumn("salted_key", F.concat("customer_id", F.lit("_"), "salt"))

# Join on salted keys
salted_join = salted_orders.join(
    customers_expanded, 
    "salted_key", 
    "inner"
)

# Remove salt
balanced_result = unsalt_dataframe(salted_join, "customer_id")

print(f"\\nOriginal partitions: {orders_df.rdd.getNumPartitions()}")
print(f"Salted result partitions: {balanced_result.rdd.getNumPartitions()}")

# Check if skew is reduced
final_skew = detect_skew_symptoms(balanced_result, "Salted Result")
```

### Strategy 2: Isolate and Handle Separately

**Process hot keys and normal keys separately**

```python
def isolate_hot_keys(df, key_column, hot_key_threshold_percent=1.0):
    """Isolate hot keys for separate processing"""
    
    # Calculate threshold
    total_rows = df.count()
    threshold = int(total_rows * (hot_key_threshold_percent / 100))
    
    # Identify hot keys
    key_counts = df.groupBy(key_column).count()
    hot_keys_df = key_counts.filter(f"count > {threshold}").select(key_column)
    
    # Split data
    hot_data = df.join(F.broadcast(hot_keys_df), key_column, "inner")
    normal_data = df.join(F.broadcast(hot_keys_df), key_column, "left_anti")
    
    print(f"Hot keys threshold: {threshold} rows")
    print(f"Hot data: {hot_data.count():,} rows")
    print(f"Normal data: {normal_data.count():,} rows")
    
    return hot_data, normal_data, hot_keys_df

# Example usage
print("\\n=== Isolate Hot Keys Strategy ===")

hot_orders, normal_orders, hot_keys = isolate_hot_keys(orders_df, "customer_id", 2.0)

# Process differently
# Hot keys: Use broadcast joins (small number of keys)
hot_results = hot_orders.join(
    F.broadcast(customers_df), 
    "customer_id", 
    "inner"
)

# Normal keys: Use regular joins
normal_results = normal_orders.join(
    customers_df, 
    "customer_id", 
    "inner"
)

# Combine results
final_results = hot_results.union(normal_results)
print(f"Combined results: {final_results.count():,} rows")
```

### Strategy 3: Pre-aggregation

**Aggregate before operations that cause skew**

```python
def pre_aggregation_skew_handling(df, group_keys, agg_expressions):
    """Handle skew through pre-aggregation"""
    
    print("=== Pre-aggregation Strategy ===")
    
    # Check original skew
    original_skew = detect_skew_symptoms(df, "Original Data")
    
    # Pre-aggregate to reduce data volume
    aggregated_df = df.groupBy(*group_keys).agg(*agg_expressions)
    
    # Check aggregation skew
    agg_skew = detect_skew_symptoms(aggregated_df, "Aggregated Data")
    
    reduction_ratio = df.count() / aggregated_df.count()
    print(f"\\nData reduction: {reduction_ratio:.1f}x (from {df.count():,} to {aggregated_df.count():,} rows)")
    
    if agg_skew < original_skew:
        print("✅ Pre-aggregation reduced skew")
    else:
        print("⚠️  Pre-aggregation did not reduce skew")
    
    return aggregated_df

# Example usage
agg_exprs = [
    F.sum("amount").alias("total_amount"),
    F.count("*").alias("order_count"),
    F.avg("amount").alias("avg_amount")
]

aggregated_orders = pre_aggregation_skew_handling(
    orders_df, 
    ["customer_id"], 
    agg_exprs
)
```

### Strategy 4: Custom Partitioning

**Use custom partitioner for better distribution**

```python
from pyspark import RDD

class SkewAwarePartitioner:
    """Custom partitioner that handles hot keys specially"""
    
    def __init__(self, num_partitions, hot_keys=None, hot_key_partitions=4):
        self.num_partitions = num_partitions
        self.hot_keys = set(hot_keys or [])
        self.hot_key_partitions = hot_key_partitions
        self.normal_partitions_start = hot_key_partitions
    
    def __call__(self, key):
        if key in self.hot_keys:
            # Distribute hot keys across dedicated partitions
            return hash(key) % self.hot_key_partitions
        else:
            # Normal keys use remaining partitions
            normal_partitions = self.num_partitions - self.hot_key_partitions
            return self.hot_key_partitions + (hash(key) % normal_partitions)

    def numPartitions(self):
        return self.num_partitions

# Usage
print("\\n=== Custom Partitioning Strategy ===")

# Identify hot keys
hot_keys_list = ["customer_1", "customer_2", "customer_3"]  # From analysis

# Create custom partitioner
custom_partitioner = SkewAwarePartitioner(
    num_partitions=32, 
    hot_keys=hot_keys_list, 
    hot_key_partitions=8
)

# Apply custom partitioning
rdd_partitioned = orders_df.rdd.map(lambda row: (row.customer_id, row)).partitionBy(
    32, custom_partitioner
).map(lambda x: x[1])

custom_partitioned_df = spark.createDataFrame(rdd_partitioned, orders_df.schema)

print("Applied custom partitioning to handle hot keys")
```

### Strategy 5: Dynamic Skew Handling

**Automatically detect and handle skew**

```python
def dynamic_skew_handling(df, key_column, operation="join"):
    """Automatically choose skew handling strategy"""
    
    print(f"=== Dynamic Skew Handling for {operation} ===")
    
    # Analyze current skew
    key_dist, stats = analyze_key_distribution(df, key_column)
    
    # Determine skew severity
    total_rows = stats.total_rows
    max_key_rows = stats.max_count_per_key
    skew_percentage = (max_key_rows / total_rows) * 100
    
    print(f"\\nSkew analysis: {skew_percentage:.2f}% of data in largest key")
    
    # Choose strategy based on skew severity
    if skew_percentage > 10:
        print("🚨 SEVERE SKEW: Using salting strategy")
        num_salts = min(50, max(10, int(skew_percentage / 2)))  # Adaptive salt count
        result_df = salt_dataframe(df, key_column, num_salts)
        strategy = "salting"
        
    elif skew_percentage > 5:
        print("⚠️  MODERATE SKEW: Using isolate strategy")
        hot_data, normal_data, _ = isolate_hot_keys(df, key_column, skew_percentage)
        result_df = (hot_data, normal_data)  # Return tuple for separate processing
        strategy = "isolate"
        
    elif skew_percentage > 2:
        print("📊 MILD SKEW: Using pre-aggregation")
        agg_exprs = [F.count("*").alias("record_count")]
        result_df = df.groupBy(key_column).agg(*agg_exprs)
        strategy = "pre_aggregate"
        
    else:
        print("✅ LOW SKEW: No action needed")
        result_df = df
        strategy = "none"
    
    return result_df, strategy, skew_percentage

# Usage
handled_df, strategy_used, skew_level = dynamic_skew_handling(
    orders_df, "customer_id", "aggregation"
)

print(f"\\nApplied strategy: {strategy_used}")
print(f"Original skew level: {skew_level:.2f}%")
```

## ⚡ Adaptive Query Execution

### Spark 3.0+ Automatic Skew Handling

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Configure skew detection
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

print("Adaptive Query Execution enabled for automatic skew handling")

# Spark automatically:
# - Detects skewed partitions during execution
# - Splits large partitions into smaller ones
# - Rebalances data distribution
# - Optimizes join performance
```

## 📊 Monitoring and Prevention

### Skew Monitoring Dashboard

```python
def create_skew_monitoring_dashboard(df, key_columns, operation_name="DataFrame"):
    """Create comprehensive skew monitoring dashboard"""
    
    print(f"🔍 SKEW MONITORING DASHBOARD: {operation_name}")
    print("=" * 60)
    
    dashboard_results = {}
    
    for key_col in key_columns:
        print(f"\\n📊 Analyzing {key_col}:")
        
        # Key distribution
        key_dist, stats = analyze_key_distribution(df, key_col)
        
        # Partition skew
        partition_skew = detect_skew_symptoms(df, f"{key_col}_partitioning")
        
        # Recommendations
        recommendations = []
        
        if stats and stats.max_count_per_key / stats.total_rows > 0.1:
            recommendations.append("Use salting for joins/aggregations")
        
        if partition_skew and partition_skew > 3:
            recommendations.append("Consider repartitioning on different key")
        
        if len(recommendations) == 0:
            recommendations.append("Data distribution looks good")
        
        dashboard_results[key_col] = {
            "distinct_keys": stats.distinct_keys,
            "total_rows": stats.total_rows,
            "max_key_percentage": (stats.max_count_per_key / stats.total_rows) * 100,
            "partition_skew_ratio": partition_skew,
            "recommendations": recommendations
        }
        
        print(f"✅ Recommendations: {', '.join(recommendations)}")
    
    return dashboard_results

# Usage
key_columns_to_monitor = ["customer_id", "product_id", "category"]
dashboard = create_skew_monitoring_dashboard(orders_df, key_columns_to_monitor)
```

### Proactive Skew Prevention

```python
def prevent_skew_in_etl(raw_df, key_columns, target_skew_ratio=2.0):
    """Prevent skew during ETL pipeline"""
    
    print("🛡️ SKEW PREVENTION IN ETL")
    
    processed_df = raw_df
    
    for key_col in key_columns:
        # Analyze potential skew
        _, stats = analyze_key_distribution(processed_df, key_col)
        
        if stats:
            max_ratio = stats.max_count_per_key / stats.avg_count_per_key
            
            if max_ratio > target_skew_ratio:
                print(f"\\n⚠️  Potential skew detected in {key_col} (ratio: {max_ratio:.2f})")
                
                # Apply prevention strategy
                if max_ratio > 10:
                    # Severe skew - use salting
                    processed_df = salt_dataframe(processed_df, key_col, 20)
                    print(f"Applied salting to {key_col}")
                elif max_ratio > 5:
                    # Moderate skew - repartition
                    processed_df = processed_df.repartition(key_col)
                    print(f"Repartitioned on {key_col}")
                else:
                    # Mild skew - just monitor
                    print(f"Monitoring {key_col} for future skew")
    
    return processed_df

# Usage in ETL pipeline
cleaned_df = prevent_skew_in_etl(raw_orders_df, ["customer_id", "product_id"])
```

## 🎯 Performance Benchmarking

### Skew Handling Performance Comparison

```python
import time

def benchmark_skew_strategies(df, key_column, strategies=None):
    """Benchmark different skew handling strategies"""
    
    if strategies is None:
        strategies = ["none", "salting", "isolate", "repartition"]
    
    results = {}
    
    print(f"🎯 BENCHMARKING SKEW HANDLING: {key_column}")
    print("=" * 50)
    
    for strategy in strategies:
        print(f"\\nTesting {strategy} strategy:")
        start_time = time.time()
        
        try:
            if strategy == "none":
                result = df.groupBy(key_column).count()
                
            elif strategy == "salting":
                salted = salt_dataframe(df, key_column, 10)
                result = salted.groupBy("salted_key").count()
                
            elif strategy == "isolate":
                hot_data, normal_data, _ = isolate_hot_keys(df, key_column, 2.0)
                hot_result = hot_data.groupBy(key_column).count()
                normal_result = normal_data.groupBy(key_column).count()
                result = hot_result.union(normal_result)
                
            elif strategy == "repartition":
                repartitioned = df.repartition(key_column)
                result = repartitioned.groupBy(key_column).count()
            
            # Force execution
            count = result.count()
            execution_time = time.time() - start_time
            
            # Check final skew
            final_skew = detect_skew_symptoms(result, f"{strategy}_result")
            
            results[strategy] = {
                "time": execution_time,
                "count": count,
                "skew_ratio": final_skew
            }
            
            print(f"  Time: {execution_time:.3f}s")
            print(f"  Count: {count:,}")
            print(f"  Final skew: {final_skew:.2f}x")
            
        except Exception as e:
            print(f"  ERROR: {e}")
            results[strategy] = {"error": str(e)}
    
    # Summary
    print("\\n🏆 PERFORMANCE SUMMARY:")
    successful_strategies = {k: v for k, v in results.items() if "error" not in v}
    
    if successful_strategies:
        fastest = min(successful_strategies.keys(), 
                     key=lambda k: successful_strategies[k]["time"])
        most_balanced = min(successful_strategies.keys(), 
                           key=lambda k: successful_strategies[k]["skew_ratio"])
        
        print(f"Fastest: {fastest} ({successful_strategies[fastest]['time']:.3f}s)")
        print(f"Most balanced: {most_balanced} ({successful_strategies[most_balanced]['skew_ratio']:.2f}x skew)")
    
    return results

# Usage
benchmark_results = benchmark_skew_strategies(
    orders_df, 
    "customer_id", 
    ["none", "salting", "repartition"]
)
```

## 🚨 Common Skew Mistakes

### Mistake 1: Ignoring Skew Until Production

```python
# ❌ Bad: Test with uniform data, deploy to production with skew
# Result: Production failures, timeouts, OOM errors

# ✅ Good: Test with production-like data distributions
# Include skew handling in development and testing
```

### Mistake 2: Over-Engineering Simple Cases

```python
# ❌ Bad: Use salting for perfectly balanced data
# Result: Unnecessary complexity and overhead

# ✅ Good: Only apply skew handling when skew is detected
# Use monitoring to determine when intervention is needed
```

### Mistake 3: Wrong Salt Count

```python
# ❌ Bad: Too few salts (still skewed)
salted_df = salt_dataframe(df, "key", 3)  # Hot key still concentrated

# ❌ Bad: Too many salts (overhead)
salted_df = salt_dataframe(df, "key", 1000)  # Excessive processing

# ✅ Good: Adaptive salt count based on skew severity
skew_percentage = calculate_skew_percentage(df, "key")
num_salts = max(5, min(50, int(skew_percentage / 2)))
```

### Mistake 4: Not Monitoring After Fixes

```python
# ❌ Bad: Apply skew fix, never check if it worked
# Result: False sense of security, persistent issues

# ✅ Good: Monitor skew metrics after implementing fixes
# Set up alerts for skew regression
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What is data skew in Spark and why does it matter?**
2. **How do you detect data skew?**
3. **What are strategies to handle data skew?**
4. **When should you use salting vs other techniques?**
5. **How does adaptive query execution help with skew?**

### Answers:
- **Data skew**: Uneven data distribution causing slow tasks and performance issues
- **Detection**: Monitor partition sizes, task durations, Spark UI metrics
- **Strategies**: Salting, isolation, pre-aggregation, custom partitioning, AQE
- **Salting**: Best for joins/aggregations with hot keys, distributes load evenly
- **AQE**: Automatically detects and handles skew by splitting large partitions

## 📚 Summary

### Skew Handling Decision Tree:

```
Data Skew Detected?
    │
    ├── Yes → Analyze severity
    │   │
    │   ├── Severe (>10% in hot keys) → Salting
    │   ├── Moderate (5-10%) → Isolation
    │   └── Mild (2-5%) → Pre-aggregation
    │
    └── No → Monitor for future skew
```

### Key Metrics to Monitor:
- **Skew ratio**: Max partition / avg partition (>2.0 concerning)
- **Hot key percentage**: Data in largest key (>5% problematic)
- **Task duration variance**: Between fastest/slowest tasks
- **Memory usage**: Per executor (OOM indicators)

### Best Practices:
- **Detect early**: Monitor partition sizes and key distributions
- **Choose appropriate strategy**: Match technique to skew severity
- **Test thoroughly**: Validate fixes work with production data
- **Monitor continuously**: Set up alerts for skew regression
- **Use AQE**: Enable adaptive execution for automatic handling

**Mastering skew handling separates good Spark developers from great ones!**

---

**🎉 You now have the power to handle any data skew scenario!**
