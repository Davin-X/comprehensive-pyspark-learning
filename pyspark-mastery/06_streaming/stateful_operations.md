# 🔄 Stateful Operations: Maintaining State in Streaming

## 🎯 Overview

**Stateful operations** in Structured Streaming maintain and update state across batches of streaming data. Unlike stateless operations that process each batch independently, stateful operations remember information from previous batches, enabling complex analytics like running totals, session tracking, and deduplication.

## 🔍 Understanding Stateful Operations

### Stateless vs Stateful Processing

**Stateless operations** process each batch independently:
```python
# Stateless: Each batch processed separately
stateless_df = streaming_df \\
    .filter(col("value") > 10) \\
    .select("timestamp", "value")
```

**Stateful operations** maintain state across batches:
```python
# Stateful: Maintains running count across batches
stateful_df = streaming_df \\
    .groupBy("user_id") \\
    .agg(sum("amount").alias("running_total"))
```

### State Storage and Management

**State is automatically managed by Spark:**
- Stored in memory with disk overflow
- Checkpointed for fault tolerance
- Cleaned up based on watermarks
- Distributed across executors

## 📊 Basic Stateful Operations

### Running Aggregations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum as sum_func
import pyspark.sql.functions as F

spark = SparkSession.builder \\
    .appName("Stateful_Operations") \\
    .master("local[*]") \\
    .getOrCreate()

# Create streaming data
streaming_df = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", 3) \\
    .load() \\
    .withColumn("user_id", (col("value") % 5).cast("string")) \\
    .withColumn("amount", col("value") * 10)

print("Streaming data with user_id and amount:")
streaming_df.printSchema()
```

### Global Running Totals

```python
# Global running total (across all data)
global_total = streaming_df \\
    .groupBy() \\  # No grouping = global aggregation
    .agg(
        count("*").alias("total_events"),
        sum_func("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount")
    )

print("Global running aggregations:")
global_total.printSchema()

# Execute global aggregation
global_query = global_total.writeStream \\
    .outputMode("complete") \\  # Complete mode for global aggregations
    .format("console") \\
    .trigger(processingTime="5 seconds") \\
    .start()

print("Global aggregation query started...")
print("Shows running totals across all streaming data")

import time
time.sleep(20)
global_query.stop()
print("\\nGlobal aggregation completed")
```

### Grouped Running Aggregations

```python
# Per-group running aggregations
user_totals = streaming_df \\
    .groupBy("user_id") \\
    .agg(
        count("*").alias("user_events"),
        sum_func("amount").alias("user_total_amount"),
        F.max("amount").alias("max_single_amount"),
        F.min("amount").alias("min_single_amount")
    )

print("\\nPer-user running aggregations:")
user_totals.printSchema()

# Execute user aggregations
user_query = user_totals.writeStream \\
    .outputMode("update") \\  # Update mode for grouped aggregations
    .format("console") \\
    .trigger(processingTime="5 seconds") \\
    .start()

print("Per-user aggregation query started...")
print("Shows running totals for each user")

time.sleep(25)
user_query.stop()
print("\\nUser aggregation completed")
```

## 🪟 Windowed Stateful Operations

### Windowed Aggregations with State

```python
# Windowed aggregations maintain state within time windows
windowed_aggregations = streaming_df \\
    .groupBy(
        window("timestamp", "2 minutes"),  # 2-minute tumbling windows
        "user_id"
    ) \\
    .agg(
        count("*").alias("window_events"),
        sum_func("amount").alias("window_amount"),
        F.collect_list("amount").alias("amounts_list")  # Stateful collection
    )

print("Windowed stateful aggregations:")
windowed_aggregations.printSchema()

# Execute windowed aggregations
window_query = windowed_aggregations.writeStream \\
    .outputMode("update") \\
    .format("console") \\
    .option("truncate", "false") \\
    .trigger(processingTime="8 seconds") \\
    .start()

print("\\nWindowed aggregation query started...")
print("Maintains state within each 2-minute window")

time.sleep(32)
window_query.stop()
print("\\nWindowed aggregation completed")
```

### Sliding Window State

```python
# Sliding windows maintain overlapping state
sliding_state = streaming_df \\
    .groupBy(
        window("timestamp", "3 minutes", "1 minute"),  # 3-min window, 1-min slide
        "user_id"
    ) \\
    .agg(
        count("*").alias("sliding_count"),
        sum_func("amount").alias("sliding_total"),
        F.approx_count_distinct("amount").alias("unique_amounts")
    )

print("\\nSliding window stateful operations:")
sliding_state.printSchema()

# Execute sliding windows
sliding_query = sliding_state.writeStream \\
    .outputMode("update") \\
    .format("console") \\
    .trigger(processingTime="10 seconds") \\
    .start()

print("Sliding window query started...")
print("Overlapping windows share state efficiently")

time.sleep(40)
sliding_query.stop()
```

## 🔄 Advanced Stateful Patterns

### Deduplication with State

```python
# Deduplication using state (exact once processing)
deduplication_df = streaming_df \\
    .withColumn("event_key", F.concat("user_id", F.lit("_"), "value")) \\
    .dropDuplicates("event_key")  # Stateful deduplication

print("Deduplication with state:")
print("Removes duplicate events based on composite key")
deduplication_df.printSchema()

# Execute deduplication
dedup_query = deduplication_df.writeStream \\
    .outputMode("append") \\
    .format("console") \\
    .trigger(processingTime="5 seconds") \\
    .start()

print("\\nDeduplication query started...")

time.sleep(20)
dedup_query.stop()
```

### Session Tracking

```python
# Session tracking with state
# Note: Spark doesn't have built-in session windows yet
# This is a simplified approximation

session_tracking = streaming_df \\
    .withColumn("session_id", 
                F.concat("user_id", F.lit("_"), 
                        F.date_format("timestamp", "yyyy-MM-dd-HH"))) \\
    .groupBy("session_id") \\
    .agg(
        F.min("timestamp").alias("session_start"),
        F.max("timestamp").alias("session_end"),
        count("*").alias("session_events"),
        sum_func("amount").alias("session_amount"),
        F.collect_list("timestamp").alias("event_times")
    )

print("\\nSession tracking (simplified):")
print("Groups events by user and hour")
session_tracking.printSchema()

# Execute session tracking
session_query = session_tracking.writeStream \\
    .outputMode("update") \\
    .format("console") \\
    .trigger(processingTime="10 seconds") \\
    .start()

time.sleep(30)
session_query.stop()
```

### Custom Stateful Operations

```python
# Custom stateful logic using window functions
advanced_state = streaming_df \\
    .withColumn("rank_in_window", 
                F.row_number().over(
                    Window.partitionBy(window("timestamp", "2 minutes"))
                         .orderBy(col("amount").desc())
                )) \\
    .withColumn("cumulative_amount",
                F.sum("amount").over(
                    Window.partitionBy("user_id")
                         .orderBy("timestamp")
                         .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                )) \\
    .filter(col("rank_in_window") <= 3)  # Top 3 amounts per window

print("\\nAdvanced stateful operations:")
print("- Row numbering within windows")
print("- Cumulative sums over user history")
print("- Filtering based on stateful rankings")

advanced_state.printSchema()

# Execute advanced operations
advanced_query = advanced_state.writeStream \\
    .outputMode("append") \\
    .format("console") \\
    .trigger(processingTime="8 seconds") \\
    .start()

time.sleep(32)
advanced_query.stop()
```

## 💾 State Management and Optimization

### State Store Configuration

```python
# Configure state store settings
state_config = {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.stateStore.compression.codec": "lz4",
    "spark.sql.streaming.statefulOperator.stateStoreMaintenanceInterval": "30s",
    "spark.sql.streaming.stateStore.minDeltasForSnapshot": "10",
    "spark.sql.streaming.stateStore.formatValidation.enabled": "false"
}

print("STATE STORE CONFIGURATION:")
for key, value in state_config.items():
    print(f"  {key} = {value}")
    spark.conf.set(key, value)
```

### State Size Monitoring

```python
# Monitor state size and performance
def monitor_stateful_operation(query, operation_name):
    """Monitor stateful operation performance"""
    
    print(f"Monitoring stateful operation: {operation_name}")
    
    # In production, monitor via Spark UI metrics
    monitoring_points = {
        "State Store Size": "Check memory/disk usage in Storage tab",
        "State Store Operations": "Number of get/put operations",
        "State Store Latency": "Average time for state operations",
        "Memory Usage": "JVM heap usage for state",
        "Disk Spill": "State spilled to disk"
    }
    
    print("\\nMonitoring points:")
    for metric, description in monitoring_points.items():
        print(f"  {metric}: {description}")
    
    # Common issues to watch for
    issues = [
        "State growing without bound (missing watermarks)",
        "High latency for state operations",
        "Frequent disk spills",
        "Memory pressure causing GC",
        "State store corruption"
    ]
    
    print("\\nCommon issues to watch:")
    for issue in issues:
        print(f"  ⚠️  {issue}")
    
    return monitoring_points

# Monitor our stateful operations
state_metrics = monitor_stateful_operation(None, "Running Aggregations")
```

### State Cleanup and Optimization

```python
# State cleanup strategies
cleanup_strategies = {
    "watermarks": "Automatic cleanup of old state",
    "state_timeout": "Explicit state expiration",
    "checkpointing": "Regular state snapshots",
    "compaction": "Merge small state entries",
    "filtering": "Remove unnecessary state"
}

print("\\nSTATE CLEANUP STRATEGIES:")
for strategy, benefit in cleanup_strategies.items():
    print(f"  {strategy.upper()}: {benefit}")

# Example: State cleanup with watermarks
watermarked_df = streaming_df \\
    .withWatermark("timestamp", "10 minutes") \\  # Allow 10min for late data
    .groupBy(window("timestamp", "5 minutes"), "user_id") \\
    .agg(sum_func("amount").alias("window_total"))

print("\\nWatermarked stateful operation:")
print("Automatically cleans up state older than watermark")
watermarked_df.printSchema()
```

## 🎯 Stateful Operation Patterns

### Pattern 1: Real-time Counters

```python
# Real-time counters (dashboard metrics)
real_time_counters = streaming_df \\
    .groupBy("user_id") \\
    .agg(
        count("*").alias("total_actions"),
        sum_func("amount").alias("total_spent"),
        F.max("timestamp").alias("last_activity"),
        F.approx_count_distinct("value").alias("unique_products")
    )

print("Real-time counters pattern:")
print("Maintains running totals for each user")
print("Useful for real-time dashboards and personalization")
```

### Pattern 2: Trend Detection

```python
# Trend detection with state
trend_detection = streaming_df \\
    .groupBy(window("timestamp", "10 minutes"), "user_id") \\
    .agg(
        count("*").alias("recent_actions"),
        sum_func("amount").alias("recent_spending"),
        F.avg("amount").alias("avg_recent_amount")
    ) \\
    .withColumn("activity_level",
                F.when(col("recent_actions") > 5, "High")
                 .when(col("recent_actions") > 2, "Medium")
                 .otherwise("Low"))

print("\\nTrend detection pattern:")
print("Analyzes user behavior patterns over time")
print("Enables real-time personalization and recommendations")
```

### Pattern 3: Anomaly Detection

```python
# Anomaly detection with historical state
anomaly_detection = streaming_df \\
    .groupBy("user_id") \\
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.stddev("amount").alias("amount_stddev"),
        F.last("amount").alias("latest_amount"),
        count("*").alias("total_transactions")
    ) \\
    .filter(col("total_transactions") > 3) \\  # Need some history
    .withColumn("is_anomaly",
                F.when(
                    (col("latest_amount") > col("avg_amount") + 2 * col("amount_stddev")) |
                    (col("latest_amount") < col("avg_amount") - 2 * col("amount_stddev")),
                    "Anomaly"
                ).otherwise("Normal"))

print("\\nAnomaly detection pattern:")
print("Compares current behavior against historical patterns")
print("Flags unusual transactions or behavior")
```

### Pattern 4: Sessionization

```python
# Sessionization (grouping related events)
# Simplified sessionization using time windows
sessionized = streaming_df \\
    .groupBy(
        "user_id",
        window("timestamp", "30 minutes")  # 30-minute sessions
    ) \\
    .agg(
        count("*").alias("session_events"),
        F.min("timestamp").alias("session_start"),
        F.max("timestamp").alias("session_end"),
        sum_func("amount").alias("session_value"),
        F.collect_list("value").alias("event_sequence")
    ) \\
    .withColumn("session_duration_minutes",
                (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60)

print("\\nSessionization pattern:")
print("Groups user events into sessions")
print("Analyzes session behavior and engagement")
```

## 🚨 Stateful Operation Challenges

### Challenge 1: State Explosion

```python
# Problem: State grows without bound
print("CHALLENGE: State Explosion")

unbounded_state = streaming_df \\
    .groupBy("user_id") \\
    .agg(
        count("*").alias("lifetime_actions"),  # Grows forever
        F.collect_list("amount").alias("all_amounts")  # Grows forever
    )

print("\\nProblematic unbounded state:")
print("State grows indefinitely, causing memory issues")
print("Solution: Use watermarks and state timeouts")

# Solution: Bounded state with watermarks
bounded_state = streaming_df \\
    .withWatermark("timestamp", "1 hour") \\  # Clean up old state
    .groupBy(window("timestamp", "10 minutes"), "user_id") \\
    .agg(count("*").alias("window_actions"))

print("\\nSolution: Bounded state with watermarks")
print("Automatically cleans up state older than watermark")
```

### Challenge 2: Late Data Handling

```python
# Problem: Late data arrives after state is cleaned up
print("\\nCHALLENGE: Late Data Handling")

late_data_handling = streaming_df \\
    .withWatermark("timestamp", "10 minutes") \\
    .groupBy(window("timestamp", "5 minutes")) \\
    .agg(count("*").alias("window_count"))

print("Late data handling:")
print("- Data arriving after watermark is dropped")
print("- Balances completeness vs state size")
print("- May miss some late-arriving data")
```

### Challenge 3: State Consistency

```python
# Problem: Ensuring state consistency across failures
print("\\nCHALLENGE: State Consistency")

consistency_solutions = [
    "Use checkpointing for automatic recovery",
    "Implement idempotent operations",
    "Use exactly-once processing guarantees",
    "Monitor for state corruption",
    "Test failure scenarios thoroughly"
]

print("State consistency solutions:")
for solution in consistency_solutions:
    print(f"  ✓ {solution}")
```

## ⚡ Performance Optimization for Stateful Operations

### Memory Optimization

```python
# Memory optimization strategies
memory_optimizations = {
    "state_compression": "Compress state data (lz4/gzip)",
    "state_partitioning": "Distribute state across executors",
    "state_caching": "Cache frequently accessed state",
    "memory_monitoring": "Monitor state size and GC pressure",
    "state_cleanup": "Regular cleanup of old state"
}

print("MEMORY OPTIMIZATION FOR STATEFUL OPS:")
for opt, benefit in memory_optimizations.items():
    print(f"  {opt.upper()}: {benefit}")
```

### Performance Monitoring

```python
# Monitor stateful operation performance
def benchmark_stateful_operation(df, operation_name):
    """Benchmark stateful operation performance"""
    
    print(f"Benchmarking: {operation_name}")
    
    # Different stateful operations to test
    operations = {
        "global_agg": lambda: df.groupBy().agg(count("*")),
        "grouped_agg": lambda: df.groupBy("user_id").agg(sum_func("amount")),
        "windowed_agg": lambda: df.groupBy(window("timestamp", "1 minute")).agg(count("*")),
        "complex_state": lambda: df.groupBy("user_id").agg(
            count("*"), sum_func("amount"), F.collect_list("value")
        )
    }
    
    results = {}
    
    for op_name, op_func in operations.items():
        try:
            start_time = time.time()
            result_df = op_func()
            
            # Force execution to measure state creation time
            result_df.writeStream \\
                .outputMode("update") \\
                .format("memory") \\
                .queryName(f"benchmark_{op_name}") \\
                .start()
            
            execution_time = time.time() - start_time
            results[op_name] = execution_time
            
            print(f"  {op_name}: {execution_time:.3f}s")
            
        except Exception as e:
            print(f"  {op_name}: ERROR - {e}")
    
    return results

# Run benchmarks
benchmark_results = benchmark_stateful_operation(streaming_df, "Stateful Operations")
```

## 🎯 Best Practices for Stateful Operations

### Development Best Practices

```python
# Stateful operation best practices
stateful_best_practices = {
    "watermarking": "Always use watermarks to bound state",
    "checkpointing": "Enable checkpointing for fault tolerance",
    "monitoring": "Monitor state size and performance",
    "testing": "Test with realistic data volumes",
    "optimization": "Balance state size vs query performance",
    "documentation": "Document stateful assumptions and limitations"
}

print("STATEFUL OPERATION BEST PRACTICES:")
for practice, guideline in stateful_best_practices.items():
    print(f"  {practice.upper()}: {guideline}")
```

### Production Considerations

```python
# Production considerations for stateful operations
production_considerations = [
    "Plan for state growth and cleanup",
    "Monitor state store performance",
    "Implement proper error handling",
    "Test disaster recovery scenarios",
    "Document stateful operation SLAs",
    "Consider state migration for schema changes",
    "Implement state size alerts",
    "Plan for state store backup/restore"
]

print("\\nPRODUCTION CONSIDERATIONS:")
for consideration in production_considerations:
    print(f"  ✓ {consideration}")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What are stateful operations in Spark Streaming?**
2. **How does Spark manage state in streaming queries?**
3. **What happens to state when a streaming query fails?**
4. **How do you optimize stateful operations performance?**
5. **What are the limitations of stateful operations?**

### Answers:
- **Stateful operations**: Maintain state across batches (aggregations, deduplication, sessions)
- **State management**: Stored in memory with disk overflow, checkpointed, cleaned by watermarks
- **Failure handling**: Checkpointing enables automatic state recovery
- **Optimization**: Use watermarks, monitor state size, choose appropriate output modes, compress state
- **Limitations**: Memory usage, state size limits, complexity of exactly-once guarantees

## 📚 Summary

### Stateful Operations Mastered:

1. **Running Aggregations**: Cumulative totals and statistics
2. **Windowed State**: Time-bounded state management
3. **Deduplication**: Exactly-once processing guarantees
4. **Session Tracking**: User behavior analysis
5. **Custom State Logic**: Complex stateful transformations

### Key Stateful Patterns:

- **Real-time dashboards**: Continuous metric updates
- **Anomaly detection**: Historical comparison
- **Trend analysis**: Pattern recognition over time
- **User personalization**: Behavior-based recommendations
- **Fraud detection**: Unusual pattern identification

### State Management Essentials:

- **Watermarks**: Prevent state explosion
- **Checkpointing**: Fault tolerance
- **Monitoring**: Performance and health tracking
- **Optimization**: Memory and performance tuning
- **Testing**: Realistic load and failure scenarios

### Performance Considerations:

- **State size**: Monitor and control growth
- **Memory usage**: Balance with other operations
- **Update frequency**: Balance freshness vs overhead
- **Fault tolerance**: Checkpointing overhead
- **Scalability**: Distribute state across cluster

**Stateful operations enable complex real-time analytics on streaming data!**

---

**🎉 You now master stateful operations in Spark Streaming!**
