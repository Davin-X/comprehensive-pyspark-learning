# 🪟 Windowing Operations: Time-Based Aggregations

## 🎯 Overview

**Windowing operations** in Structured Streaming enable time-based aggregations on streaming data. Windows allow you to group and analyze data within specific time intervals, making it possible to compute metrics like "sales in the last hour" or "user activity in the last 5 minutes" on continuously arriving data.

## 🔍 Understanding Windows

### Window Types in Spark Streaming

**Spark Structured Streaming supports several windowing strategies:**

1. **Tumbling Windows**: Fixed-size, non-overlapping windows
2. **Sliding Windows**: Fixed-size windows that slide by a smaller interval
3. **Session Windows**: Dynamic windows based on activity gaps

### Window Anatomy

```python
# Basic window structure
window(col("timestamp"), "10 minutes")  # Tumbling window
window(col("timestamp"), "10 minutes", "5 minutes")  # Sliding window
```

**Windows group records by time intervals for aggregation.**

## 🎯 Tumbling Windows

### Fixed-Size, Non-Overlapping Windows

**Tumbling windows** divide time into fixed, non-overlapping intervals.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, sum as sum_func, avg
import pyspark.sql.functions as F

spark = SparkSession.builder \\
    .appName("Windowing_Operations") \\
    .master("local[*]") \\
    .getOrCreate()

# Create sample streaming data
streaming_df = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", 5) \\
    .load() \\
    .withColumn("event_type", F.when(F.col("value") % 3 == 0, "click") \\
                .when(F.col("value") % 3 == 1, "view") \\
                .otherwise("purchase")) \\
    .withColumn("revenue", F.when(F.col("event_type") == "purchase", F.col("value")) \\
                .otherwise(0))

print("Sample streaming data:")
streaming_df.printSchema()
```

### Tumbling Window Aggregations

```python
# Tumbling window aggregations
tumbling_aggregations = streaming_df \\
    .groupBy(
        window("timestamp", "1 minute"),  # 1-minute tumbling windows
        "event_type"
    ) \\
    .agg(
        count("*").alias("event_count"),
        sum_func("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_revenue")
    )

print("Tumbling window aggregations:")
tumbling_aggregations.printSchema()

# Write to console for monitoring
tumbling_query = tumbling_aggregations.writeStream \\
    .outputMode("update") \\
    .format("console") \\
    .option("truncate", "false") \\
    .trigger(processingTime="10 seconds") \\
    .start()

print("Tumbling window query started...")
print("Each output shows aggregations for completed 1-minute windows")

import time
time.sleep(30)
tumbling_query.stop()
print("\\nTumbling window demonstration completed")
```

### Tumbling Window Characteristics

- **Fixed size**: Each window covers exactly the specified duration
- **Non-overlapping**: No record belongs to multiple windows
- **Complete processing**: Windows are processed only when complete
- **Predictable**: Easy to understand and reason about

## 🔄 Sliding Windows

### Overlapping Windows for Smoother Aggregations

**Sliding windows** move by a smaller interval than their size, creating overlapping windows.

```python
# Sliding window aggregations
sliding_aggregations = streaming_df \\
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),  # 5-min window, 1-min slide
        "event_type"
    ) \\
    .agg(
        count("*").alias("event_count"),
        sum_func("revenue").alias("total_revenue"),
        F.approx_count_distinct("value").alias("unique_values")
    )

print("Sliding window aggregations:")
print("Window duration: 5 minutes")
print("Slide interval: 1 minute")
print("Result: Overlapping windows updated every minute")

# Demonstrate sliding windows
sliding_query = sliding_aggregations.writeStream \\
    .outputMode("update") \\
    .format("console") \\
    .option("truncate", "false") \\
    .trigger(processingTime="15 seconds") \\
    .start()

print("\\nSliding window query started...")
print("Notice how windows overlap and update more frequently")

time.sleep(45)
sliding_query.stop()
print("\\nSliding window demonstration completed")
```

### Sliding Window Use Cases

```python
# Different sliding window configurations
sliding_configs = [
    ("5 minutes", "1 minute", "High-frequency updates"),
    ("1 hour", "10 minutes", "Dashboard metrics"),
    ("24 hours", "1 hour", "Daily rolling aggregations"),
    ("7 days", "1 day", "Weekly trends")
]

print("SLIDING WINDOW CONFIGURATIONS:")
for duration, slide, use_case in sliding_configs:
    print(f"  {duration} window, {slide} slide: {use_case}")

# Example: Real-time dashboard metrics
dashboard_metrics = streaming_df \\
    .groupBy(window("timestamp", "10 minutes", "2 minutes")) \\
    .agg(
        count("*").alias("total_events"),
        sum_func(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        sum_func("revenue").alias("revenue"),
        F.avg(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_rate")
    )

print("\\nDashboard-style sliding window aggregations:")
dashboard_metrics.printSchema()
```

## 🎛️ Advanced Windowing Techniques

### Multiple Time Windows

```python
# Multiple window sizes for comprehensive analysis
multi_window_analysis = streaming_df \\
    .withColumn("1min_window", window("timestamp", "1 minute")) \\
    .withColumn("5min_window", window("timestamp", "5 minutes")) \\
    .withColumn("1hour_window", window("timestamp", "1 hour")) \\
    .groupBy("event_type") \\
    .agg(
        # 1-minute metrics
        count(F.when(F.col("1min_window").isNotNull(), 1)).alias("1min_count"),
        # 5-minute metrics  
        count(F.when(F.col("5min_window").isNotNull(), 1)).alias("5min_count"),
        # 1-hour metrics
        count(F.when(F.col("1hour_window").isNotNull(), 1)).alias("1hour_count")
    )

print("Multi-window analysis:")
multi_window_analysis.printSchema()
```

### Window with Additional Grouping

```python
# Windows combined with other dimensions
comprehensive_analysis = streaming_df \\
    .groupBy(
        window("timestamp", "10 minutes"),
        "event_type",
        F.hour("timestamp").alias("hour")  # Additional grouping
    ) \\
    .agg(
        count("*").alias("event_count"),
        sum_func("revenue").alias("total_revenue"),
        F.approx_count_distinct("value").alias("unique_sessions"),
        F.max("value").alias("max_value"),
        F.min("value").alias("min_value")
    ) \\
    .orderBy("window", "event_type", "hour")

print("Comprehensive windowed analysis:")
comprehensive_analysis.printSchema()

# Execute the analysis
analysis_query = comprehensive_analysis.writeStream \\
    .outputMode("complete") \\
    .format("console") \\
    .option("truncate", "false") \\
    .trigger(processingTime="20 seconds") \\
    .start()

time.sleep(40)
analysis_query.stop()
```

### Custom Window Functions

```python
# Custom window-based calculations
custom_window_metrics = streaming_df \\
    .withColumn("window_start", F.window("timestamp", "5 minutes").start) \\
    .withColumn("window_end", F.window("timestamp", "5 minutes").end) \\
    .withColumn("is_peak_hour", 
                F.when(F.hour("timestamp").between(9, 17), "Peak").otherwise("Off-peak")) \\
    .groupBy(window("timestamp", "5 minutes"), "is_peak_hour") \\
    .agg(
        count("*").alias("event_count"),
        sum_func("revenue").alias("revenue"),
        F.avg("value").alias("avg_value"),
        F.stddev("value").alias("value_stddev")
    )

print("Custom window metrics:")
custom_window_metrics.printSchema()
```

## 📊 Window Performance Optimization

### Window State Management

**Windows maintain state across batches - optimize carefully!**

```python
# Monitor window state
def monitor_window_state(query, window_duration):
    """Monitor window state size and performance"""
    
    print(f"Monitoring {window_duration} windows...")
    
    # In production, monitor via Spark UI
    # - State store size
    # - Number of stateful operators
    # - Memory usage per window
    
    print("Key metrics to monitor:")
    print(f"  - State size per {window_duration} window")
    print("  - Memory usage growth rate")
    print("  - Garbage collection frequency")
    print("  - Checkpoint size")
    
    return {
        "window_duration": window_duration,
        "estimated_state_mb": "Varies by data volume",
        "retention_policy": "Based on watermarking"
    }

# Different window state characteristics
window_characteristics = {
    "1 minute": "Low state, high frequency updates",
    "10 minutes": "Moderate state, balanced updates", 
    "1 hour": "High state, low frequency updates",
    "24 hours": "Very high state, batch-like updates"
}

print("WINDOW STATE CHARACTERISTICS:")
for duration, characteristic in window_characteristics.items():
    print(f"  {duration}: {characteristic}")
```

### Optimizing Window Performance

```python
# Performance optimization strategies
window_optimizations = {
    "watermarking": "Limit state growth with watermarks",
    "checkpointing": "Regular state checkpoints",
    "state_timeout": "Automatic state cleanup",
    "parallelism": "Distribute windows across executors",
    "compression": "Compress window state",
    "offloading": "Move cold state to disk"
}

print("\\nWINDOW PERFORMANCE OPTIMIZATIONS:")
for optimization, benefit in window_optimizations.items():
    print(f"  {optimization.upper()}: {benefit}")

# Example: Optimized window configuration
optimized_window = streaming_df \\
    .withWatermark("timestamp", "10 minutes") \\  # Watermark for state cleanup
    .groupBy(window("timestamp", "5 minutes")) \\
    .agg(count("*").alias("count"))

print("\\nOptimized window with watermark:")
optimized_window.printSchema()
```

## 🔍 Window Debugging and Monitoring

### Understanding Window Behavior

```python
# Debug window operations
def debug_window_operations(df, window_spec):
    """Debug and understand window operations"""
    
    print(f"Debugging window: {window_spec}")
    
    # Show window boundaries
    with_windows = df.withColumn("window_info", window_spec)
    
    # Sample window data
    sample_data = with_windows.limit(10).collect()
    
    print("Sample window data:")
    for row in sample_data:
        window_info = row.window_info
        print(f"  Timestamp: {row.timestamp}")
        print(f"  Window: {window_info.start} to {window_info.end}")
        print()
    
    return with_windows

# Debug different window types
print("DEBUGGING WINDOW OPERATIONS:")

# Tumbling window
tumbling_window = window("timestamp", "2 minutes")
debug_tumbling = debug_window_operations(streaming_df, tumbling_window)

# Sliding window  
sliding_window = window("timestamp", "5 minutes", "2 minutes")
debug_sliding = debug_window_operations(streaming_df, sliding_window)
```

### Window Health Monitoring

```python
# Monitor window query health
def monitor_window_health(query, expected_window_duration):
    """Monitor window query performance and health"""
    
    print(f"Monitoring window health (expected: {expected_window_duration})")
    
    # Track metrics
    metrics = {
        "state_size_mb": "Monitor in Spark UI",
        "processing_delay": "Compare window duration vs processing time",
        "memory_growth": "Watch for steady increase",
        "checkpoint_frequency": "Balance with state size"
    }
    
    print("\\nHealth metrics to monitor:")
    for metric, description in metrics.items():
        print(f"  {metric}: {description}")
    
    # Common issues
    issues = [
        "State growing without bound (missing watermark)",
        "Processing slower than window duration",
        "Memory usage increasing steadily",
        "Late data arriving after window closes"
    ]
    
    print("\\nCommon window issues:")
    for issue in issues:
        print(f"  ⚠️  {issue}")
    
    return metrics

# Monitor window health
health_metrics = monitor_window_health(None, "5 minutes")
```

## 🎯 Window Use Cases and Patterns

### Real-World Windowing Patterns

```python
# Pattern 1: Real-time Analytics Dashboard
dashboard_windows = streaming_df \\
    .groupBy(window("timestamp", "1 minute", "10 seconds")) \\
    .agg(
        count("*").alias("events_per_window"),
        sum_func("revenue").alias("revenue_trend"),
        F.approx_count_distinct("value").alias("unique_users")
    )

print("Real-time dashboard pattern:")
print("  - 1-minute windows sliding every 10 seconds")
print("  - Continuous updates for live dashboards")

# Pattern 2: Fraud Detection
fraud_detection = streaming_df \\
    .groupBy(window("timestamp", "5 minutes"), "value") \\
    .agg(count("*").alias("frequency")) \\
    .filter("frequency > 10")  # Unusual frequency

print("\\nFraud detection pattern:")
print("  - Detect unusual activity patterns")
print("  - 5-minute windows for short-term analysis")

# Pattern 3: User Session Analysis
user_sessions = streaming_df \\
    .groupBy(window("timestamp", "30 minutes"), "value") \\  # value as user_id
    .agg(
        count("*").alias("session_events"),
        F.min("timestamp").alias("session_start"),
        F.max("timestamp").alias("session_end"),
        sum_func("revenue").alias("session_revenue")
    )

print("\\nUser session analysis pattern:")
print("  - 30-minute sessions for user behavior")
print("  - Track session duration and activity")

# Pattern 4: Trend Analysis
trend_analysis = streaming_df \\
    .groupBy(window("timestamp", "1 hour", "10 minutes")) \\
    .agg(
        F.avg("value").alias("avg_value"),
        F.stddev("value").alias("value_volatility"),
        F.percentile_approx("value", 0.5).alias("median_value")
    )

print("\\nTrend analysis pattern:")
print("  - 1-hour windows sliding every 10 minutes")
print("  - Statistical analysis of trends")
```

## 🚨 Windowing Challenges and Solutions

### Challenge 1: Late Data

```python
# Problem: Late data arrives after window closes
print("CHALLENGE: Late Data Handling")

# Solution: Use watermarks (covered in separate section)
late_data_solution = """
WITH WATERMARK:
- Define how long to wait for late data
- Automatically drop state for old windows
- Balance latency vs completeness

Example:
df.withWatermark('timestamp', '10 minutes')
  .groupBy(window('timestamp', '5 minutes'))
  .count()
"""

print(late_data_solution)
```

### Challenge 2: State Explosion

```python
# Problem: Too many windows = too much state
print("\\nCHALLENGE: State Explosion")

state_solutions = [
    "Use appropriate window sizes (not too small)",
    "Implement watermarking for state cleanup",
    "Choose sliding windows carefully (overlap increases state)",
    "Monitor state size and alert when too large",
    "Consider reducing window duration for high-volume data"
]

print("Solutions for state explosion:")
for solution in state_solutions:
    print(f"  ✓ {solution}")
```

### Challenge 3: Window Skew

```python
# Problem: Uneven data distribution across windows
print("\\nCHALLENGE: Window Skew")

skew_solutions = [
    "Analyze data distribution before windowing",
    "Choose window sizes that match data patterns",
    "Use salting for known skew patterns",
    "Monitor window sizes and repartition if needed",
    "Consider custom partitioning strategies"
]

print("Solutions for window skew:")
for solution in skew_solutions:
    print(f"  ✓ {solution}")
```

## 🎯 Best Practices for Windowing

### Window Design Guidelines

```python
# Best practices for window design
window_best_practices = {
    "size_selection": "Match window size to business requirements",
    "slide_interval": "Balance update frequency vs computational cost",
    "state_management": "Always use watermarks to limit state growth",
    "performance_monitoring": "Track state size and processing latency",
    "error_handling": "Handle late data and state timeouts",
    "testing": "Test with production-like data volumes and patterns"
}

print("WINDOW DESIGN BEST PRACTICES:")
for practice, guideline in window_best_practices.items():
    print(f"  {practice.upper()}: {guideline}")

# Example: Well-designed window configuration
optimal_window_config = streaming_df \\
    .withWatermark("timestamp", "30 minutes") \\  # Allow 30min for late data
    .groupBy(window("timestamp", "10 minutes", "2 minutes")) \\  # 10min windows, 2min updates
    .agg(
        count("*").alias("event_count"),
        sum_func("revenue").alias("revenue_sum")
    )

print("\\nExample optimal window configuration:")
print("  - Watermark: 30 minutes (handles late data)")
print("  - Window: 10 minutes (reasonable aggregation size)")
print("  - Slide: 2 minutes (frequent updates without overhead)")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What are tumbling and sliding windows in Spark Streaming?**
2. **How do you handle late data in windowed operations?**
3. **What's the difference between processing time and event time windows?**
4. **How do you optimize window performance in Spark?**
5. **What are watermarks and why are they important?**

### Answers:
- **Tumbling vs Sliding**: Tumbling are fixed non-overlapping windows, sliding are overlapping windows that slide by smaller intervals
- **Late data**: Use watermarks to define how long to wait for late data and when to drop window state
- **Processing vs Event time**: Processing time uses wall clock, event time uses data timestamps (more accurate but complex)
- **Optimization**: Use appropriate window sizes, implement watermarks, monitor state size, choose right output modes
- **Watermarks**: Define thresholds for late data, enable automatic state cleanup, balance latency vs completeness

## 📚 Summary

### Windowing Concepts Mastered:

1. **Tumbling Windows**: Fixed-size, non-overlapping intervals
2. **Sliding Windows**: Overlapping windows for smoother aggregations  
3. **Window Functions**: Time-based grouping and aggregation
4. **State Management**: Window state lifecycle and cleanup
5. **Performance Optimization**: Balancing latency vs throughput

### Key Windowing Patterns:

- **Real-time dashboards**: Sliding windows for frequent updates
- **Trend analysis**: Tumbling windows for periodic aggregations
- **Session analysis**: Activity-based windowing
- **Fraud detection**: Short windows for anomaly detection
- **Business metrics**: Various window sizes for KPIs

### Performance Considerations:

- **State size**: Windows maintain state - monitor memory usage
- **Late data**: Watermarks prevent infinite state growth
- **Update frequency**: Balance business needs vs computational cost
- **Output modes**: Choose append/update/complete based on requirements
- **Checkpointing**: Essential for fault tolerance in windowed operations

**Windowing transforms streaming data into actionable insights over time!**

---

**🎉 You now master time-based aggregations in Spark Streaming!**
