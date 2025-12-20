# ❓ Common PySpark Interview Questions & Answers

## 🎯 Overview

**50+ frequently asked PySpark interview questions** with detailed answers covering all major topics. Perfect for interview preparation and quick reference.

## 📋 Question Categories

- ✅ **Basic Concepts** - RDD, DataFrame, Dataset differences
- ✅ **Performance Tuning** - Optimization, caching, partitioning
- ✅ **Data Processing** - Transformations, actions, joins
- ✅ **Streaming** - Real-time processing, windowing
- ✅ **MLlib** - Machine learning pipelines
- ✅ **Troubleshooting** - Common issues and solutions
- ✅ **Architecture** - Cluster setup, resource management
- ✅ **Best Practices** - Production deployment, monitoring

---

## 🔰 **Basic Concepts Questions**

### **Q1: What is the difference between RDD, DataFrame, and Dataset?**

**Answer:**
```
RDD (Resilient Distributed Dataset):
- Low-level API, provides distributed objects
- No schema enforcement, works with any data type
- More control but requires more code
- Best for unstructured data and custom transformations

DataFrame:
- High-level API with schema enforcement
- Optimized execution plans (Catalyst optimizer)
- Better performance for structured data
- SQL-like operations available

Dataset:
- Type-safe version of DataFrame (Scala/Java only)
- Compile-time type checking
- Best of both worlds: type safety + performance
- Not available in Python (DataFrame is the equivalent)
```

**When to use:**
- RDD: Maximum control, unstructured data, custom logic
- DataFrame: Structured data, SQL operations, performance
- Dataset: Type safety required (Scala/Java)

---

### **Q2: Explain lazy evaluation in Spark**

**Answer:**
Lazy evaluation means transformations are not executed immediately but are recorded in a DAG (Directed Acyclic Graph). Actions trigger the actual execution.

**Benefits:**
- Optimization opportunities (pipelining, tree pruning)
- Fault tolerance (recomputation on failure)
- Efficiency (only necessary operations executed)

**Example:**
```python
# Transformations (lazy - not executed)
df = spark.read.csv("data.csv")
filtered = df.filter(col("age") > 21)
grouped = filtered.groupBy("city").count()

# Action (triggers execution)
grouped.show()  # Now the DAG is executed
```

---

### **Q3: What are narrow vs wide transformations?**

**Answer:**
```
Narrow Transformations:
- Each partition depends only on data from a single parent partition
- No shuffling required
- Examples: map, filter, union, coalesce

Wide Transformations:
- Data from multiple partitions needed
- Shuffling across network required
- Examples: groupByKey, reduceByKey, join, distinct
```

**Performance Impact:**
- Narrow: Fast, local operations
- Wide: Expensive, network shuffling, potential skew

---

## ⚡ **Performance Tuning Questions**

### **Q4: How do you optimize a slow Spark job?**

**Answer:**
**Step-by-step optimization approach:**

1. **Identify bottleneck:**
   - Check Spark UI for stage times
   - Look for data skew, spill to disk
   - Monitor executor memory/CPU usage

2. **Data optimization:**
   - Use appropriate file formats (Parquet > CSV)
   - Enable compression (ZSTD, Snappy)
   - Partition data strategically

3. **Resource tuning:**
   - Adjust executor cores/memory
   - Configure dynamic allocation
   - Set proper parallelism

4. **Code optimization:**
   - Prefer DataFrame over RDD where possible
   - Use broadcast joins for small tables
   - Cache frequently used data

5. **Configuration:**
   - Enable adaptive query execution
   - Configure shuffle partitions appropriately
   - Tune JVM garbage collection

---

### **Q5: Explain data skew and how to handle it**

**Answer:**
Data skew occurs when data is unevenly distributed across partitions, causing some tasks to be much slower.

**Detection:**
```python
# Check partition sizes
df.groupBy(spark_partition_id()).count().show()

# Check key distribution
df.groupBy("key").count().orderBy(desc("count")).show()
```

**Solutions:**
1. **Salting technique:**
```python
# Add salt to skewed keys
salted_df = df.withColumn(
    "salted_key", 
    concat(col("key"), lit("_"), (hash(col("key")) % 10).cast("string"))
)

# Join on salted key, then remove salt
```

2. **Broadcast join for small tables**
3. **Repartition with salt:**
```python
df.repartition(100, "salted_key")
```

4. **Increase shuffle partitions**
5. **Use skewed join optimization** (Spark 3.0+)

---

### **Q6: When should you use cache() vs persist()?**

**Answer:**
Both store RDD/DataFrame in memory, but `persist()` offers more storage options.

```python
# cache() - MEMORY_ONLY by default
df.cache()

# persist() - Choose storage level
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY_SER)
```

**Storage Levels:**
- `MEMORY_ONLY`: Fastest, may spill to disk
- `MEMORY_AND_DISK`: Spill to disk if no memory
- `MEMORY_ONLY_SER`: Serialized, saves space
- `DISK_ONLY`: Disk only, slower

**When to use:**
- `cache()`: Quick temporary storage
- `persist()`: Control storage level, long-running jobs

---

## 🔄 **Data Processing Questions**

### **Q7: Explain different join types and their use cases**

**Answer:**
```
Inner Join: Only matching rows (default)
- Use: Find matching records between tables

Left Join: All rows from left, matching from right
- Use: Preserve all records from primary table

Right Join: All rows from right, matching from left  
- Use: Preserve all records from secondary table

Full Outer Join: All rows from both tables
- Use: Combine all data, fill missing with null

Anti Join: Rows in left NOT in right
- Use: Find missing records

Cross Join: Cartesian product
- Use: All combinations (usually avoid for large data)
```

**Performance Considerations:**
- Broadcast join for small tables (< 100MB)
- Sort-merge join for large sorted tables
- Shuffle hash join for unsorted data

---

### **Q8: How do you handle null values in PySpark?**

**Answer:**
Multiple approaches depending on requirements:

```python
# Remove null rows
df.dropna()  # Drop any row with null
df.dropna(subset=["col1", "col2"])  # Specific columns

# Fill null values
df.fillna(0)  # Fill all nulls with 0
df.fillna({"col1": 0, "col2": "unknown"})  # Column-specific

# Replace specific values
df.na.replace({"old_value": "new_value"})

# Filter nulls
df.filter(col("column").isNotNull())
df.filter("column IS NOT NULL")
```

**Best Practices:**
- Understand business requirements for null handling
- Use `coalesce()` for default values
- Consider data quality implications

---

## 📊 **Streaming Questions**

### **Q9: Explain watermarking in Structured Streaming**

**Answer:**
Watermarking handles late-arriving data in streaming applications by specifying how long to wait for late events.

```python
# Define watermark
stream_df = stream_df.withWatermark("timestamp", "10 minutes")

# Use in windowed operations
windowed = stream_df.groupBy(
    window("timestamp", "5 minutes", "1 minute")
).count()
```

**How it works:**
1. Tracks maximum event time seen so far
2. Calculates watermark = max_event_time - delay_threshold
3. Drops events older than watermark
4. Allows state cleanup for completed windows

**Benefits:**
- Handles out-of-order events
- Prevents unbounded state growth
- Balances latency vs correctness

---

### **Q10: Difference between batch processing and streaming?**

**Answer:**
```
Batch Processing:
- Processes data in chunks/fixed intervals
- Higher latency but higher throughput
- Easier debugging and reprocessing
- Examples: Daily ETL jobs, historical analysis

Streaming Processing:
- Processes data as it arrives (real-time)
- Lower latency but more complex
- Handles continuous data flow
- Examples: Real-time analytics, fraud detection
```

**When to choose:**
- **Batch**: Historical analysis, periodic reports, complex transformations
- **Streaming**: Real-time dashboards, alerts, immediate actions, event-driven systems

---

## 🤖 **MLlib Questions**

### **Q11: Explain the ML pipeline in PySpark**

**Answer:**
ML Pipeline consists of transformers and estimators arranged in stages:

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

# Define pipeline stages
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
lr = LogisticRegression(featuresCol="scaled_features", labelCol="label")

# Create pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])

# Fit pipeline
model = pipeline.fit(training_data)

# Transform data
predictions = model.transform(test_data)
```

**Components:**
- **Transformers**: Feature engineering (encoding, scaling)
- **Estimators**: ML algorithms that can be fit
- **Pipeline**: Orchestrates the workflow
- **PipelineModel**: Fitted pipeline for predictions

---

### **Q12: How do you handle imbalanced datasets in PySpark?**

**Answer:**
Several techniques for imbalanced classification:

```python
# 1. Class weights
lr = LogisticRegression(weightCol="class_weights")

# 2. Oversampling minority class
minority_df = df.filter("label = 1")
oversampled = minority_df.sample(withReplacement=True, fraction=2.0)
balanced_df = df.union(oversampled)

# 3. Undersampling majority class
majority_df = df.filter("label = 0").sample(0.5)
balanced_df = majority_df.union(df.filter("label = 1"))

# 4. SMOTE (synthetic oversampling)
# Use third-party libraries or custom UDFs
```

**Evaluation Metrics:**
- Use weighted F1-score, AUC-PR instead of accuracy
- Confusion matrix analysis
- Precision-Recall curves

---

## 🔧 **Troubleshooting Questions**

### **Q13: How do you debug a failing Spark job?**

**Answer:**
Systematic debugging approach:

1. **Check logs:**
```bash
# Driver logs
tail -f $SPARK_HOME/logs/spark-*.out

# Executor logs in YARN
yarn logs -applicationId <app_id>
```

2. **Use Spark UI:**
- Check failed stages and tasks
- Monitor memory usage and GC
- Identify slow tasks (data skew)

3. **Common issues:**
- OutOfMemoryError: Increase executor memory
- Task failed: Check data types, null handling
- Shuffle failed: Network issues, insufficient shuffle space

4. **Debugging tools:**
```python
# Add debug logging
df.withColumn("debug_info", 
    when(col("column").isNull(), "NULL_FOUND")
    .otherwise("OK")
).show()

# Sample problematic data
df.filter("problematic_condition").show()
```

---

### **Q14: What causes 'Container killed by YARN' errors?**

**Answer:**
Usually memory-related issues:

**Causes:**
1. **Executor memory insufficient**
2. **Driver memory too low**
3. **Too many executors with small memory**
4. **Memory leaks in UDFs**
5. **Large broadcast variables**

**Solutions:**
```bash
# Increase executor memory
spark-submit --executor-memory 4g

# Increase driver memory
spark-submit --driver-memory 2g

# Adjust executor instances
spark-submit --num-executors 10

# Check broadcast size
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Default 100MB
```

---

## 🏗️ **Architecture Questions**

### **Q15: How do you choose between different cluster managers?**

**Answer:**
```
Standalone Mode:
- Simple setup, no dependencies
- Good for development/testing
- Limited high availability

YARN (Hadoop):
- Mature, widely used
- Resource management with Hadoop
- Good for large Hadoop deployments

Kubernetes:
- Modern container orchestration
- Auto-scaling and rolling updates
- Cloud-native deployments

Mesos:
- Fine-grained resource sharing
- Complex setup and maintenance
- Less common now
```

**Decision Factors:**
- Existing infrastructure
- Team expertise
- Scalability requirements
- Cloud vs on-premises

---

### **Q16: Explain dynamic allocation in Spark**

**Answer:**
Dynamic allocation allows Spark to scale executors based on workload:

```python
# Enable dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

**How it works:**
1. Starts with minimum executors
2. Monitors pending tasks
3. Scales up when tasks queue up
4. Scales down when executors idle

**Benefits:**
- Efficient resource utilization
- Cost optimization (pay for what you use)
- Automatic scaling based on workload

---

## 📈 **Advanced Questions**

### **Q17: How do you implement exactly-once processing in streaming?**

**Answer:**
Exactly-once requires handling both processing and output:

**Techniques:**
1. **Idempotent operations:** Safe to rerun
2. **Transactional output:** Use Delta Lake or databases with transactions
3. **Checkpointing:** Save progress safely

```python
# Enable checkpointing
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .outputMode("append") \
    .start()

# Use Delta Lake for transactional guarantees
df.write \
    .format("delta") \
    .mode("append") \
    .save("/path/to/delta/table")
```

**Streaming Semantics:**
- **At-least-once:** May process duplicates (default)
- **At-most-once:** May miss records
- **Exactly-once:** Process each record exactly once

---

### **Q18: Explain cost-based optimization in Spark SQL**

**Answer:**
CBO uses table/column statistics to choose optimal execution plans:

```python
# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")

# Analyze tables for statistics
spark.sql("ANALYZE TABLE table_name COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1, col2")
```

**Benefits:**
- Better join order selection
- Optimal physical operators
- Improved filter pushdown
- Statistics-aware decisions

**Statistics tracked:**
- Row count, column nullability
- Min/max values, distinct counts
- Data size and distribution

---

## 🎯 **Interview Tips:**

1. **Explain concepts clearly** - Use examples and diagrams
2. **Know performance implications** - Discuss trade-offs
3. **Practice coding problems** - Focus on transformations and optimizations
4. **Understand architecture** - Explain distributed computing concepts
5. **Be ready for follow-ups** - Prepare deeper dives into complex topics

**Practice these questions regularly to build confidence and expertise! 🚀**
