# 🏗️ Apache Spark Architecture Deep Dive

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.6-red.svg)](https://spark.apache.org/)

> **Understanding the Engine** - Master Spark's distributed computing architecture

## 🎯 Architecture Overview

Apache Spark is a **unified analytics engine** for large-scale data processing. Understanding its architecture is crucial for writing efficient, scalable PySpark applications.

## 🏛️ Core Components

### **Spark Application**
A Spark application consists of:
- **Driver Program**: Runs main() function and creates SparkContext
- **Cluster Manager**: Allocates resources (YARN, Kubernetes, Mesos, or Standalone)
- **Worker Nodes**: Execute tasks and store data
- **Executors**: JVM processes that run tasks and cache data

### **SparkContext vs SparkSession**
```python
# Spark 1.x (RDD-focused)
from pyspark import SparkContext
sc = SparkContext("local[*]", "MyApp")

# Spark 2.x+ (Unified API)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
sc = spark.sparkContext  # Still available
```

## 📊 Data Abstractions

### **Resilient Distributed Datasets (RDDs)**
- **Immutable**: Cannot be changed after creation
- **Distributed**: Partitioned across cluster nodes
- **Fault-tolerant**: Automatically recovered via lineage
- **Lazy evaluation**: Transformations are not executed until actions called

```python
# RDD Creation
rdd = sc.parallelize([1, 2, 3, 4, 5])  # From collection
rdd = sc.textFile("data.txt")           # From file
rdd = spark.sparkContext.textFile("data.txt")  # From SparkSession
```

### **DataFrames & Datasets**
- **Structured data** with schema information
- **Optimized execution** via Catalyst optimizer
- **Type safety** (Datasets in Java/Scala)
- **High-level APIs** for common operations

```python
# DataFrame Creation
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

## 🔄 Execution Model

### **Job Execution Flow**
1. **User Code** → Creates RDD/DataFrame operations
2. **DAG Scheduler** → Builds execution plan (Directed Acyclic Graph)
3. **Task Scheduler** → Submits tasks to cluster manager
4. **Cluster Manager** → Allocates resources and launches executors
5. **Executors** → Execute tasks and return results

### **Narrow vs Wide Transformations**
```python
# Narrow Transformations (No Shuffle)
rdd.map(lambda x: x * 2)        # One-to-one mapping
rdd.filter(lambda x: x > 5)     # Filtering
rdd.union(other_rdd)           # Combining partitions

# Wide Transformations (Require Shuffle)
rdd.groupByKey()               # Data redistribution
rdd.join(other_rdd)            # Key-based matching
rdd.distinct()                 # Removing duplicates
```

## 🎯 Transformations vs Actions

### **Transformations** (Lazy - Return RDD/DataFrame)
```python
# RDD Transformations
rdd.map(func)           # Apply function to each element
rdd.filter(func)        # Filter elements
rdd.flatMap(func)       # Apply function and flatten
rdd.groupByKey()        # Group by key
rdd.reduceByKey(func)   # Aggregate by key
rdd.join(other)         # Join by key

# DataFrame Transformations
df.select(*cols)        # Select columns
df.filter(condition)    # Filter rows
df.groupBy(*cols)       # Group data
df.join(other, on)      # Join DataFrames
df.withColumn(col, expr)# Add column
```

### **Actions** (Eager - Return Results)
```python
# RDD Actions
rdd.collect()           # Return all elements to driver
rdd.take(n)             # Return first N elements
rdd.count()             # Count elements
rdd.first()             # Return first element
rdd.reduce(func)        # Aggregate all elements

# DataFrame Actions
df.show()               # Display DataFrame
df.collect()            # Return all rows
df.count()              # Count rows
df.write.save(path)     # Save data
```

## 🗂️ Storage & Caching

### **Storage Levels**
```python
from pyspark import StorageLevel

# Memory Only
StorageLevel.MEMORY_ONLY           # Fastest, no serialization
StorageLevel.MEMORY_ONLY_SER       # Serialized, less memory
StorageLevel.MEMORY_AND_DISK       # Spill to disk if needed
StorageLevel.MEMORY_AND_DISK_SER   # Serialized + disk spill

# With Replication
StorageLevel.MEMORY_ONLY_2         # Replicated for fault tolerance
StorageLevel.MEMORY_AND_DISK_2     # Replicated + disk spill
```

### **Caching Strategies**
```python
# Cache DataFrame/RDD
df.cache()                                    # MEMORY_AND_DISK
rdd.persist(StorageLevel.MEMORY_ONLY)        # Custom storage level

# Check storage level
print(f"Storage level: {df.storageLevel}")

# Force evaluation (triggers caching)
count = df.count()

# Remove from cache
df.unpersist()
```

## 🚀 Performance Optimization

### **Key Optimization Techniques**

#### **1. Minimize Shuffles**
```python
# Bad: Multiple shuffles
rdd.groupByKey().mapValues(lambda x: sum(x))

# Good: Single shuffle
rdd.reduceByKey(lambda x, y: x + y)
```

#### **2. Use Appropriate Data Structures**
```python
# Use DataFrames for structured data
df = spark.read.parquet("data.parquet")  # Optimized columnar format

# Use RDDs for unstructured/binary data
rdd = sc.binaryFiles("images/*")
```

#### **3. Optimize Joins**
```python
# Broadcast small tables
small_df = spark.createDataFrame(small_data)
large_df.join(broadcast(small_df), "key")

# Use appropriate join types
df1.join(df2, "key", "inner")     # Most common
df1.join(df2, "key", "left_outer") # Preserve left side
```

### **Memory Management**
```python
# Configure memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()
```

## 🔧 Cluster Deployment Modes

### **Local Mode**
```bash
# Single machine, multiple cores
spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()
```

### **Standalone Cluster**
```bash
# Spark's built-in cluster manager
spark = SparkSession.builder \
    .master("spark://master:7077") \
    .getOrCreate()
```

### **YARN (Hadoop)**
```bash
# Hadoop YARN
spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.yarn.queue", "default") \
    .getOrCreate()
```

### **Kubernetes**
```bash
# Kubernetes cluster
spark = SparkSession.builder \
    .master("k8s://https://kubernetes:443") \
    .config("spark.kubernetes.container.image", "my-pyspark:latest") \
    .getOrCreate()
```

## 📈 Monitoring & Debugging

### **Spark UI**
- **Port 4040**: Job execution details
- **Stages**: Task execution breakdown
- **Storage**: Cached RDDs/DataFrames
- **Environment**: Configuration and dependencies

### **Common Performance Issues**
```python
# Monitor job execution
spark.sparkContext.setLogLevel("INFO")

# Check partition count
print(f"Partitions: {rdd.getNumPartitions()}")

# Repartition if needed
optimized_rdd = rdd.repartition(100)  # Increase parallelism
optimized_rdd = rdd.coalesce(10)      # Reduce without shuffle
```

## 🎯 Best Practices

### **Data Organization**
- Use **Parquet** for analytical workloads
- **Compress** data to reduce storage and I/O
- **Partition** data by common filter columns
- **Cache** frequently accessed data

### **Code Optimization**
- **Minimize shuffles** by using `reduceByKey` over `groupByKey`
- **Use broadcast joins** for small tables
- **Persist/cache** intermediate results when reused
- **Tune parallelism** based on cluster size

### **Resource Management**
- **Configure memory** appropriately for your workload
- **Monitor garbage collection** in production
- **Use appropriate storage levels** for caching
- **Scale executors** based on data size

---

**🎯 Understanding Spark architecture is fundamental to writing efficient PySpark applications. Focus on the execution model and optimization techniques for production deployments!**

**📖 Next**: [DataFrame Basics](../../1_core_concepts/dataframe_basics/) - Start with high-level DataFrame operations
