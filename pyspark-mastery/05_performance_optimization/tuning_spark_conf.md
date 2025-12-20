# ⚙️ Tuning Spark Configuration: Cluster Optimization

## 🎯 Overview

**Spark configuration tuning** is critical for optimizing performance, memory usage, and resource utilization in production clusters. Understanding configuration parameters and their impact on different workloads can improve performance by 2-5x. This guide covers essential Spark configurations for optimal performance.

## 🔍 Understanding Spark Architecture

### Key Components and Their Configurations

```
Driver Program
    ├── spark.driver.memory → Driver JVM heap
    ├── spark.driver.cores → Driver CPU cores
    └── spark.driver.maxResultSize → Max result size

Cluster Manager (YARN/K8s)
    ├── spark.executor.instances → Number of executors
    ├── spark.executor.cores → Cores per executor
    └── spark.executor.memory → Memory per executor

Execution Engine
    ├── spark.sql.shuffle.partitions → Shuffle parallelism
    ├── spark.default.parallelism → RDD parallelism
    └── spark.memory.fraction → Memory allocation
```

**Configuration hierarchy: Application > Cluster > Defaults**

## 📊 Memory Configuration

### JVM Memory Management

```python
# Basic memory settings
spark_conf = {
    # Driver memory
    "spark.driver.memory": "4g",                    # Driver heap size
    "spark.driver.memoryOverhead": "1g",            # Off-heap memory
    
    # Executor memory
    "spark.executor.memory": "8g",                  # Executor heap size
    "spark.executor.memoryOverhead": "2g",          # Off-heap memory (executor * 0.1, min 384MB)
    
    # Memory fractions
    "spark.memory.fraction": "0.6",                 # Heap for execution + storage (default 0.6)
    "spark.memory.storageFraction": "0.5",          # Fraction of heap for storage (default 0.5)
}

# Apply configurations
for key, value in spark_conf.items():
    spark.conf.set(key, value)

print("Memory configurations applied")
```

### Memory Tuning Guidelines

```python
def calculate_optimal_memory_config(total_cluster_memory_gb, num_executors, cores_per_executor):
    """Calculate optimal memory configuration for cluster"""
    
    # Reserve memory for OS and overhead (20%)
    available_memory = total_cluster_memory_gb * 0.8
    
    # Calculate per-executor memory
    executor_memory_gb = available_memory / num_executors
    
    # Account for cores (more cores = more overhead)
    if cores_per_executor > 4:
        executor_memory_gb = executor_memory_gb * 0.9  # Reduce for overhead
    
    # Round to reasonable values
    if executor_memory_gb >= 16:
        executor_memory = "16g"
        overhead = "4g"
    elif executor_memory_gb >= 8:
        executor_memory = "8g"
        overhead = "2g"
    elif executor_memory_gb >= 4:
        executor_memory = "4g"
        overhead = "1g"
    else:
        executor_memory = "2g"
        overhead = "512m"
    
    # Driver memory (typically smaller)
    driver_memory = "4g" if executor_memory_gb >= 8 else "2g"
    
    config = {
        "spark.executor.memory": executor_memory,
        "spark.executor.memoryOverhead": overhead,
        "spark.driver.memory": driver_memory,
        "total_cluster_memory": f"{total_cluster_memory_gb}GB",
        "num_executors": num_executors,
        "memory_per_executor": executor_memory
    }
    
    print("Optimal Memory Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    return config

# Example: 100GB cluster, 10 executors
optimal_config = calculate_optimal_memory_config(
    total_cluster_memory_gb=100,
    num_executors=10,
    cores_per_executor=4
)
```

## ⚡ Parallelism and CPU Configuration

### Core Allocation Strategy

```python
# CPU configuration
cpu_config = {
    # Executor cores
    "spark.executor.cores": "4",                    # Cores per executor (2-8 typical)
    
    # Total executors
    "spark.executor.instances": "10",               # Total executor instances
    
    # Driver cores
    "spark.driver.cores": "2",                      # Driver CPU cores
    
    # Parallelism
    "spark.default.parallelism": "200",             # RDD default parallelism
    "spark.sql.shuffle.partitions": "200",          # SQL shuffle partitions
}

# Dynamic calculation based on cluster
def calculate_optimal_parallelism(total_cores, cores_per_executor, safety_factor=0.8):
    """Calculate optimal parallelism settings"""
    
    # Available cores after safety margin
    available_cores = int(total_cores * safety_factor)
    
    # Executors
    num_executors = available_cores // cores_per_executor
    total_executor_cores = num_executors * cores_per_executor
    
    # Parallelism settings
    shuffle_partitions = total_executor_cores * 2  # 2x cores for shuffle
    rdd_parallelism = total_executor_cores * 2     # 2x cores for RDDs
    
    config = {
        "spark.executor.instances": str(num_executors),
        "spark.executor.cores": str(cores_per_executor),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.default.parallelism": str(rdd_parallelism),
        "total_cluster_cores": total_cores,
        "utilized_cores": total_executor_cores,
        "efficiency": f"{(total_executor_cores/total_cores)*100:.1f}%"
    }
    
    print("Optimal Parallelism Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    return config

# Example: 40-core cluster, 4 cores per executor
parallelism_config = calculate_optimal_parallelism(
    total_cores=40,
    cores_per_executor=4,
    safety_factor=0.8
)
```

### Task Scheduling Optimization

```python
# Task scheduling configuration
scheduling_config = {
    # Task scheduling
    "spark.task.cpus": "1",                         # CPUs per task (usually 1)
    "spark.scheduler.mode": "FAIR",                 # FAIR or FIFO
    
    # Speculative execution
    "spark.speculation": "true",                    # Enable speculative execution
    "spark.speculation.interval": "100ms",          # Check interval
    "spark.speculation.multiplier": "1.5",          # Slow task threshold
    "spark.speculation.quantile": "0.75",           # Quantile for speculation
    
    # Dynamic allocation (for YARN/K8s)
    "spark.dynamicAllocation.enabled": "true",      # Enable dynamic allocation
    "spark.dynamicAllocation.minExecutors": "2",    # Minimum executors
    "spark.dynamicAllocation.maxExecutors": "20",   # Maximum executors
    "spark.dynamicAllocation.executorIdleTimeout": "60s",  # Idle timeout
}

# Dynamic allocation is great for:
# - Variable workloads
# - Cost optimization
# - Auto-scaling clusters
```

## 🔄 Shuffle Configuration

### Shuffle Performance Tuning

```python
# Shuffle optimization
shuffle_config = {
    # Shuffle partitions (set to 2x total cores)
    "spark.sql.shuffle.partitions": "200",
    
    # Shuffle buffer
    "spark.shuffle.file.buffer": "32k",              # Buffer size for shuffle files
    
    # Shuffle service (for external shuffle)
    "spark.shuffle.service.enabled": "true",         # Enable external shuffle service
    "spark.shuffle.service.port": "7337",            # Shuffle service port
    
    # Sort shuffle
    "spark.shuffle.sort.bypassMergeThreshold": "200", # Bypass merge for small shuffles
    
    # Compression
    "spark.shuffle.compress": "true",                # Compress shuffle data
    "spark.shuffle.spill.compress": "true",          # Compress spilled data
    
    # Spill thresholds
    "spark.shuffle.spill.numElementsForceSpillThreshold": "1000000",  # Spill threshold
}

# Shuffle monitoring
def monitor_shuffle_performance():
    """Monitor shuffle performance metrics"""
    
    print("Shuffle Performance Monitoring:")
    
    # Check current shuffle partitions
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"Shuffle partitions: {shuffle_partitions}")
    
    # Recommended: 2x total executor cores
    executor_instances = int(spark.conf.get("spark.executor.instances", "1"))
    executor_cores = int(spark.conf.get("spark.executor.cores", "1"))
    total_cores = executor_instances * executor_cores
    recommended = total_cores * 2
    
    print(f"Total executor cores: {total_cores}")
    print(f"Recommended shuffle partitions: {recommended}")
    
    if int(shuffle_partitions) != recommended:
        print(f"⚠️  Consider setting spark.sql.shuffle.partitions={recommended}")
    else:
        print("✅ Shuffle partitions optimally configured")

# Usage
monitor_shuffle_performance()
```

## 📊 Storage and I/O Configuration

### Storage System Tuning

```python
# Storage and I/O configuration
storage_config = {
    # Parquet optimizations
    "spark.sql.parquet.compression.codec": "snappy",    # Compression codec
    "spark.sql.parquet.filterPushdown": "true",         # Predicate pushdown
    "spark.sql.parquet.mergeSchema": "true",            # Schema merging
    
    # ORC optimizations
    "spark.sql.orc.compression.codec": "snappy",        # ORC compression
    "spark.sql.orc.filterPushdown": "true",             # ORC pushdown
    
    # File system settings
    "spark.hadoop.fs.s3a.connection.maximum": "100",     # Max S3 connections
    "spark.hadoop.fs.s3a.threads.max": "50",             # Max S3 threads
    
    # Broadcast configuration
    "spark.sql.autoBroadcastJoinThreshold": "10485760", # 10MB broadcast threshold
}

# File format selection based on use case
def recommend_file_format(use_case, data_size_gb):
    """Recommend optimal file format and configuration"""
    
    if use_case == "analytics":
        format = "parquet"
        compression = "snappy"
        block_size = "256MB"
    elif use_case == "archival":
        format = "parquet"
        compression = "gzip"
        block_size = "512MB"
    elif use_case == "streaming":
        format = "parquet"
        compression = "lz4"
        block_size = "128MB"
    elif use_case == "temporary":
        format = "json"  # Human readable
        compression = "none"
        block_size = "64MB"
    else:
        format = "parquet"
        compression = "snappy"
        block_size = "256MB"
    
    config = {
        "format": format,
        "compression": compression,
        "block_size": block_size,
        "use_case": use_case,
        "data_size": f"{data_size_gb}GB"
    }
    
    print(f"Recommended configuration for {use_case} ({data_size_gb}GB):")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    return config

# Usage
format_config = recommend_file_format("analytics", 100)
```

## ⚡ Adaptive Query Execution

### AQE Configuration (Spark 3.0+)

```python
# Adaptive Query Execution configuration
aqe_config = {
    # Enable AQE
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.forceApply": "false",           # Force AQE even with hints
    
    # Coalesce partitions
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    
    # Skew join optimization
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "10",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    
    # Non-shuffle sort merge join
    "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin": "0.2",
    
    # Logging
    "spark.sql.adaptive.logLevel": "INFO",              # AQE logging level
}

# Apply AQE configuration
for key, value in aqe_config.items():
    spark.conf.set(key, value)

print("Adaptive Query Execution enabled with optimal settings")

# AQE benefits:
# - Automatically adjusts parallelism
# - Handles data skew
# - Optimizes join strategies
# - Reduces post-shuffle partitions
```

## 🎯 Workload-Specific Tuning

### Analytical Workloads

```python
def configure_for_analytics(total_memory_gb=100, total_cores=40):
    """Configure Spark for analytical workloads"""
    
    analytics_config = {
        # Memory: Favor execution over storage
        "spark.memory.fraction": "0.8",                 # More for execution
        "spark.memory.storageFraction": "0.3",          # Less for caching
        
        # Parallelism: High parallelism for aggregations
        "spark.sql.shuffle.partitions": str(total_cores * 3),
        "spark.default.parallelism": str(total_cores * 3),
        
        # Caching: Aggressive caching for repeated queries
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
        
        # Joins: Favor broadcast for small tables
        "spark.sql.autoBroadcastJoinThreshold": "134217728",  # 128MB
        
        # Compression: Fast compression for analytics
        "spark.sql.parquet.compression.codec": "snappy",
    }
    
    print("Analytics workload configuration:")
    for key, value in analytics_config.items():
        print(f"  {key} = {value}")
        spark.conf.set(key, value)
    
    return analytics_config

# Usage
analytics_config = configure_for_analytics()
```

### Streaming Workloads

```python
def configure_for_streaming(batch_interval="30 seconds"):
    """Configure Spark for streaming workloads"""
    
    streaming_config = {
        # Memory: Favor storage for state management
        "spark.memory.fraction": "0.5",
        "spark.memory.storageFraction": "0.6",
        
        # Streaming-specific
        "spark.streaming.backpressure.enabled": "true",       # Enable backpressure
        "spark.streaming.kafka.maxRatePerPartition": "1000",  # Rate limiting
        "spark.streaming.receiver.maxRate": "10000",          # Receiver rate
        
        # Checkpointing
        "spark.streaming.checkpoint.directory": "/tmp/checkpoint",
        
        # Parallelism: Match Kafka partitions
        "spark.streaming.concurrentJobs": "4",
        
        # Compression: Fast for streaming
        "spark.sql.parquet.compression.codec": "lz4",
    }
    
    print("Streaming workload configuration:")
    for key, value in streaming_config.items():
        print(f"  {key} = {value}")
        spark.conf.set(key, value)
    
    return streaming_config

# Usage
streaming_config = configure_for_streaming()
```

### ML Workloads

```python
def configure_for_ml(total_memory_gb=100, total_cores=40):
    """Configure Spark for machine learning workloads"""
    
    ml_config = {
        # Memory: High memory for model training
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "4g",
        
        # Storage: High storage fraction for datasets
        "spark.memory.storageFraction": "0.8",
        
        # Parallelism: Lower for ML stability
        "spark.sql.shuffle.partitions": str(total_cores * 1.5),
        
        # Serialization: Kryo for ML objects
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrationRequired": "false",
        
        # Caching: Essential for iterative algorithms
        "spark.rdd.compress": "true",
        
        # Network: Optimize for ML communication
        "spark.rpc.message.maxSize": "512",  # MB
    }
    
    print("ML workload configuration:")
    for key, value in ml_config.items():
        print(f"  {key} = {value}")
        spark.conf.set(key, value)
    
    return ml_config

# Usage
ml_config = configure_for_ml()
```

## 📈 Configuration Monitoring

### Performance Monitoring Dashboard

```python
def create_configuration_dashboard():
    """Create comprehensive configuration monitoring dashboard"""
    
    print("⚙️  SPARK CONFIGURATION DASHBOARD")
    print("=" * 50)
    
    # Memory configuration
    print("\\n📊 MEMORY CONFIGURATION:")
    memory_configs = [
        "spark.driver.memory",
        "spark.executor.memory", 
        "spark.executor.memoryOverhead",
        "spark.memory.fraction",
        "spark.memory.storageFraction"
    ]
    
    for config in memory_configs:
        value = spark.conf.get(config, "Not set")
        print(f"  {config}: {value}")
    
    # CPU/Parallelism configuration
    print("\\n⚡ CPU & PARALLELISM:")
    cpu_configs = [
        "spark.executor.instances",
        "spark.executor.cores",
        "spark.driver.cores",
        "spark.sql.shuffle.partitions",
        "spark.default.parallelism"
    ]
    
    for config in cpu_configs:
        value = spark.conf.get(config, "Not set")
        print(f"  {config}: {value}")
    
    # Storage configuration
    print("\\n💾 STORAGE & I/O:")
    storage_configs = [
        "spark.sql.parquet.compression.codec",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.adaptive.enabled"
    ]
    
    for config in storage_configs:
        value = spark.conf.get(config, "Not set")
        print(f"  {config}: {value}")
    
    # Performance recommendations
    print("\\n💡 RECOMMENDATIONS:")
    
    # Check memory fraction
    memory_fraction = float(spark.conf.get("spark.memory.fraction", "0.6"))
    if memory_fraction < 0.5:
        print("  ⚠️  Low memory fraction - consider increasing")
    elif memory_fraction > 0.8:
        print("  ⚠️  High memory fraction - monitor for GC pressure")
    else:
        print("  ✅ Memory fraction looks good")
    
    # Check parallelism
    shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
    executor_cores = int(spark.conf.get("spark.executor.cores", "1"))
    executor_instances = int(spark.conf.get("spark.executor.instances", "1"))
    total_cores = executor_cores * executor_instances
    
    if shuffle_partitions < total_cores:
        print("  ⚠️  Shuffle partitions too low for cluster size")
    elif shuffle_partitions > total_cores * 3:
        print("  ⚠️  Shuffle partitions may be too high")
    else:
        print("  ✅ Shuffle partitions appropriately configured")
    
    return {
        "memory_fraction": memory_fraction,
        "shuffle_partitions": shuffle_partitions,
        "total_cores": total_cores
    }

# Usage
dashboard = create_configuration_dashboard()
```

## 🚨 Common Configuration Mistakes

### Mistake 1: Over-allocating Memory

```python
# ❌ Bad: Using all available memory
spark.conf.set("spark.executor.memory", "60g")  # On 64GB node
spark.conf.set("spark.memory.fraction", "0.95")  # 95% of heap

# Result: No memory for JVM overhead, frequent GC, OOM

# ✅ Good: Leave headroom
spark.conf.set("spark.executor.memory", "48g")  # 75% of node memory
spark.conf.set("spark.memory.fraction", "0.8")  # 80% of heap
```

### Mistake 2: Wrong Parallelism Settings

```python
# ❌ Bad: Too few shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")  # On 40-core cluster

# Result: Underutilized cluster, slow shuffles

# ✅ Good: Match cluster capacity
spark.conf.set("spark.sql.shuffle.partitions", "80")  # 2x cores
```

### Mistake 3: Ignoring Compression

```python
# ❌ Bad: No compression configuration
# Uses defaults, may be suboptimal

# ✅ Good: Explicit compression tuning
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.shuffle.compress", "true")
```

### Mistake 4: Static Configurations

```python
# ❌ Bad: Same config for all workloads
# Analytics and streaming use same settings

# ✅ Good: Workload-specific tuning
# Use different configs for different job types
```

## 🎯 Performance Tuning Checklist

### Pre-Job Configuration
- [ ] **Memory allocation**: Set appropriate heap sizes
- [ ] **Core allocation**: Match cluster resources
- [ ] **Parallelism**: Set shuffle partitions to 2x cores
- [ ] **Compression**: Choose optimal codecs
- [ ] **Caching**: Configure storage fractions
- [ ] **AQE**: Enable adaptive execution

### Runtime Monitoring
- [ ] **Memory usage**: Monitor heap utilization
- [ ] **GC pressure**: Watch for long GC pauses
- [ ] **Shuffle spills**: Minimize disk spills
- [ ] **Task skew**: Check task duration variance
- [ ] **Network I/O**: Monitor shuffle data volume
- [ ] **Disk I/O**: Watch storage performance

### Post-Job Analysis
- [ ] **Execution time**: Compare against baselines
- [ ] **Resource utilization**: Check CPU/memory efficiency
- [ ] **Bottlenecks**: Identify slow stages
- [ ] **Configuration impact**: Measure tuning effectiveness
- [ ] **Optimization opportunities**: Plan improvements

## 🎯 Interview Questions

### Common Interview Questions:
1. **How do you tune Spark memory configuration?**
2. **What's the optimal shuffle partition count?**
3. **How do you choose between different compression codecs?**
4. **What is adaptive query execution and when to use it?**
5. **How do you configure Spark for different workloads?**

### Answers:
- **Memory tuning**: Set executor memory to 75% of node RAM, adjust memory fractions based on workload
- **Shuffle partitions**: 2x total executor cores for balance of parallelism and overhead
- **Compression**: Snappy for speed, GZip for size, LZ4 for streaming
- **AQE**: Automatically optimizes query plans, enables in Spark 3.0+ for dynamic adjustments
- **Workload config**: Analytics needs high parallelism, streaming needs backpressure, ML needs Kryo serialization

## 📚 Summary

### Configuration Hierarchy:

1. **Workload Type**: Analytics vs Streaming vs ML
2. **Cluster Resources**: Memory, CPU, network constraints  
3. **Data Characteristics**: Size, format, access patterns
4. **Performance Goals**: Latency vs throughput priorities
5. **Monitoring & Adjustment**: Measure and tune iteratively

### Key Configuration Groups:

| Category | Key Parameters | Impact |
|----------|----------------|---------|
| **Memory** | executor.memory, memory.fraction | Prevents OOM, improves GC |
| **CPU** | executor.cores, shuffle.partitions | Controls parallelism |
| **Storage** | compression.codec, broadcast.threshold | Affects I/O performance |
| **Network** | shuffle service, adaptive execution | Optimizes data movement |
| **Adaptive** | AQE settings, dynamic allocation | Auto-tuning capabilities |

### Performance Tuning Process:

1. **Baseline**: Run with default configurations
2. **Identify bottlenecks**: Memory, CPU, I/O, network
3. **Apply targeted tuning**: Adjust specific parameters
4. **Test and measure**: Compare against baseline
5. **Iterate**: Fine-tune based on results
6. **Monitor**: Set up ongoing performance tracking

**Proper configuration tuning can improve performance by 2-10x!**

---

**🎉 You now master Spark configuration tuning for optimal cluster performance!**
