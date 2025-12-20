# 📁 File Formats: Optimizing Storage and I/O

## 🎯 Overview

**File format choice** dramatically impacts Spark performance, storage efficiency, and query speed. Different formats optimize for different access patterns, compression, and data structures. Choosing the right format can improve performance by 5-10x.

## 🔍 Understanding File Formats

### Format Characteristics Matrix

| Format | Compression | Splittable | Schema | Best For |
|--------|-------------|------------|--------|----------|
| **Parquet** | Excellent | Yes | Yes | Analytics, columnar access |
| **ORC** | Excellent | Yes | Yes | Hive, columnar access |
| **Avro** | Good | Limited | Yes | Schema evolution, row access |
| **JSON** | Poor | Yes | No | Nested data, human readable |
| **CSV** | Poor | Limited | No | Simple tabular data |
| **Delta** | Excellent | Yes | Yes | ACID transactions, time travel |

**Parquet is the default choice for most Spark workloads!**

## 📊 Parquet: The Analytics Standard

### Parquet Deep Dive

**Parquet** is a columnar storage format optimized for analytics workloads.

```python
# Writing Parquet with optimization
df.write \\
    .mode("overwrite") \\
    .option("compression", "snappy") \\
    .option("parquet.block.size", "256MB") \\
    .option("parquet.page.size", "1MB") \\
    .option("parquet.dictionary.page.size", "1MB") \\
    .parquet("optimized_output/")

# Reading with optimizations
df = spark.read \\
    .option("mergeSchema", "true") \\
    .option("inferSchema", "false") \\
    .parquet("input_path/")
```

### Parquet Performance Features

1. **Columnar Storage**: Read only needed columns
2. **Predicate Pushdown**: Filter at storage level
3. **Dictionary Encoding**: Efficient for low-cardinality columns
4. **Run-Length Encoding**: Compress repeated values
5. **Statistics**: Min/max per column chunk for pruning

```python
# Demonstrate columnar access
large_df = spark.read.parquet("large_dataset/")

# Only read needed columns (fast!)
subset = large_df.select("user_id", "amount", "date")
print(f"Read only 3 columns from {len(large_df.columns)} total")

# Predicate pushdown (filters at storage level)
filtered = large_df.filter("amount > 1000 AND date >= '2023-01-01'")
print("Filter applied during Parquet read, not after")
```

## ⚡ Format Performance Comparison

### Read Performance Test

```python
import time

def benchmark_format_read(format_name, path, query):
    """Benchmark reading different file formats"""
    
    start_time = time.time()
    
    if format_name == "parquet":
        df = spark.read.parquet(path)
    elif format_name == "json":
        df = spark.read.json(path)
    elif format_name == "csv":
        df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Execute query
    result = df.filter(query).count()
    execution_time = time.time() - start_time
    
    return result, execution_time

# Create test data
test_data = [(i, f"name_{i}", i * 10, f"2023-{(i%12)+1:02d}-01") 
             for i in range(100000)]

test_df = spark.createDataFrame(test_data, ["id", "name", "value", "date"])

# Test different formats
formats = ["parquet", "json", "csv"]
results = {}

for fmt in formats:
    path = f"test_data_{fmt}/"
    
    # Write in format
    if fmt == "parquet":
        test_df.write.mode("overwrite").parquet(path)
    elif fmt == "json":
        test_df.write.mode("overwrite").json(path)
    elif fmt == "csv":
        test_df.write.mode("overwrite").option("header", "true").csv(path)
    
    # Read and benchmark
    count, read_time = benchmark_format_read(fmt, path, "value > 500")
    results[fmt] = (count, read_time)
    
    print(f"{fmt.upper()}: {count:,} rows in {read_time:.3f}s")

# Performance analysis
fastest = min(results.keys(), key=lambda k: results[k][1])
slowest = max(results.keys(), key=lambda k: results[k][1])
speedup = results[slowest][1] / results[fastest][1]

print(f"\\n🎯 {fastest.upper()} is {speedup:.1f}x faster than {slowest.upper()}!")
```

## 🗜️ Compression Strategies

### Compression Algorithm Comparison

```python
# Test different compression algorithms
compression_tests = [
    ("none", "No compression"),
    ("snappy", "Fast compression/decompression"),
    ("gzip", "High compression ratio"),
    ("lz4", "Fast with good ratio"),
    ("zstd", "Modern high-performance")
]

test_df = spark.createDataFrame([(i, f"data_{i}" * 100) for i in range(10000)], 
                               ["id", "large_text"])

for codec, description in compression_tests:
    output_path = f"compression_test_{codec}/"
    
    # Write with compression
    test_df.write \\
        .mode("overwrite") \\
        .option("compression", codec) \\
        .parquet(output_path)
    
    # Check file sizes
    import os
    total_size = sum(os.path.getsize(f"{output_path}/{f}") 
                    for f in os.listdir(output_path) if f.endswith('.parquet'))
    
    print(f"{codec.upper()}: {total_size/1024/1024:.1f}MB - {description}")

# Snappy typically provides best balance of speed and compression
```

### Adaptive Compression

```python
def choose_compression_strategy(df, use_case):
    """Choose optimal compression based on use case"""
    
    strategies = {
        "analytics": "snappy",      # Fast reads, good compression
        "archive": "gzip",          # Maximum compression
        "streaming": "lz4",         # Fast compression for streaming
        "mixed": "zstd"             # Modern all-purpose
    }
    
    recommended = strategies.get(use_case, "snappy")
    print(f"Recommended compression for {use_case}: {recommended}")
    
    return recommended

# Usage
compression = choose_compression_strategy(large_df, "analytics")
df.write.option("compression", compression).parquet("output/")
```

## 📊 Schema Evolution and Compatibility

### Schema Evolution in Different Formats

```python
# Parquet schema evolution
# Version 1: Basic schema
base_schema = ["id", "name", "email"]
base_df = spark.createDataFrame([(1, "Alice", "alice@email.com")], base_schema)
base_df.write.mode("overwrite").parquet("user_data_v1/")

# Version 2: Add columns (backwards compatible)
extended_df = spark.createDataFrame([(2, "Bob", "bob@email.com", 25, "Engineer")], 
                                   ["id", "name", "email", "age", "department"])
extended_df.write.mode("append").parquet("user_data_v1/")

# Read with evolved schema
evolved_df = spark.read \\
    .option("mergeSchema", "true") \\
    .parquet("user_data_v1/")

print("Schema evolution in Parquet:")
evolved_df.printSchema()
evolved_df.show()
```

### Format-Specific Schema Handling

```python
# Avro schema evolution
from pyspark.sql.avro.functions import from_avro, to_avro

# Define Avro schema
avro_schema = '''
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
'''

# Write with Avro
df.write \\
    .format("avro") \\
    .option("avroSchema", avro_schema) \\
    .save("user_data_avro/")

# Delta Lake automatic schema evolution
df.write \\
    .format("delta") \\
    .option("mergeSchema", "true") \\
    .mode("append") \\
    .save("user_data_delta/")
```

## ⚡ I/O Optimization Techniques

### Parallelism and Partitioning

```python
# Optimize file parallelism
df.write \\
    .mode("overwrite") \\
    .option("maxRecordsPerFile", 1000000) \\  # Control file sizes
    .option("parquet.block.size", "256MB") \\ # Parquet block size
    .repartition(50) \\                         # Control output files
    .parquet("optimized_output/")

print("Output will have ~50 files, each ~256MB")

# Read optimization
df = spark.read \\
    .option("spark.sql.files.maxPartitionBytes", "128MB") \\  # Input split size
    .option("spark.sql.files.openCostInBytes", "4194304") \\  # 4MB open cost
    .parquet("large_dataset/")
```

### File Size Optimization

```python
def optimize_file_sizes(df, target_file_size_mb=256):
    """Optimize DataFrame for target file sizes"""
    
    # Estimate output size
    sample_size = df.limit(1000).rdd.map(lambda row: len(str(row))).sum()
    estimated_total_bytes = (sample_size / 1000) * df.count()
    estimated_mb = estimated_total_bytes / 1024 / 1024
    
    # Calculate optimal partitions
    optimal_partitions = max(1, int(estimated_mb / target_file_size_mb))
    
    print(f"Estimated output: {estimated_mb:.0f}MB")
    print(f"Target file size: {target_file_size_mb}MB")
    print(f"Optimal partitions: {optimal_partitions}")
    
    # Repartition and write
    df.repartition(optimal_partitions).write \\
        .mode("overwrite") \\
        .parquet("optimized_output/")
    
    return optimal_partitions

# Usage
partitions = optimize_file_sizes(large_df, 128)  # 128MB target
```

## 🎯 Format Selection Guide

### Choose Format by Use Case

```python
def recommend_format(data_characteristics, access_patterns, requirements):
    """Recommend optimal file format based on requirements"""
    
    recommendations = []
    
    # Analytics workload
    if "analytics" in access_patterns:
        recommendations.append(("Parquet", "Columnar access, compression, pushdown"))
    
    # Schema evolution needed
    if "schema_evolution" in requirements:
        recommendations.append(("Avro", "Schema evolution, compatibility"))
        recommendations.append(("Delta", "ACID, schema evolution, time travel"))
    
    # Human readable
    if "human_readable" in requirements:
        recommendations.append(("JSON", "Readable, flexible schema"))
    
    # Hive integration
    if "hive_integration" in requirements:
        recommendations.append(("ORC", "Hive native, good compression"))
    
    # Simple tabular data
    if data_characteristics.get("simple_tabular"):
        recommendations.append(("CSV", "Simple, universal"))
    
    # ACID transactions
    if "acid_transactions" in requirements:
        recommendations.append(("Delta", "ACID, time travel, schema evolution"))
    
    return recommendations

# Usage examples
analytics_rec = recommend_format(
    {"complex_nested": False}, 
    ["analytics", "reporting"], 
    ["performance"]
)
print("Analytics recommendations:", analytics_rec)

streaming_rec = recommend_format(
    {"streaming": True}, 
    ["real_time"], 
    ["low_latency"]
)
print("Streaming recommendations:", streaming_rec)
```

## 📈 Advanced Format Features

### Delta Lake Optimizations

```python
# Delta Lake advanced features
df.write \\
    .format("delta") \\
    .option("optimizeWrite", "true") \\           # Automatic file optimization
    .option("autoCompact", "true") \\             # Automatic compaction
    .option("zOrderBy", "user_id,date") \\        # Z-ordering for query optimization
    .save("optimized_delta/")

# Delta Lake time travel
historical_df = spark.read \\
    .format("delta") \\
    .option("timestampAsOf", "2023-01-01 00:00:00") \\
    .load("optimized_delta/")

# Delta Lake optimization
spark.sql("OPTIMIZE optimized_delta")  # Compact small files
spark.sql("VACUUM optimized_delta RETAIN 168 HOURS")  # Clean old versions
```

### ORC Optimizations

```python
# ORC advanced options
df.write \\
    .format("orc") \\
    .option("orc.compress", "snappy") \\
    .option("orc.stripe.size", "67108864") \\     # 64MB stripes
    .option("orc.row.index.stride", "10000") \\  # Row index every 10k rows
    .option("orc.bloom.filter.columns", "user_id") \\  # Bloom filters
    .save("optimized_orc/")
```

## 📊 Performance Monitoring

### Format Performance Analysis

```python
def analyze_format_performance(df, format_name):
    """Analyze file format performance characteristics"""
    
    print(f"=== {format_name.upper()} Performance Analysis ===")
    
    # Write performance
    write_start = time.time()
    df.write.mode("overwrite").format(format_name).save(f"temp_{format_name}/")
    write_time = time.time() - write_start
    
    # Read performance
    read_start = time.time()
    read_df = spark.read.format(format_name).load(f"temp_{format_name}/")
    count = read_df.count()
    read_time = time.time() - read_start
    
    # Size analysis
    import os
    total_size = sum(
        os.path.getsize(f"temp_{format_name}/{f}") 
        for f in os.listdir(f"temp_{format_name}/") 
        if not f.startswith("_")
    )
    
    compression_ratio = (df.count() * len(df.columns) * 50) / total_size  # Rough estimate
    
    print(f"Write time: {write_time:.3f}s")
    print(f"Read time: {read_time:.3f}s")
    print(f"Total size: {total_size/1024/1024:.1f}MB")
    print(f"Compression ratio: {compression_ratio:.1f}x")
    print(f"Rows/sec write: {df.count()/write_time:,.0f}")
    print(f"Rows/sec read: {count/read_time:,.0f}")
    
    return {
        "write_time": write_time,
        "read_time": read_time,
        "size_mb": total_size/1024/1024,
        "compression_ratio": compression_ratio
    }

# Compare formats
formats = ["parquet", "orc", "json", "csv"]
results = {}

for fmt in formats:
    try:
        results[fmt] = analyze_format_performance(test_df, fmt)
    except Exception as e:
        print(f"Error testing {fmt}: {e}")

# Summary
best_write = min(results.keys(), key=lambda k: results[k]["write_time"])
best_read = min(results.keys(), key=lambda k: results[k]["read_time"])
best_compression = max(results.keys(), key=lambda k: results[k]["compression_ratio"])

print("
🎯 BEST PERFORMERS:")
print(f"Write: {best_write.upper()}")
print(f"Read: {best_read.upper()}")
print(f"Compression: {best_compression.upper()}")
```

## 🚨 Common Format Mistakes

### Mistake 1: Using CSV for Analytics

```python
# ❌ Bad: CSV for large analytics workloads
large_df.write.mode("overwrite").csv("output/")  # Poor compression, slow reads

# ✅ Better: Parquet for analytics
large_df.write.mode("overwrite").parquet("output/")  # Optimized for analytics
```

### Mistake 2: Ignoring Compression

```python
# ❌ Bad: No compression specified
df.write.parquet("output/")  # Uses default (usually good, but explicit is better)

# ✅ Better: Explicit compression
df.write \\
    .option("compression", "snappy") \\
    .parquet("output/")
```

### Mistake 3: Wrong Format for Access Pattern

```python
# ❌ Bad: Parquet for row-by-row access
# Parquet is columnar - bad for single row lookups

# ✅ Better: Choose format for access pattern
# Parquet: Columnar analytics
# Avro: Row-based with schema evolution
# JSON: Flexible nested structures
```

### Mistake 4: Not Considering Schema Evolution

```python
# ❌ Bad: CSV with changing schemas
# Schema changes break CSV files

# ✅ Better: Formats supporting schema evolution
# Parquet, Avro, Delta Lake support schema evolution
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What file format would you choose for analytics workloads?**
2. **Why is Parquet better than CSV for Spark?**
3. **How does compression affect Spark performance?**
4. **What is schema evolution and which formats support it?**
5. **How do you optimize file sizes in Spark?**

### Answers:
- **Analytics format**: Parquet - columnar, compressed, predicate pushdown
- **Parquet vs CSV**: Columnar storage, compression, only read needed columns
- **Compression**: Reduces I/O, but decompression has CPU cost - balance with Snappy
- **Schema evolution**: Avro, Parquet, Delta Lake support adding/removing columns
- **File size optimization**: Control partitions, use appropriate block sizes, monitor skew

## 📚 Summary

### Format Selection Matrix:

| Use Case | Recommended Format | Key Features |
|----------|-------------------|--------------|
| **Analytics** | Parquet | Columnar, compression, pushdown |
| **Streaming** | Avro | Schema evolution, splittable |
| **ACID** | Delta | Transactions, time travel |
| **Hive** | ORC | Hive native, compression |
| **Simple** | CSV | Universal, human readable |
| **Nested** | JSON | Flexible, human readable |

### Performance Optimization:

1. **Choose Parquet** for most analytics workloads
2. **Use Snappy compression** for balance of speed and size
3. **Optimize file sizes** (100MB - 1GB per file)
4. **Enable predicate pushdown** with partitioning
5. **Consider schema evolution** needs
6. **Monitor compression ratios** and read/write performance

### Configuration Best Practices:

```python
# Parquet optimizations
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# General I/O optimizations
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**File format choice can make or break your Spark performance!**

---

**🎉 You now master file formats for optimal Spark performance!**
