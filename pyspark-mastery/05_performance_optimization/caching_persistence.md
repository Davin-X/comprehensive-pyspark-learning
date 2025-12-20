# 💾 Caching & Persistence: Optimizing Data Reuse

## 🎯 Overview

**Caching and persistence** are critical Spark optimizations for reusing expensive computations. Understanding when and how to cache data can improve performance by orders of magnitude, especially for iterative algorithms and complex ETL pipelines.

## 🔍 Understanding Caching in Spark

### What is Caching?

**Caching** stores DataFrames/RDDs in memory (or disk) for fast reuse across multiple operations.

```python
# Cache a DataFrame
df.cache()  # Store in memory
df.count()  # Trigger caching

# Persist with storage levels
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # Memory + disk fallback

# Automatic caching with SQL
spark.sql("CACHE TABLE expensive_table")  # Cache SQL table
```

### Why Caching Matters

1. **Performance**: 10-100x faster for repeated operations
2. **Resource Efficiency**: Avoid recomputing expensive operations
3. **Iterative Algorithms**: Essential for ML training loops
4. **Complex Pipelines**: Reuse intermediate results
5. **Memory Management**: Control data placement

## 📊 Storage Levels

### Available Storage Levels

```python
from pyspark.storagelevel import StorageLevel

# Memory only (fastest, default)
StorageLevel.MEMORY_ONLY          # RAM only, no serialization
StorageLevel.MEMORY_ONLY_SER      # RAM only, serialized (compact)
StorageLevel.MEMORY_ONLY_2        # RAM only, replicated (fault tolerant)

# Memory + Disk (balanced)
StorageLevel.MEMORY_AND_DISK       # RAM preferred, disk fallback
StorageLevel.MEMORY_AND_DISK_SER   # RAM + disk, serialized
StorageLevel.MEMORY_AND_DISK_2     # RAM + disk, replicated

# Disk only (slowest)
StorageLevel.DISK_ONLY             # Disk only
StorageLevel.DISK_ONLY_2           # Disk only, replicated

# Off-heap (experimental)
StorageLevel.OFF_HEAP              # Tachyon/Alluxio
```

### Choosing Storage Levels

```python
def choose_storage_level(memory_pressure, fault_tolerance_needed, speed_priority):
    """Choose optimal storage level based on requirements"""
    
    if memory_pressure == "high":
        if fault_tolerance_needed:
            return StorageLevel.MEMORY_AND_DISK_2
        else:
            return StorageLevel.MEMORY_AND_DISK
    elif speed_priority == "high":
        if fault_tolerance_needed:
            return StorageLevel.MEMORY_ONLY_2
        else:
            return StorageLevel.MEMORY_ONLY
    else:
        return StorageLevel.MEMORY_ONLY_SER  # Compact storage

# Usage examples
ml_training = StorageLevel.MEMORY_ONLY  # Fast iteration
batch_processing = StorageLevel.MEMORY_AND_DISK  # Reliable fallback
streaming_data = StorageLevel.MEMORY_ONLY_SER  # Memory efficient
```

## 🎯 When to Cache

### Cache This Data

```python
# 1. Frequently accessed lookup tables
countries_df = spark.read.parquet("countries/")
countries_df.cache()  # Used in multiple joins

# 2. Intermediate results in complex pipelines
raw_data = spark.read.json("input/")
cleaned_data = raw_data.filter("status = 'valid'").cache()
# Used for multiple downstream operations

# 3. Small dimension tables
products_df = spark.read.parquet("products/")
products_df.persist(StorageLevel.MEMORY_ONLY)  # < 10MB

# 4. Results of expensive aggregations
monthly_sales = sales_df.groupBy("month").sum("amount").cache()
# Used for multiple reports

# 5. ML feature engineering
features_df = raw_data.select("feature1", "feature2", "label").cache()
# Reused in training loop
```

### Don't Cache This Data

```python
# ❌ Don't cache large fact tables
large_fact_table = spark.read.parquet("1TB_sales/")  # Don't cache!
# Use partitioning instead

# ❌ Don't cache streaming data
streaming_df = spark.readStream.format("kafka").load()  # Don't cache
# Cache processed results instead

# ❌ Don't cache temporary results
temp_calc = df.withColumn("temp", col("a") + col("b"))  # Don't cache
# Use for immediate operation only

# ❌ Don't over-cache (memory pressure)
df1.cache()
df2.cache()
df3.cache()  # May cause OOM
# Cache selectively
```

## ⚡ Caching Best Practices

### Strategic Caching Pattern

```python
def optimize_etl_with_caching(raw_data):
    """ETL pipeline with strategic caching"""
    
    # Step 1: Clean and validate (cache for multiple uses)
    cleaned_data = raw_data.filter("quality_score > 0.8").cache()
    
    # Step 2: Enrich with lookups (don't cache intermediate)
    enriched_data = cleaned_data.join(
        F.broadcast(countries_df), "country_code"
    ).join(
        F.broadcast(products_df), "product_id"
    )
    
    # Step 3: Aggregate results (cache final aggregations)
    sales_by_country = enriched_data.groupBy("country").sum("amount").cache()
    sales_by_product = enriched_data.groupBy("product_id").sum("amount").cache()
    
    # Step 4: Generate reports (use cached data)
    reports = {
        "country_sales": sales_by_country.toPandas(),
        "product_sales": sales_by_product.toPandas(),
        "total_summary": enriched_data.agg(F.sum("amount")).collect()
    }
    
    # Step 5: Cleanup
    cleaned_data.unpersist()
    sales_by_country.unpersist()
    sales_by_product.unpersist()
    
    return reports

# Usage
results = optimize_etl_with_caching(raw_sales_df)
```

### Cache Lifecycle Management

```python
class CacheManager:
    """Manage DataFrame caching lifecycle"""
    
    def __init__(self):
        self.cached_dfs = {}
    
    def cache_df(self, df, name, storage_level=StorageLevel.MEMORY_ONLY):
        """Cache DataFrame with tracking"""
        df.persist(storage_level)
        self.cached_dfs[name] = df
        print(f"✅ Cached: {name}")
        return df
    
    def get_cached(self, name):
        """Get cached DataFrame"""
        return self.cached_dfs.get(name)
    
    def unpersist_all(self):
        """Clean up all cached DataFrames"""
        for name, df in self.cached_dfs.items():
            try:
                df.unpersist()
                print(f"🗑️  Uncached: {name}")
            except Exception as e:
                print(f"⚠️  Failed to unpersist {name}: {e}")
        
        self.cached_dfs.clear()
    
    def show_cache_status(self):
        """Show current cache status"""
        print("=== Cache Status ===")
        for name, df in self.cached_dfs.items():
            try:
                count = df.count()  # Trigger computation if needed
                print(f"📊 {name}: {count:,} rows")
            except:
                print(f"📊 {name}: (not computed yet)")

# Usage
cache_mgr = CacheManager()

# Cache important DataFrames
cache_mgr.cache_df(countries_df, "countries")
cache_mgr.cache_df(products_df, "products", StorageLevel.MEMORY_AND_DISK)

# Use cached data
enriched = orders_df.join(cache_mgr.get_cached("countries"), "country_code")

# Cleanup when done
cache_mgr.unpersist_all()
```

## 🔄 Cache vs Persist

### Understanding the Difference

```python
# cache() - convenience method
df.cache()  # Equivalent to persist(MEMORY_ONLY)

# persist() - full control
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Key differences:
# - cache(): Always MEMORY_ONLY, no serialization
# - persist(): Full control over storage level
# - Both lazy: Data cached on first action
```

### Choosing Between Cache and Persist

```python
# Use cache() for:
# - Simple memory caching
# - Development/prototyping
# - Small datasets (< 10GB)
# - When serialization overhead not critical

df.cache()  # Simple, fast, good for most cases

# Use persist() for:
# - Large datasets (> 10GB)
# - Memory-constrained environments
# - Fault-tolerant requirements
# - Custom storage strategies

df.persist(StorageLevel.MEMORY_AND_DISK_2)  # Replicated, with disk fallback
```

## 📈 Monitoring Cache Performance

### Cache Hit Analysis

```python
def analyze_cache_performance(df, operation_name="DataFrame"):
    """Analyze caching performance and memory usage"""
    
    print(f"=== Cache Analysis: {operation_name} ===")
    
    # Check if cached
    if hasattr(df, '_cached') or df.storageLevel != StorageLevel.NONE:
        print("✅ DataFrame is cached")
        print(f"Storage Level: {df.storageLevel}")
    else:
        print("❌ DataFrame is not cached")
        return
    
    # Memory estimation
    try:
        row_count = df.count()
        sample_row = df.limit(1).collect()
        if sample_row:
            row_size = len(str(sample_row[0]))
            estimated_mb = (row_count * row_size) / (1024 * 1024)
            print(",.0f"    except Exception as e:
        print(f"⚠️  Could not estimate memory usage: {e}")
    
    # Performance test
    import time
    start_time = time.time()
    count_result = df.count()  # Fast if cached
    elapsed = time.time() - start_time
    
    if elapsed < 0.1:  # Very fast = likely cached
        print(".3f"    elif elapsed < 1.0:  # Reasonably fast
        print(".3f"    else:  # Slow = likely not cached or computed
        print(".3f"    
    return {
        'is_cached': True,
        'row_count': row_count,
        'estimated_mb': estimated_mb if 'estimated_mb' in locals() else None,
        'access_time': elapsed
    }

# Usage
cache_stats = analyze_cache_performance(cached_df, "Sales Data")
```

### Cache Efficiency Metrics

```python
def monitor_cache_efficiency():
    """Monitor overall cache efficiency"""
    
    # Get Spark metrics
    metrics = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
    
    print("=== Spark Cache Efficiency ===")
    
    # RDD storage info
    rdd_storage = spark.sparkContext._jsc.getPersistentRDDs()
    print(f"Persistent RDDs: {len(rdd_storage)}")
    
    # Cache hit estimation (rough)
    # In production, use Spark UI metrics
    
    # Memory usage
    memory_info = spark.sparkContext.getExecutorMemoryStatus()
    print(f"Executor memory status: {len(memory_info)} executors")
    
    # Recommendations
    if len(rdd_storage) > 10:
        print("⚠️  Many cached RDDs - monitor memory usage")
    elif len(rdd_storage) == 0:
        print("ℹ️  No cached data - consider caching frequently used datasets")
    else:
        print("✅ Reasonable caching level")

# Usage
monitor_cache_efficiency()
```

## 🎯 Advanced Caching Patterns

### 1. Conditional Caching

```python
def smart_cache(df, name, size_threshold_mb=100):
    """Cache only if beneficial"""
    
    # Estimate size
    sample = df.limit(1000).collect()
    if sample:
        avg_row_size = sum(len(str(row)) for row in sample) / len(sample)
        estimated_total_mb = (df.count() * avg_row_size) / (1024 * 1024)
        
        if estimated_total_mb < size_threshold_mb:
            print(".0f"            df.cache()
            return True
        else:
            print(".0f"            return False
    
    return False

# Usage
should_cache = smart_cache(large_df, "large_dataset")
```

### 2. Time-based Caching

```python
import time

class TimedCache:
    """Cache with automatic expiration"""
    
    def __init__(self, ttl_seconds=3600):  # 1 hour default
        self.cache = {}
        self.timestamps = {}
        self.ttl = ttl_seconds
    
    def put(self, key, df):
        """Cache DataFrame with timestamp"""
        df.cache()
        self.cache[key] = df
        self.timestamps[key] = time.time()
    
    def get(self, key):
        """Get cached DataFrame, refresh if expired"""
        if key in self.cache:
            if time.time() - self.timestamps[key] > self.ttl:
                # Expired, remove old cache
                self.cache[key].unpersist()
                del self.cache[key]
                del self.timestamps[key]
                return None
            else:
                return self.cache[key]
        return None
    
    def cleanup_expired(self):
        """Remove expired entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp > self.ttl
        ]
        
        for key in expired_keys:
            if key in self.cache:
                self.cache[key].unpersist()
            del self.cache[key]
            del self.timestamps[key]
        
        print(f"Cleaned up {len(expired_keys)} expired cache entries")

# Usage
timed_cache = TimedCache(ttl_seconds=1800)  # 30 minutes

# Cache with expiration
timed_cache.put("hot_data", frequently_used_df)

# Retrieve (auto-refresh if expired)
cached_data = timed_cache.get("hot_data")
```

### 3. Hierarchical Caching

```python
class HierarchicalCache:
    """Multi-level caching strategy"""
    
    def __init__(self):
        self.l1_cache = {}  # Fast, small (MEMORY_ONLY)
        self.l2_cache = {}  # Slower, larger (MEMORY_AND_DISK)
        self.l3_cache = {}  # Disk only (DISK_ONLY)
    
    def cache_l1(self, df, key):
        """Level 1: Fast memory cache"""
        df.persist(StorageLevel.MEMORY_ONLY)
        self.l1_cache[key] = df
        print(f"L1 cached: {key}")
    
    def cache_l2(self, df, key):
        """Level 2: Memory + disk cache"""
        df.persist(StorageLevel.MEMORY_AND_DISK)
        self.l2_cache[key] = df
        print(f"L2 cached: {key}")
    
    def cache_l3(self, df, key):
        """Level 3: Disk cache"""
        df.persist(StorageLevel.DISK_ONLY)
        self.l3_cache[key] = df
        print(f"L3 cached: {key}")
    
    def get(self, key):
        """Get from fastest available cache"""
        if key in self.l1_cache:
            return self.l1_cache[key], "L1"
        elif key in self.l2_cache:
            return self.l2_cache[key], "L2"
        elif key in self.l3_cache:
            return self.l3_cache[key], "L3"
        else:
            return None, None
    
    def optimize_caching(self, df, key, size_mb):
        """Automatically choose cache level based on size"""
        if size_mb < 100:  # Small
            self.cache_l1(df, key)
        elif size_mb < 1000:  # Medium
            self.cache_l2(df, key)
        else:  # Large
            self.cache_l3(df, key)

# Usage
cache = HierarchicalCache()

# Auto-optimize based on size
estimated_size = 50  # MB
cache.optimize_caching(frequent_df, "frequent_data", estimated_size)

# Retrieve from fastest cache
data, level = cache.get("frequent_data")
print(f"Retrieved from {level} cache")
```

## 🚨 Common Caching Mistakes

### Mistake 1: Over-caching

```python
# ❌ Bad: Cache everything
df1.cache()
df2.cache()
df3.cache()
df4.cache()  # OOM crash!

# ✅ Better: Cache selectively
important_df.cache()  # Only cache what's needed
lookup_df.persist(StorageLevel.MEMORY_AND_DISK)  # Smart persistence
```

### Mistake 2: Not Triggering Cache

```python
# ❌ Bad: Cache not triggered
df.cache()  # Lazy - not computed yet
result = df.collect()  # First action triggers cache

# ✅ Better: Explicit cache trigger
df.cache()
df.count()  # Trigger caching explicitly
result = df.collect()  # Now uses cache
```

### Mistake 3: Ignoring Cache Invalidation

```python
# ❌ Bad: Stale cached data
df.cache()
df.count()  # Cached

# Data changes but cache not updated
new_df = df.filter("status = 'new'")
# new_df uses old cached df!

# ✅ Better: Recreate cache after changes
new_df = df.filter("status = 'new'")
new_df.cache()  # Cache fresh data
new_df.count()  # Trigger new cache
```

### Mistake 4: Wrong Storage Level

```python
# ❌ Bad: MEMORY_ONLY for large data
huge_df.persist(StorageLevel.MEMORY_ONLY)  # Will cause OOM

# ✅ Better: MEMORY_AND_DISK for large data
huge_df.persist(StorageLevel.MEMORY_AND_DISK)  # Graceful fallback
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What's the difference between cache() and persist()?**
2. **When should you cache data in Spark?**
3. **What are the different storage levels in Spark?**
4. **How do you monitor cache performance?**
5. **What happens if you run out of memory while caching?**

### Answers:
- **cache() vs persist()**: cache() is MEMORY_ONLY convenience method, persist() allows custom storage levels
- **When to cache**: Frequently used data, expensive computations, iterative algorithms
- **Storage levels**: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, with serialization and replication options
- **Monitor cache**: Use Spark UI, check storage tab, monitor memory usage
- **OOM handling**: Use MEMORY_AND_DISK, reduce cache size, or don't cache large datasets

## 📚 Summary

### Caching Strategy:

1. **Identify hot data**: Frequently accessed, expensive to compute
2. **Choose storage level**: Balance speed vs memory vs reliability
3. **Trigger caching**: Use count() or similar to populate cache
4. **Monitor usage**: Check Spark UI for cache hit ratios
5. **Clean up**: Unpersist when no longer needed

### Performance Impact:
- **Cache hit**: ~100x faster than recomputation
- **Memory usage**: 2-3x data size (depends on serialization)
- **Network reduction**: Eliminates shuffle for cached data
- **Iterative speed**: 10-50x faster for ML training

### Best Practices:
- **Cache after filtering/aggregation**: Reduce cached data size
- **Use MEMORY_AND_DISK**: For reliability with large datasets
- **Monitor memory pressure**: Don't over-cache
- **Clean up caches**: Unpersist when done
- **Cache smart**: Focus on high-impact data

**Strategic caching can transform slow jobs into fast ones!**
