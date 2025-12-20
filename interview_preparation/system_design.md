# 🏗️ System Design: PySpark Architecture & Scalability

## 🎯 Overview

**System design interviews** test your ability to design scalable, reliable, and maintainable systems. For PySpark roles, this means architecting data pipelines, distributed processing systems, and big data solutions that handle real-world challenges.

## 🎯 Learning Objectives

- ✅ **Scalable Architecture Design** - Design systems that scale to billions of records
- ✅ **Data Pipeline Architecture** - ETL, ELT, streaming pipeline design patterns
- ✅ **Storage & Processing** - Choosing right storage, optimizing for performance
- ✅ **Fault Tolerance & Reliability** - High availability, disaster recovery
- ✅ **Cost Optimization** - Resource utilization, cloud cost management
- ✅ **Security & Compliance** - Data governance, privacy, compliance
- ✅ **Monitoring & Observability** - Production monitoring, alerting, debugging

## 📋 System Design Framework

### Step-by-Step Design Process

1. **Understand Requirements** - Functional & non-functional requirements
2. **Estimate Scale** - Data volume, throughput, latency requirements  
3. **High-Level Design** - Component architecture, data flow
4. **Detailed Design** - Specific technologies, APIs, schemas
5. **Trade-off Analysis** - Cost, performance, complexity decisions
6. **Failure Scenarios** - Fault tolerance, disaster recovery

### Key Metrics to Consider

- **Throughput**: Records/second, GB/hour processing capacity
- **Latency**: End-to-end processing time, query response time
- **Cost**: Compute, storage, data transfer costs
- **Reliability**: Uptime, data consistency, error handling
- **Scalability**: Horizontal/vertical scaling capabilities
- **Maintainability**: Code quality, monitoring, deployment ease

## 🔧 Data Pipeline Design Patterns

### Lambda Architecture

```
Speed Layer (Real-time)    Serving Layer (Batch + Real-time)
        ↓                           ↓
   Kafka → Spark Streaming → Redis/Cassandra
        ↓                           ↓
Batch Layer (Historical)    Batch Views → Druid/Presto
        ↓                           ↓
   HDFS/S3 → Spark Batch → Parquet/Delta
```

**PySpark Implementation:**
```python
# Batch Layer - Historical processing
def batch_layer_pipeline():
    spark = SparkSession.builder.appName("BatchLayer").getOrCreate()
    
    # Read historical data
    historical_df = spark.read.parquet("s3://data-lake/historical/")
    
    # Complex transformations
    processed_df = (historical_df
        .withColumn("processed_at", current_timestamp())
        .groupBy(window("event_time", "1 day"))
        .agg(sum("revenue").alias("daily_revenue"))
    )
    
    # Write batch views
    processed_df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet("s3://data-lake/batch-views/")
    
    return processed_df

# Speed Layer - Real-time processing  
def speed_layer_pipeline():
    spark = SparkSession.builder.appName("SpeedLayer").getOrCreate()
    
    # Read streaming data
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load()
    
    # Simple aggregations for real-time
    real_time_metrics = stream_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window("timestamp", "1 minute")) \
        .agg(count("*").alias("events_per_minute"))
    
    # Write to real-time storage
    query = real_time_metrics.writeStream \
        .format("redis") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    return query

# Serving Layer - Merge batch + real-time
def serving_layer_query(user_query):
    # Query batch views + real-time data
    batch_data = spark.read.parquet("s3://data-lake/batch-views/")
    realtime_data = spark.read.format("redis").load()
    
    # Merge and serve
    combined_data = batch_data.unionByName(realtime_data)
    result = combined_data.filter(user_query)
    
    return result
```

### Kappa Architecture (Simplified Lambda)

```
All data through streaming
           ↓
   Kafka/Event Hub
           ↓
   Spark Streaming (Batch + Real-time)
           ↓
   Delta Lake/ADLS
           ↓
   Serving Layer (Presto/Trino)
```

**Advantages:**
- Simpler architecture (one processing path)
- Easier maintenance and testing
- Better for event-driven systems

**PySpark Implementation:**
```python
def kappa_architecture_pipeline():
    spark = SparkSession.builder \
        .appName("KappaArchitecture") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()
    
    # Single streaming pipeline handles everything
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "all-events") \
        .load()
    
    # Process all data types uniformly
    processed_stream = stream_df \
        .select(from_json(col("value"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())
    
    # Branch processing based on event type
    user_events = processed_stream.filter("event_type = 'user_action'")
    system_events = processed_stream.filter("event_type = 'system_metric'")
    
    # User analytics
    user_analytics = user_events \
        .withWatermark("timestamp", "1 hour") \
        .groupBy("user_id", window("timestamp", "1 hour")) \
        .agg(count("*").alias("actions_per_hour"))
    
    # System monitoring
    system_metrics = system_events \
        .groupBy(window("timestamp", "5 minutes")) \
        .agg(
            avg("cpu_usage").alias("avg_cpu"),
            max("memory_usage").alias("max_memory")
        )
    
    # Write everything to Delta Lake
    user_query = user_analytics.writeStream \
        .format("delta") \
        .option("checkpointLocation", "/tmp/user_checkpoint") \
        .outputMode("append") \
        .trigger(processingTime="10 minutes") \
        .start("s3://data-lake/user-analytics/")
    
    system_query = system_metrics.writeStream \
        .format("delta") \
        .option("checkpointLocation", "/tmp/system_checkpoint") \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .start("s3://data-lake/system-metrics/")
    
    return [user_query, system_query]
```

## 📊 Storage Architecture Design

### Data Lake vs Data Warehouse

| Aspect | Data Lake | Data Warehouse |
|--------|-----------|----------------|
| **Data Type** | Raw, unstructured, semi-structured | Structured, processed |
| **Schema** | Schema-on-read | Schema-on-write |
| **Users** | Data scientists, analysts | Business analysts, reporting |
| **Processing** | ETL, ML training | BI, reporting queries |
| **Cost** | Lower storage costs | Higher compute costs |
| **PySpark Usage** | Heavy (data processing) | Light (data loading) |

### Delta Lake Architecture

```
Raw Data → Bronze Layer → Silver Layer → Gold Layer
    ↓           ↓              ↓            ↓
 S3/ADLS    Raw tables     Clean tables   Business tables
            Partitioned    Transformed    Aggregated
            Time-series    Validated      KPIs/Metrics
```

**PySpark Implementation:**
```python
def medallion_architecture():
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()
    
    # Bronze Layer - Raw ingestion
    def bronze_layer():
        raw_df = spark.read.json("s3://landing-zone/events/")
        
        bronze_df = raw_df \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("source_file", input_file_name())
        
        bronze_df.write \
            .format("delta") \
            .partitionBy("date", "event_type") \
            .save("s3://data-lake/bronze/events/")
        
        return bronze_df
    
    # Silver Layer - Cleansing & standardization
    def silver_layer():
        bronze_df = spark.read.format("delta").load("s3://data-lake/bronze/events/")
        
        silver_df = bronze_df \
            .filter("event_data is not null") \
            .withColumn("user_id", coalesce(col("user_id"), lit("unknown"))) \
            .withColumn("event_timestamp", to_timestamp("timestamp")) \
            .dropDuplicates(["event_id"]) \
            .withColumn("processed_at", current_timestamp())
        
        silver_df.write \
            .format("delta") \
            .partitionBy("date") \
            .save("s3://data-lake/silver/events/")
        
        return silver_df
    
    # Gold Layer - Business aggregations
    def gold_layer():
        silver_df = spark.read.format("delta").load("s3://data-lake/silver/events/")
        
        # User behavior aggregations
        user_metrics = silver_df.groupBy("user_id", "date").agg(
            count("*").alias("daily_events"),
            countDistinct("event_type").alias("unique_event_types"),
            sum("revenue").alias("daily_revenue")
        )
        
        # Product performance
        product_metrics = silver_df.groupBy("product_id", "date").agg(
            count("*").alias("product_views"),
            sum("purchase_amount").alias("total_sales"),
            avg("rating").alias("avg_rating")
        )
        
        user_metrics.write \
            .format("delta") \
            .partitionBy("date") \
            .save("s3://data-lake/gold/user_metrics/")
        
        product_metrics.write \
            .format("delta") \
            .partitionBy("date") \
            .save("s3://data-lake/gold/product_metrics/")
        
        return user_metrics, product_metrics
    
    # Execute pipeline
    bronze_layer()
    silver_layer()
    gold_layer()
    
    print("✅ Medallion architecture pipeline completed")
```

## ⚡ Performance Optimization Strategies

### Cluster Sizing & Configuration

```python
# Production Spark configuration
def production_spark_config():
    config = {
        # Memory settings
        "spark.driver.memory": "8g",
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "4g",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        
        # CPU settings
        "spark.executor.cores": "4",
        "spark.task.cpus": "1",
        "spark.executor.instances": "50",
        
        # Dynamic allocation
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "200",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        
        # Shuffle optimization
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "30s",
        
        # SQL optimization
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        
        # Caching strategy
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
        "spark.sql.inMemoryColumnarStorage.batchSize": "10000"
    }
    
    return config

# Apply configuration
spark_conf = production_spark_config()
spark = SparkSession.builder \
    .appName("ProductionPipeline") \
    .config(spark_conf) \
    .getOrCreate()
```

### Data Partitioning Strategy

```python
def optimal_partitioning_strategy(df, target_size_mb=128):
    """
    Calculate optimal partitioning for target file size
    """
    # Estimate data size
    row_count = df.count()
    sample_row = df.limit(1).collect()[0]
    avg_row_size = len(str(sample_row).encode('utf-8'))
    
    total_size_mb = (row_count * avg_row_size) / (1024 * 1024)
    
    # Calculate optimal partitions
    optimal_partitions = max(1, int(total_size_mb / target_size_mb))
    optimal_partitions = min(optimal_partitions, 1000)  # Cap at reasonable limit
    
    print(f"Data size: {total_size_mb:.1f} MB")
    print(f"Optimal partitions: {optimal_partitions}")
    
    return optimal_partitions

def repartition_for_performance(df, partition_strategy="hash"):
    """
    Repartition DataFrame for optimal performance
    """
    
    if partition_strategy == "hash":
        # Hash partitioning for balanced distribution
        optimal_parts = optimal_partitioning_strategy(df)
        repartitioned_df = df.repartition(optimal_parts)
        
    elif partition_strategy == "range":
        # Range partitioning for sorted access
        repartitioned_df = df.repartitionByRange("timestamp")
        
    elif partition_strategy == "bucket":
        # Bucketing for join optimization
        repartitioned_df = df.repartitionByRange(8, "user_id")
    
    return repartitioned_df

# Usage example
large_df = spark.read.parquet("s3://data-lake/large-table/")
optimized_df = repartition_for_performance(large_df, "hash")

# Write with optimal partitioning
optimized_df.write \
    .mode("overwrite") \
    .partitionBy("date", "region") \
    .parquet("s3://data-lake/optimized-table/")
```

## 🔒 Security & Compliance Design

### Data Governance Framework

```python
# Security and compliance implementation
def secure_data_pipeline():
    spark = SparkSession.builder \
        .config("spark.hadoop.security.authentication", "kerberos") \
        .config("spark.hadoop.security.authorization", "true") \
        .getOrCreate()
    
    # Data classification and masking
    def classify_and_mask_data(df):
        """Apply data classification and masking"""
        
        # PII fields that need masking
        pii_columns = ["email", "phone", "ssn", "credit_card"]
        
        masked_df = df
        for col in pii_columns:
            if col in df.columns:
                # Apply masking based on data classification
                masked_df = masked_df.withColumn(
                    f"{col}_masked",
                    when(col("data_classification") == "public", col(col))
                    .when(col("data_classification") == "internal", 
                          regexp_replace(col(col), ".{4}$", "****"))
                    .otherwise("REDACTED")
                )
        
        return masked_df
    
    # Audit logging
    def audit_data_access(df, user_id, operation):
        """Log data access for compliance"""
        
        audit_record = {
            "timestamp": current_timestamp(),
            "user_id": user_id,
            "operation": operation,
            "record_count": df.count(),
            "columns_accessed": df.columns,
            "source_ip": get_source_ip(),  # Implementation specific
            "query_fingerprint": hash(str(df)),  # Query fingerprint
        }
        
        # Write audit log
        audit_df = spark.createDataFrame([audit_record])
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .save("s3://audit-logs/data-access/")
        
        return df
    
    # Encryption at rest and in transit
    spark.conf.set("spark.io.encryption.enabled", "true")
    spark.conf.set("spark.ssl.enabled", "true")
    
    return {
        "classify_and_mask": classify_and_mask_data,
        "audit_access": audit_data_access
    }

# Usage
security_utils = secure_data_pipeline()
sensitive_data = spark.read.parquet("s3://sensitive-data/")

# Apply security measures
secured_data = security_utils["classify_and_mask"](sensitive_data)
audited_data = security_utils["audit_access"](secured_data, "user123", "READ")
```

## 📊 Monitoring & Alerting Architecture

### Production Monitoring Stack

```python
# Comprehensive monitoring setup
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import time

class SparkMonitoringService:
    def __init__(self, app_name):
        self.app_name = app_name
        self.registry = CollectorRegistry()
        
        # Application metrics
        self.job_duration = Histogram(
            'spark_job_duration_seconds',
            'Job execution time',
            ['job_type', 'status'],
            registry=self.registry
        )
        
        self.data_processed = Counter(
            'spark_data_processed_total',
            'Total records processed',
            ['pipeline_stage'],
            registry=self.registry
        )
        
        self.error_rate = Counter(
            'spark_errors_total',
            'Error count by type',
            ['error_type', 'severity'],
            registry=self.registry
        )
        
        self.resource_usage = Gauge(
            'spark_resource_usage_percent',
            'Resource utilization',
            ['resource_type'],
            registry=self.registry
        )
    
    def monitor_job_execution(self, job_func, job_name):
        """Monitor job execution with metrics"""
        start_time = time.time()
        success = False
        
        try:
            result = job_func()
            success = True
            return result
        except Exception as e:
            self.error_rate.labels(error_type=type(e).__name__, severity="error").inc()
            raise
        finally:
            duration = time.time() - start_time
            status = "success" if success else "failure"
            self.job_duration.labels(job_type=job_name, status=status).observe(duration)
    
    def record_data_processing(self, count, stage):
        """Record data processing metrics"""
        self.data_processed.labels(pipeline_stage=stage).inc(count)
    
    def update_resource_metrics(self):
        """Update resource utilization metrics"""
        # Get Spark metrics
        metrics = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
        
        # Calculate utilization (simplified)
        total_memory = sum(status.maxMem for status in metrics.values())
        used_memory = sum(status.memUsed for status in metrics.values())
        
        utilization = (used_memory / total_memory) * 100 if total_memory > 0 else 0
        self.resource_usage.labels(resource_type="memory").set(utilization)

# Initialize monitoring
monitoring = SparkMonitoringService("production-pipeline")

# Alerting thresholds
ALERT_THRESHOLDS = {
    "job_duration": 3600,  # 1 hour
    "error_rate": 0.05,    # 5% error rate
    "memory_usage": 85,    # 85% memory usage
    "data_lag": 300        # 5 minutes lag
}

def check_alerts(metrics):
    """Check metrics against alert thresholds"""
    alerts = []
    
    if metrics.get("job_duration", 0) > ALERT_THRESHOLDS["job_duration"]:
        alerts.append("Job running too long")
    
    if metrics.get("error_rate", 0) > ALERT_THRESHOLDS["error_rate"]:
        alerts.append("High error rate detected")
    
    if metrics.get("memory_usage", 0) > ALERT_THRESHOLDS["memory_usage"]:
        alerts.append("High memory usage")
    
    return alerts

# Integration with pipeline
@monitoring.monitor_job_execution("data_processing")
def process_data_pipeline():
    """Monitored data processing pipeline"""
    
    # Read data
    df = spark.read.parquet("s3://input-data/")
    monitoring.record_data_processing(df.count(), "input")
    
    # Process data
    processed_df = df.filter("status = 'active'")
    monitoring.record_data_processing(processed_df.count(), "filtered")
    
    # Transform data
    transformed_df = processed_df.withColumn("processed_at", current_timestamp())
    monitoring.record_data_processing(transformed_df.count(), "transformed")
    
    # Write results
    transformed_df.write.parquet("s3://output-data/")
    monitoring.record_data_processing(transformed_df.count(), "output")
    
    return transformed_df

# Execute monitored pipeline
result = process_data_pipeline()

# Check for alerts
current_metrics = {
    "job_duration": monitoring.job_duration._sum.get(),
    "error_rate": monitoring.error_rate._value,
    "memory_usage": monitoring.resource_usage.labels(resource_type="memory")._value
}

alerts = check_alerts(current_metrics)
if alerts:
    print("🚨 ALERTS:", alerts)
    # Send notifications (email, Slack, etc.)
else:
    print("✅ All systems normal")
```

## 🎯 Common System Design Interview Questions

### 1. Design a Real-Time Analytics Dashboard

**Requirements:**
- Process 1M events/second from mobile apps
- Real-time metrics with <5 second latency
- Historical analysis for last 2 years
- 99.9% uptime requirement

**Solution Architecture:**
```python
# High-level design
def realtime_analytics_design():
    return {
        "data_ingestion": {
            "technology": "Kafka",
            "partitions": 100,
            "replication": 3,
            "throughput": "1M msgs/sec"
        },
        "stream_processing": {
            "technology": "Spark Structured Streaming",
            "cluster": "50 executors × 4 cores",
            "watermarking": "10 minutes",
            "checkpointing": "S3"
        },
        "real_time_storage": {
            "technology": "Redis + Cassandra",
            "use_case": "Low-latency queries",
            "retention": "24 hours"
        },
        "historical_storage": {
            "technology": "Delta Lake on S3",
            "compression": "ZSTD",
            "partitioning": "Date + event_type",
            "optimization": "Z-Ordering"
        },
        "serving_layer": {
            "technology": "Presto + Redis",
            "caching": "Redis for hot data",
            "query_engine": "Presto for analytics"
        },
        "monitoring": {
            "metrics": "Prometheus + Grafana",
            "alerting": "PagerDuty integration",
            "logging": "ELK Stack"
        }
    }
```

**Scaling Strategy:**
- Horizontal scaling with Kubernetes
- Auto-scaling based on throughput metrics
- Multi-region deployment for disaster recovery
- CDN for global user base

### 2. Design a Data Lake for Analytics

**Requirements:**
- Store 100PB of data across multiple formats
- Support SQL and ML workloads
- Data governance and security
- Cost-effective storage

**Architecture:**
```python
def data_lake_design():
    return {
        "storage_layer": {
            "technology": "S3/ADLS",
            "structure": "Medallion (Bronze/Silver/Gold)",
            "encryption": "Server-side encryption",
            "versioning": "Enabled for governance"
        },
        "processing_layer": {
            "batch": "Spark on Databricks",
            "streaming": "Spark Structured Streaming",
            "ml": "Spark MLlib + MLflow"
        },
        "metadata_layer": {
            "catalog": "AWS Glue / Azure Purview",
            "schema": "Delta Lake schema enforcement",
            "lineage": "Automatic tracking"
        },
        "security_layer": {
            "authentication": "Azure AD / AWS IAM",
            "authorization": "Ranger / Lake Formation",
            "encryption": "Column-level + transport"
        },
        "governance": {
            "data_quality": "Great Expectations",
            "monitoring": "Datadog integration",
            "compliance": "GDPR/CCPA support"
        }
    }
```

### 3. Design a High-Throughput ETL Pipeline

**Requirements:**
- Process 10TB/day of clickstream data
- <2 hour end-to-end latency
- Handle schema evolution
- Zero data loss guarantee

**Design Decisions:**
- **Ingestion:** Kafka for buffering and reliability
- **Processing:** Spark for distributed processing
- **Storage:** Delta Lake for ACID transactions
- **Orchestration:** Airflow for workflow management
- **Monitoring:** Comprehensive metrics and alerting

### 4. Design for Cost Optimization

**Key Strategies:**
1. **Compute Optimization**
   - Use spot instances for non-critical workloads
   - Auto-scaling based on workload patterns
   - Right-sizing cluster configurations

2. **Storage Optimization**
   - Compression (ZSTD, Snappy)
   - Partitioning and Z-Ordering
   - Lifecycle policies for cold data

3. **Query Optimization**
   - Caching frequently accessed data
   - Materialized views for complex aggregations
   - Query result caching

## 📚 Summary

### System Design Mastered:

1. **Architecture Patterns** - Lambda, Kappa, Medallion
2. **Scalability Design** - Horizontal scaling, partitioning, caching
3. **Reliability Engineering** - Fault tolerance, monitoring, alerting
4. **Security Implementation** - Authentication, encryption, compliance
5. **Cost Optimization** - Resource utilization, cloud cost management
6. **Performance Tuning** - Cluster configuration, query optimization

### Interview Preparation:

- **Structured Approach** - Requirements → Design → Trade-offs
- **Technology Choices** - Justify selections with reasoning
- **Scalability Analysis** - Performance, bottlenecks, solutions
- **Failure Scenarios** - Edge cases, disaster recovery
- **Cost-Benefit Analysis** - Technical vs business trade-offs

**Master system design to architect production-ready PySpark systems! 🚀**

---

**🎉 You now master PySpark system design for interviews and production!**
