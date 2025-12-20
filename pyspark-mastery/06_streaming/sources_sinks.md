# 🌊 Sources & Sinks: Connecting Streaming Pipelines

## 🎯 Overview

**Sources and sinks** are the entry and exit points for Structured Streaming pipelines. Sources read data from external systems, while sinks write processed results to destinations. Choosing the right sources and sinks is crucial for building robust, scalable streaming applications.

## 📥 Streaming Sources

### Built-in Sources

**Spark Structured Streaming provides several built-in sources:**

1. **File Source**: Read from files (CSV, JSON, Parquet)
2. **Kafka Source**: Read from Apache Kafka topics
3. **Socket Source**: Read from network sockets (development)
4. **Rate Source**: Generate test data (development)

### File Source

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("Streaming_Sources_Sinks") \\
    .master("local[*]") \\
    .getOrCreate()

# File source - reads new files as they appear
file_stream = spark.readStream \\
    .format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("input_directory/")  # Directory to monitor

print("File source streaming DataFrame:")
file_stream.printSchema()

# Options for file sources
file_options = {
    "path": "Directory to monitor for new files",
    "maxFilesPerTrigger": "Maximum files to read per batch (default: no limit)",
    "latestFirst": "Read latest files first (default: false)",
    "fileNameOnly": "Only include filename in metadata (default: false)",
    "maxFileAge": "Maximum age of files to consider"
}

print("\\nFile source options:")
for option, description in file_options.items():
    print(f"  {option}: {description}")
```

### Kafka Source

```python
# Kafka source - most common for production
kafka_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "topic1,topic2") \\  # Topics to subscribe to
    .option("startingOffsets", "latest") \\  # earliest, latest, or specific offsets
    .load()

print("Kafka source streaming DataFrame:")
kafka_stream.printSchema()

# Kafka-specific options
kafka_options = {
    "kafka.bootstrap.servers": "Kafka broker addresses",
    "subscribe": "Comma-separated topic names",
    "subscribePattern": "Regex pattern for topic matching",
    "assign": "JSON string with topic partitions",
    "startingOffsets": "Where to start reading (earliest/latest)",
    "endingOffsets": "Where to stop reading (latest only)",
    "failOnDataLoss": "Fail if data loss detected",
    "maxOffsetsPerTrigger": "Max offsets per batch",
    "minPartitions": "Minimum partition count"
}

print("\\nKafka source options:")
for option, description in kafka_options.items():
    print(f"  {option}: {description}")

# Process Kafka messages
processed_kafka = kafka_stream \\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \\
    .withColumn("processed_value", F.upper("value")) \\
    .withColumn("timestamp", F.current_timestamp())

print("\\nProcessed Kafka stream:")
processed_kafka.printSchema()
```

### Rate Source (Development)

```python
# Rate source for testing and development
rate_stream = spark.readStream \\
    .format("rate") \\
    .option("rowsPerSecond", 5) \\    # Events per second
    .option("rampUpTime", "0s") \\    # Time to ramp up to full rate
    .option("numPartitions", "2") \\  # Number of partitions
    .load()

print("Rate source for development:")
rate_stream.printSchema()

# Rate source produces:
# - timestamp: Event timestamp
# - value: Incremental long value

# Enhanced rate source for testing
test_stream = rate_stream \\
    .withColumn("event_type", 
                F.when(F.col("value") % 3 == 0, "click")
                 .when(F.col("value") % 3 == 1, "view")
                 .otherwise("purchase")) \\
    .withColumn("user_id", (F.col("value") % 100).cast("string")) \\
    .withColumn("amount", F.col("value") * 1.5)

print("\\nEnhanced test stream:")
test_stream.printSchema()
```

### Socket Source (Development)

```python
# Socket source for simple testing
socket_stream = spark.readStream \\
    .format("socket") \\
    .option("host", "localhost") \\
    .option("port", "9999") \\
    .load()

print("Socket source streaming DataFrame:")
socket_stream.printSchema()

# Socket source reads text lines from network socket
# Useful for testing with netcat: nc -lk 9999
```

## 📤 Streaming Sinks

### Built-in Sinks

**Spark Structured Streaming supports several output destinations:**

1. **File Sink**: Write to files (CSV, JSON, Parquet)
2. **Kafka Sink**: Write to Kafka topics
3. **Console Sink**: Print to console (development)
4. **Memory Sink**: Store in memory table (development)
5. **Foreach Sink**: Custom processing per record

### File Sink

```python
# File sink - write to files
file_sink_query = processed_kafka.writeStream \\
    .format("parquet") \\
    .option("path", "output_directory/") \\
    .option("checkpointLocation", "checkpoint_dir/") \\
    .trigger(processingTime="1 minute") \\
    .start()

print("File sink started - writing to Parquet files")
print("Output will be partitioned by date if partitioning enabled")

# File sink options
file_sink_options = {
    "path": "Output directory path",
    "checkpointLocation": "Directory for checkpointing",
    "compression": "Compression codec (gzip, snappy, lz4)",
    "partitionBy": "Columns to partition output by"
}

print("\\nFile sink options:")
for option, description in file_sink_options.items():
    print(f"  {option}: {description}")
```

### Kafka Sink

```python
# Kafka sink - write to Kafka topics
kafka_sink_query = processed_kafka.writeStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("topic", "processed_events") \\
    .option("checkpointLocation", "checkpoint_dir/") \\
    .trigger(processingTime="30 seconds") \\
    .start()

print("Kafka sink started - writing to Kafka topic")
print("Each row becomes a Kafka message")

# Kafka sink options
kafka_sink_options = {
    "kafka.bootstrap.servers": "Kafka broker addresses",
    "topic": "Target topic name",
    "key": "Column to use as message key",
    "value": "Column to use as message value",
    "includeHeaders": "Include Kafka headers"
}

print("\\nKafka sink options:")
for option, description in kafka_sink_options.items():
    print(f"  {option}: {description}")
```

### Console Sink (Development)

```python
# Console sink for development and debugging
console_query = test_stream.writeStream \\
    .format("console") \\
    .option("truncate", "false") \\    # Don't truncate output
    .option("numRows", "10") \\        # Rows per batch to show
    .trigger(processingTime="10 seconds") \\
    .start()

print("Console sink started - showing streaming output")
print("Useful for development and monitoring")

import time
time.sleep(30)
console_query.stop()
```

### Memory Sink (Development)

```python
# Memory sink - store in in-memory table
memory_query = test_stream.limit(100).writeStream \\
    .format("memory") \\
    .queryName("streaming_results") \\  # Table name
    .trigger(once=True) \\              # Run once
    .start()

# Wait for completion
memory_query.awaitTermination()

# Query the in-memory table
result_df = spark.sql("SELECT * FROM streaming_results")
print(f"Memory sink results: {result_df.count()} rows")
result_df.show(5)
```

### Foreach Sink (Custom Processing)

```python
# Foreach sink for custom per-record processing
def process_record(row):
    """Custom processing function"""
    print(f"Processing: {row.user_id}, {row.amount}")
    # Could write to database, send to API, etc.

# Foreach sink
foreach_query = test_stream.writeStream \\
    .foreach(process_record) \\
    .trigger(processingTime="5 seconds") \\
    .start()

print("Foreach sink started - custom processing per record")

time.sleep(20)
foreach_query.stop()
```

## 🔧 Advanced Source/Sink Patterns

### Multiple Sources

```python
# Combine multiple sources
source1 = spark.readStream.format("kafka") \\
    .option("subscribe", "topic1") \\
    .load()

source2 = spark.readStream.format("kafka") \\
    .option("subscribe", "topic2") \\
    .load()

# Union sources
combined_sources = source1.union(source2) \\
    .withColumn("source_topic", F.lit("combined"))

print("Combined multiple Kafka sources:")
combined_sources.printSchema()

# Process combined stream
combined_query = combined_sources.writeStream \\
    .format("console") \\
    .start()

time.sleep(15)
combined_query.stop()
```

### Conditional Sinks

```python
# Route data to different sinks based on conditions
high_value_stream = test_stream.filter("amount > 100")
low_value_stream = test_stream.filter("amount <= 100")

# High-value events to Kafka
high_value_query = high_value_stream.writeStream \\
    .format("kafka") \\
    .option("topic", "high_value_events") \\
    .start()

# Low-value events to files
low_value_query = low_value_stream.writeStream \\
    .format("parquet") \\
    .option("path", "low_value_events/") \\
    .start()

print("Conditional routing: high-value → Kafka, low-value → files")

time.sleep(25)
high_value_query.stop()
low_value_query.stop()
```

### Source with Schema Evolution

```python
# Handle schema changes in sources
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema for structured data
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("properties", StringType(), True)  # JSON string
])

# JSON file source with schema
json_stream = spark.readStream \\
    .schema(event_schema) \\  # Explicit schema
    .json("events/")        # Directory with JSON files

print("JSON source with explicit schema:")
json_stream.printSchema()

# Process with schema evolution handling
processed_json = json_stream \\
    .withColumn("parsed_timestamp", F.to_timestamp("timestamp")) \\
    .withColumn("properties_map", F.from_json("properties", 
        "MAP<STRING, STRING>"))  # Parse JSON properties

print("\\nProcessed JSON stream:")
processed_json.printSchema()
```

## ⚡ Performance Optimization

### Source Optimization

```python
# Optimize file source performance
optimized_file_source = spark.readStream \\
    .format("parquet") \\
    .option("path", "input/") \\
    .option("maxFilesPerTrigger", 10) \\     # Limit files per batch
    .option("maxFileAge", "1d") \\           # Ignore old files
    .option("latestFirst", "true") \\        # Process recent files first
    .load()

print("Optimized file source configuration")
```

### Sink Optimization

```python
# Optimize file sink performance
optimized_file_sink = processed_kafka.writeStream \\
    .format("parquet") \\
    .option("path", "output/") \\
    .option("checkpointLocation", "checkpoint/") \\
    .option("compression", "snappy") \\       # Fast compression
    .partitionBy("year", "month", "day") \\   # Partition output
    .trigger(processingTime="5 minutes") \\   # Batch writes
    .start()

print("Optimized file sink with partitioning and compression")
```

### Kafka Optimization

```python
# Optimize Kafka source
optimized_kafka_source = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "events") \\
    .option("startingOffsets", "latest") \\
    .option("maxOffsetsPerTrigger", 10000) \\     # Limit batch size
    .option("minPartitions", 4) \\                # Minimum partitions
    .option("failOnDataLoss", "false") \\         # Handle data loss gracefully
    .load()

# Optimize Kafka sink
optimized_kafka_sink = processed_kafka.writeStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("topic", "processed_events") \\
    .option("checkpointLocation", "checkpoint/") \\
    .trigger(processingTime="30 seconds") \\      # Batch writes
    .start()

print("Optimized Kafka source and sink configuration")
```

## 🔒 Fault Tolerance and Reliability

### Checkpointing Strategy

```python
# Comprehensive checkpointing
checkpointed_pipeline = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "input_topic") \\
    .option("failOnDataLoss", "false") \\
    .load() \\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \\
    .withColumn("processed_at", F.current_timestamp()) \\
    .writeStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("topic", "output_topic") \\
    .option("checkpointLocation", "/path/to/checkpoint") \\  # Essential for recovery
    .option("failOnDataLoss", "false") \\
    .trigger(processingTime="1 minute") \\
    .start()

print("Fault-tolerant pipeline with comprehensive checkpointing")
print("Can recover from failures and resume processing")
```

### Error Handling

```python
# Handle source errors gracefully
def resilient_source():
    """Create source with error handling"""
    
    try:
        # Primary source
        stream = spark.readStream \\
            .format("kafka") \\
            .option("kafka.bootstrap.servers", "primary:9092") \\
            .option("subscribe", "events") \\
            .load()
        print("Connected to primary Kafka cluster")
        return stream
        
    except Exception as e:
        print(f"Primary source failed: {e}")
        
        # Fallback to file source
        try:
            stream = spark.readStream \\
                .format("parquet") \\
                .load("backup_directory/")
            print("Using file source as fallback")
            return stream
            
        except Exception as e2:
            print(f"Fallback source also failed: {e2}")
            raise

resilient_stream = resilient_source()
```

## 📊 Monitoring Sources and Sinks

### Source Metrics

```python
def monitor_streaming_source(query, source_name):
    """Monitor streaming source performance"""
    
    print(f"Monitoring {source_name} source:")
    
    # Source-specific metrics to monitor
    source_metrics = {
        "Input Rate": "Records per second arriving",
        "Processing Lag": "Time between event and processing",
        "Batch Size": "Records per micro-batch",
        "Source Lag": "Lag behind source system (Kafka)",
        "Error Rate": "Failed record processing rate"
    }
    
    print("\\nKey metrics:")
    for metric, description in source_metrics.items():
        print(f"  {metric}: {description}")
    
    # Spark UI monitoring points
    ui_metrics = [
        "Streaming tab → Input rate",
        "Streaming tab → Processing time",
        "Executors tab → Input size",
        "Storage tab → State size (if stateful)"
    ]
    
    print("\\nSpark UI monitoring:")
    for metric in ui_metrics:
        print(f"  • {metric}")

monitor_streaming_source(None, "Kafka")
```

### Sink Metrics

```python
def monitor_streaming_sink(query, sink_name):
    """Monitor streaming sink performance"""
    
    print(f"Monitoring {sink_name} sink:")
    
    # Sink-specific metrics
    sink_metrics = {
        "Output Rate": "Records per second written",
        "Batch Duration": "Time to write each batch",
        "Backpressure": "When sink can't keep up",
        "Error Rate": "Failed write operations",
        "Throughput": "Overall records per second"
    }
    
    print("\\nKey metrics:")
    for metric, description in sink_metrics.items():
        print(f"  {metric}: {description}")
    
    # Common issues to watch
    issues = [
        "Slow writes causing backpressure",
        "Failed writes accumulating",
        "Memory pressure from buffering",
        "Network issues for remote sinks",
        "Disk space for file sinks"
    ]
    
    print("\\nCommon issues:")
    for issue in issues:
        print(f"  ⚠️  {issue}")

monitor_streaming_sink(None, "Kafka")
```

## 🎯 Production Patterns

### Exactly-Once Processing

```python
# Ensure exactly-once processing with idempotent sinks
idempotent_pipeline = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "events") \\
    .option("kafka.group.id", "streaming_app") \\  # Consumer group
    .option("kafka.enable.auto.commit", "false") \\ # Manual offset management
    .load() \\
    .selectExpr("CAST(key AS STRING) as event_key", 
                "CAST(value AS STRING) as event_value") \\
    .withColumn("processed_at", F.current_timestamp()) \\
    .withColumn("batch_id", F.monotonically_increasing_id()) \\  # Unique ID
    .writeStream \\
    .format("delta") \\  # ACID-compliant sink
    .option("checkpointLocation", "checkpoint/") \\
    .option("path", "processed_events/") \\
    .outputMode("append") \\
    .trigger(processingTime="1 minute") \\
    .start()

print("Exactly-once pipeline with Delta Lake sink")
print("Provides ACID guarantees and exactly-once processing")
```

### Scalable Architecture

```python
# Scalable streaming architecture
scalable_pipeline = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka-cluster:9092") \\
    .option("subscribe", "events") \\
    .option("minPartitions", 8) \\  # At least 8 partitions
    .option("maxOffsetsPerTrigger", 100000) \\  # Control batch size
    .load() \\
    .repartition(16) \\  # Scale processing parallelism
    .withColumn("processed_at", F.current_timestamp()) \\
    .writeStream \\
    .format("parquet") \\
    .option("path", "processed_events/") \\
    .option("checkpointLocation", "checkpoint/") \\
    .partitionBy("date", "hour") \\  # Partition output
    .trigger(processingTime="5 minutes") \\  # Batch writes
    .option("maxRecordsPerFile", 1000000) \\  # Control file sizes
    .start()

print("Scalable streaming architecture:")
print("- Parallel Kafka consumption")
print("- Repartitioned processing")
print("- Partitioned output")
print("- Controlled file sizes")
```

## 🚨 Common Source/Sink Issues

### Source Issues

```python
# Common source problems and solutions
source_issues = {
    "Slow file discovery": "Use fewer, larger files; optimize directory structure",
    "Kafka lag": "Increase partitions; tune consumer settings; scale executors",
    "Schema drift": "Use schema inference carefully; validate schemas",
    "Data loss": "Configure appropriate startingOffsets and failOnDataLoss",
    "Rate limiting": "Use maxOffsetsPerTrigger; adjust trigger intervals"
}

print("COMMON SOURCE ISSUES:")
for issue, solution in source_issues.items():
    print(f"  {issue.upper()}: {solution}")
```

### Sink Issues

```python
# Common sink problems and solutions
sink_issues = {
    "Backpressure": "Increase trigger interval; add more executors; optimize sink",
    "Small files": "Use coalesce() or repartition() before writing",
    "Slow writes": "Use compression; optimize file formats; tune batch sizes",
    "Memory pressure": "Reduce batch sizes; increase executor memory",
    "Consistency issues": "Use transactional sinks; implement idempotency"
}

print("\\nCOMMON SINK ISSUES:")
for issue, solution in sink_issues.items():
    print(f"  {issue.upper()}: {solution}")
```

## 🎯 Best Practices

### Source Best Practices

```python
# Source configuration best practices
source_best_practices = [
    "Choose appropriate source for your use case (Kafka for events, files for batch)",
    "Configure proper schemas to avoid inference overhead",
    "Set reasonable limits (maxOffsetsPerTrigger, maxFilesPerTrigger)",
    "Handle schema evolution gracefully",
    "Monitor source lag and throughput",
    "Implement proper error handling and retries",
    "Use secure connections for production sources"
]

print("SOURCE BEST PRACTICES:")
for practice in source_best_practices:
    print(f"  ✓ {practice}")
```

### Sink Best Practices

```python
# Sink configuration best practices
sink_best_practices = [
    "Choose sink based on downstream consumers (Kafka for streaming, files for batch)",
    "Configure appropriate output modes for your use case",
    "Use compression to reduce storage and network costs",
    "Implement partitioning for query optimization",
    "Set up proper checkpointing for fault tolerance",
    "Monitor sink performance and backpressure",
    "Ensure data quality and validation before sinking"
]

print("\\nSINK BEST PRACTICES:")
for practice in sink_best_practices:
    print(f"  ✓ {practice}")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What are sources and sinks in Structured Streaming?**
2. **When should you use Kafka vs file sources?**
3. **How do you handle schema evolution in streaming?**
4. **What are the different output modes and when to use them?**
5. **How do you ensure exactly-once processing in streaming?**

### Answers:
- **Sources/Sinks**: Sources read data (Kafka, files), sinks write results (Kafka, files, databases)
- **Kafka vs Files**: Kafka for real-time events and continuous processing, files for batch-like or archival data
- **Schema evolution**: Use explicit schemas, handle missing fields gracefully, consider schema registry
- **Output modes**: Append for new data, Complete for full results, Update for changed data
- **Exactly-once**: Use transactional sinks (Delta, databases), checkpointing, idempotent operations

## 📚 Summary

### Sources Mastered:

1. **File Sources**: CSV, JSON, Parquet with schema handling
2. **Kafka Sources**: Topic subscription, offset management, consumer groups
3. **Development Sources**: Rate and socket for testing
4. **Advanced Sources**: Multi-source, schema evolution, error handling

### Sinks Mastered:

1. **File Sinks**: Partitioned output, compression, file size control
2. **Kafka Sinks**: Topic publishing, message formatting
3. **Development Sinks**: Console and memory for testing
4. **Custom Sinks**: Foreach for flexible processing

### Production Considerations:

- **Scalability**: Configure appropriate parallelism and partitioning
- **Reliability**: Implement checkpointing and error handling
- **Performance**: Monitor throughput, latency, and resource usage
- **Data Quality**: Validate schemas and handle corrupted data
- **Cost Optimization**: Choose efficient formats and compression

### Key Patterns:

- **Event Streaming**: Kafka → Processing → Kafka
- **Batch Integration**: Files → Processing → Files
- **Hybrid**: Kafka → Processing → Files (lambda architecture)
- **Testing**: Rate → Processing → Console/Memory

**Sources and sinks are the foundation of streaming pipelines!**

---

**🎉 You now master streaming sources and sinks!**
