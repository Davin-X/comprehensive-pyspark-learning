# 2. Core PySpark Concepts

Master the fundamental operations and transformations in PySpark.

## 📁 Organization

### RDD Operations
Located in [`rdd_operations/`](rdd_operations/)
- [combineByKey_example.py](rdd_operations/combineByKey_example.py) - Advanced RDD aggregations
- [count_min_max.py](rdd_operations/count_min_max.py) - Basic aggregations
- [groupbykey_and_reducebykey_example.ipynb](rdd_operations/groupbykey_and_reducebykey_example.ipynb) - Key-based operations

### DataFrame Operations
Located in [`dataframe_operations/`](dataframe_operations/)
- [dataframe-examples.md](dataframe_operations/dataframe-examples.md) - DataFrame usage patterns

### Examples & Concepts
Located in [`examples/`](examples/)
- [understanding_partitions.txt](examples/understanding_partitions.txt) - Partition concepts

### Archived Sessions
Historical learning sessions moved to [`../session_logs/archived_sessions/`](../session_logs/archived_sessions/)

## 🎯 Key Topics Covered

### RDD Operations
- **Transformations**: map, filter, flatMap, reduceByKey
- **Actions**: collect, count, first, take
- **Aggregations**: combineByKey, groupByKey, reduceByKey
- **Partitioning**: Understanding data distribution

### DataFrame Operations
- **Creation**: From RDDs, files, databases
- **Transformations**: select, filter, groupBy, join
- **Actions**: show, collect, count
- **SQL Integration**: Spark SQL queries

## 📚 Learning Path

1. **Start with RDDs** - Understand distributed computing fundamentals
2. **Move to DataFrames** - Higher-level abstractions for structured data
3. **Study partitions** - Learn about data distribution and performance
4. **Practice aggregations** - Master key-based operations

## 🔗 Navigation

| Previous | Current | Next |
|----------|---------|------|
| [1_Basics_and_Setup](../1_Basics_and_Setup/) | **2_Core_Concepts** | [3_Advanced_Techniques](../3_Advanced_Techniques/) |

## 💡 Tips

- **RDDs** are lower-level but give you full control
- **DataFrames** are easier to use and often faster
- Always consider **partitioning** for performance
- Use **actions** sparingly - they trigger computation
