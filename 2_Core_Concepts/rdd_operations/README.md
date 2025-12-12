# RDD Operations

Examples of Resilient Distributed Dataset (RDD) operations in PySpark.

## Files

- [groupbykey_and_reducebykey_example.ipynb](groupbykey_and_reducebykey_example.ipynb) - Interactive notebook demonstrating key-based RDD operations including groupByKey and reduceByKey

## Key Concepts Covered

### Transformations
- **groupByKey()**: Groups values by key
- **reduceByKey()**: Aggregates values by key with combiner
- **map()**: Applies function to each element
- **filter()**: Selects elements based on condition

### Actions
- **collect()**: Returns all elements to driver
- **count()**: Returns number of elements
- **first()**: Returns first element
- **take(n)**: Returns first n elements

## Learning Notes

- **groupByKey vs reduceByKey**: reduceByKey is more efficient as it uses a combiner on the map side
- **Actions trigger computation**: Transformations are lazy, actions execute the DAG
- **Memory considerations**: collect() can cause OutOfMemoryError for large datasets

## Related Topics

- [Understanding Partitions](../examples/understanding_partitions.txt)
- [DataFrame Operations](../dataframe_operations/dataframe-examples.md)
