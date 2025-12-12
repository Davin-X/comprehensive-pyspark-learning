# 5. Algorithms and Data Structures

Advanced distributed algorithms and data structure implementations in PySpark.

## Directory Structure

```
5_Algorithms_and_DataStructures/
├── algorithms/              # Algorithm implementations and guides
│   ├── combine_by_key.ipynb       # Interactive combineByKey tutorial
│   ├── combine_by_key.md          # combineByKey detailed guide
│   ├── dna_base_count.md          # DNA sequence analysis
│   ├── standard_deviation_combine_by_key.md  # Statistical algorithms
│   ├── word_count.ipynb           # Interactive word count tutorial
│   ├── word_count.txt             # Classic word count algorithm
│   └── word_count_shorthand.txt   # Simplified implementation
├── data/                    # Sample datasets
│   └── dna_seq.txt          # DNA sequence data
├── scripts/                 # Utility scripts
│   ├── basemapper.py        # Base mapper implementation
│   ├── word_count.py        # Word count script
│   ├── run_word_count.sh    # Execution script
│   └── run_word_count_ver2.sh  # Alternative execution
└── README.md               # This file
```

## Key Algorithms Covered

### Data Aggregation
- **combineByKey** ⭐⭐⭐: Advanced key-based aggregations (Interactive Notebook)
- **Standard Deviation**: Statistical calculations with combineByKey
- **Word Count**: Classic MapReduce pattern (Interactive Notebook)

### Biological Computing
- **DNA Base Counting**: Sequence analysis algorithms
- **Pattern Matching**: Distributed biological data processing

## Interactive Notebooks 🎯

### [combine_by_key.ipynb](algorithms/combine_by_key.ipynb)
**Best starting point!** Interactive tutorial covering:
- Basic word count with combineByKey
- Advanced statistics (min/max/count per key)
- Complex moving averages
- Performance optimization concepts

### [word_count.ipynb](algorithms/word_count.ipynb)
Classic distributed algorithm with multiple implementations:
- RDD-based word count
- DataFrame-based word count
- Performance comparisons

## Learning Focus

### Distributed Computing Concepts
- **MapReduce Patterns**: Classic distributed processing
- **Key-Based Operations**: Grouping and aggregation strategies
- **Statistical Algorithms**: Distributed statistical computations

### Performance Considerations
- **Memory Management**: Efficient data structures for distributed computing
- **Partitioning Strategies**: Data distribution optimization
- **Scalability Patterns**: Handling large datasets

## Prerequisites

Complete Phases 1-4 before diving into these advanced algorithms:
- Basic PySpark operations
- RDD transformations and actions
- DataFrame operations
- Performance optimization basics

## Quick Start

1. **Start with Interactive Notebooks**:
   - `combine_by_key.ipynb` - Learn the most important algorithm
   - `word_count.ipynb` - Understand distributed processing

2. **Explore Advanced Topics**:
   - DNA sequence analysis
   - Statistical computations
   - Custom aggregations

3. **Run Example Scripts**:
   - Use provided shell scripts to test algorithms
   - Modify parameters to understand behavior

## Usage Examples

### combineByKey for Custom Aggregation
```python
# See combine_by_key.ipynb for complete interactive examples
result = rdd.combineByKey(
    lambda v: (v, v, 1),                    # createCombiner
    lambda acc, v: (min(acc[0], v), max(acc[1], v), acc[2] + 1),  # mergeValue
    lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]), acc1[2] + acc2[2])  # mergeCombiners
)
```

### Word Count Implementation
```python
# See word_count.ipynb for complete examples
counts = text_file \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
```

## Related Topics

- [RDD Operations](../2_Core_Concepts/rdd_operations/) - Basic RDD operations
- [DataFrame Operations](../2_Core_Concepts/dataframe_operations/) - Structured data processing
- [Performance Tuning](../3_Advanced_Techniques/) - Optimization techniques

## 📚 Educational Resources

**🎯 Start Here:**
- [combine_by_key.ipynb](algorithms/combine_by_key.ipynb) - Interactive learning
- [word_count.ipynb](algorithms/word_count.ipynb) - Classic algorithm

**📖 Deep Dives:**
- [combine_by_key.md](algorithms/combine_by_key.md) - Complete reference
- [dna_base_count.md](algorithms/dna_base_count.md) - Biological algorithms
- [standard_deviation_combine_by_key.md](algorithms/standard_deviation_combine_by_key.md) - Statistics
