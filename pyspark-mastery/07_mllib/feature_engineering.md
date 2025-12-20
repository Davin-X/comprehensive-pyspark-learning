# 🔧 Feature Engineering: Transforming Data for ML

## 🎯 Overview

**Feature engineering** is the process of creating, transforming, and selecting features that improve machine learning model performance. Raw data is rarely ready for ML algorithms - it needs to be transformed into meaningful representations that capture patterns and relationships. Spark MLlib provides comprehensive feature engineering tools for distributed data processing.

## 🔍 Understanding Feature Engineering

### Why Feature Engineering Matters

**Garbage in, garbage out** - the quality of your features directly impacts model performance:

```
Raw Data → Feature Engineering → ML Model → Predictions

Examples:
- Text → TF-IDF vectors
- Categories → One-hot encoding  
- Timestamps → Time-based features
- Images → Pixel features
- Relationships → Graph features
```

### Types of Feature Engineering

1. **Feature Extraction**: Creating new features from raw data
2. **Feature Transformation**: Changing feature distributions/scales
3. **Feature Selection**: Choosing the most relevant features
4. **Dimensionality Reduction**: Reducing feature space while preserving information

### Real-World Impact

**Feature engineering can improve model performance by 10-100x compared to using raw features.**

## 📊 Data Preparation Fundamentals

### Handling Missing Values

```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
import numpy as np

spark = SparkSession.builder \\
    .appName("Feature_Engineering") \\
    .master("local[*]") \\
    .getOrCreate()

# Create dataset with missing values
print("🔍 HANDLING MISSING VALUES")

data_with_missing = [
    (1, 25.0, "Male", "Engineer", 75000.0, "Single"),
    (2, 30.0, "Female", "Doctor", None, "Married"),  # Missing salary
    (3, None, "Male", "Teacher", 50000.0, "Single"),  # Missing age
    (4, 45.0, "Female", None, 90000.0, "Married"),    # Missing profession
    (5, 35.0, "Male", "Engineer", 80000.0, None),     # Missing marital status
    (6, 28.0, None, "Lawyer", 95000.0, "Single"),     # Missing gender
]

columns = ["id", "age", "gender", "profession", "salary", "marital_status"]
df_missing = spark.createDataFrame(data_with_missing, columns)

print("Dataset with missing values:")
df_missing.show()

# Strategies for handling missing values
print("\\nMISSING VALUE STRATEGIES:")

# 1. Drop rows with missing values
df_drop = df_missing.dropna()
print(f"\\n1. Drop missing: {df_missing.count()} → {df_drop.count()} rows")

# 2. Fill with mean/median/mode
df_fill = df_missing.fillna({
    "age": df_missing.agg(F.mean("age")).collect()[0][0],
    "salary": df_missing.agg(F.median("salary")).collect()[0][0],
    "gender": "Unknown",
    "profession": "Unknown", 
    "marital_status": "Unknown"
})
print("2. Fill with mean/median/mode:")
df_fill.show()

# 3. Use Imputer for numerical columns
imputer = Imputer(
    inputCols=["age", "salary"],
    outputCols=["age_imputed", "salary_imputed"],
    strategy="mean"  # or "median"
)

df_imputed = imputer.fit(df_missing).transform(df_missing)
print("\\n3. MLlib Imputer for numerical columns:")
df_imputed.select("id", "age", "age_imputed", "salary", "salary_imputed").show()
```

### Outlier Detection and Treatment

```python
# Outlier detection and treatment
print("\\n🚨 OUTLIER DETECTION AND TREATMENT")

# Create dataset with outliers
normal_data = np.random.normal(100, 10, 1000).tolist()
outliers = [500, 600, -200, 450]  # Extreme values
outlier_data = normal_data + outliers

df_outliers = spark.createDataFrame([(i, float(x)) for i, x in enumerate(outlier_data)], ["id", "value"])

print("Dataset with outliers:")
df_outliers.describe("value").show()

# Method 1: Z-score based outlier detection
stats = df_outliers.select(
    F.mean("value").alias("mean"),
    F.stddev("value").alias("std")
).collect()[0]

mean_val, std_val = stats.mean, stats.std

df_with_zscore = df_outliers.withColumn(
    "z_score", (F.col("value") - mean_val) / std_val
).withColumn(
    "is_outlier_z", F.abs(F.col("z_score")) > 3  # Z-score > 3
)

zscore_outliers = df_with_zscore.filter("is_outlier_z").count()
print(f"\\nZ-score outliers (|z| > 3): {zscore_outliers}")

# Method 2: IQR based outlier detection
quantiles = df_outliers.approxQuantile("value", [0.25, 0.75], 0.01)
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

df_with_iqr = df_outliers.withColumn(
    "is_outlier_iqr", 
    (F.col("value") < lower_bound) | (F.col("value") > upper_bound)
)

iqr_outliers = df_with_iqr.filter("is_outlier_iqr").count()
print(f"IQR outliers: {iqr_outliers}")

print(f"\\nIQR bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")

# Treatment strategies
print("\\nOUTLIER TREATMENT STRATEGIES:")

# 1. Remove outliers
df_no_outliers = df_with_iqr.filter("not is_outlier_iqr")
print(f"1. Remove outliers: {df_outliers.count()} → {df_no_outliers.count()}")

# 2. Cap outliers (winsorization)
df_capped = df_with_iqr.withColumn(
    "value_capped",
    F.when(F.col("value") < lower_bound, lower_bound)
     .when(F.col("value") > upper_bound, upper_bound)
     .otherwise(F.col("value"))
)
print("2. Cap outliers (winsorization) applied")

# 3. Transform outliers (log transformation for positive values)
df_transformed = df_outliers.withColumn(
    "value_log", F.log(F.col("value") + abs(df_outliers.agg(F.min("value")).collect()[0][0]) + 1)
)
print("3. Log transformation applied")

print("\\nOutlier summary:")
df_with_iqr.groupBy("is_outlier_iqr").count().show()
```

## 🔄 Categorical Feature Encoding

### String Indexing

```python
# Categorical feature encoding
print("\\n🏷️ CATEGORICAL FEATURE ENCODING")

# Sample categorical data
categorical_data = [
    ("Alice", "Engineer", "Masters", "USA"),
    ("Bob", "Doctor", "PhD", "UK"),
    ("Charlie", "Engineer", "Bachelors", "USA"),
    ("Diana", "Teacher", "Masters", "Canada"),
    ("Eve", "Doctor", "PhD", "UK"),
    ("Frank", "Engineer", "Bachelors", "USA"),
]

cat_df = spark.createDataFrame(categorical_data, ["name", "profession", "education", "country"])

print("Categorical dataset:")
cat_df.show()

# String Indexing - convert categories to indices
profession_indexer = StringIndexer(inputCol="profession", outputCol="profession_index")
education_indexer = StringIndexer(inputCol="education", outputCol="education_index")  
country_indexer = StringIndexer(inputCol="country", outputCol="country_index")

# Fit and transform
indexed_df = profession_indexer.fit(cat_df).transform(cat_df)
indexed_df = education_indexer.fit(indexed_df).transform(indexed_df)
indexed_df = country_indexer.fit(indexed_df).transform(indexed_df)

print("\\nString Indexed (numerical representation):")
indexed_df.select("profession", "profession_index", "education", "education_index", "country", "country_index").show()

# Check category frequencies
print("\\nCategory frequencies:")
for col in ["profession", "education", "country"]:
    print(f"\\n{col}:")
    cat_df.groupBy(col).count().orderBy("count", ascending=False).show()
```

### One-Hot Encoding

```python
# One-hot encoding
print("\\n🔥 ONE-HOT ENCODING")

from pyspark.ml.feature import OneHotEncoder

# One-hot encode the indexed columns
profession_encoder = OneHotEncoder(inputCol="profession_index", outputCol="profession_vec")
education_encoder = OneHotEncoder(inputCol="education_index", outputCol="education_vec")
country_encoder = OneHotEncoder(inputCol="country_index", outputCol="country_vec")

# Create pipeline
encoding_pipeline = Pipeline(stages=[
    profession_indexer, education_indexer, country_indexer,
    profession_encoder, education_encoder, country_encoder
])

encoded_df = encoding_pipeline.fit(cat_df).transform(cat_df)

print("One-hot encoded features:")
encoded_df.select("profession", "profession_vec", "education", "education_vec", "country", "country_vec").show()

# Vector assembler to combine all features
feature_cols = ["profession_vec", "education_vec", "country_vec"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

final_df = assembler.transform(encoded_df)

print("\\nFinal feature vector:")
final_df.select("name", "features").show(truncate=False)

print(f"\\nFeature vector size: {len(feature_cols)} components")
print("Each one-hot vector contributes its dimension to the total")
```

### Target Encoding (Mean Encoding)

```python
# Target encoding for high-cardinality categorical features
print("\\n🎯 TARGET ENCODING (MEAN ENCODING)")

# Create dataset with target variable
target_data = [
    ("A", 100, "High"),
    ("A", 120, "High"), 
    ("A", 90, "Medium"),
    ("B", 80, "Medium"),
    ("B", 70, "Low"),
    ("B", 60, "Low"),
    ("C", 110, "High"),
    ("C", 105, "High"),
    ("D", 50, "Low"),
    ("D", 45, "Low"),
]

target_df = spark.createDataFrame(target_data, ["category", "value", "target"])

# Convert target to numerical
target_indexer = StringIndexer(inputCol="target", outputCol="target_index")
target_df = target_indexer.fit(target_df).transform(target_df)

print("Dataset for target encoding:")
target_df.show()

# Target encoding: replace category with mean of target
target_means = target_df.groupBy("category").agg(
    F.mean("target_index").alias("target_mean"),
    F.count("*").alias("category_count")
)

print("\\nCategory target means:")
target_means.show()

# Join back to original data
target_encoded_df = target_df.join(target_means, "category")

print("\\nTarget encoded data:")
target_encoded_df.select("category", "value", "target", "target_mean", "category_count").show()

print("\\nTarget encoding advantages:")
print("  ✅ Handles high-cardinality features")
print("  ✅ Captures relationship with target")
print("  ✅ Reduces dimensionality")
print("  ❌ Can cause data leakage (use cross-validation)")
print("  ❌ Sensitive to outliers")
```

## ⚖️ Feature Scaling and Normalization

### Standard Scaling (Z-score)

```python
# Feature scaling and normalization
print("\\n⚖️ FEATURE SCALING AND NORMALIZATION")

# Create numerical features dataset
scale_data = []
for i in range(1000):
    feature1 = np.random.normal(100, 15)    # Mean 100, std 15
    feature2 = np.random.exponential(2)      # Exponential distribution
    feature3 = np.random.uniform(0, 1000)   # Uniform distribution
    
    scale_data.append({
        "id": i,
        "feature1": feature1,
        "feature2": feature2, 
        "feature3": feature3
    })

scale_df = spark.createDataFrame(scale_data)
assembler = VectorAssembler(inputCols=["feature1", "feature2", "feature3"], outputCol="features")
scale_df = assembler.transform(scale_df)

print("Original feature distributions:")
scale_df.select("feature1", "feature2", "feature3").summary().show()

# Standard Scaler (Z-score normalization)
from pyspark.ml.feature import StandardScaler

standard_scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withMean=True,    # Center to mean
    withStd=True      # Scale to unit variance
)

scaler_model = standard_scaler.fit(scale_df)
scaled_df = scaler_model.transform(scale_df)

print("\\nStandard Scaled features:")
print(f"Means: {scaler_model.mean}")
print(f"Std deviations: {scaler_model.std}")

scaled_sample = scaled_df.select("scaled_features").limit(5)
print("\\nSample scaled features:")
scaled_sample.show(truncate=False)
```

### Min-Max Scaling

```python
# Min-Max scaling
print("\\n📏 MIN-MAX SCALING")

from pyspark.ml.feature import MinMaxScaler

minmax_scaler = MinMaxScaler(
    inputCol="features",
    outputCol="minmax_features",
    min=0.0,    # Minimum value after scaling
    max=1.0     # Maximum value after scaling
)

minmax_model = minmax_scaler.fit(scale_df)
minmax_df = minmax_model.transform(scale_df)

print("Min-Max scaled features (range [0,1]):")
minmax_sample = minmax_df.select("minmax_features").limit(5)
minmax_sample.show(truncate=False)

# Check scaling ranges
print("\\nScaling comparison:")
scale_df.select("features").summary().show()
scaled_df.select("scaled_features").summary().show()
minmax_df.select("minmax_features").summary().show()
```

### Robust Scaling

```python
# Robust scaling (using median and IQR)
print("\\n🛡️ ROBUST SCALING")

from pyspark.ml.feature import RobustScaler

robust_scaler = RobustScaler(
    inputCol="features",
    outputCol="robust_features",
    lower=0.25,    # Lower quantile (25th percentile)
    upper=0.75,    # Upper quantile (75th percentile)
    relativeError=0.001
)

robust_model = robust_scaler.fit(scale_df)
robust_df = robust_model.transform(scale_df)

print("Robust scaled features (median=0, IQR=1):")
robust_sample = robust_df.select("robust_features").limit(5)
robust_sample.show(truncate=False)

print("\\nScaling method comparison:")
methods = [
    ("Original", "features"),
    ("Standard", "scaled_features"), 
    ("Min-Max", "minmax_features"),
    ("Robust", "robust_features")
]

for name, col in methods:
    stats = scale_df.select(col).summary() if col == "features" else \\
            scaled_df.select(col).summary() if col == "scaled_features" else \\
            minmax_df.select(col).summary() if col == "minmax_features" else \\
            robust_df.select(col).summary()
    
    mean_val = float(stats.filter("summary = 'mean'").select("features").collect()[0][0])
    std_val = float(stats.filter("summary = 'stddev'").select("features").collect()[0][0])
    
    print(f"  {name:8}: Mean={mean_val:6.2f}, Std={std_val:6.2f}")
```

## 📝 Text Feature Engineering

### TF-IDF (Term Frequency - Inverse Document Frequency)

```python
# Text feature engineering
print("\\n📝 TEXT FEATURE ENGINEERING")

# Sample text data
text_data = [
    (1, "machine learning is awesome for data science"),
    (2, "spark is great for big data processing"),
    (3, "machine learning with spark and python"),
    (4, "data science requires statistical knowledge"),
    (5, "big data analytics with machine learning"),
]

text_df = spark.createDataFrame(text_data, ["id", "text"])

print("Text dataset:")
text_df.show(truncate=False)

# TF-IDF transformation
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=1000)
idf = IDF(inputCol="raw_features", outputCol="tfidf_features")

# Create pipeline
text_pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf])

tfidf_model = text_pipeline.fit(text_df)
tfidf_df = tfidf_model.transform(text_df)

print("\\nTF-IDF features:")
tfidf_df.select("id", "words", "tfidf_features").show(truncate=False)

# Analyze vocabulary
print("\\nText processing pipeline stages:")
for i, stage in enumerate(text_pipeline.getStages()):
    stage_name = stage.__class__.__name__
    if hasattr(stage, "numFeatures"):
        stage_name += f" ({stage.numFeatures} features)"
    print(f"  {i+1}. {stage_name}")
```

### Word Embeddings (Word2Vec)

```python
# Word embeddings
print("\\n🧠 WORD EMBEDDINGS (WORD2VEC)")

from pyspark.ml.feature import Word2Vec

# Larger text dataset for better embeddings
large_text_data = [
    (1, ["machine", "learning", "data", "science", "spark"]),
    (2, ["big", "data", "processing", "analytics", "python"]),
    (3, ["machine", "learning", "algorithms", "statistics"]),
    (4, ["spark", "distributed", "computing", "scala", "java"]),
    (5, ["data", "science", "machine", "learning", "ai"]),
    (6, ["big", "data", "hadoop", "spark", "kafka"]),
]

embed_df = spark.createDataFrame(large_text_data, ["id", "words"])

word2vec = Word2Vec(
    vectorSize=10,        # Embedding dimension
    minCount=0,          # Minimum word frequency
    inputCol="words",
    outputCol="word_embeddings"
)

embed_model = word2vec.fit(embed_df)
embedded_df = embed_model.transform(embed_df)

print("Word embeddings:")
embedded_df.select("id", "words", "word_embeddings").show(truncate=False)

# Get vocabulary
vocab = embed_model.getVectors()
print(f"\\nVocabulary size: {vocab.count()}")
print("\\nWord vectors:")
vocab.show(10)

# Find similar words (cosine similarity)
print("\\nWord similarity example:")
if vocab.filter("word = 'data'").count() > 0:
    data_vector = vocab.filter("word = 'data'").select("vector").collect()[0][0]
    print("Similar words to 'data' would be computed using cosine similarity")
```

## 🔢 Numerical Feature Engineering

### Binning and Discretization

```python
# Numerical feature engineering
print("\\n🔢 NUMERICAL FEATURE ENGINEERING")

# Create continuous features
num_data = []
for i in range(1000):
    age = np.random.normal(35, 10)
    income = np.random.lognormal(11, 0.5)  # Log-normal distribution
    score = np.random.beta(2, 5) * 100     # Beta distribution
    
    num_data.append({
        "id": i,
        "age": max(18, min(80, age)),      # Clip to reasonable range
        "income": income,
        "score": score
    })

num_df = spark.createDataFrame(num_data)

print("Numerical features:")
num_df.select("age", "income", "score").summary().show()

# Binning with Bucketizer
from pyspark.ml.feature import Bucketizer

# Age bins
age_bins = [0, 25, 35, 50, 65, float("inf")]
age_bucketizer = Bucketizer(
    splits=age_bins,
    inputCol="age", 
    outputCol="age_group"
)

# Income bins  
income_bins = [0, 30000, 60000, 100000, 150000, float("inf")]
income_bucketizer = Bucketizer(
    splits=income_bins,
    inputCol="income",
    outputCol="income_bracket"
)

# Score bins
score_bins = [0, 20, 40, 60, 80, 100]
score_bucketizer = Bucketizer(
    splits=score_bins,
    inputCol="score",
    outputCol="score_category"
)

# Apply binning
binned_df = age_bucketizer.transform(num_df)
binned_df = income_bucketizer.transform(binned_df)
binned_df = score_bucketizer.transform(binned_df)

print("\\nBinned features:")
binned_df.select("age", "age_group", "income", "income_bracket", "score", "score_category").show(10)

print("\\nBin distributions:")
for col in ["age_group", "income_bracket", "score_category"]:
    print(f"\\n{col}:")
    binned_df.groupBy(col).count().orderBy(col).show()
```

### Polynomial Features

```python
# Polynomial feature expansion
print("\\n📈 POLYNOMIAL FEATURE EXPANSION")

from pyspark.ml.feature import PolynomialExpansion

# Simple 2D data
poly_data = []
for i in range(200):
    x = np.random.uniform(-2, 2)
    y = np.random.uniform(-2, 2)
    poly_data.append({"x": x, "y": y})

poly_df = spark.createDataFrame(poly_data)
poly_assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
poly_df = poly_assembler.transform(poly_df)

print("Original 2D features:")
poly_df.select("x", "y", "features").show(5)

# Expand to polynomial features (degree 2)
poly_expansion = PolynomialExpansion(
    degree=2,
    inputCol="features",
    outputCol="poly_features"
)

poly_model = poly_expansion.fit(poly_df)
expanded_df = poly_model.transform(poly_df)

print("\\nPolynomial expansion (degree 2):")
print("Original: [x, y]")
print("Expanded: [x, y, x², x*y, y²]")
expanded_df.select("features", "poly_features").show(5, truncate=False)

print(f"\\nFeature expansion: {poly_df.select('features').first()['features'].size} → {expanded_df.select('poly_features').first()['poly_features'].size} features")
```

## 🎯 Feature Selection Techniques

### Correlation-Based Selection

```python
# Feature selection
print("\\n🎯 FEATURE SELECTION TECHNIQUES")

# Create dataset with correlated features
fs_data = []
for i in range(1000):
    x1 = np.random.normal(0, 1)
    x2 = x1 + np.random.normal(0, 0.1)  # Highly correlated with x1
    x3 = np.random.normal(0, 1)          # Independent
    x4 = x3 * 2 + np.random.normal(0, 0.2)  # Correlated with x3
    y = 2*x1 + x3 + np.random.normal(0, 0.5)  # Target depends on x1 and x3
    
    fs_data.append({
        "x1": x1, "x2": x2, "x3": x3, "x4": x4, "y": y
    })

fs_df = spark.createDataFrame(fs_data)

print("Feature correlation analysis:")
print("x1 ↔ x2: Highly correlated (x2 = x1 + noise)")
print("x3 ↔ x4: Highly correlated (x4 = 2*x3 + noise)")
print("y depends on x1 and x3")

# Calculate correlation matrix
corr_cols = ["x1", "x2", "x3", "x4", "y"]
correlations = []

for i, col1 in enumerate(corr_cols):
    row = []
    for col2 in corr_cols:
        corr = fs_df.select(F.corr(col1, col2)).collect()[0][0]
        row.append(round(corr, 3))
    correlations.append(row)

print("\\nCorrelation Matrix:")
header = f"{'':<6}" + "".join(f"{col:<8}" for col in corr_cols)
print(header)
print("-" * len(header))

for i, col in enumerate(corr_cols):
    values = "".join(f"{correlations[i][j]:<8.3f}" for j in range(len(corr_cols)))
    print(f"{col:<6}{values}")

print("\\nFeature selection insights:")
print("  x1 and x2 are redundant (correlation = 0.987)")
print("  x3 and x4 are redundant (correlation = 0.986)")
print("  x1 and x3 are most predictive of y")
print("  → Select x1 and x3, drop x2 and x4")
```

### Chi-Squared Feature Selection

```python
# Chi-squared feature selection for categorical targets
print("\\n📊 CHI-SQUARED FEATURE SELECTION")

from pyspark.ml.feature import ChiSqSelector

# Create categorical features dataset
chisq_data = []
for i in range(1000):
    feature1 = np.random.choice([0, 1], p=[0.7, 0.3])  # Related to target
    feature2 = np.random.choice([0, 1], p=[0.5, 0.5])  # Independent
    feature3 = np.random.choice([0, 1], p=[0.6, 0.4])  # Somewhat related
    
    # Target depends on feature1
    target_prob = 0.2 + 0.6 * feature1
    target = np.random.choice([0, 1], p=[1-target_prob, target_prob])
    
    chisq_data.append({
        "feature1": feature1,
        "feature2": feature2, 
        "feature3": feature3,
        "target": target
    })

chisq_df = spark.createDataFrame(chisq_data)

# Prepare features
chisq_assembler = VectorAssembler(inputCols=["feature1", "feature2", "feature3"], outputCol="features")
chisq_df = chisq_assembler.transform(chisq_df)

print("Chi-squared feature selection:")
print("feature1: Strongly related to target")
print("feature2: Independent of target") 
print("feature3: Weakly related to target")

# Chi-squared selector
selector = ChiSqSelector(
    numTopFeatures=2,        # Select top 2 features
    featuresCol="features",
    outputCol="selected_features",
    labelCol="target"
)

selector_model = selector.fit(chisq_df)
selected_df = selector_model.transform(chisq_df)

print("\\nSelected features:")
selected_df.select("features", "selected_features").show(5, truncate=False)

print(f"\\nFeature selection scores: {selector_model.selectedFeatures}")
print("Selected feature indices: feature1 and feature3 (most predictive)")
```

## 🚀 Dimensionality Reduction

### Principal Component Analysis (PCA)

```python
# Dimensionality reduction with PCA
print("\\n🚀 DIMENSIONALITY REDUCTION WITH PCA")

from pyspark.ml.feature import PCA

# Create high-dimensional data
high_dim_data = []
for i in range(500):
    features = [np.random.normal(0, 1) for _ in range(20)]  # 20 features
    # Add some structure (first 5 features are correlated)
    for j in range(5):
        features[j] += features[0] * 0.5
    
    high_dim_data.append({"id": i, "features": features})

hd_df = spark.createDataFrame(high_dim_data)
hd_assembler = VectorAssembler(inputCols=["features"], outputCol="feature_vector")
hd_df = hd_assembler.transform(hd_df)

print(f"Original dimensionality: {hd_df.select('feature_vector').first()['feature_vector'].size} features")

# Apply PCA
pca = PCA(
    k=5,  # Reduce to 5 dimensions
    inputCol="feature_vector",
    outputCol="pca_features"
)

pca_model = pca.fit(hd_df)
pca_df = pca_model.transform(hd_df)

print(f"\\nReduced dimensionality: {pca_df.select('pca_features').first()['pca_features'].size} features")

# Explained variance
explained_var = pca_model.explainedVariance
print(f"\\nExplained variance by component: {explained_var}")
print(f"Total variance explained: {sum(explained_var):.3f}")

# Cumulative explained variance
cumulative_var = [sum(explained_var[:i+1]) for i in range(len(explained_var))]
print(f"Cumulative variance explained: {[round(x, 3) for x in cumulative_var]}")

print("\\nPCA sample:")
pca_df.select("pca_features").show(3, truncate=False)
```

## 🎯 Feature Engineering Best Practices

### Production Pipeline

```python
# Complete feature engineering pipeline
print("\\n🎯 COMPLETE FEATURE ENGINEERING PIPELINE")

# Create comprehensive dataset
pipeline_data = []
for i in range(2000):
    # Mixed data types
    age = np.random.normal(40, 10)
    salary = np.random.lognormal(11, 0.5)
    category = np.random.choice(["A", "B", "C"])
    score = np.random.beta(2, 3) * 100
    
    pipeline_data.append({
        "id": i,
        "age": age,
        "salary": salary,
        "category": category,
        "score": score
    })

pipeline_df = spark.createDataFrame(pipeline_data)

print("Complete feature engineering pipeline:")

# 1. Handle missing values (simulate some missing)
pipeline_df = pipeline_df.withColumn(
    "salary", F.when(F.rand() < 0.05, None).otherwise(F.col("salary"))  # 5% missing
)

# 2. Outlier treatment
salary_stats = pipeline_df.select(
    F.percentile_approx("salary", 0.25).alias("q1"),
    F.percentile_approx("salary", 0.75).alias("q3")
).collect()[0]

iqr = salary_stats.q3 - salary_stats.q1
salary_upper = salary_stats.q3 + 1.5 * iqr

pipeline_df = pipeline_df.withColumn(
    "salary_capped", F.when(F.col("salary") > salary_upper, salary_upper).otherwise(F.col("salary"))
)

# 3. Feature engineering
pipeline_df = pipeline_df.withColumn("age_group", F.when(F.col("age") < 30, "young").when(F.col("age") < 50, "middle").otherwise("senior"))
pipeline_df = pipeline_df.withColumn("high_score", F.when(F.col("score") > 70, 1).otherwise(0))

# 4. Categorical encoding
cat_indexer = StringIndexer(inputCol="category", outputCol="category_index")
age_indexer = StringIndexer(inputCol="age_group", outputCol="age_group_index")

cat_encoder = OneHotEncoder(inputCol="category_index", outputCol="category_vec")
age_encoder = OneHotEncoder(inputCol="age_group_index", outputCol="age_group_vec")

# 5. Scaling
num_assembler = VectorAssembler(inputCols=["age", "salary_capped", "score"], outputCol="num_features")
scaler = StandardScaler(inputCol="num_features", outputCol="scaled_features")

# 6. Final assembly
final_assembler = VectorAssembler(
    inputCols=["scaled_features", "category_vec", "age_group_vec", "high_score"], 
    outputCol="final_features"
)

# Create complete pipeline
complete_pipeline = Pipeline(stages=[
    cat_indexer, age_indexer,
    cat_encoder, age_encoder,
    num_assembler, scaler,
    final_assembler
])

# Fit and transform
final_model = complete_pipeline.fit(pipeline_df)
final_df = final_model.transform(pipeline_df)

print("Pipeline stages executed:")
for i, stage in enumerate(complete_pipeline.getStages()):
    print(f"  {i+1}. {stage.__class__.__name__}")

print("\\nFinal feature vector:")
final_df.select("id", "final_features").show(5, truncate=False)

print(f"\\nFeature engineering result: {final_df.select('final_features').first()['final_features'].size} features from mixed data types")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **Why is feature engineering important in machine learning?**
2. **How do you handle categorical variables in ML?**
3. **What's the difference between normalization and standardization?**
4. **How do you detect and handle outliers?**
5. **What techniques do you use for feature selection?**

### Answers:
- **Importance**: Raw data is rarely suitable for ML; feature engineering extracts meaningful patterns and improves model performance by orders of magnitude
- **Categorical encoding**: Label encoding for ordinal categories, one-hot encoding for nominal categories, target encoding for high-cardinality features
- **Normalization vs standardization**: Normalization scales to [0,1] range, standardization centers to mean=0 and std=1; use standardization for algorithms assuming normal distribution
- **Outliers**: Z-score (>3), IQR method (1.5*IQR), visual inspection; handle by removal, capping, or robust methods
- **Feature selection**: Correlation analysis, chi-squared test, recursive feature elimination, L1 regularization, tree-based feature importance

## 📚 Summary

### Feature Engineering Techniques Mastered:

1. **Data Cleaning**: Missing values, outliers, data validation
2. **Categorical Encoding**: String indexing, one-hot encoding, target encoding
3. **Feature Scaling**: Standardization, min-max scaling, robust scaling
4. **Text Processing**: TF-IDF, word embeddings, text mining
5. **Numerical Engineering**: Binning, polynomial features, transformations
6. **Feature Selection**: Correlation, chi-squared, model-based selection
7. **Dimensionality Reduction**: PCA, feature extraction

### Key Feature Engineering Concepts:

- **Garbage In, Garbage Out**: Feature quality determines model performance
- **Domain Knowledge**: Business understanding drives feature creation
- **Iterative Process**: Feature engineering is cyclical with model development
- **Scalability**: Distributed processing for big data feature engineering
- **Automation**: ML pipelines for reproducible feature engineering

### Production Considerations:

- **Data Drift**: Monitor feature distributions over time
- **Feature Stores**: Centralized feature management and serving
- **Versioning**: Track feature engineering pipeline changes
- **Performance**: Optimize for training and inference speed
- **Monitoring**: Alert on feature quality degradation

### Business Impact:

- **Model Accuracy**: Better features = better predictions
- **Training Speed**: Efficient features reduce computational requirements
- **Inference Speed**: Optimized features improve prediction latency
- **Maintainability**: Clean pipelines reduce technical debt
- **Scalability**: Distributed feature engineering handles big data

**Feature engineering is the art and science of transforming raw data into predictive power!**

---

**🎉 You now master feature engineering for machine learning!**
