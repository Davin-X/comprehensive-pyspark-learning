# 📈 Regression Techniques: Predicting Continuous Values

## 🎯 Overview

**Regression** is a supervised learning technique that predicts continuous numerical values rather than categorical classes. Unlike classification which predicts discrete labels, regression models predict quantities like prices, temperatures, or sales volumes. Spark MLlib provides powerful distributed regression algorithms for big data scenarios.

## 🔍 Understanding Regression

### What is Regression?

**Regression** predicts continuous target variables from input features:

```
Input Features:    [size, bedrooms, location, age]
Prediction Task:   What is the house price?
Output:           $350,000 (continuous value)
```

### Types of Regression

1. **Simple Linear Regression**: One feature predicts one target
2. **Multiple Linear Regression**: Multiple features predict one target  
3. **Polynomial Regression**: Non-linear relationships
4. **Regularized Regression**: L1/L2 penalties prevent overfitting

### Real-World Regression Examples

- **House Price Prediction**: Features → Price
- **Stock Price Forecasting**: Market data → Stock price
- **Demand Forecasting**: Historical sales → Future demand
- **Temperature Prediction**: Weather patterns → Temperature
- **Credit Scoring**: Financial data → Credit limit

## 📊 Linear Regression Fundamentals

### Understanding Linear Regression

**Linear regression** finds the best-fitting line through data points:

```
y = mx + b
y = β₀ + β₁x₁ + β₂x₂ + ... + βₙxₙ + ε
```

Where:
- **y**: Target variable (what we predict)
- **x**: Feature variables (predictors)
- **β**: Coefficients (model parameters)
- **ε**: Error term (unexplained variation)

### Assumptions of Linear Regression

1. **Linearity**: Relationship between features and target is linear
2. **Independence**: Observations are independent
3. **Homoscedasticity**: Constant variance of errors
4. **Normality**: Errors are normally distributed
5. **No Multicollinearity**: Features are not highly correlated

## 🎯 Implementing Linear Regression in Spark

### Basic Linear Regression

```python
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F

spark = SparkSession.builder \\
    .appName("Regression_Techniques") \\
    .master("local[*]") \\
    .getOrCreate()

# Create sample house price dataset
house_data = []
for i in range(10000):
    # House features
    size_sqft = 800 + (i % 2000)  # 800-2800 sqft
    bedrooms = 1 + (i % 5)        # 1-5 bedrooms
    bathrooms = 1 + (i % 3)       # 1-3 bathrooms
    age_years = i % 50            # 0-49 years old
    
    # Location factor (neighborhood quality)
    location_factor = 0.5 + (i % 10) * 0.1  # 0.5-1.4
    
    # Price calculation with some noise
    base_price = 50000
    price = (base_price + 
             size_sqft * 50 +           # $50 per sqft
             bedrooms * 15000 +         # $15k per bedroom
             bathrooms * 10000 +        # $10k per bathroom
             -age_years * 800)          # -$800 per year of age
    
    price = price * location_factor + (i % 50000 - 25000)  # Add noise
    
    house_data.append({
        "house_id": f"HOUSE_{i:04d}",
        "size_sqft": size_sqft,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "age_years": age_years,
        "location_factor": location_factor,
        "price": max(price, 30000)  # Minimum price
    })

# Create DataFrame
houses_df = spark.createDataFrame(house_data)

print("House price dataset created:")
print(f"Total houses: {houses_df.count():,}")
print(f"Price range: ${houses_df.agg(F.min('price')).collect()[0][0]:,.0f} - ${houses_df.agg(F.max('price')).collect()[0][0]:,.0f}")
```

### Feature Engineering for Regression

```python
# Feature engineering
print("\\n🔧 FEATURE ENGINEERING FOR REGRESSION")

# Create additional features
houses_featured = houses_df.withColumn(
    "size_per_bedroom", F.col("size_sqft") / F.col("bedrooms")
).withColumn(
    "age_category", 
    F.when(F.col("age_years") < 5, "New")
     .when(F.col("age_years") < 20, "Modern") 
     .when(F.col("age_years") < 40, "Established")
     .otherwise("Old")
).withColumn(
    "luxury_score", 
    (F.col("size_sqft") / 1000) * F.col("bedrooms") * F.col("location_factor")
)

# String indexer for categorical features
from pyspark.ml.feature import StringIndexer
age_indexer = StringIndexer(inputCol="age_category", outputCol="age_category_index")

# Assemble features
feature_cols = [
    "size_sqft", "bedrooms", "bathrooms", "age_years", 
    "location_factor", "size_per_bedroom", "age_category_index", "luxury_score"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Create preprocessing pipeline
from pyspark.ml import Pipeline
preprocessing_pipeline = Pipeline(stages=[age_indexer, assembler])

# Fit and transform
preprocessing_model = preprocessing_pipeline.fit(houses_featured)
processed_houses = preprocessing_model.transform(houses_featured)

print("Features engineered and assembled:")
print(f"Total features: {len(feature_cols)}")
processed_houses.select("house_id", "features", "price").show(5, truncate=False)
```

### Train/Test Split

```python
# Train/test split
train_df, test_df = processed_houses.randomSplit([0.8, 0.2], seed=42)

print(f"\\nTrain set: {train_df.count():,} houses")
print(f"Test set: {test_df.count():,} houses")
print(".1f")
```

### Training Linear Regression Model

```python
# Train linear regression model
print("\\n📈 TRAINING LINEAR REGRESSION MODEL")

lr = LinearRegression(
    featuresCol="features",
    labelCol="price",
    maxIter=100,
    regParam=0.1,        # L2 regularization
    elasticNetParam=0.0, # L2 only
    solver="auto"
)

print("Training Linear Regression...")
lr_model = lr.fit(train_df)

print("✅ Model trained successfully")

# Model summary
print(f"\\nModel Summary:")
print(f"Coefficients: {len(lr_model.coefficients)} features")
print(f"Intercept: ${lr_model.intercept:,.2f}")
print(f"R² on training data: {lr_model.summary.r2:.4f}")
print(f"RMSE on training data: ${lr_model.summary.rootMeanSquaredError:,.2f}")

# Feature importance
feature_importance = list(zip(feature_cols, lr_model.coefficients))
feature_importance.sort(key=lambda x: abs(x[1]), reverse=True)

print("\\nTop 5 most important features:")
for feature, coef in feature_importance[:5]:
    print(f"  {feature}: ${coef:,.2f}")
```

### Model Evaluation

```python
# Evaluate on test set
print("\\n📊 MODEL EVALUATION")

# Make predictions
predictions = lr_model.transform(test_df)

# Calculate evaluation metrics
evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction")

mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})  
rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

print("Test Set Performance:")
print(f"Mean Absolute Error: ${mae:,.2f}")
print(f"Root Mean Squared Error: ${rmse:,.2f}")
print(f"R² Score: {r2:.4f}")
print(".1f"
# Show sample predictions
predictions.select("house_id", "price", "prediction") \\
    .withColumn("error", F.abs(F.col("price") - F.col("prediction"))) \\
    .withColumn("error_pct", F.abs(F.col("price") - F.col("prediction")) / F.col("price") * 100) \\
    .show(10)
```

## 🌳 Decision Tree Regression

### Understanding Decision Tree Regression

**Decision trees** can also be used for regression by predicting continuous values instead of classes:

```
Root Node
├── Feature A > threshold?
│   ├── Yes → Average value of subgroup: $250,000
│   └── No → Feature B > threshold?
│       ├── Yes → Average value of subgroup: $180,000
│       └── No → Average value of subgroup: $320,000
```

### Implementing Decision Tree Regression

```python
from pyspark.ml.regression import DecisionTreeRegressor

# Train decision tree regressor
print("\\n🌳 DECISION TREE REGRESSION")

dt = DecisionTreeRegressor(
    featuresCol="features",
    labelCol="price",
    maxDepth=8,
    maxBins=64,
    minInstancesPerNode=10,
    seed=42
)

print("Training Decision Tree Regressor...")
dt_model = dt.fit(train_df)

print("✅ Decision Tree trained successfully")

# Model info
print(f"\\nTree Info:")
print(f"Depth: {dt_model.depth}")
print(f"Number of nodes: {dt_model.numNodes}")

# Feature importance
dt_importance = list(zip(feature_cols, dt_model.featureImportances.toArray()))
dt_importance.sort(key=lambda x: x[1], reverse=True)

print("\\nTop 5 important features:")
for feature, importance in dt_importance[:5]:
    print(f"  {feature}: {importance:.4f}")

# Evaluate decision tree
dt_predictions = dt_model.transform(test_df)

dt_rmse = evaluator.evaluate(dt_predictions, {evaluator.metricName: "rmse"})
dt_r2 = evaluator.evaluate(dt_predictions, {evaluator.metricName: "r2"})

print(f"\\nDecision Tree Performance:")
print(f"RMSE: ${dt_rmse:,.2f}")
print(f"R²: {dt_r2:.4f}")

# Compare with linear regression
print(f"\\nComparison with Linear Regression:")
print(f"Linear Regression RMSE: ${rmse:,.2f}")
print(f"Decision Tree RMSE:     ${dt_rmse:,.2f}")
print(f"Improvement: {(rmse - dt_rmse) / rmse * 100:.1f}%")
```

## 🌲 Random Forest Regression

### Understanding Random Forest Regression

**Random Forest** combines multiple decision trees for better accuracy and reduced overfitting:

1. **Bootstrap Sampling**: Each tree trained on random subset of data
2. **Feature Randomness**: Each split considers random subset of features  
3. **Ensemble Prediction**: Average of all tree predictions

### Implementing Random Forest Regression

```python
from pyspark.ml.regression import RandomForestRegressor

# Train random forest regressor
print("\\n🌲 RANDOM FOREST REGRESSION")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=50,          # Number of trees
    maxDepth=8,           # Maximum depth per tree
    maxBins=64,           # Bins for continuous features
    minInstancesPerNode=5,
    seed=42
)

print("Training Random Forest Regressor...")
rf_model = rf.fit(train_df)

print("✅ Random Forest trained successfully")

# Model info
print(f"\\nForest Info:")
print(f"Number of trees: {rf_model.getNumTrees}")
print(f"Total nodes: {rf_model.totalNumNodes}")

# Feature importance
rf_importance = list(zip(feature_cols, rf_model.featureImportances.toArray()))
rf_importance.sort(key=lambda x: x[1], reverse=True)

print("\\nTop 5 important features:")
for feature, importance in rf_importance[:5]:
    print(f"  {feature}: {importance:.4f}")

# Evaluate random forest
rf_predictions = rf_model.transform(test_df)

rf_rmse = evaluator.evaluate(rf_predictions, {evaluator.metricName: "rmse"})
rf_r2 = evaluator.evaluate(rf_predictions, {evaluator.metricName: "r2"})

print(f"\\nRandom Forest Performance:")
print(f"RMSE: ${rf_rmse:,.2f}")
print(f"R²: {rf_r2:.4f}")

# Compare all models
print(f"\\nMODEL COMPARISON:")
print(f"Linear Regression: RMSE=${rmse:,.2f}, R²={r2:.4f}")
print(f"Decision Tree:     RMSE=${dt_rmse:,.2f}, R²={dt_r2:.4f}")
print(f"Random Forest:     RMSE=${rf_rmse:,.2f}, R²={rf_r2:.4f}")

best_model = min([
    ("Linear Regression", rmse),
    ("Decision Tree", dt_rmse),
    ("Random Forest", rf_rmse)
], key=lambda x: x[1])

print(f"\\n🏆 Best performing model: {best_model[0]} (RMSE: ${best_model[1]:,.2f})")
```

## ⚙️ Regularized Regression Techniques

### Understanding Regularization

**Regularization** prevents overfitting by adding penalties to large coefficients:

1. **L1 Regularization (Lasso)**: Adds absolute value of coefficients
2. **L2 Regularization (Ridge)**: Adds squared value of coefficients  
3. **Elastic Net**: Combination of L1 and L2

### Implementing Regularized Regression

```python
# Compare regularization techniques
print("\\n⚙️ REGULARIZED REGRESSION")

regularization_configs = [
    {"name": "No Regularization", "regParam": 0.0, "elasticNetParam": 0.0},
    {"name": "L2 (Ridge)", "regParam": 0.1, "elasticNetParam": 0.0},
    {"name": "L1 (Lasso)", "regParam": 0.1, "elasticNetParam": 1.0},
    {"name": "Elastic Net", "regParam": 0.1, "elasticNetParam": 0.5}
]

results = []

for config in regularization_configs:
    print(f"\\nTraining {config['name']}...")
    
    reg_lr = LinearRegression(
        featuresCol="features",
        labelCol="price",
        maxIter=100,
        regParam=config["regParam"],
        elasticNetParam=config["elasticNetParam"]
    )
    
    reg_model = reg_lr.fit(train_df)
    reg_predictions = reg_model.transform(test_df)
    
    reg_rmse = evaluator.evaluate(reg_predictions, {evaluator.metricName: "rmse"})
    reg_r2 = evaluator.evaluate(reg_predictions, {evaluator.metricName: "r2"})
    
    results.append({
        "model": config["name"],
        "rmse": reg_rmse,
        "r2": reg_r2,
        "coefficients": reg_model.coefficients
    })
    
    print(f"  RMSE: ${reg_rmse:,.2f}")
    print(f"  R²: {reg_r2:.4f}")
    print(f"  Non-zero coefficients: {(reg_model.coefficients != 0).sum()}")

# Compare results
print(f"\\nREGULARIZATION COMPARISON:")
for result in results:
    print(f"{result['model']:15} | RMSE: ${result['rmse']:8,.0f} | R²: {result['r2']:.3f} | Non-zero: {result['coefficients'].size - (result['coefficients'] == 0).sum()}")

# L1 regularization often produces sparse models
l1_result = next(r for r in results if r["model"] == "L1 (Lasso)")
print(f"\\nL1 regularization created sparse model with {(l1_result['coefficients'] != 0).sum()} non-zero coefficients")
```

## 📊 Advanced Regression Evaluation

### Beyond Basic Metrics

```python
# Advanced evaluation metrics
print("\\n📊 ADVANCED REGRESSION EVALUATION")

# Calculate additional metrics
def calculate_advanced_metrics(predictions_df, actual_col="price", pred_col="prediction"):
    """Calculate advanced regression metrics"""
    
    metrics_df = predictions_df.select(
        F.abs(F.col(actual_col) - F.col(pred_col)).alias("abs_error"),
        ((F.col(actual_col) - F.col(pred_col)) / F.col(actual_col)).alias("rel_error"),
        F.col(actual_col),
        F.col(pred_col)
    )
    
    # Calculate metrics
    metrics = metrics_df.select(
        F.mean("abs_error").alias("mae"),
        F.sqrt(F.mean(F.pow(F.col("abs_error"), 2))).alias("rmse"),
        F.mean("rel_error").alias("mape"),
        F.stddev("abs_error").alias("mae_std"),
        F.count("*").alias("count")
    ).collect()[0]
    
    return metrics

# Calculate metrics for all models
models_to_evaluate = [
    ("Linear Regression", predictions),
    ("Decision Tree", dt_predictions),
    ("Random Forest", rf_predictions)
]

print("Advanced Model Evaluation:")
print("=" * 60)

for model_name, preds in models_to_evaluate:
    metrics = calculate_advanced_metrics(preds)
    
    print(f"\\n{model_name}:")
    print(f"  Mean Absolute Error:     ${metrics.mae:,.2f}")
    print(f"  Root Mean Squared Error: ${metrics.rmse:,.2f}")
    print(f"  Mean Absolute % Error:   {metrics.mape:.2%}")
    print(f"  Error Std Deviation:     ${metrics.mae_std:,.2f}")
    print(f"  Sample Size:             {metrics.count:,}")
```

### Residual Analysis

```python
# Residual analysis
print("\\n🔍 RESIDUAL ANALYSIS")

# Calculate residuals for random forest (best performing)
residuals_df = rf_predictions.withColumn(
    "residual", F.col("price") - F.col("prediction")
).withColumn(
    "abs_residual", F.abs(F.col("residual"))
).withColumn(
    "residual_pct", F.col("residual") / F.col("price")
)

# Residual statistics
residual_stats = residuals_df.select(
    F.mean("residual").alias("mean_residual"),
    F.stddev("residual").alias("std_residual"),
    F.min("residual").alias("min_residual"),
    F.max("residual").alias("max_residual"),
    F.mean("abs_residual").alias("mae")
).collect()[0]

print("Residual Statistics:")
print(f"  Mean Residual:     ${residual_stats.mean_residual:,.2f}")
print(f"  Std Deviation:     ${residual_stats.std_residual:,.2f}")
print(f"  Min Residual:      ${residual_stats.min_residual:,.2f}")
print(f"  Max Residual:      ${residual_stats.max_residual:,.2f}")
print(f"  Mean Absolute:     ${residual_stats.mae:,.2f}")

# Check for patterns in residuals
print("\\nChecking for residual patterns...")

# Residuals by feature ranges
residual_patterns = residuals_df.withColumn(
    "size_range", 
    F.when(F.col("size_sqft") < 1500, "Small")
     .when(F.col("size_sqft") < 2500, "Medium")
     .otherwise("Large")
).groupBy("size_range").agg(
    F.avg("residual").alias("avg_residual"),
    F.count("*").alias("count")
).orderBy("size_range")

print("Residuals by house size:")
residual_patterns.show()

# If residuals show patterns, model may be missing important features
if abs(residual_stats.mean_residual) > 1000:
    print("\\n⚠️  Large mean residual indicates systematic bias")
if residual_stats.std_residual > residual_stats.mae * 2:
    print("⚠️  High residual variance indicates inconsistent predictions")
```

## 🔧 Building Regression Pipelines

### Complete ML Pipeline

```python
from pyspark.ml import Pipeline

# Complete regression pipeline
print("\\n🔧 COMPLETE REGRESSION PIPELINE")

# Create pipeline stages
stages = [
    # Preprocessing
    age_indexer,
    assembler,
    
    # Model
    RandomForestRegressor(
        featuresCol="features",
        labelCol="price",
        numTrees=50,
        maxDepth=8,
        seed=42
    )
]

# Create and fit pipeline
pipeline = Pipeline(stages=stages)

print("Training complete pipeline...")
pipeline_model = pipeline.fit(houses_featured)

print("✅ Pipeline trained successfully")

# Make predictions with pipeline
pipeline_predictions = pipeline_model.transform(houses_featured)

# Evaluate pipeline
pipeline_rmse = evaluator.evaluate(pipeline_predictions, {evaluator.metricName: "rmse"})
pipeline_r2 = evaluator.evaluate(pipeline_predictions, {evaluator.metricName: "r2"})

print(f"\\nPipeline Performance:")
print(f"RMSE: ${pipeline_rmse:,.2f}")
print(f"R²: {pipeline_r2:.4f}")

# Save pipeline
pipeline_path = "/tmp/house_price_pipeline"
pipeline_model.write().overwrite().save(pipeline_path)
print(f"\\nPipeline saved to: {pipeline_path}")
```

## ⚙️ Hyperparameter Tuning for Regression

### Grid Search with Cross-Validation

```python
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Hyperparameter tuning
print("\\n⚙️ HYPERPARAMETER TUNING")

# Parameter grid for Random Forest
param_grid = ParamGridBuilder() \\
    .addGrid(rf.numTrees, [25, 50, 75]) \\
    .addGrid(rf.maxDepth, [6, 8, 10]) \\
    .build()

print(f"Parameter combinations: {len(param_grid)}")

# Cross-validator
crossval = CrossValidator(
    estimator=rf,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

# Tune on sample for speed
tuning_sample = train_df.sample(0.5, seed=42)
print(f"Tuning on sample: {tuning_sample.count()} records")

# Perform tuning
print("Performing cross-validation...")
cv_model = crossval.fit(tuning_sample)

print("✅ Tuning completed")

# Best parameters
best_rf_model = cv_model.bestModel
best_params = cv_model.getEstimatorParamMaps()[cv_model.avgMetrics.index(max(cv_model.avgMetrics))]

print(f"\\nBest Parameters Found:")
print(f"  numTrees: {best_params[rf.numTrees]}")
print(f"  maxDepth: {best_params[rf.maxDepth]}")
print(f"  Best RMSE: ${min(cv_model.avgMetrics):,.2f}")

# Evaluate best model
tuned_predictions = best_rf_model.transform(test_df)
tuned_rmse = evaluator.evaluate(tuned_predictions, {evaluator.metricName: "rmse"})

print(f"\\nTuned Model Performance:")
print(f"RMSE: ${tuned_rmse:,.2f}")
print(f"Improvement: {(rf_rmse - tuned_rmse) / rf_rmse * 100:.1f}%")
```

## 🎯 Production Deployment

### Model Serialization and Loading

```python
# Production deployment
print("\\n🎯 PRODUCTION DEPLOYMENT")

# Save best model
model_path = "/tmp/house_price_model"
best_rf_model.write().overwrite().save(model_path)

print(f"Model saved to: {model_path}")

# Create prediction function
def predict_house_price(house_features):
    """
    Production prediction function for house prices
    
    Args:
        house_features: DataFrame with house features
        
    Returns:
        DataFrame with price predictions
    """
    try:
        # Load model (in production, load once at startup)
        from pyspark.ml.regression import RandomForestRegressionModel
        loaded_model = RandomForestRegressionModel.load(model_path)
        
        # Load preprocessing pipeline
        from pyspark.ml import PipelineModel
        loaded_pipeline = PipelineModel.load(pipeline_path)
        
        # Process and predict
        processed = loaded_pipeline.transform(house_features)
        predictions = loaded_model.transform(processed)
        
        return predictions.select("house_id", "prediction")
        
    except Exception as e:
        print(f"Prediction error: {e}")
        return None

# Test prediction function
test_houses = houses_featured.limit(5)
print("\\nTesting production prediction function:")
print("Production function ready for deployment")
```

## 🚨 Common Regression Challenges

### Handling Outliers

```python
# Outlier detection and handling
print("\\n🚨 HANDLING OUTLIERS")

# Detect outliers using IQR method
price_stats = houses_df.select(
    F.percentile_approx("price", 0.25).alias("q1"),
    F.percentile_approx("price", 0.75).alias("q3")
).collect()[0]

iqr = price_stats.q3 - price_stats.q1
lower_bound = price_stats.q1 - 1.5 * iqr
upper_bound = price_stats.q3 + 1.5 * iqr

print(f"Price outliers detection:")
print(f"  Q1: ${price_stats.q1:,.0f}")
print(f"  Q3: ${price_stats.q3:,.0f}")
print(f"  IQR: ${iqr:,.0f}")
print(f"  Lower bound: ${lower_bound:,.0f}")
print(f"  Upper bound: ${upper_bound:,.0f}")

# Count outliers
outliers = houses_df.filter(
    (F.col("price") < lower_bound) | (F.col("price") > upper_bound)
).count()

print(f"  Outliers detected: {outliers} ({outliers/houses_df.count():.1%})")

# Strategies for handling outliers:
# 1. Remove outliers
# 2. Cap extreme values
# 3. Use robust regression algorithms
# 4. Transform target variable
```

### Multicollinearity Issues

```python
# Check for multicollinearity
print("\\n🚨 MULTICOLLINEARITY CHECK")

# Calculate correlation matrix
numeric_cols = ["size_sqft", "bedrooms", "bathrooms", "age_years", "location_factor"]

correlation_matrix = []
for col1 in numeric_cols:
    row = []
    for col2 in numeric_cols:
        corr = houses_df.select(F.corr(col1, col2)).collect()[0][0]
        row.append(round(corr, 3))
    correlation_matrix.append(row)

print("Correlation Matrix:")
header = f"{'':<15}" + "".join(f"{col:<12}" for col in numeric_cols)
print(header)
print("-" * len(header))

for i, col in enumerate(numeric_cols):
    values = "".join(f"{correlation_matrix[i][j]:<12.3f}" for j in range(len(numeric_cols)))
    print(f"{col:<15}{values}")

# Check for high correlations (> 0.8)
high_corr_pairs = []
for i in range(len(numeric_cols)):
    for j in range(i+1, len(numeric_cols)):
        if abs(correlation_matrix[i][j]) > 0.8:
            high_corr_pairs.append((numeric_cols[i], numeric_cols[j], correlation_matrix[i][j]))

if high_corr_pairs:
    print("\\n⚠️  High correlations detected:")
    for col1, col2, corr in high_corr_pairs:
        print(f"  {col1} ↔ {col2}: {corr:.3f}")
    print("\\nConsider:")
    print("  - Removing redundant features")
    print("  - Using regularization (L2)")
    print("  - Principal component analysis")
else:
    print("\\n✅ No problematic multicollinearity detected")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What's the difference between regression and classification?**
2. **How do you evaluate regression model performance?**
3. **What are regularization techniques and why use them?**
4. **How do you handle multicollinearity in regression?**
5. **When should you use decision trees vs linear regression?**

### Answers:
- **Regression vs Classification**: Regression predicts continuous values, classification predicts categories
- **Evaluation**: RMSE, MAE, R², residual analysis, cross-validation
- **Regularization**: L1 (feature selection), L2 (prevent overfitting), Elastic Net (both)
- **Multicollinearity**: Check correlation matrix, use VIF, apply regularization, remove redundant features
- **Trees vs Linear**: Trees for non-linear relationships and interactions, linear for interpretable coefficients and linear relationships

## 📚 Summary

### Regression Algorithms Mastered:

1. **Linear Regression**: Fast, interpretable, good baseline
2. **Decision Trees**: Non-linear relationships, feature interactions
3. **Random Forest**: Ensemble method, robust performance
4. **Regularized Regression**: Handle overfitting and multicollinearity

### Key Regression Concepts:

- **Feature Engineering**: Transform and create predictive features
- **Model Evaluation**: RMSE, MAE, R², residual analysis
- **Regularization**: Prevent overfitting with L1/L2 penalties
- **Hyperparameter Tuning**: Cross-validation for optimal parameters
- **Production Deployment**: Model serialization and serving

### Performance Optimization:

- **Feature Selection**: Remove irrelevant/redundant features
- **Regularization**: Balance bias-variance trade-off
- **Ensemble Methods**: Combine multiple models for better accuracy
- **Cross-Validation**: Reliable performance estimation
- **Outlier Handling**: Robust to extreme values

### Business Applications:

- **Price Prediction**: Real estate, retail, finance
- **Demand Forecasting**: Inventory management, capacity planning
- **Risk Assessment**: Credit scoring, insurance pricing
- **Performance Prediction**: Sales forecasting, resource allocation

**Regression enables quantitative predictions for business decision-making!**

---

**🎉 You now master regression techniques in Spark MLlib!**
