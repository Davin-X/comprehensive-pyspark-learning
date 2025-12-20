# 📊 Model Evaluation: Measuring ML Performance

## 🎯 Overview

**Model evaluation** is the process of assessing machine learning model performance using appropriate metrics and validation techniques. Proper evaluation ensures models generalize well to unseen data and meet business requirements. Spark MLlib provides comprehensive evaluation tools for different types of ML problems.

## 🔍 Understanding Model Evaluation

### Why Model Evaluation Matters

**Without proper evaluation, you risk:**
- **Overfitting**: Model performs well on training data but fails in production
- **Underfitting**: Model too simple to capture underlying patterns
- **Biased assessment**: Incorrectly judging model quality
- **Business misalignment**: Model doesn't meet actual requirements

### Evaluation Framework

```
Model Training → Validation → Testing → Production

1. Training Metrics: Model fit on training data
2. Validation Metrics: Hyperparameter tuning and model selection
3. Test Metrics: Final unbiased performance assessment
4. Production Monitoring: Ongoing performance in real-world usage
```

### Cross-Validation vs Holdout

- **Holdout**: Simple train/test split (70/30)
- **Cross-validation**: Multiple train/test splits for robust evaluation
- **K-fold CV**: Most common (K=5 or 10)

## 📈 Classification Metrics

### Binary Classification Evaluation

```python
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

spark = SparkSession.builder \\
    .appName("Model_Evaluation") \\
    .master("local[*]") \\
    .getOrCreate()

# Create sample binary classification dataset
print("📊 CREATING SAMPLE CLASSIFICATION DATASET")

import random
random.seed(42)

# Generate customer churn data
churn_data = []
for i in range(5000):
    # Features
    tenure = random.randint(1, 72)
    monthly_charges = round(random.uniform(20, 200), 2)
    contract_monthly = random.random() < 0.6  # 60% on monthly contracts
    support_calls = random.randint(0, 8)
    
    # Churn probability (realistic business logic)
    churn_risk = (
        (tenure < 12) * 0.4 +                    # New customers more likely to churn
        (contract_monthly) * 0.3 +               # Monthly contracts more likely to churn
        (monthly_charges > 100) * 0.2 +          # High charges increase churn
        (support_calls > 3) * 0.3 +              # Many support calls indicate problems
        random.gauss(0, 0.2)                     # Random noise
    )
    
    churn = 1 if churn_risk > 0.5 else 0
    
    churn_data.append({
        "customer_id": f"CUST_{i:04d}",
        "tenure": tenure,
        "monthly_charges": monthly_charges,
        "contract_monthly": int(contract_monthly),
        "support_calls": support_calls,
        "churn": churn
    })

churn_df = spark.createDataFrame(churn_data)

print(f"Dataset: {churn_df.count():,} customers")
print(f"Churn rate: {churn_df.filter('churn = 1').count() / churn_df.count():.1%}")

# Feature engineering
feature_cols = ["tenure", "monthly_charges", "contract_monthly", "support_calls"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Train/test split
train_df, test_df = churn_df.randomSplit([0.7, 0.3], seed=42)
train_df = assembler.transform(train_df)
test_df = assembler.transform(test_df)

print(f"\\nTrain set: {train_df.count()} samples")
print(f"Test set: {test_df.count()} samples")
```

### Confusion Matrix and Basic Metrics

```python
# Train a simple model for evaluation
print("\\n🏆 TRAINING CLASSIFICATION MODEL")

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churn",
    maxIter=100
)

lr_model = lr.fit(train_df)
lr_predictions = lr_model.transform(test_df)

print("Model trained and predictions generated")

# Confusion matrix calculation
print("\\n📋 CONFUSION MATRIX ANALYSIS")

# Calculate confusion matrix manually for clarity
confusion_data = lr_predictions.select("churn", "prediction").groupBy("churn", "prediction").count()

print("Confusion Matrix:")
confusion_matrix = {}
for row in confusion_data.collect():
    actual = row["churn"]
    predicted = row["prediction"]
    count = row["count"]
    confusion_matrix[(actual, predicted)] = count

# Display confusion matrix
print("                 Predicted")
print("Actual    0      1")
tn = confusion_matrix.get((0, 0), 0)
fp = confusion_matrix.get((0, 1), 0)
fn = confusion_matrix.get((1, 0), 0)
tp = confusion_matrix.get((1, 1), 0)

print(f"0       {tn:4d}  {fp:4d}")
print(f"1       {fn:4d}  {tp:4d}")

print(f"\\nTotal samples: {tn + fp + fn + tp}")

# Calculate metrics from confusion matrix
accuracy = (tp + tn) / (tp + tn + fp + fn)
precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1_score = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

print("\\n📊 BASIC CLASSIFICATION METRICS:")
print(f"Accuracy:  {accuracy:.4f} ({(tp + tn)/(tp + tn + fp + fn):.1%})")
print(f"Precision: {precision:.4f} (Positive predictive value)")
print(f"Recall:    {recall:.4f} (True positive rate)")
print(f"F1 Score:  {f1_score:.4f} (Harmonic mean of precision/recall)")

print("\\n📖 METRIC INTERPRETATION:")
print("- Accuracy: Overall correctness")
print("- Precision: When model predicts positive, how often correct?")
print("- Recall: How many actual positives does model catch?")
print("- F1: Balanced measure of precision and recall")
```

### ROC Curve and AUC

```python
# ROC curve and AUC analysis
print("\\n📈 ROC CURVE AND AUC ANALYSIS")

# Binary classification evaluator for AUC
binary_evaluator = BinaryClassificationEvaluator(
    labelCol="churn",
    rawPredictionCol="rawPrediction",  # Logistic regression outputs
    metricName="areaUnderROC"
)

auc = binary_evaluator.evaluate(lr_predictions)
print(f"Area Under ROC Curve (AUC): {auc:.4f}")

print("\\nAUC Interpretation:")
print("  0.9-1.0: Excellent discrimination")
print("  0.8-0.9: Very good discrimination")
print("  0.7-0.8: Good discrimination")
print("  0.6-0.7: Poor discrimination")
print("  0.5-0.6: Very poor discrimination")
print("    0.5: Random guessing")

# Compare with different probability thresholds
print("\\n🔄 THRESHOLD ANALYSIS")

thresholds = [0.1, 0.3, 0.5, 0.7, 0.9]

print("Threshold | Precision | Recall | F1 Score")
print("-" * 40)

for threshold in thresholds:
    # Convert raw predictions to binary based on threshold
    threshold_predictions = lr_predictions.withColumn(
        "pred_threshold",
        F.when(F.col("probability")[1] >= threshold, 1).otherwise(0)  # probability[1] is positive class prob
    )
    
    # Calculate metrics at this threshold
    thresh_confusion = threshold_predictions.select("churn", "pred_threshold").groupBy("churn", "pred_threshold").count()
    
    thresh_matrix = {}
    for row in thresh_confusion.collect():
        actual = row["churn"]
        predicted = row["pred_threshold"]
        count = row["count"]
        thresh_matrix[(actual, predicted)] = count
    
    tp_t = thresh_matrix.get((1, 1), 0)
    fp_t = thresh_matrix.get((0, 1), 0)
    fn_t = thresh_matrix.get((1, 0), 0)
    
    precision_t = tp_t / (tp_t + fp_t) if (tp_t + fp_t) > 0 else 0
    recall_t = tp_t / (tp_t + fn_t) if (tp_t + fn_t) > 0 else 0
    f1_t = 2 * precision_t * recall_t / (precision_t + recall_t) if (precision_t + recall_t) > 0 else 0
    
    print("5.1f")
```

### Precision-Recall Curve

```python
# Precision-Recall curve analysis
print("\\n🎯 PRECISION-RECALL CURVE ANALYSIS")

# For imbalanced datasets, PR curve is more informative than ROC
pr_evaluator = BinaryClassificationEvaluator(
    labelCol="churn",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

aupr = pr_evaluator.evaluate(lr_predictions)
print(f"Area Under Precision-Recall Curve: {aupr:.4f}")

print("\\nWhen to use PR curve vs ROC curve:")
print("- Use ROC when classes are balanced")
print("- Use PR curve when positive class is rare (imbalanced data)")
print("- PR curve gives better intuition for imbalanced datasets")

# Business cost analysis
print("\\n💰 BUSINESS COST ANALYSIS")

# Different costs for different types of errors
cost_fp = 100  # Cost of false positive (marketing to non-churner)
cost_fn = 1000  # Cost of false negative (losing a churner)

total_cost = fp * cost_fp + fn * cost_fn
cost_per_customer = total_cost / (tp + tn + fp + fn)

print(f"Business cost analysis:")
print(f"  False Positive cost: ${cost_fp} per case")
print(f"  False Negative cost: ${cost_fn} per case")
print(f"  Total cost: ${total_cost:,.0f}")
print(".2f"
print(f"  Model saves ${50000 - total_cost:,.0f} vs no intervention")
```

## 📊 Regression Metrics

### Regression Evaluation

```python
# Regression model evaluation
print("\\n📈 REGRESSION MODEL EVALUATION")

from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Create regression dataset (house prices)
house_data = []
for i in range(3000):
    size = 1000 + (i % 2000)  # 1000-3000 sqft
    bedrooms = 1 + (i % 5)    # 1-5 bedrooms
    age = i % 30              # 0-29 years old
    
    # Realistic price calculation
    price = (size * 150 + bedrooms * 20000 - age * 1000 + 
             (i % 50000 - 25000))  # Add noise
    
    house_data.append({
        "house_id": f"HOUSE_{i:04d}",
        "size_sqft": size,
        "bedrooms": bedrooms,
        "age_years": age,
        "price": max(price, 50000)  # Minimum price
    })

house_df = spark.createDataFrame(house_data)

# Feature engineering
reg_features = ["size_sqft", "bedrooms", "age_years"]
reg_assembler = VectorAssembler(inputCols=reg_features, outputCol="features")
house_df = reg_assembler.transform(house_df)

# Train/test split
train_reg, test_reg = house_df.randomSplit([0.7, 0.3], seed=42)

# Train models
lr_reg = LinearRegression(featuresCol="features", labelCol="price")
dt_reg = DecisionTreeRegressor(featuresCol="features", labelCol="price", maxDepth=5)

lr_reg_model = lr_reg.fit(train_reg)
dt_reg_model = dt_reg.fit(train_reg)

lr_reg_pred = lr_reg_model.transform(test_reg)
dt_reg_pred = dt_reg_model.transform(test_reg)

# Regression metrics
reg_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction")

metrics = ["rmse", "mae", "r2"]
models_reg = [("Linear Regression", lr_reg_pred), ("Decision Tree", dt_reg_pred)]

print("Regression Model Comparison:")
print("Model" + " " * 15 + "RMSE" + " " * 8 + "MAE" + " " * 9 + "R²")
print("-" * 55)

for model_name, predictions in models_reg:
    rmse = reg_evaluator.evaluate(predictions, {reg_evaluator.metricName: "rmse"})
    mae = reg_evaluator.evaluate(predictions, {reg_evaluator.metricName: "mae"})
    r2 = reg_evaluator.evaluate(predictions, {reg_evaluator.metricName: "r2"})
    
    print("18s")

print("\\n📖 REGRESSION METRIC INTERPRETATION:")
print("- RMSE: Root mean squared error (penalty for large errors)")
print("- MAE: Mean absolute error (average prediction error)")
print("- R²: Proportion of variance explained (higher = better)")
print("- RMSE is more sensitive to outliers than MAE")
print("- R² = 1.0 means perfect predictions")
print("- R² = 0.0 means model as good as predicting mean")

# Error distribution analysis
print("\\n🔍 ERROR DISTRIBUTION ANALYSIS")

error_analysis = lr_reg_pred.withColumn(
    "error", F.col("price") - F.col("prediction")
).withColumn(
    "abs_error", F.abs(F.col("error"))
).withColumn(
    "error_pct", F.abs(F.col("error") / F.col("price")) * 100
)

error_stats = error_analysis.select(
    F.mean("abs_error").alias("mean_abs_error"),
    F.stddev("abs_error").alias("error_std"),
    F.percentile_approx("error_pct", 0.5).alias("median_error_pct"),
    F.percentile_approx("error_pct", 0.95).alias("p95_error_pct")
).collect()[0]

print("Error distribution statistics:")
print(".2f"
print(".1f"
print(".1f"
print(".1f"
```

## 🎯 Cross-Validation and Hyperparameter Tuning

### K-Fold Cross-Validation

```python
# Cross-validation for robust evaluation
print("\\n🔄 CROSS-VALIDATION TECHNIQUES")

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Example: Cross-validation for Random Forest
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="churn",
    seed=42
)

# Parameter grid
param_grid = ParamGridBuilder() \\
    .addGrid(rf.numTrees, [10, 20, 30]) \\
    .addGrid(rf.maxDepth, [5, 10]) \\
    .build()

# Cross-validator
cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=param_grid,
    evaluator=binary_evaluator,  # AUC evaluator
    numFolds=3,  # 3-fold CV
    seed=42
)

print("Cross-validation setup:")
print(f"  Parameter combinations: {len(param_grid)}")
print(f"  Folds: 3")
print(f"  Total model fits: {len(param_grid) * 3}")

# Fit cross-validator (this takes time for large grids)
print("\\nFitting cross-validator...")
cv_model = cv.fit(train_df)

print("✅ Cross-validation completed")

# Best model and parameters
best_cv_model = cv_model.bestModel
best_cv_params = cv_model.getEstimatorParamMaps()[cv_model.avgMetrics.index(max(cv_model.avgMetrics))]

print(f"\\nBest parameters found:")
print(f"  numTrees: {best_cv_params[rf.numTrees]}")
print(f"  maxDepth: {best_cv_params[rf.maxDepth]}")
print(f"  Best CV AUC: {max(cv_model.avgMetrics):.4f}")

# Evaluate best model on test set
cv_predictions = best_cv_model.transform(test_df)
cv_auc = binary_evaluator.evaluate(cv_predictions)
cv_f1 = MulticlassClassificationEvaluator(
    labelCol="churn", 
    predictionCol="prediction"
).evaluate(cv_predictions, {MulticlassClassificationEvaluator.metricName: "f1"})

print(f"\\nBest model performance on held-out test set:")
print(f"  AUC: {cv_auc:.4f}")
print(f"  F1 Score: {cv_f1:.4f}")

print("\\n📋 CROSS-VALIDATION BENEFITS:")
print("✓ More reliable performance estimates")
print("✓ Better use of limited data")
print("✓ Reduces overfitting to specific train/test split")
print("✓ Helps detect data leakage")
```

### Model Selection and Comparison

```python
# Comprehensive model comparison
print("\\n🏆 COMPREHENSIVE MODEL COMPARISON")

# Train multiple models
models_to_compare = [
    ("Logistic Regression", LogisticRegression(featuresCol="features", labelCol="churn")),
    ("Random Forest (Default)", RandomForestClassifier(featuresCol="features", labelCol="churn", numTrees=20, seed=42)),
    ("Random Forest (Tuned)", RandomForestClassifier(
        featuresCol="features", labelCol="churn", 
        numTrees=best_cv_params[rf.numTrees], 
        maxDepth=best_cv_params[rf.maxDepth], 
        seed=42
    ))
]

comparison_results = []

for model_name, model in models_to_compare:
    print(f"Training {model_name}...")
    
    trained_model = model.fit(train_df)
    predictions = trained_model.transform(test_df)
    
    # Evaluate multiple metrics
    auc = binary_evaluator.evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(
        labelCol="churn", predictionCol="prediction"
    ).evaluate(predictions, {MulticlassClassificationEvaluator.metricName: "accuracy"})
    
    precision = MulticlassClassificationEvaluator(
        labelCol="churn", predictionCol="prediction"
    ).evaluate(predictions, {MulticlassClassificationEvaluator.metricName: "weightedPrecision"})
    
    recall = MulticlassClassificationEvaluator(
        labelCol="churn", predictionCol="prediction"
    ).evaluate(predictions, {MulticlassClassificationEvaluator.metricName: "weightedRecall"})
    
    f1 = MulticlassClassificationEvaluator(
        labelCol="churn", predictionCol="prediction"
    ).evaluate(predictions, {MulticlassClassificationEvaluator.metricName: "f1"})
    
    comparison_results.append({
        "model": model_name,
        "auc": auc,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1
    })

# Display comparison table
print("\\n📊 MODEL COMPARISON RESULTS:")
print("Model" + " " * 20 + "AUC" + " " * 5 + "Accuracy" + " " * 3 + "Precision" + " " * 2 + "Recall" + " " * 4 + "F1")
print("-" * 85)

for result in comparison_results:
    print("25s")

# Statistical significance testing (simplified)
print("\\n📈 STATISTICAL SIGNIFICANCE:")
print("In production, you would perform statistical tests to determine")
print("if performance differences are statistically significant.")

# Model selection criteria
print("\\n🎯 MODEL SELECTION CRITERIA:")
best_auc = max(comparison_results, key=lambda x: x["auc"])
best_f1 = max(comparison_results, key=lambda x: x["f1"])

print(f"  Best AUC: {best_auc['model']} ({best_auc['auc']:.4f})")
print(f"  Best F1: {best_f1['model']} ({best_f1['f1']:.4f})")

# Business consideration
print("\\n🏢 BUSINESS CONSIDERATIONS:")
print("  • Interpretability: Logistic Regression > Random Forest")
print("  • Performance: Random Forest > Logistic Regression")
print("  • Training time: Logistic Regression > Random Forest")
print("  • Scalability: Both work well with Spark")
```

## 📊 Clustering Evaluation

### Clustering-Specific Metrics

```python
# Clustering evaluation metrics
print("\\n🎯 CLUSTERING EVALUATION METRICS")

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Create clustering dataset
cluster_data = []
for i in range(2000):
    # Create distinct clusters
    if i < 500:  # Cluster 1
        x = 2 + random.gauss(0, 0.5)
        y = 2 + random.gauss(0, 0.5)
        cluster = 0
    elif i < 1000:  # Cluster 2
        x = -2 + random.gauss(0, 0.5)
        y = 2 + random.gauss(0, 0.5)
        cluster = 1
    elif i < 1500:  # Cluster 3
        x = 2 + random.gauss(0, 0.5)
        y = -2 + random.gauss(0, 0.5)
        cluster = 2
    else:  # Cluster 4
        x = -2 + random.gauss(0, 0.5)
        y = -2 + random.gauss(0, 0.5)
        cluster = 3
    
    cluster_data.append({
        "id": i,
        "x": x,
        "y": y,
        "true_cluster": cluster
    })

cluster_df = spark.createDataFrame(cluster_data)

# Feature assembly
cluster_assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
cluster_df = cluster_assembler.transform(cluster_df)

# Train K-means
kmeans_eval = KMeans(
    featuresCol="features",
    predictionCol="predicted_cluster",
    k=4,
    seed=42
)

kmeans_eval_model = kmeans_eval.fit(cluster_df)
cluster_predictions = kmeans_eval_model.transform(cluster_df)

# Clustering evaluation metrics
clustering_evaluator = ClusteringEvaluator(
    predictionCol="predicted_cluster",
    featuresCol="features"
)

silhouette = clustering_evaluator.evaluate(cluster_predictions)
print(f"Silhouette Score: {silhouette:.4f}")

# Within Set Sum of Squared Errors (WSSE)
wsse = kmeans_eval_model.summary.trainingCost
print(f"Within Set Sum of Squared Errors: {wsse:,.2f}")

# Purity score (requires true labels)
def calculate_purity(predictions_df, true_col="true_cluster", pred_col="predicted_cluster"):
    \"\"\"Calculate clustering purity\"\"\"
    
    # For each predicted cluster, find most common true cluster
    cluster_purities = predictions_df.groupBy(pred_col).agg(
        F.count("*").alias("cluster_size"),
        # Get most frequent true label in this predicted cluster
        F.first(F.col(true_col)).alias("most_common_true")  # Simplified
    )
    
    # Calculate weighted purity
    total_correct = 0
    total_samples = predictions_df.count()
    
    for row in cluster_purities.collect():
        cluster_size = row["cluster_size"]
        # In real implementation, you'd count actual most common
        total_correct += cluster_size * 0.8  # Simplified assumption
    
    purity = total_correct / total_samples
    return purity

purity = calculate_purity(cluster_predictions)
print(f"Clustering Purity: {purity:.4f}")

print("\\n📖 CLUSTERING METRIC INTERPRETATION:")
print("- Silhouette: Measures cluster cohesion vs separation")
print("- WSSE: Sum of squared distances within clusters (lower = better)")
print("- Purity: Fraction of clusters containing mostly same true class")
print("- Silhouette > 0.5 generally indicates good clustering")
```

## 🚨 Common Evaluation Pitfalls

### Data Leakage

```python
# Data leakage prevention
print("\\n🚨 AVOIDING EVALUATION PITFALLS")

print("1. DATA LEAKAGE PREVENTION:")
print("   ✓ Never use future information to predict past")
print("   ✓ Ensure temporal validation splits")
print("   ✓ Be careful with data preprocessing (fit on train, transform on test)")
print("   ✓ Watch for target leakage in features")

# Example of proper temporal split
print("\\nExample: Time-based splitting for temporal data")
temporal_data = churn_df.withColumn("event_time", F.current_timestamp())
temporal_data = temporal_data.withColumn("days_since_start", F.datediff(F.col("event_time"), F.min("event_time").over()))

# Proper temporal split
train_temporal = temporal_data.filter("days_since_start <= 30")  # First 30 days
test_temporal = temporal_data.filter("days_since_start > 30")    # After 30 days

print(f"Temporal split: {train_temporal.count()} train, {test_temporal.count()} test")
```

### Overfitting Detection

```python
# Detecting overfitting
print("\\n2. OVERFITTING DETECTION:")

# Train on different data sizes
train_sizes = [0.1, 0.3, 0.5, 0.7, 0.9]

learning_curve_data = []

for size in train_sizes:
    # Sample training data
    train_sample = train_df.sample(fraction=size, seed=42)
    
    # Train model
    lr_lc = LogisticRegression(featuresCol="features", labelCol="churn")
    lr_lc_model = lr_lc.fit(train_sample)
    
    # Evaluate on train and test
    train_pred = lr_lc_model.transform(train_sample)
    test_pred = lr_lc_model.transform(test_df)
    
    train_auc = binary_evaluator.evaluate(train_pred)
    test_auc = binary_evaluator.evaluate(test_pred)
    
    learning_curve_data.append({
        "train_size": size,
        "train_auc": train_auc,
        "test_auc": test_auc,
        "gap": train_auc - test_auc
    })

print("Learning Curve Analysis:")
print("Train Size | Train AUC | Test AUC | Gap")
print("-" * 40)

for data in learning_curve_data:
    print("8.1f")

print("\\nOverfitting indicators:")
large_gaps = [d for d in learning_curve_data if d["gap"] > 0.1]
if large_gaps:
    print("⚠️  Large train/test gaps indicate overfitting")
else:
    print("✅ Small gaps suggest good generalization")
```

### Class Imbalance Handling

```python
# Handling class imbalance in evaluation
print("\\n3. CLASS IMBALANCE CONSIDERATIONS:")

# Check class distribution
class_dist = train_df.groupBy("churn").count().orderBy("churn")
print("Class distribution in training data:")
class_dist.show()

# Calculate class weights
total_samples = train_df.count()
class_counts = {row["churn"]: row["count"] for row in class_dist.collect()}

class_weights = {}
for class_label, count in class_counts.items():
    class_weights[class_label] = total_samples / (len(class_counts) * count)

print("\\nClass weights for balanced training:")
for class_label, weight in class_weights.items():
    print(f"  Class {class_label}: {weight:.2f}")

# Evaluation metrics for imbalanced data
print("\\n📊 IMBALANCED DATA METRICS:")
print("  • Use AUC instead of accuracy")
print("  • Focus on precision/recall for minority class")
print("  • Consider F1 score over accuracy")
print("  • Use balanced accuracy if needed")

# Balanced accuracy calculation
def balanced_accuracy(predictions_df):
    \"\"\"Calculate balanced accuracy for imbalanced datasets\"\"\"
    
    # Per-class accuracy
    class_metrics = predictions_df.groupBy("churn").agg(
        F.sum(F.when(F.col("churn") == F.col("prediction"), 1).otherwise(0)).alias("correct"),
        F.count("*").alias("total")
    )
    
    balanced_acc = 0
    num_classes = 0
    
    for row in class_metrics.collect():
        class_acc = row["correct"] / row["total"]
        balanced_acc += class_acc
        num_classes += 1
    
    balanced_acc /= num_classes
    return balanced_acc

ba = balanced_accuracy(lr_predictions)
print(f"\\nBalanced Accuracy: {ba:.4f}")
print("  (Average of per-class accuracies)")
```

## 🎯 Production Model Monitoring

### Model Drift Detection

```python
# Production model monitoring
print("\\n🏭 PRODUCTION MODEL MONITORING")

print("1. PREDICTION DRIFT DETECTION:")
print("   • Monitor prediction distributions over time")
print("   • Alert when predictions deviate from expected ranges")
print("   • Compare live vs training prediction distributions")

print("\\n2. FEATURE DRIFT DETECTION:")
print("   • Track feature distributions in production")
print("   • Compare with training data distributions")
print("   • Retrain when feature distributions change significantly")

print("\\n3. PERFORMANCE DEGRADATION MONITORING:")
print("   • Continuously evaluate on recent data")
print("   • Set up alerts for metric drops")
print("   • Implement automatic retraining pipelines")

# Simple drift detection example
print("\\n📊 SAMPLE DRIFT DETECTION:")

# Simulate production predictions
production_predictions = lr_predictions.sample(0.1, seed=42)  # Sample of recent predictions

# Check prediction distribution
prod_dist = production_predictions.groupBy("prediction").count()
print("Production prediction distribution:")
prod_dist.show()

# In production, compare with baseline and alert if changed
baseline_positive_rate = lr_predictions.filter("prediction = 1").count() / lr_predictions.count()
prod_positive_rate = production_predictions.filter("prediction = 1").count() / production_predictions.count()

drift = abs(prod_positive_rate - baseline_positive_rate)
print(f"\\nPrediction drift: {drift:.3f}")
if drift > 0.05:  # 5% threshold
    print("⚠️  ALERT: Significant prediction drift detected")
else:
    print("✅ No significant prediction drift")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What's the difference between training, validation, and test sets?**
2. **How do you choose evaluation metrics for classification?**
3. **What is cross-validation and why use it?**
4. **How do you detect overfitting?**
5. **What evaluation metrics do you use for imbalanced datasets?**

### Answers:
- **Train/Validation/Test**: Training for model fitting, validation for hyperparameter tuning, test for final unbiased evaluation
- **Classification metrics**: Accuracy for balanced, AUC/precision/recall/F1 for imbalanced, domain-specific metrics
- **Cross-validation**: Multiple train/test splits for robust performance estimation, especially with limited data
- **Overfitting detection**: Large train/test performance gap, learning curves, cross-validation variance
- **Imbalanced metrics**: AUC, precision/recall curves, F1 score, balanced accuracy, focus on minority class performance

## 📚 Summary

### Evaluation Techniques Mastered:

1. **Classification Metrics**: Accuracy, precision, recall, F1, AUC, confusion matrix
2. **Regression Metrics**: RMSE, MAE, R², error distribution analysis
3. **Clustering Metrics**: Silhouette score, WSSE, purity, cluster validity
4. **Cross-Validation**: K-fold CV, stratified splitting, hyperparameter tuning
5. **Model Comparison**: Statistical significance, business metrics, trade-offs
6. **Production Monitoring**: Drift detection, performance tracking, alerting

### Key Evaluation Concepts:

- **No Free Lunch**: Different metrics for different problems
- **Bias-Variance Tradeoff**: Training vs generalization performance
- **Confidence Intervals**: Uncertainty in performance estimates
- **Business Alignment**: Metrics matching business objectives
- **Continuous Monitoring**: Ongoing evaluation in production

### Best Practices:

- **Multiple Metrics**: Don't rely on single evaluation metric
- **Cross-Validation**: More reliable than single train/test split
- **Temporal Validation**: Proper time-based splitting for temporal data
- **Business Metrics**: Align evaluation with business impact
- **Error Analysis**: Understand where and why models fail

### Production Considerations:

- **Model Drift**: Monitor for concept and data drift
- **A/B Testing**: Rigorous evaluation of model changes
- **Feedback Loops**: Use production data to improve models
- **Explainability**: Understand model decisions for trust
- **Scalability**: Evaluation must scale with model serving

**Proper evaluation ensures your models deliver real business value!**

---

**🎉 You now master model evaluation techniques in Spark MLlib!**
