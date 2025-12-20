# 🔧 ML Pipeline Design: End-to-End Machine Learning Workflows

## 🎯 Overview

**ML Pipeline Design** involves creating reproducible, scalable, and maintainable end-to-end machine learning workflows. Pipelines orchestrate data preprocessing, feature engineering, model training, evaluation, and deployment into cohesive systems. Spark MLlib provides powerful pipeline abstractions for building production-ready ML systems.

## 🔍 Understanding ML Pipelines

### Why Pipelines Matter

**Without pipelines, ML development becomes:**
- **Error-prone**: Manual steps lead to inconsistencies
- **Not reproducible**: Hard to recreate exact same results
- **Difficult to maintain**: Changes break downstream components
- **Inefficient**: Repeated work and debugging cycles

**With pipelines:**
- **Reproducible**: Same inputs always produce same outputs
- **Maintainable**: Changes propagate correctly through workflow
- **Scalable**: Easy to run on different data sizes/environments
- **Testable**: Each component can be tested independently

### Pipeline Components

```
Raw Data → Data Ingestion → Feature Engineering → Model Training → Evaluation → Deployment

1. Data Ingestion: Load and validate data
2. Preprocessing: Clean, transform, handle missing values
3. Feature Engineering: Create and select features
4. Model Training: Fit algorithms on prepared data
5. Model Evaluation: Assess performance and validate
6. Model Deployment: Serve predictions in production
```

## 📊 Building Basic Pipelines

### Simple Classification Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pyspark.sql.functions as F

spark = SparkSession.builder \\
    .appName("ML_Pipeline_Design") \\
    .master("local[*]") \\
    .getOrCreate()

# Create sample dataset
print("📊 CREATING SAMPLE DATASET FOR PIPELINE DEMO")

# Generate customer data with mixed types
customer_data = []
for i in range(5000):
    age = 18 + (i % 60)
    income = 25000 + (i % 75000)
    category = ["A", "B", "C"][i % 3]
    score = 1 + (i % 100)
    target = 1 if (age > 40 and income > 50000) or (category == "A") else 0
    
    customer_data.append({
        "id": i,
        "age": age,
        "income": income,
        "category": category,
        "score": score,
        "target": target
    })

df = spark.createDataFrame(customer_data)
print(f"Dataset: {df.count():,} records")
print(f"Target distribution: {df.filter('target = 1').count() / df.count():.1%} positive class")

# Train/test split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"\\nTrain: {train_df.count()} records")
print(f"Test: {test_df.count()} records")
```

### Basic Pipeline Construction

```python
# Build a basic ML pipeline
print("\\n🔧 BUILDING BASIC ML PIPELINE")

# 1. Categorical encoding
category_indexer = StringIndexer(
    inputCol="category",
    outputCol="category_index",
    handleInvalid="keep"  # Handle unseen categories
)

category_encoder = OneHotEncoder(
    inputCol="category_index",
    outputCol="category_vec"
)

# 2. Feature assembly
feature_assembler = VectorAssembler(
    inputCols=["age", "income", "score", "category_vec"],
    outputCol="raw_features",
    handleInvalid="keep"  # Handle missing features
)

# 3. Feature scaling
feature_scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# 4. Model
rf_classifier = RandomForestClassifier(
    featuresCol="features",
    labelCol="target",
    numTrees=20,
    maxDepth=5,
    seed=42
)

# Create pipeline
basic_pipeline = Pipeline(stages=[
    category_indexer,
    category_encoder,
    feature_assembler,
    feature_scaler,
    rf_classifier
])

print("Pipeline stages:")
for i, stage in enumerate(basic_pipeline.getStages()):
    stage_name = stage.__class__.__name__
    if hasattr(stage, "getNumTrees"):
        stage_name += f" ({stage.getNumTrees} trees)"
    print(f"  {i+1}. {stage_name}")

# Train pipeline
print("\\n🚀 TRAINING PIPELINE...")
pipeline_model = basic_pipeline.fit(train_df)

print("✅ Pipeline trained successfully")

# Make predictions
predictions = pipeline_model.transform(test_df)

# Evaluate
evaluator = BinaryClassificationEvaluator(
    labelCol="target",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print(f"\\nPipeline AUC: {auc:.4f}")

# Show sample predictions
predictions.select("id", "target", "prediction", "probability").show(10)
```

### Pipeline Persistence and Loading

```python
# Save and load pipelines
print("\\n💾 PIPELINE PERSISTENCE")

# Save the trained pipeline
pipeline_path = "/tmp/customer_pipeline"
pipeline_model.write().overwrite().save(pipeline_path)

print(f"Pipeline saved to: {pipeline_path}")

# Load the pipeline
from pyspark.ml import PipelineModel

loaded_pipeline = PipelineModel.load(pipeline_path)
print("✅ Pipeline loaded successfully")

# Test loaded pipeline
loaded_predictions = loaded_pipeline.transform(test_df.limit(5))
loaded_predictions.select("id", "target", "prediction").show()

# Verify results are identical
original_sample = predictions.limit(5).select("id", "prediction").collect()
loaded_sample = loaded_predictions.select("id", "prediction").collect()

matches = all(orig.prediction == loaded.prediction 
             for orig, loaded in zip(original_sample, loaded_sample))
print(f"\\nPredictions identical: {matches}")
```

## 🔄 Advanced Pipeline Patterns

### Conditional Pipelines

```python
# Conditional pipeline logic
print("\\n🔀 CONDITIONAL PIPELINE PATTERNS")

from pyspark.ml import Transformer
from pyspark.sql import DataFrame

class ConditionalTransformer(Transformer):
    \"\"\"
    Custom transformer that applies different logic based on conditions
    \"\"\"
    
    def __init__(self, condition_col="category", condition_value="A"):
        super().__init__()
        self.condition_col = condition_col
        self.condition_value = condition_value
    
    def _transform(self, df: DataFrame) -> DataFrame:
        # Apply different transformations based on category
        if self.condition_value == "A":
            # For category A: boost age feature
            return df.withColumn("age_boosted", F.col("age") * 1.2)
        else:
            # For other categories: boost income feature
            return df.withColumn("income_boosted", F.col("income") * 1.1)
    
    def _transformSchema(self, schema):
        return schema

# Use conditional transformer
conditional_transformer = ConditionalTransformer()

# Create conditional pipeline
conditional_pipeline = Pipeline(stages=[
    category_indexer,
    conditional_transformer,  # Custom conditional logic
    feature_assembler,
    feature_scaler,
    rf_classifier
])

print("Conditional pipeline created with custom transformer")
```

### Multi-Output Pipelines

```python
# Multi-output pipeline for multiple targets
print("\\n🔢 MULTI-OUTPUT PIPELINES")

from pyspark.ml import Estimator
from pyspark.ml.classification import LogisticRegression

class MultiOutputEstimator(Estimator):
    \"\"\"
    Custom estimator that trains multiple models for different targets
    \"\"\"
    
    def __init__(self, featuresCol="features"):
        super().__init__()
        self.featuresCol = featuresCol
    
    def _fit(self, df: DataFrame):
        # Train separate models for different segments
        high_income_df = df.filter("income > 50000")
        low_income_df = df.filter("income <= 50000")
        
        # Model for high-income customers
        high_model = LogisticRegression(
            featuresCol=self.featuresCol,
            labelCol="target",
            maxIter=50
        ).fit(high_income_df)
        
        # Model for low-income customers  
        low_model = LogisticRegression(
            featuresCol=self.featuresCol,
            labelCol="target",
            maxIter=50
        ).fit(low_income_df)
        
        return MultiOutputModel(high_model, low_model)

class MultiOutputModel:
    \"\"\"
    Custom model for multi-output predictions
    \"\"\"
    def __init__(self, high_model, low_model):
        self.high_model = high_model
        self.low_model = low_model
    
    def transform(self, df: DataFrame) -> DataFrame:
        # Apply appropriate model based on income
        high_income_df = df.filter("income > 50000")
        low_income_df = df.filter("income <= 50000")
        
        high_preds = self.high_model.transform(high_income_df)
        low_preds = self.low_model.transform(low_income_df)
        
        # Combine results
        return high_preds.union(low_preds)

multi_estimator = MultiOutputEstimator()
print("Multi-output estimator created for different customer segments")
```

### Pipeline Branching

```python
# Pipeline branching for different data paths
print("\\n🌿 PIPELINE BRANCHING")

class PipelineBrancher(Transformer):
    \"\"\"
    Splits data into different branches for parallel processing
    \"\"\"
    
    def _transform(self, df: DataFrame) -> DataFrame:
        # Branch 1: High-value customers
        high_value = df.filter("income > 60000").withColumn("segment", F.lit("high_value"))
        
        # Branch 2: Medium-value customers
        medium_value = df.filter("income BETWEEN 30000 AND 60000").withColumn("segment", F.lit("medium_value"))
        
        # Branch 3: Low-value customers
        low_value = df.filter("income < 30000").withColumn("segment", F.lit("low_value"))
        
        # Combine branches
        return high_value.union(medium_value).union(low_value)
    
    def _transformSchema(self, schema):
        return schema

# Create branched pipeline
brancher = PipelineBrancher()

branched_pipeline = Pipeline(stages=[
    brancher,  # Split data into segments
    feature_assembler,
    feature_scaler,
    rf_classifier
])

print("Branched pipeline created for different customer segments")
```

## ⚙️ Pipeline Tuning and Optimization

### Hyperparameter Tuning in Pipelines

```python
# Hyperparameter tuning with pipelines
print("\\n⚙️ PIPELINE HYPERPARAMETER TUNING")

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Create parameter grid for pipeline tuning
param_grid = ParamGridBuilder() \\
    .addGrid(rf_classifier.numTrees, [10, 20, 30]) \\
    .addGrid(rf_classifier.maxDepth, [5, 7, 10]) \\
    .addGrid(feature_scaler.withMean, [True, False]) \\
    .build()

print(f"Parameter combinations: {len(param_grid)}")

# Cross-validator with pipeline
pipeline_cv = CrossValidator(
    estimator=basic_pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

# Tune pipeline (this may take time)
print("\\nTuning pipeline hyperparameters...")
tuned_pipeline_model = pipeline_cv.fit(train_df.sample(0.3, seed=42))  # Sample for speed

print("✅ Pipeline tuning completed")

# Best pipeline performance
best_pipeline = tuned_pipeline_model.bestModel
tuned_predictions = best_pipeline.transform(test_df)
tuned_auc = evaluator.evaluate(tuned_predictions)

print(f"\\nTuned pipeline AUC: {tuned_auc:.4f}")

# Best parameters
best_params = tuned_pipeline_model.getEstimatorParamMaps()[
    tuned_pipeline_model.avgMetrics.index(max(tuned_pipeline_model.avgMetrics))
]

print("\\nBest parameters found:")
print(f"  numTrees: {best_params[rf_classifier.numTrees]}")
print(f"  maxDepth: {best_params[rf_classifier.maxDepth]}")
print(f"  withMean: {best_params[feature_scaler.withMean]}")
```

### Pipeline Performance Optimization

```python
# Pipeline performance optimization
print("\\n🚀 PIPELINE PERFORMANCE OPTIMIZATION")

# 1. Caching for iterative operations
print("1. STRATEGIC CACHING")

# Cache intermediate results for faster iteration
cached_train = train_df.cache()
print(f"Training data cached: {cached_train.count()} records")

# 2. Pipeline stage optimization
print("\\n2. STAGE OPTIMIZATION")

# Use broadcast joins for small lookup tables
from pyspark.sql.functions import broadcast

# Example: Join with small reference data
reference_data = spark.createDataFrame([
    ("A", "Premium", 1.2),
    ("B", "Standard", 1.0),
    ("C", "Basic", 0.8)
], ["category", "tier", "multiplier"])

# Broadcast join in pipeline
broadcast_join = broadcast(reference_data)

# 3. Parallel processing
print("\\n3. PARALLEL PROCESSING")

# Configure Spark for pipeline parallelism
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.default.parallelism", "8")

print("Spark configured for parallel pipeline execution")

# 4. Memory optimization
print("\\n4. MEMORY OPTIMIZATION")

# Use smaller data types where possible
optimized_df = df.withColumn("age", F.col("age").cast("short")) \\
    .withColumn("income", F.col("income").cast("integer")) \\
    .withColumn("score", F.col("score").cast("short"))

print("Data types optimized for memory efficiency")

# 5. Incremental pipeline updates
print("\\n5. INCREMENTAL UPDATES")

# Strategy for updating pipelines without full retraining
incremental_strategies = [
    "Online learning algorithms (SGD, etc.)",
    "Mini-batch updates with new data",
    "Model ensembles with rolling windows",
    "Feature store updates with model retraining",
    "A/B testing with gradual rollout"
]

print("Incremental update strategies:")
for strategy in incremental_strategies:
    print(f"  • {strategy}")
```

## 🔧 Pipeline Validation and Testing

### Pipeline Unit Testing

```python
# Pipeline validation and testing
print("\\n🧪 PIPELINE VALIDATION AND TESTING")

def test_pipeline_stage(pipeline_stage, test_data, expected_output_cols):
    \"\"\"
    Unit test for individual pipeline stages
    \"\"\"
    try:
        # Test stage transformation
        output_data = pipeline_stage.transform(test_data)
        
        # Check output columns exist
        output_cols = output_data.columns
        missing_cols = set(expected_output_cols) - set(output_cols)
        
        if missing_cols:
            print(f"❌ Missing output columns: {missing_cols}")
            return False
        
        # Check for null values in output
        null_counts = {}
        for col in expected_output_cols:
            null_count = output_data.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        
        total_nulls = sum(null_counts.values())
        if total_nulls > 0:
            print(f"⚠️  Null values in output: {null_counts}")
        
        print("✅ Pipeline stage test passed")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline stage test failed: {e}")
        return False

# Test individual pipeline stages
print("Testing pipeline stages:")

# Test category indexer
test_data = train_df.limit(100)
test_pipeline_stage(category_indexer, test_data, ["category_index"])

# Test feature assembler  
test_pipeline_stage(feature_assembler, test_data, ["raw_features"])

print("\\nPipeline component testing completed")
```

### Pipeline Integration Testing

```python
# Pipeline integration testing
print("\\n🔗 PIPELINE INTEGRATION TESTING")

def test_pipeline_integration(pipeline, train_data, test_data, expected_metrics):
    \"\"\"
    Integration test for complete pipeline
    \"\"\"
    try:
        # Train pipeline
        model = pipeline.fit(train_data)
        
        # Make predictions
        predictions = model.transform(test_data)
        
        # Validate predictions
        pred_cols = ["prediction", "probability", "rawPrediction"]
        missing_pred_cols = set(pred_cols) - set(predictions.columns)
        
        if missing_pred_cols:
            print(f"❌ Missing prediction columns: {missing_pred_cols}")
            return False
        
        # Check prediction distributions
        pred_dist = predictions.groupBy("prediction").count()
        print("Prediction distribution:")
        pred_dist.show()
        
        # Evaluate metrics
        auc = evaluator.evaluate(predictions)
        print(f"Pipeline AUC: {auc:.4f}")
        
        # Check against expected metrics
        if auc < expected_metrics.get("min_auc", 0.5):
            print(f"⚠️  AUC below threshold: {auc:.4f}")
            return False
        
        print("✅ Pipeline integration test passed")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline integration test failed: {e}")
        return False

# Run integration test
integration_test_passed = test_pipeline_integration(
    basic_pipeline, 
    train_df.limit(500),  # Smaller sample for testing
    test_df.limit(200),
    {"min_auc": 0.6}
)

print(f"\\nIntegration test result: {'PASSED' if integration_test_passed else 'FAILED'}")
```

### Pipeline Monitoring and Alerting

```python
# Pipeline monitoring and alerting
print("\\n📊 PIPELINE MONITORING AND ALERTING")

# Pipeline health metrics
pipeline_metrics = {
    "training_time": "Time to train pipeline",
    "prediction_latency": "Time for single prediction",
    "throughput": "Predictions per second",
    "memory_usage": "Memory consumption during training",
    "data_drift": "Changes in input data distribution",
    "model_drift": "Changes in prediction patterns"
}

print("Pipeline monitoring metrics:")
for metric, description in pipeline_metrics.items():
    print(f"  • {metric}: {description}")

# Alerting thresholds
alert_thresholds = {
    "training_time": "> 2x baseline",
    "prediction_latency": "> 100ms",
    "throughput": "< 1000 predictions/second", 
    "memory_usage": "> 80% of available memory",
    "auc_drop": "> 0.05 from baseline"
}

print("\\nAlert thresholds:")
for alert, threshold in alert_thresholds.items():
    print(f"  🚨 {alert}: {threshold}")

# Automated monitoring function
def monitor_pipeline_health(pipeline_model, test_data, baseline_metrics):
    \"\"\"
    Monitor pipeline health and trigger alerts
    \"\"\"
    alerts = []
    
    try:
        # Test predictions
        predictions = pipeline_model.transform(test_data.limit(1000))
        current_auc = evaluator.evaluate(predictions)
        
        # Check AUC drop
        auc_drop = baseline_metrics["auc"] - current_auc
        if auc_drop > 0.05:
            alerts.append(f"AUC dropped by {auc_drop:.3f}")
        
        # Check prediction distribution
        pred_dist = predictions.groupBy("prediction").count().collect()
        total_preds = sum(row["count"] for row in pred_dist)
        
        for row in pred_dist:
            pct = row["count"] / total_preds
            if pct < 0.05:  # Alert if any class < 5%
                alerts.append(f"Low prediction frequency for class {row['prediction']}: {pct:.1%}")
        
        if not alerts:
            print("✅ Pipeline health: All metrics within normal range")
        else:
            print("⚠️  Pipeline health alerts:")
            for alert in alerts:
                print(f"    • {alert}")
                
    except Exception as e:
        alerts.append(f"Pipeline error: {e}")
        print(f"❌ Pipeline health check failed: {e}")
    
    return alerts

# Example monitoring
baseline_metrics = {"auc": 0.82}
alerts = monitor_pipeline_health(pipeline_model, test_df, baseline_metrics)

print(f"\\nMonitoring completed with {len(alerts)} alerts")
```

## 🎯 Production Pipeline Patterns

### Model Versioning and Rollback

```python
# Production pipeline versioning
print("\\n🏭 PRODUCTION PIPELINE PATTERNS")

print("1. MODEL VERSIONING AND ROLLBACK")

# Version control for pipelines
import datetime

def version_pipeline(pipeline_model, version_name=None):
    \"\"\"
    Version and save pipeline with metadata
    \"\"\"
    if version_name is None:
        version_name = f"pipeline_v{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Save pipeline
    version_path = f"/tmp/pipelines/{version_name}"
    pipeline_model.write().overwrite().save(version_path)
    
    # Save metadata
    metadata = {
        "version": version_name,
        "created_at": datetime.datetime.now().isoformat(),
        "spark_version": spark.version,
        "data_shape": f"{train_df.count()} training samples",
        "pipeline_stages": len(pipeline_model.stages),
        "performance": {
            "auc": auc,
            "training_time": "measured in production"
        }
    }
    
    import json
    metadata_path = f"{version_path}/metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"✅ Pipeline versioned: {version_name}")
    return version_name, version_path

# Version current pipeline
version_name, version_path = version_pipeline(pipeline_model)

# Rollback function
def rollback_to_version(version_name):
    \"\"\"
    Rollback to specific pipeline version
    \"\"\"
    rollback_path = f"/tmp/pipelines/{version_name}"
    try:
        rolled_back_pipeline = PipelineModel.load(rollback_path)
        print(f"✅ Rolled back to version: {version_name}")
        return rolled_back_pipeline
    except Exception as e:
        print(f"❌ Rollback failed: {e}")
        return None

print("\\nRollback capability implemented")
```

### A/B Testing Pipelines

```python
# A/B testing for pipelines
print("\\n🧪 A/B TESTING PIPELINES")

def ab_test_pipelines(pipeline_a, pipeline_b, test_data, test_size=0.5):
    \"\"\"
    A/B test two pipeline versions
    \"\"\"
    # Split test data
    a_data, b_data = test_data.randomSplit([test_size, 1-test_size], seed=42)
    
    # Evaluate pipeline A
    a_predictions = pipeline_a.transform(a_data)
    a_auc = evaluator.evaluate(a_predictions)
    
    # Evaluate pipeline B
    b_predictions = pipeline_b.transform(b_data)
    b_auc = evaluator.evaluate(b_predictions)
    
    # Statistical comparison (simplified)
    auc_diff = abs(a_auc - b_auc)
    winner = "A" if a_auc > b_auc else "B"
    
    results = {
        "pipeline_a_auc": a_auc,
        "pipeline_b_auc": b_auc,
        "auc_difference": auc_diff,
        "winner": winner,
        "confidence": "high" if auc_diff > 0.02 else "low"
    }
    
    print("A/B Test Results:")
    print(f"  Pipeline A AUC: {a_auc:.4f}")
    print(f"  Pipeline B AUC: {b_auc:.4f}")
    print(f"  Winner: Pipeline {winner}")
    print(f"  Confidence: {results['confidence']}")
    
    return results

# Example A/B test (would use different pipeline versions in practice)
ab_results = ab_test_pipelines(pipeline_model, pipeline_model, test_df)
```

### Continuous Learning Pipelines

```python
# Continuous learning pipeline
print("\\n🔄 CONTINUOUS LEARNING PIPELINES")

continuous_learning_strategies = [
    "Online learning algorithms that update with streaming data",
    "Periodic batch retraining with recent data windows",
    "Ensemble methods combining old and new models",
    "Active learning for selective data labeling",
    "Model monitoring with automatic retraining triggers"
]

print("Continuous learning strategies:")
for strategy in continuous_learning_strategies:
    print(f"  • {strategy}")

# Example: Rolling window retraining
def retrain_on_rolling_window(pipeline, historical_data, window_days=30):
    \"\"\"
    Retrain pipeline on rolling window of recent data
    \"\"\"
    # In practice, this would:
    # 1. Get recent data (last window_days)
    # 2. Combine with historical data if needed
    # 3. Retrain pipeline
    # 4. Validate performance
    # 5. Deploy if improved
    
    print(f"Retraining on {window_days}-day rolling window...")
    print("  • Filtering recent data")
    print("  • Combining with historical patterns")
    print("  • Retraining pipeline")
    print("  • Validating performance")
    print("  • Deploying if improved")
    
    # Placeholder for actual implementation
    return pipeline

print("\\nRolling window retraining framework implemented")
```

## 🚨 Pipeline Failure Handling

### Pipeline Resilience Patterns

```python
# Pipeline failure handling
print("\\n🚨 PIPELINE FAILURE HANDLING")

failure_handling_patterns = {
    "Graceful Degradation": "Fallback to simpler model if complex pipeline fails",
    "Circuit Breaker": "Stop processing if error rate exceeds threshold",
    "Retry Logic": "Retry failed pipeline stages with exponential backoff",
    "Fallback Pipelines": "Maintain backup pipelines for critical predictions",
    "Error Recovery": "Log errors and continue processing valid data"
}

print("Pipeline resilience patterns:")
for pattern, description in failure_handling_patterns.items():
    print(f"  • {pattern}: {description}")

# Example: Pipeline with fallback
class ResilientPipeline:
    \"\"\"
    Pipeline with automatic fallback mechanisms
    \"\"\"
    
    def __init__(self, primary_pipeline, fallback_pipeline=None):
        self.primary_pipeline = primary_pipeline
        self.fallback_pipeline = fallback_pipeline
        self.use_fallback = False
    
    def fit(self, train_data):
        try:
            self.primary_model = self.primary_pipeline.fit(train_data)
            print("✅ Primary pipeline training successful")
            return self.primary_model
        except Exception as e:
            print(f"⚠️  Primary pipeline failed: {e}")
            if self.fallback_pipeline:
                print("🔄 Falling back to backup pipeline...")
                self.use_fallback = True
                self.fallback_model = self.fallback_pipeline.fit(train_data)
                return self.fallback_model
            else:
                raise e
    
    def transform(self, data):
        model = self.fallback_model if self.use_fallback else self.primary_model
        return model.transform(data)

# Create resilient pipeline
simple_fallback = Pipeline(stages=[feature_assembler, rf_classifier])  # Simpler pipeline
resilient_pipeline = ResilientPipeline(basic_pipeline, simple_fallback)

print("\\nResilient pipeline with fallback created")
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **What are ML pipelines and why are they important?**
2. **How do you handle pipeline failures in production?**
3. **What's the difference between pipeline training and inference?**
4. **How do you version and manage ML pipelines?**
5. **What are some common pipeline optimization techniques?**

### Answers:
- **ML Pipelines**: Orchestrated workflows combining data preprocessing, feature engineering, model training, and evaluation for reproducible ML
- **Pipeline failures**: Graceful degradation, circuit breakers, retry logic, fallback pipelines, comprehensive error logging
- **Training vs inference**: Training fits pipeline parameters on data; inference applies trained pipeline to new data for predictions
- **Versioning**: Git for code, semantic versioning for models, metadata tracking, rollback capabilities
- **Optimization**: Caching, parallelization, memory optimization, incremental updates, performance monitoring

## 📚 Summary

### Pipeline Design Patterns Mastered:

1. **Basic Pipelines**: Linear sequences of preprocessing + model
2. **Conditional Pipelines**: Branching logic based on data conditions
3. **Multi-Output Pipelines**: Multiple models for different targets
4. **Resilient Pipelines**: Failure handling and fallback mechanisms
5. **Production Pipelines**: Versioning, monitoring, A/B testing

### Key Pipeline Concepts:

- **Reproducibility**: Same inputs produce same outputs
- **Modularity**: Independent, testable components
- **Scalability**: Distributed processing capabilities
- **Maintainability**: Easy to modify and extend
- **Monitoring**: Performance tracking and alerting

### Production Pipeline Features:

- **Version Control**: Model and pipeline versioning
- **A/B Testing**: Rigorous evaluation of changes
- **Monitoring**: Health checks and performance metrics
- **Fault Tolerance**: Graceful failure handling
- **Continuous Learning**: Incremental model updates

### Best Practices:

- **Start Simple**: Begin with basic pipelines, add complexity as needed
- **Test Thoroughly**: Unit test stages, integration test pipelines
- **Monitor Continuously**: Track performance and trigger alerts
- **Version Everything**: Code, models, data, and configurations
- **Plan for Failure**: Implement fallback and recovery mechanisms

### Business Impact:

- **Faster Development**: Reusable pipeline components
- **Higher Reliability**: Automated testing and monitoring
- **Easier Maintenance**: Modular, well-tested components
- **Better Performance**: Optimized execution and resource usage
- **Regulatory Compliance**: Auditable, versioned ML systems

**ML pipelines transform ML development from art to engineering!**

---

**🎉 You now master ML pipeline design for production systems!**
