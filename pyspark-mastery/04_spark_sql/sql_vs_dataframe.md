# ⚖️ SQL vs DataFrame API: Choosing the Right Approach

## 🎯 Overview

Spark provides two primary interfaces for working with structured data: **Spark SQL** and **DataFrame API**. Both use the same execution engine (Catalyst optimizer) but offer different programming paradigms. Understanding when to use each is crucial for writing maintainable and efficient Spark applications.

## 🔄 Interface Comparison

### Spark SQL
```python
# Declarative approach
result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
    FROM employees
    WHERE salary > 50000
    GROUP BY department
    HAVING COUNT(*) > 2
    ORDER BY avg_salary DESC
""")
```

### DataFrame API
```python
# Programmatic approach
result = employees_df.filter(col("salary") > 50000) \\
    .groupBy("department") \\
    .agg(avg("salary").alias("avg_salary"), count("*").alias("count")) \\
    .filter(col("count") > 2) \\
    .orderBy(col("avg_salary").desc())
```

**Both produce identical execution plans and performance!**

## 🎯 When to Use Spark SQL

### 1. **Team Familiarity with SQL**

**Best for teams with strong SQL background**
```python
# SQL analysts feel at home
spark.sql("""
    SELECT 
        customer_id,
        SUM(order_amount) as total_spent,
        COUNT(DISTINCT order_id) as order_count,
        MAX(order_date) as last_order_date
    FROM orders
    WHERE order_date >= '2023-01-01'
    GROUP BY customer_id
    HAVING SUM(order_amount) > 1000
""").show()
```

**Advantages:**
- Familiar syntax for SQL developers
- Easy transition from traditional databases
- Readable for business stakeholders
- Standard SQL knowledge applies

### 2. **Complex Joins and Relationships**

**Excellent for multi-table operations**
```python
spark.sql("""
    SELECT 
        c.customer_name,
        p.product_name,
        o.quantity,
        o.order_date,
        cat.category_name
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN products p ON o.product_id = p.product_id
    LEFT JOIN categories cat ON p.category_id = cat.category_id
    WHERE o.order_date >= '2023-01-01'
    ORDER BY o.order_date DESC
""").show()
```

**Advantages:**
- Clear relationship visualization
- Standard JOIN syntax
- Easy to understand table relationships
- Business logic reads like plain English

### 3. **Reporting and Analytics Queries**

**Ideal for complex analytical queries**
```python
# Complex analytics with CTEs and window functions
spark.sql("""
    WITH monthly_sales AS (
        SELECT 
            YEAR(order_date) as year,
            MONTH(order_date) as month,
            SUM(order_amount) as monthly_revenue,
            COUNT(*) as order_count
        FROM orders
        GROUP BY YEAR(order_date), MONTH(order_date)
    ),
    sales_with_growth AS (
        SELECT *,
            LAG(monthly_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
            monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month) as growth
        FROM monthly_sales
    )
    SELECT *,
        ROUND((growth / prev_month_revenue) * 100, 2) as growth_percentage
    FROM sales_with_growth
    ORDER BY year DESC, month DESC
""").show()
```

**Advantages:**
- Complex analytical queries
- Window functions and CTEs
- Easy to modify and extend
- Self-documenting business logic

### 4. **Interactive Data Exploration**

**Great for ad-hoc queries and exploration**
```python
# Interactive exploration
spark.sql("SELECT DISTINCT category FROM products").show()
spark.sql("SELECT COUNT(*) FROM orders WHERE order_date >= '2023-01-01'").show()
spark.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id ORDER BY 2 DESC LIMIT 10").show()
```

**Advantages:**
- Quick iteration and exploration
- Easy to modify queries on the fly
- No recompilation needed
- Immediate feedback

## 🛠️ When to Use DataFrame API

### 1. **Programmatic Data Processing**

**Best for dynamic, programmatic workflows**
```python
def process_customer_data(customer_df, min_orders=5, vip_threshold=10000):
    """Dynamic data processing pipeline"""
    
    # Filter active customers
    active_customers = customer_df.filter(col("last_order_date") >= "2023-01-01")
    
    # Calculate customer metrics
    customer_metrics = active_customers.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        sum("order_amount").alias("total_spent"),
        avg("order_amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date")
    )
    
    # Identify VIP customers
    vip_customers = customer_metrics.filter(
        (col("total_orders") >= min_orders) & 
        (col("total_spent") >= vip_threshold)
    )
    
    return vip_customers

# Usage
result = process_customer_data(orders_df, min_orders=3, vip_threshold=5000)
result.show()
```

**Advantages:**
- Dynamic parameter passing
- Conditional logic in code
- Reusable functions
- Type safety and IDE support

### 2. **Complex Data Transformations**

**Excellent for multi-step transformations**
```python
# Complex ETL pipeline
cleaned_data = (raw_data
    # Remove invalid records
    .filter(col("amount").isNotNull() & (col("amount") > 0))
    
    # Standardize data formats
    .withColumn("customer_name", trim(upper(col("customer_name"))))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    
    # Add calculated columns
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn("is_large_order", when(col("amount") > 1000, "Yes").otherwise("No"))
    
    # Handle duplicates
    .dropDuplicates(["customer_id", "order_date", "amount"])
    
    # Add row numbers for auditing
    .withColumn("row_num", row_number().over(
        Window.partitionBy("customer_id").orderBy(col("order_date").desc())
    ))
)

final_data = cleaned_data.filter(col("row_num") <= 10)  # Keep latest 10 orders per customer
```

**Advantages:**
- Step-by-step debugging
- Complex conditional logic
- Method chaining for readability
- Easy to add/remove transformation steps

### 3. **Type-Safe Operations**

**Compile-time type checking**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema for type safety
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True)
])

# Type-safe operations
def validate_customer_data(df):
    """Type-safe data validation"""
    
    # Type-safe column references
    validated_df = df.filter(
        col("customer_id").isNotNull() & 
        (length(col("customer_id")) > 0)
    ).withColumn(
        "is_valid_email", 
        col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    ).withColumn(
        "signup_date_parsed",
        to_date(col("signup_date"), "yyyy-MM-dd")
    )
    
    return validated_df

# Usage with compile-time checking
validated_customers = validate_customer_data(customers_df)
```

**Advantages:**
- Compile-time error detection
- IDE autocompletion and refactoring
- Type-safe column operations
- Better maintainability

### 4. **UDFs and Custom Logic**

**Necessary for user-defined functions**
```python
# Define UDFs (only possible in DataFrame API)
def calculate_customer_tier(total_spent, total_orders):
    """Complex business logic for customer tiering"""
    if total_spent >= 50000 and total_orders >= 50:
        return "Platinum"
    elif total_spent >= 25000 and total_orders >= 25:
        return "Gold"
    elif total_spent >= 10000 and total_orders >= 10:
        return "Silver"
    else:
        return "Bronze"

tier_udf = udf(calculate_customer_tier, StringType())

# Apply UDF in DataFrame operations
customer_tiers = customer_metrics.withColumn(
    "customer_tier", 
    tier_udf(col("total_spent"), col("total_orders"))
)

# Chain with other operations
final_result = customer_tiers.filter(col("customer_tier").isin(["Gold", "Platinum"])) \\
    .orderBy(col("total_spent").desc())
```

**Advantages:**
- Custom business logic implementation
- Complex calculations not possible in SQL
- Integration with external libraries
- Advanced data transformations

## 🎨 Mixing SQL and DataFrame API

### Best of Both Worlds Approach

```python
# Pattern 1: SQL for data access, DataFrame for processing
def analyze_sales_data(product_filter=None):
    """Mixed approach for optimal development"""
    
    # Use SQL for complex data access
    base_query = f"""
        SELECT 
            o.order_id,
            o.customer_id,
            o.product_id,
            o.quantity,
            o.order_date,
            p.product_name,
            p.category,
            p.price
        FROM orders o
        INNER JOIN products p ON o.product_id = p.product_id
        {"WHERE p.category = '" + product_filter + "'" if product_filter else ""}
    """
    
    sales_data = spark.sql(base_query)
    
    # Use DataFrame API for complex transformations
    analysis = (sales_data
        .withColumn("order_month", date_format("order_date", "yyyy-MM"))
        .withColumn("revenue", col("quantity") * col("price"))
        .withColumn("is_large_order", when(col("revenue") > 500, "Yes").otherwise("No"))
        .groupBy("order_month", "category")
        .agg(
            sum("revenue").alias("monthly_revenue"),
            count("order_id").alias("order_count"),
            avg("revenue").alias("avg_order_value")
        )
        .orderBy("order_month", "monthly_revenue")
    )
    
    return analysis

# Usage
monthly_analysis = analyze_sales_data("Electronics")
monthly_analysis.show()
```

### Pattern 2: DataFrame for ETL, SQL for Reporting

```python
# ETL Pipeline with DataFrame API
def etl_pipeline(raw_data):
    """ETL pipeline using DataFrame API"""
    
    # Data cleansing
    cleaned_data = (raw_data
        .filter(col("amount").isNotNull())
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_date", to_date(col("order_date")))
        .dropDuplicates()
    )
    
    # Create temporary view for SQL reporting
    cleaned_data.createOrReplaceTempView("cleaned_orders")
    
    return cleaned_data

# Reporting with SQL
def generate_reports():
    """Generate business reports using SQL"""
    
    # Customer summary report
    customer_report = spark.sql("""
        SELECT 
            customer_id,
            COUNT(*) as total_orders,
            SUM(amount) as total_spent,
            AVG(amount) as avg_order,
            MAX(order_date) as last_order_date,
            MIN(order_date) as first_order_date
        FROM cleaned_orders
        GROUP BY customer_id
        ORDER BY total_spent DESC
    """)
    
    # Monthly sales report
    monthly_report = spark.sql("""
        SELECT 
            DATE_FORMAT(order_date, 'yyyy-MM') as month,
            COUNT(*) as order_count,
            SUM(amount) as monthly_revenue,
            AVG(amount) as avg_order_value
        FROM cleaned_orders
        GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
        ORDER BY month
    """)
    
    return customer_report, monthly_report

# Complete workflow
processed_data = etl_pipeline(raw_orders_df)
customer_report, monthly_report = generate_reports()
```

## ⚡ Performance Considerations

### Execution Plan Analysis

```python
# Compare execution plans
sql_query = """
    SELECT department, AVG(salary), COUNT(*)
    FROM employees
    WHERE salary > 50000
    GROUP BY department
"""

df_operations = employees_df.filter(col("salary") > 50000) \\
    .groupBy("department") \\
    .agg(avg("salary"), count("*"))

# Both produce identical plans
print("SQL Plan:")
spark.sql(sql_query).explain(mode="formatted")

print("\\nDataFrame Plan:")
df_operations.explain(mode="formatted")
```

### Performance Guidelines

| Scenario | Recommended Approach | Reason |
|----------|---------------------|---------|
| **Simple queries** | SQL | More readable, familiar |
| **Complex ETL** | DataFrame API | Better debugging, step-by-step |
| **Ad-hoc analysis** | SQL | Quick iteration |
| **Production pipelines** | DataFrame API | Type safety, maintainability |
| **Team with SQL skills** | SQL | Faster development |
| **Complex UDFs** | DataFrame API | Native UDF support |
| **Dynamic logic** | DataFrame API | Programmatic control |
| **Reporting** | SQL | Standard reporting queries |

## 🎯 Decision Framework

### Choose Based on Context

```python
def choose_interface(operation_type, team_skills, complexity):
    """
    Decision framework for choosing SQL vs DataFrame API
    
    Args:
        operation_type: 'etl', 'reporting', 'analysis', 'exploration'
        team_skills: 'sql_heavy', 'programming_heavy', 'mixed'
        complexity: 'simple', 'medium', 'complex'
    """
    
    # Reporting and analysis often favor SQL
    if operation_type in ['reporting', 'analysis']:
        if team_skills == 'sql_heavy':
            return "SQL - team familiarity"
        elif complexity == 'simple':
            return "SQL - standard queries"
    
    # ETL and complex transformations favor DataFrame API
    if operation_type == 'etl' or complexity == 'complex':
        if team_skills in ['programming_heavy', 'mixed']:
            return "DataFrame API - complex logic"
        else:
            return "Mixed approach - SQL for access, DataFrame for processing"
    
    # Exploration favors SQL
    if operation_type == 'exploration':
        return "SQL - quick iteration"
    
    # Default recommendation
    return "Mixed approach - best of both worlds"

# Usage examples
print(choose_interface('reporting', 'sql_heavy', 'simple'))      # SQL
print(choose_interface('etl', 'programming_heavy', 'complex'))  # DataFrame API
print(choose_interface('analysis', 'mixed', 'medium'))          # Mixed
```

## 🚨 Common Anti-Patterns

### 1. **Using Only SQL**

```python
# ❌ Anti-pattern: Complex business logic in SQL strings
def process_data():
    complex_sql = """
        SELECT customer_id,
            CASE WHEN total_spent > 1000 AND order_count > 5 THEN 
                CASE WHEN avg_order > 200 THEN 'VIP' ELSE 'Gold' END
            ELSE 'Standard' END as customer_tier
        FROM (...) nested subquery with complex logic
    """
    return spark.sql(complex_sql)

# ✅ Better: Use DataFrame API for complex logic
def process_data_better():
    customer_metrics = calculate_customer_metrics()
    
    return customer_metrics.withColumn("customer_tier", 
        when((col("total_spent") > 1000) & (col("order_count") > 5),
            when(col("avg_order") > 200, "VIP").otherwise("Gold")
        ).otherwise("Standard")
    )
```

### 2. **Using Only DataFrame API**

```python
# ❌ Anti-pattern: Simple reporting queries in DataFrame API
result = (df.filter(col("status") == "active")
    .groupBy("department")
    .agg(count("*").alias("count"))
    .orderBy("count"))

# ✅ Better: Use SQL for simple reporting
result = spark.sql("""
    SELECT department, COUNT(*) as count
    FROM table
    WHERE status = 'active'
    GROUP BY department
    ORDER BY count
""")
```

### 3. **Not Considering Team Skills**

```python
# ❌ Ignoring team capabilities
# Forcing SQL on a team that only knows Python
# Forcing DataFrame API on a team that only knows SQL
```

## 🎯 Best Practices

### 1. **Team Standards**

```python
# Establish team conventions
TEAM_SQL_SCENARIOS = [
    'reporting_queries',
    'ad_hoc_analysis', 
    'simple_aggregations',
    'data_exploration'
]

TEAM_DF_SCENARIOS = [
    'complex_etl',
    'machine_learning_prep',
    'custom_transformations',
    'production_pipelines'
]

def get_recommended_approach(use_case):
    """Get team-recommended approach"""
    if use_case in TEAM_SQL_SCENARIOS:
        return "SQL"
    elif use_case in TEAM_DF_SCENARIOS:
        return "DataFrame API"
    else:
        return "Mixed - evaluate per case"
```

### 2. **Code Organization**

```python
# Organize code by interface
# sql_queries.py - All SQL queries
# dataframe_transforms.py - DataFrame operations
# etl_pipeline.py - Mixed approach pipelines

# Example structure
class DataProcessor:
    def __init__(self, spark):
        self.spark = spark
    
    def extract_data(self):
        """SQL for data extraction"""
        return self.spark.sql("SELECT * FROM source_table")
    
    def transform_data(self, df):
        """DataFrame API for transformations"""
        return df.filter(...).withColumn(...).groupBy(...)
    
    def load_data(self, df):
        """SQL for final reporting"""
        df.createOrReplaceTempView("processed_data")
        return self.spark.sql("SELECT * FROM processed_data")
```

### 3. **Testing and Validation**

```python
# Test both approaches for critical operations
def test_equivalence(sql_query, df_operations):
    """Test that SQL and DataFrame produce same results"""
    
    sql_result = spark.sql(sql_query).orderBy("*")
    df_result = df_operations.orderBy("*")
    
    # Compare schemas
    assert sql_result.schema == df_result.schema
    
    # Compare data (for small datasets)
    sql_data = sql_result.collect()
    df_data = df_result.collect()
    
    assert sql_data == df_data
    print("✅ SQL and DataFrame results are equivalent")

# Usage
sql_q = "SELECT department, AVG(salary) FROM employees GROUP BY department"
df_ops = employees_df.groupBy("department").agg(avg("salary"))

test_equivalence(sql_q, df_ops)
```

## 🎯 Interview Questions

### Common Interview Questions:
1. **When should you use Spark SQL vs DataFrame API?**
2. **What's the performance difference between SQL and DataFrame API?**
3. **Can you mix SQL and DataFrame operations?**
4. **Which interface is better for complex ETL pipelines?**

### Answers:
- **Use SQL for**: Team familiarity, reporting, ad-hoc queries, complex joins
- **Use DataFrame API for**: Programmatic logic, type safety, UDFs, complex transformations
- **Performance**: Identical - both use same Catalyst optimizer
- **Mixing**: Yes, create views from DataFrames and query with SQL
- **ETL pipelines**: DataFrame API for complex logic, SQL for simple operations

## 📚 Summary

### Interface Selection Guide:

| Factor | Favor SQL | Favor DataFrame API | Mixed Approach |
|--------|-----------|-------------------|----------------|
| **Team Skills** | SQL experts | Python/programming | Mixed team |
| **Use Case** | Reporting, analysis | ETL, transformations | Complex workflows |
| **Complexity** | Simple to medium | High complexity | Medium complexity |
| **Maintenance** | SQL familiarity | Code structure | Best of both |
| **Debugging** | Query analysis | Step-by-step | Component testing |

### Key Takeaways:
- **Both interfaces are equally performant** - same execution engine
- **Choose based on team skills and use case**
- **SQL** for familiar syntax and quick queries
- **DataFrame API** for complex logic and type safety
- **Mix both** for optimal development experience
- **Consider maintainability** and team preferences

**The best approach is the one your team can maintain effectively!**
