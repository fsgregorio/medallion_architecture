# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Star Schema Implementation
# MAGIC
# MAGIC This notebook creates the Gold layer with a star schema design for business analytics.
# MAGIC
# MAGIC ## Star Schema Design
# MAGIC
# MAGIC ### Fact Table: `fato_vendas`
# MAGIC - Granularity: Order Item level
# MAGIC - Measures: quantity, unit_price, subtotal
# MAGIC - Foreign Keys: customer_key, product_key, category_key, date_key, order_key
# MAGIC
# MAGIC ### Dimension Tables:
# MAGIC - **dim_customer**: Customer attributes (SCD Type 2 - maintains history)
# MAGIC - **dim_product**: Product attributes (SCD Type 2 - maintains history)
# MAGIC - **dim_category**: Category attributes (SCD Type 1 - overwrites)
# MAGIC - **dim_time**: Date dimension with hierarchies (static)
# MAGIC - **dim_order**: Order attributes as degenerate dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# Unity Catalog configuration
CATALOG_NAME = "ecommerce_data"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Set catalog and schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

print(f"✅ Using catalog: {CATALOG_NAME}")
print(f"✅ Source schema: {SILVER_SCHEMA}")
print(f"✅ Target schema: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function

# COMMAND ----------

def save_gold_table(df, table_name, mode="overwrite"):
    """Save DataFrame to Gold layer"""
    full_table_path = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{table_name}"
    
    df.write \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_path)
    
    print(f"✅ Gold table created: {full_table_path}")
    return full_table_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create dim_category

# COMMAND ----------

print("Creating dim_category...")

# Read from silver
df_categories = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.categories")

# Create dimension table with surrogate key (SCD Type 1)
df_dim_category = df_categories.select(
    F.col("category_id").alias("category_key"),  # Surrogate key
    F.col("category_id").alias("category_id"),  # Natural key
    F.col("category_name"),
    F.col("description"),
    F.current_timestamp().alias("created_at"),
    F.current_timestamp().alias("updated_at")
)

print(f"📊 Records: {df_dim_category.count()}")

# Save to gold
save_gold_table(df_dim_category, "dim_category")

print(f"✅ dim_category created: {df_dim_category.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create dim_customer

# COMMAND ----------

print("Creating dim_customer...")

# Read from silver
df_customers = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers")

# Create dimension table with SCD Type 2 (maintains history)
# For initial load, all records are current
# Generate surrogate key using row_number (deterministic)
window_spec = Window.partitionBy("customer_id").orderBy("registration_date")
df_dim_customer = df_customers.withColumn(
    "row_num", F.row_number().over(window_spec)
).select(
    # Surrogate key: hash of natural key + row number for uniqueness
    (F.hash(F.col("customer_id").cast("string")) * 1000 + F.col("row_num")).alias("customer_key"),
    F.col("customer_id").alias("customer_id"),   # Natural key
    F.col("first_name"),
    F.col("last_name"),
    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
    F.col("email"),
    F.col("phone"),
    F.col("birth_date"),
    (F.year(F.current_date()) - F.year(F.col("birth_date"))).alias("age"),
    F.col("city"),
    F.col("state"),
    F.col("zip_code"),
    F.col("registration_date"),
    F.datediff(F.current_date(), F.col("registration_date")).alias("days_since_registration"),
    # SCD Type 2 fields
    F.coalesce(F.col("registration_date"), F.current_date()).alias("valid_from"),
    F.lit(None).cast("date").alias("valid_to"),  # NULL means current
    F.lit(1).alias("is_current"),  # 1 = current record
    F.current_timestamp().alias("created_at"),
    F.current_timestamp().alias("updated_at")
)

print(f"📊 Records: {df_dim_customer.count()}")

# Save to gold
save_gold_table(df_dim_customer, "dim_customer")

print(f"✅ dim_customer created: {df_dim_customer.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create dim_product

# COMMAND ----------

print("Creating dim_product...")

# Read from silver
df_products = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.products")

# Join with categories to get category name
df_categories_silver = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.categories")

# Generate surrogate key using row_number (deterministic)
window_spec_product = Window.partitionBy("product_id").orderBy("created_date")

df_dim_product = df_products.join(
    df_categories_silver.select("category_id", "category_name"),
    on="category_id",
    how="left"
).withColumn(
    "row_num", F.row_number().over(window_spec_product)
).select(
    # Surrogate key: hash of natural key + row number for uniqueness
    (F.hash(F.col("product_id").cast("string")) * 1000 + F.col("row_num")).alias("product_key"),
    F.col("product_id").alias("product_id"),   # Natural key
    F.col("product_name"),
    F.col("category_id"),
    F.col("category_name"),
    F.col("price"),
    F.col("stock_quantity"),
    F.when(F.col("stock_quantity") > 0, "In Stock")
     .otherwise("Out of Stock").alias("stock_status"),
    F.col("created_date"),
    # SCD Type 2 fields
    F.coalesce(F.col("created_date"), F.current_date()).alias("valid_from"),
    F.lit(None).cast("date").alias("valid_to"),  # NULL means current
    F.lit(1).alias("is_current"),  # 1 = current record
    F.current_timestamp().alias("created_at"),
    F.current_timestamp().alias("updated_at")
)

print(f"📊 Records: {df_dim_product.count()}")

# Save to gold
save_gold_table(df_dim_product, "dim_product")

print(f"✅ dim_product created: {df_dim_product.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create dim_time

# COMMAND ----------

print("Creating dim_time...")

# Get date range from orders
df_orders = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders")
date_range_df = df_orders.agg(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date")
).collect()[0]

min_date = date_range_df["min_date"]
max_date = date_range_df["max_date"]

print(f"📅 Date range: {min_date} to {max_date}")

# Generate date dimension using Spark SQL sequence
# Calculate number of days
days_diff = (max_date - min_date).days + 1

# Create date sequence using Spark SQL
df_time = spark.sql(f"""
    SELECT 
        date_add('{min_date}', pos) as full_date
    FROM (
        SELECT posexplode(split(space({days_diff - 1}), ' ')) as (pos, val)
    )
""")

# Add all date attributes using Spark functions
df_dim_time = df_time.select(
    (F.year(F.col("full_date")) * 10000 + 
     F.month(F.col("full_date")) * 100 + 
     F.dayofmonth(F.col("full_date"))).alias("date_key"),
    F.col("full_date"),
    F.year(F.col("full_date")).alias("year"),
    F.quarter(F.col("full_date")).alias("quarter"),
    F.month(F.col("full_date")).alias("month"),
    F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
    F.date_format(F.col("full_date"), "MMM").alias("month_abbr"),
    F.weekofyear(F.col("full_date")).alias("week"),
    F.dayofmonth(F.col("full_date")).alias("day_of_month"),
    F.dayofweek(F.col("full_date")).alias("day_of_week"),
    F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
    F.date_format(F.col("full_date"), "EEE").alias("day_abbr"),
    F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), 1).otherwise(0).alias("is_weekend"),
    F.when(F.dayofmonth(F.col("full_date")) == F.dayofmonth(F.last_day(F.col("full_date"))), 1).otherwise(0).alias("is_month_end"),
    F.concat(
        F.year(F.col("full_date")), 
        F.lit("-Q"), 
        F.quarter(F.col("full_date"))
    ).alias("year_quarter"),
    F.date_format(F.col("full_date"), "yyyy-MM").alias("year_month")
)

print(f"📊 Records: {df_dim_time.count()}")

# Save to gold
save_gold_table(df_dim_time, "dim_time")

print(f"✅ dim_time created: {df_dim_time.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create dim_order (Degenerate Dimension)

# COMMAND ----------

print("Creating dim_order...")

# Read from silver
df_orders = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders")

# Create degenerate dimension with order attributes
df_dim_order = df_orders.select(
    F.col("order_id").alias("order_key"),  # Surrogate key
    F.col("order_id").alias("order_id"),   # Natural key
    F.col("status"),
    F.col("payment_type"),
    F.current_timestamp().alias("created_at"),
    F.current_timestamp().alias("updated_at")
)

print(f"📊 Records: {df_dim_order.count()}")

# Save to gold
save_gold_table(df_dim_order, "dim_order")

print(f"✅ dim_order created: {df_dim_order.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create fato_vendas (Fact Table)

# COMMAND ----------

print("Creating fato_vendas...")

# Read from silver
df_order_items = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.order_items")
df_orders = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders")

# Join order_items with orders to get order_date
df_fact_base = df_order_items.join(
    df_orders.select("order_id", "order_date", "customer_id"),
    on="order_id",
    how="inner"
)

# Get dimension keys
# For SCD Type 2, we need to join only current records (is_current = 1)
df_dim_customer = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.dim_customer").filter(F.col("is_current") == 1)
df_dim_product = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.dim_product").filter(F.col("is_current") == 1)
df_dim_category = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.dim_category")
df_dim_time = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.dim_time")
df_dim_order = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.dim_order")

# Create fact table with foreign keys and measures
# In Star Schema, ALL dimensions connect directly to the fact table
df_fato_vendas = df_fact_base.join(
    df_dim_customer.select("customer_id", "customer_key"),
    on="customer_id",
    how="inner"
).join(
    df_dim_product.select("product_id", "product_key", "category_id"),
    on="product_id",
    how="inner"
).join(
    df_dim_category.select("category_id", "category_key"),
    on="category_id",
    how="inner"
).join(
    df_dim_time.select("full_date", "date_key"),
    df_fact_base["order_date"] == df_dim_time["full_date"],
    how="inner"
).join(
    df_dim_order.select("order_id", "order_key"),
    on="order_id",
    how="inner"
).select(
    # Foreign Keys (all dimensions connect to fact table)
    F.col("customer_key"),
    F.col("product_key"),
    F.col("category_key"),  # Direct connection to fact table
    F.col("date_key"),
    F.col("order_key"),
    # Natural Keys (for reference)
    F.col("order_id"),
    F.col("item_id"),
    F.col("product_id"),
    # Measures
    F.col("quantity"),
    F.col("unit_price"),
    F.col("subtotal"),
    # Calculated measures
    (F.col("quantity") * F.col("unit_price")).alias("calculated_subtotal"),
    # Metadata
    F.current_timestamp().alias("created_at")
)

print(f"📊 Records: {df_fato_vendas.count()}")

# Save to gold
save_gold_table(df_fato_vendas, "fato_vendas")

print(f"✅ fato_vendas created: {df_fato_vendas.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ GOLD LAYER STAR SCHEMA CREATED SUCCESSFULLY!")
print("="*60)

# Count records
tables = ["dim_category", "dim_customer", "dim_product", "dim_time", "dim_order", "fato_vendas"]

print("\n📊 Gold Layer Tables:")
print(f"{'Table':<20} {'Records':<15}")
print("-" * 35)

for table in tables:
    count = spark.table(f"{CATALOG_NAME}.{GOLD_SCHEMA}.{table}").count()
    print(f"{table:<20} {count:<15}")

print("\n✨ Star Schema Structure:")
print("  • Fact Table: fato_vendas")
print("  • Dimensions: dim_customer, dim_product, dim_category, dim_time, dim_order")
print("\n📈 Ready for analytics and reporting!")
print("\n" + "="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Queries
# MAGIC
# MAGIC ### Sales by Category
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     c.category_name,
# MAGIC     SUM(f.subtotal) as total_sales,
# MAGIC     SUM(f.quantity) as total_quantity
# MAGIC FROM ecommerce_data.gold.fato_vendas f
# MAGIC JOIN ecommerce_data.gold.dim_product p ON f.product_key = p.product_key
# MAGIC JOIN ecommerce_data.gold.dim_category c ON p.category_id = c.category_id
# MAGIC GROUP BY c.category_name
# MAGIC ORDER BY total_sales DESC;
# MAGIC ```
# MAGIC
# MAGIC ### Sales by Month
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     t.year_month,
# MAGIC     t.month_name,
# MAGIC     SUM(f.subtotal) as total_sales
# MAGIC FROM ecommerce_data.gold.fato_vendas f
# MAGIC JOIN ecommerce_data.gold.dim_time t ON f.date_key = t.date_key
# MAGIC GROUP BY t.year_month, t.month_name
# MAGIC ORDER BY t.year_month;
# MAGIC ```
# MAGIC
# MAGIC ### Top Customers
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     c.full_name,
# MAGIC     c.city,
# MAGIC     SUM(f.subtotal) as total_spent,
# MAGIC     COUNT(DISTINCT f.order_id) as total_orders
# MAGIC FROM ecommerce_data.gold.fato_vendas f
# MAGIC JOIN ecommerce_data.gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC GROUP BY c.full_name, c.city
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 10;
# MAGIC ```

