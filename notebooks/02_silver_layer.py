# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Cleaning and Validation
# MAGIC
# MAGIC This notebook cleans and validates data from the Bronze layer, preparing it for the Gold layer.
# MAGIC
# MAGIC ## Data Quality Improvements:
# MAGIC - Remove duplicates
# MAGIC - Handle null values
# MAGIC - Standardize date formats
# MAGIC - Normalize string formats (trim, case standardization)
# MAGIC - Convert data types (string to numeric where appropriate)
# MAGIC - Validate referential integrity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.udf import UserDefinedFunction

# Unity Catalog configuration
CATALOG_NAME = "ecommerce_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Set catalog and schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

print(f"✅ Using catalog: {CATALOG_NAME}")
print(f"✅ Source schema: {BRONZE_SCHEMA}")
print(f"✅ Target schema: {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def standardize_date(date_str):
    """Standardize date formats to ISO format (YYYY-MM-DD)"""
    if date_str is None or pd.isna(date_str) or date_str == '':
        return None
    
    # Convert to string if not already
    date_str = str(date_str)
    
    # Try different date formats
    date_formats = [
        "%Y-%m-%d",      # ISO format
        "%d/%m/%Y",      # DD/MM/YYYY
        "%m-%d-%Y",      # MM-DD-YYYY
        "%d-%m-%Y",      # DD-MM-YYYY
        "%Y/%m/%d",      # YYYY/MM/DD
        "%b %d, %Y",     # Jan 15, 2024
        "%B %d, %Y",     # January 15, 2024
    ]
    
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime("%Y-%m-%d")
        except:
            continue
    
    # If no format matches, return None (invalid date)
    return None

def standardize_string(text):
    """Standardize string format: trim and title case"""
    if text is None or pd.isna(text) or text == '':
        return None
    
    # Convert to string and strip whitespace
    text = str(text).strip()
    
    # Remove multiple spaces
    text = ' '.join(text.split())
    
    # Convert to title case (first letter uppercase, rest lowercase)
    return text.title()

def convert_to_numeric(value, default=None):
    """Convert string to numeric, handling errors"""
    if value is None or pd.isna(value) or value == '':
        return default
    
    # If already numeric, return as is
    if isinstance(value, (int, float)):
        return float(value) if not isinstance(value, int) or default is None else int(value)
    
    # Try to convert string to numeric
    try:
        # Remove common formatting characters
        cleaned = str(value).replace(',', '').replace('$', '').strip()
        if '.' in cleaned:
            return float(cleaned)
        else:
            return int(cleaned)
    except:
        return default

def parse_date_column(column_name):
    """Parse date column trying multiple formats using try_to_date"""
    # Get the column (should already be string)
    col = F.col(column_name)
    
    # Try multiple date formats using try_to_date (returns NULL if format doesn't match)
    # Order matters - try most common formats first
    # Use coalesce to return first non-null result
    return F.coalesce(
        F.try_to_date(col, "yyyy-MM-dd"),      # ISO format: 2024-01-15
        F.try_to_date(col, "yyyy/MM/dd"),      # Slash format: 2024/01/15 (e.g., 1948/07/14)
        F.try_to_date(col, "dd/MM/yyyy"),      # DD/MM/YYYY: 15/01/2024
        F.try_to_date(col, "MM-dd-yyyy"),      # MM-DD-YYYY: 01-15-2024
        F.try_to_date(col, "dd-MM-yyyy"),      # DD-MM-YYYY: 15-01-2024
        F.try_to_date(col, "MMM dd, yyyy"),    # Jan 15, 2024
        F.try_to_date(col, "MMMM dd, yyyy")    # January 15, 2024
    )

def save_silver_table(df, table_name, mode="overwrite"):
    """Save cleaned DataFrame to Silver layer"""
    full_table_path = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    df.write \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_path)
    
    print(f"✅ Silver table created: {full_table_path}")
    return full_table_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean Categories

# COMMAND ----------

print("Cleaning categories...")

# Read from bronze
df_categories_bronze = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.categories")

print(f"📊 Bronze records: {df_categories_bronze.count()}")

# Remove duplicates (keep first occurrence)
df_categories_silver = df_categories_bronze.dropDuplicates(["category_id"])

# Standardize strings
df_categories_silver = df_categories_silver.withColumn(
    "category_name",
    F.trim(F.initcap(F.col("category_name")))
).withColumn(
    "description",
    F.trim(F.initcap(F.col("description")))
)

# Remove rows where category_name is null (critical field)
df_categories_silver = df_categories_silver.filter(F.col("category_name").isNotNull())

print(f"📊 Silver records: {df_categories_silver.count()}")

# Save to silver
save_silver_table(df_categories_silver, "categories")

print(f"✅ Categories cleaned: {df_categories_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean Customers

# COMMAND ----------

print("Cleaning customers...")

# Read from bronze
df_customers_bronze = spark.table(
    f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers"
)

print(f"📊 Bronze records: {df_customers_bronze.count()}")

# Remove duplicates (keep first occurrence based on customer_id)
df_customers_silver = df_customers_bronze.dropDuplicates(
    ["customer_id"]
)

# Standardize string fields
df_customers_silver = df_customers_silver.withColumn(
    "first_name",
    F.trim(F.initcap(F.col("first_name")))
).withColumn(
    "last_name",
    F.trim(F.initcap(F.col("last_name")))
).withColumn(
    "city",
    F.trim(F.initcap(F.col("city")))
).withColumn(
    "state",
    F.upper(F.trim(F.col("state")))
).withColumn(
    "email",
    F.lower(F.trim(F.col("email")))
).withColumn(
    "phone",
    F.trim(F.col("phone"))
)

# Standardize dates (try multiple formats)
birth_date_str = F.col("birth_date").cast("string")
registration_date_str = F.col("registration_date").cast("string")

df_customers_silver = df_customers_silver.withColumn(
    "birth_date",
    F.coalesce(
        F.try_to_timestamp(birth_date_str, "yyyy-MM-dd"),
        F.try_to_timestamp(birth_date_str, "yyyy/MM/dd"),
        F.try_to_timestamp(birth_date_str, "dd/MM/yyyy"),
        F.try_to_timestamp(birth_date_str, "MM-dd-yyyy"),
        F.try_to_timestamp(birth_date_str, "dd-MM-yyyy"),
        F.try_to_timestamp(birth_date_str, "MMM dd, yyyy"),
        F.try_to_timestamp(birth_date_str, "MMMM dd, yyyy")
    )
).withColumn(
    "registration_date",
    F.coalesce(
        F.try_to_timestamp(registration_date_str, "yyyy-MM-dd"),
        F.try_to_timestamp(registration_date_str, "yyyy/MM/dd"),
        F.try_to_timestamp(registration_date_str, "dd/MM/yyyy"),
        F.try_to_timestamp(registration_date_str, "MM-dd-yyyy"),
        F.try_to_timestamp(registration_date_str, "dd-MM-yyyy"),
        F.try_to_timestamp(registration_date_str, "MMM dd, yyyy"),
        F.try_to_timestamp(registration_date_str, "MMMM dd, yyyy")
    )
)

# Convert zip_code to integer (handle type errors)
df_customers_silver = df_customers_silver.withColumn(
    "zip_code",
    F.when(
        F.col("zip_code").rlike("^[0-9]+$"),
        F.col("zip_code").cast("int")
    ).otherwise(None)
)

# Remove rows where critical fields are null
df_customers_silver = df_customers_silver.filter(
    F.col("customer_id").isNotNull() &
    F.col("first_name").isNotNull() &
    F.col("last_name").isNotNull() &
    F.col("email").isNotNull()
)

print(f"📊 Silver records: {df_customers_silver.count()}")

# Save to silver
save_silver_table(df_customers_silver, "customers")

print(f"✅ Customers cleaned: {df_customers_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean Products

# COMMAND ----------

print("Cleaning products...")

# Read from bronze
df_products_bronze = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products")

print(f"📊 Bronze records: {df_products_bronze.count()}")

# Remove duplicates
df_products_silver = df_products_bronze.dropDuplicates(["product_id"])

# Standardize product name
df_products_silver = df_products_silver.withColumn(
    "product_name",
    F.trim(F.initcap(F.col("product_name")))
)

# Convert price to double (handle type errors)
df_products_silver = df_products_silver.withColumn(
    "price",
    F.when(
        F.col("price").rlike("^[0-9]+\\.?[0-9]*$"),
        F.col("price").cast("double")
    ).otherwise(None)
)

# Convert stock_quantity to integer (handle type errors)
df_products_silver = df_products_silver.withColumn(
    "stock_quantity",
    F.when(
        F.col("stock_quantity").rlike("^[0-9]+$"),
        F.col("stock_quantity").cast("int")
    ).otherwise(F.lit(0))  # Default to 0 if invalid
)

# Standardize created_date (try multiple formats)
df_products_silver = df_products_silver.withColumn(
    "created_date",
    parse_date_column("created_date")
)

# Validate category_id exists in categories table
df_categories_ids = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.categories").select("category_id")
df_products_silver = df_products_silver.join(
    df_categories_ids,
    on="category_id",
    how="inner"
)

# Remove rows where critical fields are null
df_products_silver = df_products_silver.filter(
    F.col("product_id").isNotNull() &
    F.col("product_name").isNotNull() &
    F.col("category_id").isNotNull() &
    F.col("price").isNotNull()
)

print(f"📊 Silver records: {df_products_silver.count()}")

# Save to silver
save_silver_table(df_products_silver, "products")

print(f"✅ Products cleaned: {df_products_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clean Orders

# COMMAND ----------

print("Cleaning orders...")

# Read from bronze
df_orders_bronze = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders")

print(f"📊 Bronze records: {df_orders_bronze.count()}")

# Remove duplicates
df_orders_silver = df_orders_bronze.dropDuplicates(["order_id"])

# Standardize status and payment_type
df_orders_silver = df_orders_silver.withColumn(
    "status",
    F.trim(F.initcap(F.col("status")))
).withColumn(
    "payment_type",
    F.trim(F.initcap(F.col("payment_type")))
)

# Standardize order_date (try multiple formats)
df_orders_silver = df_orders_silver.withColumn(
    "order_date",
    parse_date_column("order_date")
)

# Convert total_amount to double (handle type errors)
df_orders_silver = df_orders_silver.withColumn(
    "total_amount",
    F.when(
        F.col("total_amount").rlike("^[0-9]+\\.?[0-9]*$"),
        F.col("total_amount").cast("double")
    ).otherwise(None)
)

# Validate customer_id exists in customers table
df_customers_ids = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers").select("customer_id")
df_orders_silver = df_orders_silver.join(
    df_customers_ids,
    on="customer_id",
    how="inner"
)

# Remove rows where critical fields are null
df_orders_silver = df_orders_silver.filter(
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("status").isNotNull()
)

print(f"📊 Silver records: {df_orders_silver.count()}")

# Save to silver
save_silver_table(df_orders_silver, "orders")

print(f"✅ Orders cleaned: {df_orders_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean Order Items

# COMMAND ----------

print("Cleaning order items...")

# Read from bronze
df_order_items_bronze = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.order_items")

print(f"📊 Bronze records: {df_order_items_bronze.count()}")

# Remove duplicates
df_order_items_silver = df_order_items_bronze.dropDuplicates(["item_id"])

# Convert quantity to integer (handle type errors)
df_order_items_silver = df_order_items_silver.withColumn(
    "quantity",
    F.when(
        F.col("quantity").rlike("^[0-9]+$"),
        F.col("quantity").cast("int")
    ).otherwise(F.lit(1))  # Default to 1 if invalid
)

# Convert unit_price to double (handle type errors)
df_order_items_silver = df_order_items_silver.withColumn(
    "unit_price",
    F.when(
        F.col("unit_price").rlike("^[0-9]+\\.?[0-9]*$"),
        F.col("unit_price").cast("double")
    ).otherwise(None)
)

# Convert subtotal to double and recalculate if needed
df_order_items_silver = df_order_items_silver.withColumn(
    "subtotal",
    F.when(
        F.col("subtotal").rlike("^[0-9]+\\.?[0-9]*$"),
        F.col("subtotal").cast("double")
    ).otherwise(
        F.col("quantity") * F.col("unit_price")
    )
)

# Validate order_id exists in orders table
df_orders_ids = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders").select("order_id")
df_order_items_silver = df_order_items_silver.join(
    df_orders_ids,
    on="order_id",
    how="inner"
)

# Validate product_id exists in products table
df_products_ids = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.products").select("product_id")
df_order_items_silver = df_order_items_silver.join(
    df_products_ids,
    on="product_id",
    how="inner"
)

# Remove rows where critical fields are null
df_order_items_silver = df_order_items_silver.filter(
    F.col("item_id").isNotNull() &
    F.col("order_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("quantity").isNotNull() &
    F.col("unit_price").isNotNull() &
    F.col("subtotal").isNotNull()
)

print(f"📊 Silver records: {df_order_items_silver.count()}")

# Save to silver
save_silver_table(df_order_items_silver, "order_items")

print(f"✅ Order items cleaned: {df_order_items_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ SILVER LAYER CLEANING COMPLETED!")
print("="*60)

# Count records in each layer
tables = ["categories", "customers", "products", "orders", "order_items"]

print("\n📊 Record Counts:")
print(f"{'Table':<20} {'Bronze':<15} {'Silver':<15} {'Removed':<15}")
print("-" * 65)

for table in tables:
    bronze_count = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table}").count()
    silver_count = spark.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table}").count()
    removed = bronze_count - silver_count
    
    print(f"{table:<20} {bronze_count:<15} {silver_count:<15} {removed:<15}")

print("\n✨ Data Quality Improvements:")
print("  • Duplicates removed")
print("  • Null values handled")
print("  • Date formats standardized (YYYY-MM-DD)")
print("  • String formats normalized (trimmed, title case)")
print("  • Type errors corrected (strings → numeric)")
print("  • Referential integrity validated")
print("\n" + "="*60)
