# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Bronze Layer Datasets
# MAGIC 
# MAGIC This notebook generates synthetic e-commerce data with data quality issues for practicing Medallion Architecture.
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC Generates 5 related tables:
# MAGIC - **categories**: Product categories (dimension)
# MAGIC - **customers**: Customer information (dimension)
# MAGIC - **products**: Product catalog (dimension)
# MAGIC - **orders**: Order transactions (fact)
# MAGIC - **order_items**: Order line items (fact)
# MAGIC 
# MAGIC Each table is saved to Unity Catalog in the bronze schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Configuration

# COMMAND ----------

# Configure Unity Catalog settings
CATALOG_NAME = "ecommerce_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Set catalog and schema
# Note: 'spark' is a global variable available in Databricks notebooks
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"✅ Using catalog: {CATALOG_NAME}")
print(f"✅ Using schema: {BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def add_duplicates(df, percentage=0.01):
    """Add duplicate rows (~1% of records)"""
    num_duplicates = int(len(df) * percentage)
    if num_duplicates > 0:
        duplicate_rows = df.sample(n=num_duplicates, random_state=42)
        return pd.concat([df, duplicate_rows], ignore_index=True)
    return df

def add_nulls(df, percentage=0.05):
    """Add null values randomly (~5% of values)"""
    df_with_nulls = df.copy()
    num_nulls = int(len(df) * len(df.columns) * percentage)
    
    for _ in range(num_nulls):
        row_idx = random.randint(0, len(df_with_nulls) - 1)
        col_idx = random.randint(0, len(df_with_nulls.columns) - 1)
        # Skip ID columns
        if 'id' not in df_with_nulls.columns[col_idx].lower():
            df_with_nulls.iloc[row_idx, col_idx] = np.nan
    
    return df_with_nulls

def mess_up_dates(date_str):
    """Create inconsistent date formats"""
    # Handle null/NaT values
    if pd.isna(date_str) or date_str is None:
        return date_str
    
    if isinstance(date_str, str):
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except:
            return date_str
    else:
        date_obj = date_str
    
    # Check if date_obj is NaT (pandas NaTType)
    if pd.isna(date_obj):
        return date_obj
    
    formats = [
        lambda d: d.strftime("%Y-%m-%d"),      # ISO format
        lambda d: d.strftime("%d/%m/%Y"),      # DD/MM/YYYY
        lambda d: d.strftime("%m-%d-%Y"),      # MM-DD-YYYY
        lambda d: d.strftime("%d-%m-%Y"),      # DD-MM-YYYY
        lambda d: d.strftime("%Y/%m/%d"),      # YYYY/MM/DD
        lambda d: d.strftime("%b %d, %Y"),      # Jan 15, 2024
    ]
    return random.choice(formats)(date_obj)

def mess_up_strings(text):
    """Create inconsistent string formats"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    variations = [
        text,                      # Original
        text.upper(),              # All caps
        text.lower(),              # All lowercase
        text.capitalize(),         # First letter capital
        f" {text} ",               # Spaces before/after
        f"  {text}  ",             # Multiple spaces
        text.replace(" ", "  "),   # Double spaces
        text.strip().replace(" ", ""),  # No spaces
    ]
    return random.choice(variations)

def convert_to_string(value):
    """Convert numeric values to string (type error) - ~10% chance"""
    if random.random() < 0.1:
        return str(value)
    return value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function to Save as Unity Catalog Table

# COMMAND ----------

def save_to_unity_catalog(df, table_name, schema_name=BRONZE_SCHEMA, catalog_name=CATALOG_NAME, mode="overwrite"):
    """
    Save DataFrame as Unity Catalog table
    
    Parameters:
    - df: Spark DataFrame or Pandas DataFrame
    - table_name: Name of the table
    - schema_name: Schema name (default: bronze)
    - catalog_name: Catalog name (default: ecommerce_data)
    - mode: Write mode (overwrite, append, ignore, error)
    """
    # Convert pandas DataFrame to Spark DataFrame if needed
    # Note: 'spark' is a global variable available in Databricks notebooks
    if isinstance(df, pd.DataFrame):
        spark_df = spark.createDataFrame(df)
    else:
        spark_df = df
    
    # Full table path
    full_table_path = f"{catalog_name}.{schema_name}.{table_name}"
    
    # Write to Unity Catalog
    spark_df.write \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_path)
    
    print(f"✅ Table created: {full_table_path}")
    return full_table_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Categories

# COMMAND ----------

print("Generating categories...")
categories_data = {
    'category_id': range(1, 11),
    'category_name': [
        'Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports',
        'Toys & Games', 'Health & Beauty', 'Automotive', 'Food & Beverages', 'Office Supplies'
    ],
    'description': [
        'Electronic devices and accessories',
        'Apparel and fashion items',
        'Home improvement and garden supplies',
        'Books and reading materials',
        'Sports equipment and accessories',
        'Toys and board games',
        'Health and beauty products',
        'Automotive parts and accessories',
        'Food and beverage products',
        'Office and school supplies'
    ]
}

df_categories = pd.DataFrame(categories_data)
df_categories = add_nulls(df_categories, 0.05)
df_categories = add_duplicates(df_categories, 0.01)
df_categories['category_name'] = df_categories['category_name'].apply(mess_up_strings)
df_categories['description'] = df_categories['description'].apply(mess_up_strings)

# Save to Unity Catalog
save_to_unity_catalog(df_categories, "categories")

print(f"✅ Categories generated: {len(df_categories)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Customers

# COMMAND ----------

print("Generating customers...")
n_customers = 1000

first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Jessica',
               'William', 'Ashley', 'James', 'Amanda', 'Christopher', 'Melissa', 'Daniel',
               'Michelle', 'Matthew', 'Kimberly', 'Anthony', 'Amy', 'Mark', 'Angela']

last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
              'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 'Anderson', 'Thomas',
              'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White']

cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
          'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville']

states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL']

customers_data = {
    'customer_id': range(1, n_customers + 1),
    'first_name': [random.choice(first_names) for _ in range(n_customers)],
    'last_name': [random.choice(last_names) for _ in range(n_customers)],
    'email': [],
    'phone': [],
    'birth_date': [],
    'city': [random.choice(cities) for _ in range(n_customers)],
    'state': [random.choice(states) for _ in range(n_customers)],
    'zip_code': [],
    'registration_date': []
}

for i in range(n_customers):
    first = customers_data['first_name'][i].lower()
    last = customers_data['last_name'][i].lower()
    customers_data['email'].append(f"{first}.{last}{i}@email.com")
    customers_data['phone'].append(f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}")
    
    # Birth dates (18-80 years old)
    birth_date = datetime.now() - timedelta(days=random.randint(18*365, 80*365))
    customers_data['birth_date'].append(birth_date)
    
    # Zip codes
    customers_data['zip_code'].append(f"{random.randint(10000, 99999)}")
    
    # Registration dates (last 2 years)
    reg_date = datetime.now() - timedelta(days=random.randint(0, 730))
    customers_data['registration_date'].append(reg_date)

df_customers = pd.DataFrame(customers_data)

# Introduce errors
df_customers = add_nulls(df_customers, 0.05)
df_customers = add_duplicates(df_customers, 0.01)

# Mess up dates
df_customers['birth_date'] = df_customers['birth_date'].apply(mess_up_dates)
df_customers['registration_date'] = df_customers['registration_date'].apply(mess_up_dates)

# Mess up strings
df_customers['first_name'] = df_customers['first_name'].apply(mess_up_strings)
df_customers['last_name'] = df_customers['last_name'].apply(mess_up_strings)
df_customers['city'] = df_customers['city'].apply(mess_up_strings)
df_customers['state'] = df_customers['state'].apply(mess_up_strings)

# Convert some zip codes to strings (type error)
df_customers['zip_code'] = df_customers['zip_code'].apply(convert_to_string)

# Save to Unity Catalog
save_to_unity_catalog(df_customers, "customers")

print(f"✅ Customers generated: {len(df_customers)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Products

# COMMAND ----------

print("Generating products...")
n_products = 250

product_names = {
    1: ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch'],
    2: ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Sneakers'],
    3: ['Chair', 'Table', 'Lamp', 'Plant Pot', 'Garden Tool'],
    4: ['Novel', 'Textbook', 'Magazine', 'Comic Book', 'Dictionary'],
    5: ['Basketball', 'Tennis Racket', 'Yoga Mat', 'Dumbbells', 'Running Shoes'],
    6: ['Board Game', 'Puzzle', 'Action Figure', 'Doll', 'Building Blocks'],
    7: ['Shampoo', 'Face Cream', 'Perfume', 'Toothbrush', 'Vitamins'],
    8: ['Car Battery', 'Tire', 'Oil Filter', 'Brake Pad', 'Air Freshener'],
    9: ['Coffee', 'Chocolate', 'Snacks', 'Juice', 'Tea'],
    10: ['Notebook', 'Pen', 'Stapler', 'Folder', 'Calculator']
}

products_data = {
    'product_id': range(1, n_products + 1),
    'product_name': [],
    'category_id': [],
    'price': [],
    'stock_quantity': [],
    'created_date': []
}

for i in range(n_products):
    category_id = random.randint(1, 10)
    products_data['category_id'].append(category_id)
    products_data['product_name'].append(random.choice(product_names[category_id]) + f" {random.randint(1, 1000)}")
    products_data['price'].append(round(random.uniform(10.0, 500.0), 2))
    products_data['stock_quantity'].append(random.randint(0, 1000))
    
    # Created dates (last 3 years)
    created_date = datetime.now() - timedelta(days=random.randint(0, 1095))
    products_data['created_date'].append(created_date)

df_products = pd.DataFrame(products_data)

# Introduce errors
df_products = add_nulls(df_products, 0.05)
df_products = add_duplicates(df_products, 0.01)

# Mess up dates
df_products['created_date'] = df_products['created_date'].apply(mess_up_dates)

# Mess up strings
df_products['product_name'] = df_products['product_name'].apply(mess_up_strings)

# Convert some prices and stock to strings (type error)
df_products['price'] = df_products['price'].apply(convert_to_string)
df_products['stock_quantity'] = df_products['stock_quantity'].apply(convert_to_string)

# Save to Unity Catalog
save_to_unity_catalog(df_products, "products")

print(f"✅ Products generated: {len(df_products)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Orders

# COMMAND ----------

print("Generating orders...")
n_orders = 2500

order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
payment_types = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash']

orders_data = {
    'order_id': range(1, n_orders + 1),
    'customer_id': [],
    'order_date': [],
    'status': [],
    'total_amount': [],
    'payment_type': []
}

# Generate orders (some customers have multiple orders, some have none)
customer_ids = list(range(1, n_customers + 1))
for i in range(n_orders):
    orders_data['customer_id'].append(random.choice(customer_ids))
    
    # Order dates (last year)
    order_date = datetime.now() - timedelta(days=random.randint(0, 365))
    orders_data['order_date'].append(order_date)
    
    orders_data['status'].append(random.choice(order_statuses))
    orders_data['payment_type'].append(random.choice(payment_types))
    orders_data['total_amount'].append(round(random.uniform(20.0, 1000.0), 2))

df_orders = pd.DataFrame(orders_data)

# Introduce errors
df_orders = add_nulls(df_orders, 0.05)
df_orders = add_duplicates(df_orders, 0.01)

# Mess up dates
df_orders['order_date'] = df_orders['order_date'].apply(mess_up_dates)

# Mess up strings
df_orders['status'] = df_orders['status'].apply(mess_up_strings)
df_orders['payment_type'] = df_orders['payment_type'].apply(mess_up_strings)

# Convert some amounts to strings (type error)
df_orders['total_amount'] = df_orders['total_amount'].apply(convert_to_string)

# Save to Unity Catalog
save_to_unity_catalog(df_orders, "orders")

print(f"✅ Orders generated: {len(df_orders)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Order Items

# COMMAND ----------

print("Generating order items...")
n_order_items = 7500

order_items_data = {
    'item_id': range(1, n_order_items + 1),
    'order_id': [],
    'product_id': [],
    'quantity': [],
    'unit_price': [],
    'subtotal': []
}

# Generate order items (average 3 items per order)
order_ids_list = list(range(1, n_orders + 1))
product_ids_list = list(range(1, n_products + 1))

for i in range(n_order_items):
    order_items_data['order_id'].append(random.choice(order_ids_list))
    order_items_data['product_id'].append(random.choice(product_ids_list))
    
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10.0, 200.0), 2)
    subtotal = round(quantity * unit_price, 2)
    
    order_items_data['quantity'].append(quantity)
    order_items_data['unit_price'].append(unit_price)
    order_items_data['subtotal'].append(subtotal)

df_order_items = pd.DataFrame(order_items_data)

# Introduce errors
df_order_items = add_nulls(df_order_items, 0.05)
df_order_items = add_duplicates(df_order_items, 0.01)

# Convert some numeric fields to strings (type error)
df_order_items['quantity'] = df_order_items['quantity'].apply(convert_to_string)
df_order_items['unit_price'] = df_order_items['unit_price'].apply(convert_to_string)
df_order_items['subtotal'] = df_order_items['subtotal'].apply(convert_to_string)

# Save to Unity Catalog
save_to_unity_catalog(df_order_items, "order_items")

print(f"✅ Order items generated: {len(df_order_items)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ ALL DATASETS GENERATED SUCCESSFULLY!")
print("="*60)
print(f"\n📁 Catalog: {CATALOG_NAME}.{BRONZE_SCHEMA}")
print("\n📊 Tables generated:")
print(f"  • Categories:     {len(df_categories):,} rows → {CATALOG_NAME}.{BRONZE_SCHEMA}.categories")
print(f"  • Customers:      {len(df_customers):,} rows → {CATALOG_NAME}.{BRONZE_SCHEMA}.customers")
print(f"  • Products:       {len(df_products):,} rows → {CATALOG_NAME}.{BRONZE_SCHEMA}.products")
print(f"  • Orders:         {len(df_orders):,} rows → {CATALOG_NAME}.{BRONZE_SCHEMA}.orders")
print(f"  • Order Items:    {len(df_order_items):,} rows → {CATALOG_NAME}.{BRONZE_SCHEMA}.order_items")
print(f"\n📦 Tables saved to Unity Catalog")
print("\n🐛 Data quality issues included:")
print("  • ~1% duplicate records")
print("  • ~5% null values")
print("  • Inconsistent date formats")
print("  • Inconsistent string formatting (case, spaces)")
print("  • Type errors (numbers as strings)")
print("\n" + "="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Verify data**: Query the tables in Unity Catalog
# MAGIC    ```sql
# MAGIC    SELECT * FROM ecommerce_data.bronze.categories LIMIT 10;
# MAGIC    ```
# MAGIC 2. **Silver Layer**: Create notebooks to clean and validate the data from bronze tables
# MAGIC 3. **Gold Layer**: Create dimension and fact tables for star schema

