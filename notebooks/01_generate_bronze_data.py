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
# MAGIC Each table is generated in CSV, JSON, and Parquet formats.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Output Path
# MAGIC 
# MAGIC Choose one of the following options:
# MAGIC - **DBFS** (local to workspace): `/dbfs/mnt/bronze/`
# MAGIC - **Cloud Storage**:
# MAGIC   - AWS S3: `s3://your-bucket/bronze/`
# MAGIC   - Azure ADLS: `abfss://container@account.dfs.core.windows.net/bronze/`
# MAGIC   - GCP GCS: `gs://your-bucket/bronze/`

# COMMAND ----------

# Configure output path
# Option 1: DBFS (local to workspace)
OUTPUT_BASE_PATH = "/dbfs/mnt/bronze/"

# Option 2: Cloud storage (uncomment and configure)
# OUTPUT_BASE_PATH = "s3://your-bucket/bronze/"  # AWS
# OUTPUT_BASE_PATH = "gs://your-bucket/bronze/"   # GCP
# OUTPUT_BASE_PATH = "abfss://container@account.dfs.core.windows.net/bronze/"  # Azure

# Create directories
formats = ["csv", "json", "parquet"]
tables = ["categories", "customers", "products", "orders", "order_items"]

for table in tables:
    for fmt in formats:
        Path(f"{OUTPUT_BASE_PATH}{table}/{fmt}").mkdir(parents=True, exist_ok=True)

print(f"✅ Output path configured: {OUTPUT_BASE_PATH}")

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
    if isinstance(date_str, str):
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except:
            return date_str
    else:
        date_obj = date_str
    
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

# Save in all formats
for fmt in formats:
    path = f"{OUTPUT_BASE_PATH}categories/{fmt}/categories.{fmt}"
    if fmt == "csv":
        df_categories.to_csv(path, index=False)
    elif fmt == "json":
        df_categories.to_json(path, orient="records", date_format="iso")
    elif fmt == "parquet":
        df_categories.to_parquet(path, index=False)

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

# Save in all formats
for fmt in formats:
    path = f"{OUTPUT_BASE_PATH}customers/{fmt}/customers.{fmt}"
    if fmt == "csv":
        df_customers.to_csv(path, index=False)
    elif fmt == "json":
        df_customers.to_json(path, orient="records", date_format="iso")
    elif fmt == "parquet":
        df_customers.to_parquet(path, index=False)

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

# Save in all formats
for fmt in formats:
    path = f"{OUTPUT_BASE_PATH}products/{fmt}/products.{fmt}"
    if fmt == "csv":
        df_products.to_csv(path, index=False)
    elif fmt == "json":
        df_products.to_json(path, orient="records", date_format="iso")
    elif fmt == "parquet":
        df_products.to_parquet(path, index=False)

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

# Save in all formats
for fmt in formats:
    path = f"{OUTPUT_BASE_PATH}orders/{fmt}/orders.{fmt}"
    if fmt == "csv":
        df_orders.to_csv(path, index=False)
    elif fmt == "json":
        df_orders.to_json(path, orient="records", date_format="iso")
    elif fmt == "parquet":
        df_orders.to_parquet(path, index=False)

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

# Save in all formats
for fmt in formats:
    path = f"{OUTPUT_BASE_PATH}order_items/{fmt}/order_items.{fmt}"
    if fmt == "csv":
        df_order_items.to_csv(path, index=False)
    elif fmt == "json":
        df_order_items.to_json(path, orient="records", date_format="iso")
    elif fmt == "parquet":
        df_order_items.to_parquet(path, index=False)

print(f"✅ Order items generated: {len(df_order_items)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ ALL DATASETS GENERATED SUCCESSFULLY!")
print("="*60)
print(f"\n📁 Output path: {OUTPUT_BASE_PATH}")
print("\n📊 Tables generated:")
print(f"  • Categories:     {len(df_categories):,} rows")
print(f"  • Customers:      {len(df_customers):,} rows")
print(f"  • Products:       {len(df_products):,} rows")
print(f"  • Orders:         {len(df_orders):,} rows")
print(f"  • Order Items:    {len(df_order_items):,} rows")
print(f"\n📦 Formats: CSV, JSON, Parquet")
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
# MAGIC 1. **Verify data**: Check the generated files in your storage location
# MAGIC 2. **Silver Layer**: Create notebooks to clean and validate the data
# MAGIC 3. **Gold Layer**: Create dimension and fact tables for star schema

