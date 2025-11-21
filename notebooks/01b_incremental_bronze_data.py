# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Bronze Layer Data Generator
# MAGIC
# MAGIC This notebook generates **incremental** synthetic e-commerce data to simulate continuous data ingestion.
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC - Reads existing data from bronze layer
# MAGIC - Generates new records with IDs continuing from existing data
# MAGIC - Appends new data to bronze tables (simulates streaming/continuous ingestion)
# MAGIC - Maintains referential integrity
# MAGIC - Includes same data quality issues as initial load
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC Run this notebook periodically (daily, hourly, etc.) to simulate new data arriving in the bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility (but different from initial load)
np.random.seed(123)
random.seed(123)

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
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"✅ Using catalog: {CATALOG_NAME}")
print(f"✅ Using schema: {BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration: How many new records to generate

# COMMAND ----------

# Configuration: Adjust these values to control how many new records are generated
NEW_CUSTOMERS = 50          # New customers to add
NEW_PRODUCTS = 10            # New products to add
NEW_ORDERS = 200             # New orders to add
NEW_ORDER_ITEMS = 600        # New order items to add (average 3 per order)

print(f"📊 Incremental Load Configuration:")
print(f"  • New Customers: {NEW_CUSTOMERS}")
print(f"  • New Products: {NEW_PRODUCTS}")
print(f"  • New Orders: {NEW_ORDERS}")
print(f"  • New Order Items: {NEW_ORDER_ITEMS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions (same as initial load)

# COMMAND ----------

def add_duplicates(df, percentage=0.01):
    """Add duplicate rows (~1% of records)"""
    num_duplicates = int(len(df) * percentage)
    if num_duplicates > 0:
        duplicate_rows = df.sample(n=num_duplicates, random_state=123)
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
    if pd.isna(date_str) or date_str is None:
        return date_str
    
    if isinstance(date_str, str):
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except:
            return date_str
    else:
        date_obj = date_str
    
    if pd.isna(date_obj):
        return date_obj
    
    formats = [
        lambda d: d.strftime("%Y-%m-%d"),
        lambda d: d.strftime("%Y/%m/%d"),
        lambda d: d.strftime("%d/%m/%Y"),
        lambda d: d.strftime("%m-%d-%Y"),
        lambda d: d.strftime("%d-%m-%Y"),
        lambda d: d.strftime("%b %d, %Y"),
        lambda d: d.strftime("%B %d, %Y")
    ]
    
    return random.choice(formats)(date_obj)

def mess_up_strings(s):
    """Create inconsistent string formatting"""
    if pd.isna(s) or s is None:
        return s
    
    s = str(s)
    variations = [
        lambda x: x.upper(),
        lambda x: x.lower(),
        lambda x: x.capitalize(),
        lambda x: " " + x + " ",
        lambda x: x.strip(),
        lambda x: x.replace(" ", "  "),
        lambda x: x
    ]
    
    return random.choice(variations)(s)

def convert_to_string(x):
    """Randomly convert numeric values to strings (type error)"""
    if random.random() < 0.1:  # 10% chance
        return str(x)
    return x

def save_to_unity_catalog(df, table_name, schema_name=BRONZE_SCHEMA, catalog_name=CATALOG_NAME, mode="append"):
    """
    Save DataFrame as Unity Catalog table (append mode for incremental loads)
    
    Parameters:
    - df: Spark DataFrame or Pandas DataFrame
    - table_name: Name of the table
    - schema_name: Schema name (default: bronze)
    - catalog_name: Catalog name (default: ecommerce_data)
    - mode: Write mode (append for incremental, overwrite for initial)
    """
    # Convert pandas DataFrame to Spark DataFrame if needed
    if isinstance(df, pd.DataFrame):
        # Handle columns with mixed types (object) by casting to string
        for col in df.columns:
            if df[col].dtype == 'object':
                if df[col].apply(lambda x: not isinstance(x, (int, float)) and x is not None and not pd.isna(x)).any():
                    df[col] = df[col].astype(str)
        spark_df = spark.createDataFrame(df)
    else:
        spark_df = df
    
    # Full table path
    full_table_path = f"{catalog_name}.{schema_name}.{table_name}"
    
    # Write to Unity Catalog
    spark_df.write \
        .mode(mode) \
        .option("overwriteSchema", "false") \
        .saveAsTable(full_table_path)
    
    print(f"✅ Table updated: {full_table_path} ({mode} mode)")
    return full_table_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Current Max IDs from Existing Data

# COMMAND ----------

print("📊 Reading existing data to get current max IDs...")

# Read existing tables to get max IDs
try:
    df_existing_customers = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers")
    max_customer_id = df_existing_customers.agg({"customer_id": "max"}).collect()[0][0] or 0
    existing_customer_count = df_existing_customers.count()
    print(f"  • Existing customers: {existing_customer_count}, Max customer_id: {max_customer_id}")
except:
    max_customer_id = 0
    print("  • No existing customers found, starting from 0")

try:
    df_existing_products = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products")
    max_product_id = df_existing_products.agg({"product_id": "max"}).collect()[0][0] or 0
    existing_product_count = df_existing_products.count()
    print(f"  • Existing products: {existing_product_count}, Max product_id: {max_product_id}")
except:
    max_product_id = 0
    print("  • No existing products found, starting from 0")

try:
    df_existing_orders = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders")
    max_order_id = df_existing_orders.agg({"order_id": "max"}).collect()[0][0] or 0
    existing_order_count = df_existing_orders.count()
    print(f"  • Existing orders: {existing_order_count}, Max order_id: {max_order_id}")
except:
    max_order_id = 0
    print("  • No existing orders found, starting from 0")

try:
    df_existing_order_items = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.order_items")
    max_item_id = df_existing_order_items.agg({"item_id": "max"}).collect()[0][0] or 0
    existing_item_count = df_existing_order_items.count()
    print(f"  • Existing order items: {existing_item_count}, Max item_id: {max_item_id}")
except:
    max_item_id = 0
    print("  • No existing order items found, starting from 0")

# Get existing customer and product IDs for referential integrity
try:
    existing_customer_ids = [row['customer_id'] for row in df_existing_customers.select("customer_id").distinct().collect()]
except:
    existing_customer_ids = []

try:
    existing_product_ids = [row['product_id'] for row in df_existing_products.select("product_id").distinct().collect()]
except:
    existing_product_ids = []

print(f"\n✅ Max IDs identified. Starting new records from:")
print(f"  • customer_id: {max_customer_id + 1}")
print(f"  • product_id: {max_product_id + 1}")
print(f"  • order_id: {max_order_id + 1}")
print(f"  • item_id: {max_item_id + 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate New Customers

# COMMAND ----------

print(f"\n📝 Generating {NEW_CUSTOMERS} new customers...")

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
    'customer_id': range(max_customer_id + 1, max_customer_id + 1 + NEW_CUSTOMERS),
    'first_name': [random.choice(first_names) for _ in range(NEW_CUSTOMERS)],
    'last_name': [random.choice(last_names) for _ in range(NEW_CUSTOMERS)],
    'email': [],
    'phone': [],
    'birth_date': [],
    'city': [random.choice(cities) for _ in range(NEW_CUSTOMERS)],
    'state': [random.choice(states) for _ in range(NEW_CUSTOMERS)],
    'zip_code': [],
    'registration_date': []
}

for i in range(NEW_CUSTOMERS):
    first = customers_data['first_name'][i].lower()
    last = customers_data['last_name'][i].lower()
    customer_num = max_customer_id + 1 + i
    customers_data['email'].append(f"{first}.{last}{customer_num}@email.com")
    customers_data['phone'].append(f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}")
    
    # Birth dates (18-80 years old)
    birth_date = datetime.now() - timedelta(days=random.randint(18*365, 80*365))
    customers_data['birth_date'].append(birth_date)
    
    # Zip codes
    customers_data['zip_code'].append(f"{random.randint(10000, 99999)}")
    
    # Registration dates (recent, last 30 days)
    reg_date = datetime.now() - timedelta(days=random.randint(0, 30))
    customers_data['registration_date'].append(reg_date)

df_new_customers = pd.DataFrame(customers_data)

# Introduce errors
df_new_customers = add_nulls(df_new_customers, 0.05)
df_new_customers = add_duplicates(df_new_customers, 0.01)

# Mess up dates
df_new_customers['birth_date'] = df_new_customers['birth_date'].apply(mess_up_dates)
df_new_customers['registration_date'] = df_new_customers['registration_date'].apply(mess_up_dates)

# Mess up strings
df_new_customers['first_name'] = df_new_customers['first_name'].apply(mess_up_strings)
df_new_customers['last_name'] = df_new_customers['last_name'].apply(mess_up_strings)
df_new_customers['city'] = df_new_customers['city'].apply(mess_up_strings)
df_new_customers['state'] = df_new_customers['state'].apply(mess_up_strings)

# Convert some zip codes to strings (type error)
df_new_customers['zip_code'] = df_new_customers['zip_code'].apply(convert_to_string)

# Append to Unity Catalog
save_to_unity_catalog(df_new_customers, "customers", mode="append")

print(f"✅ {len(df_new_customers)} new customers added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate New Products

# COMMAND ----------

print(f"\n📝 Generating {NEW_PRODUCTS} new products...")

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
    'product_id': range(max_product_id + 1, max_product_id + 1 + NEW_PRODUCTS),
    'product_name': [],
    'category_id': [],
    'price': [],
    'stock_quantity': [],
    'created_date': []
}

for i in range(NEW_PRODUCTS):
    category_id = random.randint(1, 10)
    products_data['category_id'].append(category_id)
    products_data['product_name'].append(random.choice(product_names[category_id]) + f" {random.randint(1000, 9999)}")
    products_data['price'].append(round(random.uniform(10.0, 500.0), 2))
    products_data['stock_quantity'].append(random.randint(0, 1000))
    
    # Created dates (recent, last 30 days)
    created_date = datetime.now() - timedelta(days=random.randint(0, 30))
    products_data['created_date'].append(created_date)

df_new_products = pd.DataFrame(products_data)

# Introduce errors
df_new_products = add_nulls(df_new_products, 0.05)
df_new_products = add_duplicates(df_new_products, 0.01)

# Mess up dates
df_new_products['created_date'] = df_new_products['created_date'].apply(mess_up_dates)

# Mess up strings
df_new_products['product_name'] = df_new_products['product_name'].apply(mess_up_strings)

# Convert some prices and stock to strings (type error)
df_new_products['price'] = df_new_products['price'].apply(convert_to_string)
df_new_products['stock_quantity'] = df_new_products['stock_quantity'].apply(convert_to_string)

# Append to Unity Catalog
save_to_unity_catalog(df_new_products, "products", mode="append")

print(f"✅ {len(df_new_products)} new products added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate New Orders

# COMMAND ----------

print(f"\n📝 Generating {NEW_ORDERS} new orders...")

order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
payment_types = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash']

# Combine existing and new customer IDs
all_customer_ids = existing_customer_ids + list(range(max_customer_id + 1, max_customer_id + 1 + NEW_CUSTOMERS))

orders_data = {
    'order_id': range(max_order_id + 1, max_order_id + 1 + NEW_ORDERS),
    'customer_id': [],
    'order_date': [],
    'status': [],
    'total_amount': [],
    'payment_type': []
}

# Generate orders (mix of existing and new customers)
for i in range(NEW_ORDERS):
    orders_data['customer_id'].append(random.choice(all_customer_ids))
    
    # Order dates (recent, last 7 days)
    order_date = datetime.now() - timedelta(days=random.randint(0, 7))
    orders_data['order_date'].append(order_date)
    
    orders_data['status'].append(random.choice(order_statuses))
    orders_data['payment_type'].append(random.choice(payment_types))
    orders_data['total_amount'].append(round(random.uniform(20.0, 1000.0), 2))

df_new_orders = pd.DataFrame(orders_data)

# Introduce errors
df_new_orders = add_nulls(df_new_orders, 0.05)
df_new_orders = add_duplicates(df_new_orders, 0.01)

# Mess up dates
df_new_orders['order_date'] = df_new_orders['order_date'].apply(mess_up_dates)

# Mess up strings
df_new_orders['status'] = df_new_orders['status'].apply(mess_up_strings)
df_new_orders['payment_type'] = df_new_orders['payment_type'].apply(mess_up_strings)

# Convert some amounts to strings (type error)
df_new_orders['total_amount'] = df_new_orders['total_amount'].apply(convert_to_string)

# Append to Unity Catalog
save_to_unity_catalog(df_new_orders, "orders", mode="append")

print(f"✅ {len(df_new_orders)} new orders added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate New Order Items

# COMMAND ----------

print(f"\n📝 Generating {NEW_ORDER_ITEMS} new order items...")

# Combine existing and new order/product IDs
all_order_ids = list(range(max_order_id + 1, max_order_id + 1 + NEW_ORDERS))
all_product_ids = existing_product_ids + list(range(max_product_id + 1, max_product_id + 1 + NEW_PRODUCTS))

order_items_data = {
    'item_id': range(max_item_id + 1, max_item_id + 1 + NEW_ORDER_ITEMS),
    'order_id': [],
    'product_id': [],
    'quantity': [],
    'unit_price': [],
    'subtotal': []
}

# Generate order items (link to new orders)
for i in range(NEW_ORDER_ITEMS):
    order_items_data['order_id'].append(random.choice(all_order_ids))
    order_items_data['product_id'].append(random.choice(all_product_ids))
    
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10.0, 200.0), 2)
    subtotal = round(quantity * unit_price, 2)
    
    order_items_data['quantity'].append(quantity)
    order_items_data['unit_price'].append(unit_price)
    order_items_data['subtotal'].append(subtotal)

df_new_order_items = pd.DataFrame(order_items_data)

# Introduce errors
df_new_order_items = add_nulls(df_new_order_items, 0.05)
df_new_order_items = add_duplicates(df_new_order_items, 0.01)

# Convert some numeric fields to strings (type error)
df_new_order_items['quantity'] = df_new_order_items['quantity'].apply(convert_to_string)
df_new_order_items['unit_price'] = df_new_order_items['unit_price'].apply(convert_to_string)
df_new_order_items['subtotal'] = df_new_order_items['subtotal'].apply(convert_to_string)

# Append to Unity Catalog
save_to_unity_catalog(df_new_order_items, "order_items", mode="append")

print(f"✅ {len(df_new_order_items)} new order items added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ INCREMENTAL DATA LOAD COMPLETED!")
print("="*60)

# Count total records after incremental load
try:
    total_customers = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers").count()
    total_products = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products").count()
    total_orders = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders").count()
    total_order_items = spark.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.order_items").count()
    
    print(f"\n📊 Total Records in Bronze Layer:")
    print(f"  • Customers:     {total_customers:,} (added {len(df_new_customers):,} new)")
    print(f"  • Products:       {total_products:,} (added {len(df_new_products):,} new)")
    print(f"  • Orders:         {total_orders:,} (added {len(df_new_orders):,} new)")
    print(f"  • Order Items:    {total_order_items:,} (added {len(df_new_order_items):,} new)")
except Exception as e:
    print(f"⚠️  Error counting records: {e}")

print(f"\n📦 New data appended to Unity Catalog:")
print(f"  • {CATALOG_NAME}.{BRONZE_SCHEMA}.customers")
print(f"  • {CATALOG_NAME}.{BRONZE_SCHEMA}.products")
print(f"  • {CATALOG_NAME}.{BRONZE_SCHEMA}.orders")
print(f"  • {CATALOG_NAME}.{BRONZE_SCHEMA}.order_items")

print(f"\n🐛 Data quality issues included (same as initial load):")
print("  • ~1% duplicate records")
print("  • ~5% null values")
print("  • Inconsistent date formats")
print("  • Inconsistent string formatting (case, spaces)")
print("  • Type errors (numbers as strings)")

print(f"\n💡 Next Steps:")
print("  1. Run Silver layer cleaning notebook to process new data")
print("  2. Run Gold layer notebook to update star schema")
print("  3. Schedule this notebook to run periodically (daily/hourly)")

print("\n" + "="*60)

