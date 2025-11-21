# Star Schema - E-commerce Data Model

## Overview

This document describes the Star Schema design for the Gold layer of the Medallion Architecture.

**Important:** In a Star Schema, dimension tables do NOT relate to each other. All dimensions connect ONLY to the fact table.

## Schema Diagram

```
                    ┌─────────────────────────────────┐
                    │        fato_vendas              │
                    │        (Fact Table)             │
                    │                                 │
                    │ Foreign Keys:                   │
                    │  - customer_key (FK)            │
                    │  - product_key (FK)             │
                    │  - category_key (FK)            │
                    │  - date_key (FK)                │
                    │  - order_key (FK)                │
                    │                                 │
                    │ Measures:                       │
                    │  - quantity                     │
                    │  - unit_price                   │
                    │  - subtotal                     │
                    │  - calculated_subtotal          │
                    │                                 │
                    │ Natural Keys:                   │
                    │  - order_id                     │
                    │  - item_id                      │
                    │  - product_id                   │
                    └───────────┬─────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│dim_customer  │      │ dim_product  │      │  dim_time    │
│(SCD Type 2)  │      │(SCD Type 2)  │      │              │
│              │      │              │      │PK: date_key  │
│PK: customer_ │      │PK: product_  │      │              │
│    key       │      │    key       │      │ Attributes:  │
│              │      │              │      │  - full_date │
│Attributes:   │      │Attributes:   │      │  - year      │
│  - customer_ │      │  - product_  │      │  - quarter   │
│    id (NK)   │      │    id (NK)   │      │  - month     │
│  - first_    │      │  - product_  │      │  - month_    │
│    name      │      │    name      │      │    name      │
│  - last_name │      │  - category_ │      │  - week      │
│  - full_name │      │    id        │      │  - day_of_   │
│  - email     │      │  - category_ │      │    week      │
│  - phone     │      │    name      │      │  - day_name  │
│  - birth_    │      │  - price     │      │  - is_       │
│    date      │      │  - stock_    │      │    weekend   │
│  - age       │      │    quantity  │      │  - year_     │
│  - city      │      │  - stock_    │      │    quarter   │
│  - state     │      │    status    │      │  - year_     │
│  - zip_code  │      │              │      │    month     │
│  - registra- │      │SCD Type 2:   │      │              │
│    tion_date │      │  - valid_    │      │              │
│  - days_     │      │    from      │      │              │
│    since_    │      │  - valid_to  │      │              │
│    registra- │      │  - is_current│      │              │
│    tion      │      │              │      │              │
│SCD Type 2:   │      │              │      │              │
│  - valid_    │      │              │      │              │
│    from      │      │              │      │              │
│  - valid_to  │      │              │      │              │
│  - is_current│      │              │      │              │
└──────────────┘      └──────────────┘      └──────────────┘
        │                       │
        │                       │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │     dim_category      │
        │    (SCD Type 1)       │
        │                        │
        │PK: category_key        │
        │                        │
        │Attributes:             │
        │  - category_id (NK)   │
        │  - category_name      │
        │  - description        │
        └────────────────────────┘

        ┌───────────────────────┐
        │     dim_order          │
        │  (Degenerate Dimension)│
        │                        │
        │PK: order_key           │
        │                        │
        │Attributes:             │
        │  - order_id (NK)       │
        │  - status              │
        │  - payment_type        │
        └────────────────────────┘
```

**Key Point:** All dimension tables connect ONLY to `fato_vendas`. They do NOT connect to each other.

## Table Descriptions

### Fact Table: `fato_vendas`

**Granularity:** Order Item level (one row per item in an order)

**Primary Key:** Composite (customer_key, product_key, category_key, date_key, order_key, item_id)

**Foreign Keys:**
- `customer_key` → `dim_customer.customer_key` (SCD Type 2 - use is_current = 1)
- `product_key` → `dim_product.product_key` (SCD Type 2 - use is_current = 1)
- `category_key` → `dim_category.category_key` (SCD Type 1)
- `date_key` → `dim_time.date_key`
- `order_key` → `dim_order.order_key`

**Measures:**
- `quantity` (int): Quantity of items ordered
- `unit_price` (double): Price per unit
- `subtotal` (double): Total for this line item
- `calculated_subtotal` (double): quantity × unit_price (for validation)

**Natural Keys (for reference):**
- `order_id`: Original order ID
- `item_id`: Original item ID
- `product_id`: Original product ID

---

### Dimension Tables

#### 1. `dim_customer`

**Type:** SCD Type 2 (Slowly Changing Dimension - maintains history)

**Primary Key:** `customer_key` (surrogate key, unique per version)

**Natural Key:** `customer_id` (can have multiple versions)

**Attributes:**
- Demographics: `first_name`, `last_name`, `full_name`, `email`, `phone`
- Location: `city`, `state`, `zip_code`
- Dates: `birth_date`, `registration_date`
- Calculated: `age`, `days_since_registration`
- **SCD Type 2 Fields:**
  - `valid_from` (date): When this version became valid
  - `valid_to` (date): When this version expired (NULL = current)
  - `is_current` (int): 1 = current record, 0 = historical
- Metadata: `created_at`, `updated_at`

**Usage:** When a customer changes address or other attributes, a new row is created with a new `customer_key`, and the old row's `valid_to` is set and `is_current` = 0.

---

#### 2. `dim_product`

**Type:** SCD Type 2 (maintains history)

**Primary Key:** `product_key` (surrogate key, unique per version)

**Natural Key:** `product_id` (can have multiple versions)

**Attributes:**
- Product Info: `product_name`, `price`, `stock_quantity`
- Category: `category_id`, `category_name` (denormalized for performance)
- Status: `stock_status` (In Stock / Out of Stock)
- Dates: `created_date`
- **SCD Type 2 Fields:**
  - `valid_from` (date): When this version became valid
  - `valid_to` (date): When this version expired (NULL = current)
  - `is_current` (int): 1 = current record, 0 = historical
- Metadata: `created_at`, `updated_at`

**Usage:** When a product price changes or other attributes change, a new row is created with a new `product_key`, and the old row's `valid_to` is set and `is_current` = 0.

---

#### 3. `dim_category`

**Type:** SCD Type 1 (overwrites - no history)

**Primary Key:** `category_key` (surrogate key)

**Natural Key:** `category_id`

**Attributes:**
- `category_name`
- `description`
- Metadata: `created_at`, `updated_at`

**Note:** Categories rarely change, so SCD Type 1 is sufficient.

---

#### 4. `dim_time`

**Type:** Static dimension (pre-populated)

**Primary Key:** `date_key` (YYYYMMDD format, e.g., 20240115)

**Attributes:**
- Date: `full_date`
- Year: `year`
- Quarter: `quarter` (1-4)
- Month: `month`, `month_name`, `month_abbr`
- Week: `week` (ISO week number)
- Day: `day_of_month`, `day_of_week`, `day_name`, `day_abbr`
- Flags: `is_weekend`, `is_month_end`
- Hierarchies: `year_quarter`, `year_month`

**Usage:** Enables time-based analysis (sales by month, quarter, year, etc.)

---

#### 5. `dim_order` (Degenerate Dimension)

**Type:** Degenerate dimension (no separate dimension table needed, but included for clarity)

**Primary Key:** `order_key` (surrogate key)

**Natural Key:** `order_id`

**Attributes:**
- `status` (Pending, Processing, Shipped, Delivered, Cancelled)
- `payment_type` (Credit Card, Debit Card, PayPal, etc.)

**Note:** Degenerate dimensions store attributes that don't warrant a separate dimension table but are useful for filtering/grouping.

---

## Relationships

**Important:** In a Star Schema, dimensions do NOT relate to each other. All relationships are through the fact table.

```
fato_vendas (Fact)
├── customer_key → dim_customer.customer_key (Many-to-One)
├── product_key → dim_product.product_key (Many-to-One)
├── category_key → dim_category.category_key (Many-to-One)
├── date_key → dim_time.date_key (Many-to-One)
└── order_key → dim_order.order_key (Many-to-One)
```

**Note:** `dim_product` contains `category_id` and `category_name` for denormalization (performance), but there is NO foreign key relationship between `dim_product` and `dim_category`. Both connect directly to `fato_vendas`.

---

## SCD Type 2 Implementation

### How SCD Type 2 Works

1. **Initial Load:** All records have `is_current = 1`, `valid_from = registration_date/created_date`, `valid_to = NULL`

2. **When a Change Occurs:**
   - Old record: Set `valid_to = change_date`, `is_current = 0`
   - New record: Create with new `customer_key`/`product_key`, `valid_from = change_date`, `valid_to = NULL`, `is_current = 1`

3. **Querying Current Records:**
   ```sql
   SELECT * FROM dim_customer WHERE is_current = 1
   ```

4. **Querying Historical Records:**
   ```sql
   SELECT * FROM dim_customer 
   WHERE customer_id = 123 
   ORDER BY valid_from
   ```

5. **Point-in-Time Queries:**
   ```sql
   SELECT * FROM dim_customer 
   WHERE customer_id = 123 
     AND '2024-01-15' >= valid_from 
     AND ('2024-01-15' < valid_to OR valid_to IS NULL)
   ```

---

## Query Examples

### 1. Sales by Category (using current product versions)
```sql
SELECT 
    p.category_name,
    SUM(f.subtotal) as total_sales,
    SUM(f.quantity) as total_quantity,
    COUNT(DISTINCT f.order_id) as total_orders
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_product p ON f.product_key = p.product_key
JOIN ecommerce_data.gold.dim_category c ON f.category_key = c.category_key
WHERE p.is_current = 1
GROUP BY p.category_name
ORDER BY total_sales DESC;
```

### 2. Sales by Month
```sql
SELECT 
    t.year_month,
    t.month_name,
    t.year,
    SUM(f.subtotal) as total_sales,
    COUNT(DISTINCT f.order_id) as total_orders
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_time t ON f.date_key = t.date_key
GROUP BY t.year_month, t.month_name, t.year
ORDER BY t.year_month;
```

### 3. Top Customers (using current customer versions)
```sql
SELECT 
    c.full_name,
    c.city,
    c.state,
    SUM(f.subtotal) as total_spent,
    COUNT(DISTINCT f.order_id) as total_orders,
    AVG(f.subtotal) as avg_order_value
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = 1
GROUP BY c.full_name, c.city, c.state
ORDER BY total_spent DESC
LIMIT 10;
```

### 4. Product Performance (using current product versions)
```sql
SELECT 
    p.product_name,
    c.category_name,
    SUM(f.quantity) as total_quantity_sold,
    SUM(f.subtotal) as total_revenue,
    AVG(f.unit_price) as avg_price
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_product p ON f.product_key = p.product_key
JOIN ecommerce_data.gold.dim_category c ON f.category_key = c.category_key
WHERE p.is_current = 1
GROUP BY p.product_name, c.category_name
ORDER BY total_revenue DESC
LIMIT 20;
```

### 5. Sales Trend by Quarter
```sql
SELECT 
    t.year,
    t.quarter,
    SUM(f.subtotal) as total_sales,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_time t ON f.date_key = t.date_key
GROUP BY t.year, t.quarter
ORDER BY t.year, t.quarter;
```

### 6. Historical Product Price Analysis (SCD Type 2)
```sql
-- See how product prices changed over time
SELECT 
    p.product_id,
    p.product_name,
    p.price,
    p.valid_from,
    p.valid_to,
    p.is_current,
    COUNT(f.item_id) as sales_count
FROM ecommerce_data.gold.dim_product p
LEFT JOIN ecommerce_data.gold.fato_vendas f 
    ON p.product_key = f.product_key
WHERE p.product_id = 123
GROUP BY p.product_id, p.product_name, p.price, p.valid_from, p.valid_to, p.is_current
ORDER BY p.valid_from;
```

---

## Benefits of Star Schema

1. **Simplified Queries**: Easy to understand and write
2. **Performance**: Optimized for analytical queries with denormalized dimensions
3. **Flexibility**: Easy to add new measures or dimensions
4. **Business-Friendly**: Aligns with how business users think about data
5. **Scalability**: Efficient for large fact tables with small dimension tables
6. **Historical Tracking**: SCD Type 2 enables point-in-time analysis

---

## SCD Type 2 Maintenance

### Adding a New Version (Example: Customer Address Change)

```sql
-- Step 1: Close current record
UPDATE ecommerce_data.gold.dim_customer
SET valid_to = '2024-01-15',
    is_current = 0,
    updated_at = current_timestamp()
WHERE customer_id = 123 
  AND is_current = 1;

-- Step 2: Insert new version
INSERT INTO ecommerce_data.gold.dim_customer
SELECT 
    monotonically_increasing_id() as customer_key,
    customer_id,
    first_name,
    last_name,
    -- ... other attributes with new values ...
    'New City' as city,
    'New State' as state,
    '2024-01-15' as valid_from,
    NULL as valid_to,
    1 as is_current,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
FROM ecommerce_data.gold.dim_customer
WHERE customer_id = 123 AND is_current = 0
LIMIT 1;
```
