# Star Schema - E-commerce Data Model

## Overview

This document describes the Star Schema design for the Gold layer of the Medallion Architecture.

## Schema Diagram

```
                    ┌─────────────────────────────────┐
                    │        fato_vendas              │
                    │        (Fact Table)             │
                    │                                 │
                    │ Foreign Keys:                   │
                    │  - customer_key (FK)            │
                    │  - product_key (FK)             │
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
│              │      │              │      │              │
│PK: customer_ │      │PK: product_  │      │PK: date_key  │
│    key       │      │    key       │      │              │
│              │      │              │      │ Attributes:  │
│Attributes:   │      │Attributes:   │      │  - full_date │
│  - customer_ │      │  - product_  │      │  - year      │
│    id (NK)   │      │    id (NK)   │      │  - quarter   │
│  - first_    │      │  - product_   │      │  - month     │
│    name      │      │    name      │      │  - month_    │
│  - last_name │      │  - category_ │      │    name      │
│  - full_name │      │    id        │      │  - week      │
│  - email     │      │  - category_  │      │  - day_of_   │
│  - phone     │      │    name      │      │    week      │
│  - birth_    │      │  - price     │      │  - day_name  │
│    date      │      │  - stock_    │      │  - is_       │
│  - age       │      │    quantity  │      │    weekend   │
│  - city      │      │  - stock_    │      │  - year_     │
│  - state     │      │    status    │      │    quarter   │
│  - zip_code  │      │              │      │  - year_     │
│  - registra- │      │              │      │    month     │
│    tion_date │      │              │      │              │
│  - days_     │      │              │      │              │
│    since_    │      │              │      │              │
│    registra- │      │              │      │              │
│    tion      │      │              │      │              │
└──────────────┘      └──────┬───────┘      └──────────────┘
                             │
                             │
                             ▼
                    ┌──────────────┐
                    │ dim_category │
                    │              │
                    │PK: category_ │
                    │    key       │
                    │              │
                    │Attributes:   │
                    │  - category_ │
                    │    id (NK)   │
                    │  - category_ │
                    │    name      │
                    │  - descrip-  │
                    │    tion      │
                    └──────────────┘

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

## Table Descriptions

### Fact Table: `fato_vendas`

**Granularity:** Order Item level (one row per item in an order)

**Primary Key:** Composite (customer_key, product_key, date_key, order_key, item_id)

**Foreign Keys:**
- `customer_key` → `dim_customer.customer_key`
- `product_key` → `dim_product.product_key`
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

**Type:** SCD Type 1 (Slowly Changing Dimension - overwrite)

**Primary Key:** `customer_key` (surrogate key)

**Natural Key:** `customer_id`

**Attributes:**
- Demographics: `first_name`, `last_name`, `full_name`, `email`, `phone`
- Location: `city`, `state`, `zip_code`
- Dates: `birth_date`, `registration_date`
- Calculated: `age`, `days_since_registration`
- Metadata: `created_at`, `updated_at`

---

#### 2. `dim_product`

**Type:** SCD Type 1

**Primary Key:** `product_key` (surrogate key)

**Natural Key:** `product_id`

**Attributes:**
- Product Info: `product_name`, `price`, `stock_quantity`
- Category: `category_id`, `category_name` (denormalized for performance)
- Status: `stock_status` (In Stock / Out of Stock)
- Dates: `created_date`
- Metadata: `created_at`, `updated_at`

---

#### 3. `dim_category`

**Type:** SCD Type 1

**Primary Key:** `category_key` (surrogate key)

**Natural Key:** `category_id`

**Attributes:**
- `category_name`
- `description`
- Metadata: `created_at`, `updated_at`

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

```
fato_vendas (Fact)
├── customer_key → dim_customer.customer_key (Many-to-One)
├── product_key → dim_product.product_key (Many-to-One)
├── date_key → dim_time.date_key (Many-to-One)
└── order_key → dim_order.order_key (Many-to-One)

dim_product
└── category_id → dim_category.category_id (Many-to-One)
```

## Query Examples

### 1. Sales by Category
```sql
SELECT 
    c.category_name,
    SUM(f.subtotal) as total_sales,
    SUM(f.quantity) as total_quantity,
    COUNT(DISTINCT f.order_id) as total_orders
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_product p ON f.product_key = p.product_key
JOIN ecommerce_data.gold.dim_category c ON p.category_id = c.category_id
GROUP BY c.category_name
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

### 3. Top Customers
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
GROUP BY c.full_name, c.city, c.state
ORDER BY total_spent DESC
LIMIT 10;
```

### 4. Product Performance
```sql
SELECT 
    p.product_name,
    c.category_name,
    SUM(f.quantity) as total_quantity_sold,
    SUM(f.subtotal) as total_revenue,
    AVG(f.unit_price) as avg_price
FROM ecommerce_data.gold.fato_vendas f
JOIN ecommerce_data.gold.dim_product p ON f.product_key = p.product_key
JOIN ecommerce_data.gold.dim_category c ON p.category_id = c.category_id
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

## Benefits of Star Schema

1. **Simplified Queries**: Easy to understand and write
2. **Performance**: Optimized for analytical queries with denormalized dimensions
3. **Flexibility**: Easy to add new measures or dimensions
4. **Business-Friendly**: Aligns with how business users think about data
5. **Scalability**: Efficient for large fact tables with small dimension tables

