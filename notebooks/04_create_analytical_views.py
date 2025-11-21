# Databricks notebook source
# MAGIC %md
# MAGIC # Create Analytical Views - Gold Layer
# MAGIC
# MAGIC This notebook creates SQL views on top of the Gold layer star schema to simplify analytical queries.
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Views are created in the `gold` schema for:
# MAGIC - Sales aggregations
# MAGIC - Customer analytics
# MAGIC - Product performance
# MAGIC - Category analysis
# MAGIC - Time-based trends
# MAGIC - Business metrics
# MAGIC
# MAGIC All views use current records from SCD Type 2 dimensions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Unity Catalog configuration
CATALOG_NAME = "ecommerce_data"
GOLD_SCHEMA = "gold"

# Set catalog and schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

print(f"✅ Using catalog: {CATALOG_NAME}")
print(f"✅ Using schema: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function to Create Views

# COMMAND ----------

def create_view(view_name, sql_query, description=""):
    """
    Create or replace a view in the gold schema
    
    Parameters:
    - view_name: Name of the view
    - sql_query: SQL query for the view
    - description: Optional description
    """
    full_view_name = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{view_name}"
    
    # Create or replace view
    spark.sql(f"CREATE OR REPLACE VIEW {full_view_name} AS {sql_query}")
    
    print(f"✅ View created: {full_view_name}")
    if description:
        print(f"   {description}")
    return full_view_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sales Summary View

# COMMAND ----------

create_view(
    "v_sales_summary",
    """
    SELECT 
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(f.item_id) as total_items,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value,
        SUM(f.calculated_subtotal) as calculated_total_revenue
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Overall sales summary with key metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sales by Category View

# COMMAND ----------

create_view(
    "v_sales_by_category",
    """
    SELECT 
        c.category_name,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(f.item_id) as total_items,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.unit_price) as avg_unit_price,
        AVG(f.subtotal) as avg_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as revenue_per_order
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_category c 
        ON f.category_key = c.category_key
    GROUP BY c.category_name
    ORDER BY total_revenue DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Sales performance by product category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sales by Month View

# COMMAND ----------

create_view(
    "v_sales_by_month",
    """
    SELECT 
        t.year,
        t.month,
        t.month_name,
        t.year_month,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as avg_order_value
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY t.year, t.month, t.month_name, t.year_month
    ORDER BY t.year DESC, t.month DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Monthly sales trends and metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sales by Quarter View

# COMMAND ----------

create_view(
    "v_sales_by_quarter",
    """
    SELECT 
        t.year,
        t.quarter,
        t.year_quarter,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as avg_order_value
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY t.year, t.quarter, t.year_quarter
    ORDER BY t.year DESC, t.quarter DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Quarterly sales performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top Customers View

# COMMAND ----------

create_view(
    "v_top_customers",
    """
    SELECT 
        c.customer_id,
        c.full_name,
        c.email,
        c.city,
        c.state,
        c.age,
        COUNT(DISTINCT f.order_id) as total_orders,
        SUM(f.quantity) as total_items_purchased,
        SUM(f.subtotal) as total_spent,
        AVG(f.subtotal) as avg_order_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as avg_order_value,
        MAX(t.full_date) as last_order_date,
        MIN(t.full_date) as first_order_date
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_customer c 
        ON f.customer_key = c.customer_key
        AND c.is_current = 1
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY c.customer_id, c.full_name, c.email, c.city, c.state, c.age
    ORDER BY total_spent DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Top customers by total spending (using current customer records)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Top Products View

# COMMAND ----------

create_view(
    "v_top_products",
    """
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        p.price as current_price,
        p.stock_status,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(f.item_id) as times_ordered,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.unit_price) as avg_selling_price,
        AVG(f.quantity) as avg_quantity_per_order
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_product p 
        ON f.product_key = p.product_key
        AND p.is_current = 1
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_category c 
        ON f.category_key = c.category_key
    GROUP BY p.product_id, p.product_name, c.category_name, p.price, p.stock_status
    ORDER BY total_revenue DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Top products by revenue (using current product records)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Customer Order History View

# COMMAND ----------

create_view(
    "v_customer_order_history",
    """
    SELECT 
        c.customer_id,
        c.full_name,
        c.email,
        c.city,
        c.state,
        f.order_id,
        o.status as order_status,
        o.payment_type,
        t.full_date as order_date,
        t.year_month,
        COUNT(f.item_id) as items_in_order,
        SUM(f.quantity) as total_quantity,
        SUM(f.subtotal) as order_total
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_customer c 
        ON f.customer_key = c.customer_key
        AND c.is_current = 1
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_order o 
        ON f.order_key = o.order_key
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY 
        c.customer_id, c.full_name, c.email, c.city, c.state,
        f.order_id, o.status, o.payment_type, t.full_date, t.year_month
    ORDER BY c.customer_id, t.full_date DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Detailed order history per customer"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Product Sales Trend View

# COMMAND ----------

create_view(
    "v_product_sales_trend",
    """
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        t.year_month,
        COUNT(DISTINCT f.order_id) as orders_count,
        SUM(f.quantity) as quantity_sold,
        SUM(f.subtotal) as revenue,
        AVG(f.unit_price) as avg_selling_price
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_product p 
        ON f.product_key = p.product_key
        AND p.is_current = 1
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_category c 
        ON f.category_key = c.category_key
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY 
        p.product_id, p.product_name, c.category_name, t.year_month
    ORDER BY p.product_id, t.year_month DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Monthly sales trends per product"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Daily Sales View

# MAGIC %md
# MAGIC ## 9. Daily Sales View

# COMMAND ----------

create_view(
    "v_daily_sales",
    """
    SELECT 
        t.full_date,
        t.year,
        t.month,
        t.day_of_month,
        t.day_name,
        t.is_weekend,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        COUNT(f.item_id) as total_items,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY 
        t.full_date, t.year, t.month, t.day_of_month, 
        t.day_name, t.is_weekend
    ORDER BY t.full_date DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Daily sales metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Category Performance View

# COMMAND ----------

create_view(
    "v_category_performance",
    """
    SELECT 
        c.category_id,
        c.category_name,
        COUNT(DISTINCT p.product_id) as total_products,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(f.item_id) as total_items_sold,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.unit_price) as avg_unit_price,
        AVG(f.subtotal) as avg_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as revenue_per_order,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT p.product_id), 0) as revenue_per_product
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_category c 
        ON f.category_key = c.category_key
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_product p 
        ON f.product_key = p.product_key
        AND p.is_current = 1
    GROUP BY c.category_id, c.category_name
    ORDER BY total_revenue DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Comprehensive category performance metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Order Status Summary View

# COMMAND ----------

create_view(
    "v_order_status_summary",
    """
    SELECT 
        o.status,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_quantity,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_order o 
        ON f.order_key = o.order_key
    GROUP BY o.status
    ORDER BY total_orders DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Sales summary by order status"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Payment Type Analysis View

# COMMAND ----------

create_view(
    "v_payment_type_analysis",
    """
    SELECT 
        o.payment_type,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value,
        SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0) as avg_order_value,
        COUNT(DISTINCT f.order_id) * 100.0 / NULLIF(
            (SELECT COUNT(DISTINCT order_id) FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas), 0
        ) as percentage_of_orders
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_order o 
        ON f.order_key = o.order_key
    GROUP BY o.payment_type
    ORDER BY total_revenue DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Payment method analysis and preferences"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Customer Segmentation View

# COMMAND ----------

create_view(
    "v_customer_segmentation",
    """
    SELECT 
        c.customer_id,
        c.full_name,
        c.city,
        c.state,
        c.age,
        CASE 
            WHEN c.age < 30 THEN 'Young (18-29)'
            WHEN c.age < 50 THEN 'Middle-aged (30-49)'
            WHEN c.age < 70 THEN 'Mature (50-69)'
            ELSE 'Senior (70+)'
        END as age_segment,
        COUNT(DISTINCT f.order_id) as total_orders,
        SUM(f.subtotal) as total_spent,
        AVG(f.subtotal) as avg_order_item_value,
        CASE 
            WHEN SUM(f.subtotal) >= 5000 THEN 'VIP'
            WHEN SUM(f.subtotal) >= 2000 THEN 'Premium'
            WHEN SUM(f.subtotal) >= 500 THEN 'Regular'
            ELSE 'New'
        END as customer_tier
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_customer c 
        ON f.customer_key = c.customer_key
        AND c.is_current = 1
    GROUP BY 
        c.customer_id, c.full_name, c.city, c.state, c.age
    ORDER BY total_spent DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Customer segmentation by age and spending"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Weekly Sales View

# COMMAND ----------

create_view(
    "v_weekly_sales",
    """
    SELECT 
        t.year,
        t.week,
        MIN(t.full_date) as week_start_date,
        MAX(t.full_date) as week_end_date,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.subtotal) as avg_order_item_value
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_time t 
        ON f.date_key = t.date_key
    GROUP BY t.year, t.week
    ORDER BY t.year DESC, t.week DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Weekly sales aggregation"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Product Category Matrix View

# COMMAND ----------

create_view(
    "v_product_category_matrix",
    """
    SELECT 
        c.category_name,
        p.product_name,
        p.price as current_price,
        COUNT(DISTINCT f.order_id) as order_count,
        SUM(f.quantity) as total_quantity_sold,
        SUM(f.subtotal) as total_revenue,
        AVG(f.unit_price) as avg_selling_price
    FROM {CATALOG_NAME}.{GOLD_SCHEMA}.fato_vendas f
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_category c 
        ON f.category_key = c.category_key
    JOIN {CATALOG_NAME}.{GOLD_SCHEMA}.dim_product p 
        ON f.product_key = p.product_key
        AND p.is_current = 1
    GROUP BY c.category_name, p.product_name, p.price
    ORDER BY c.category_name, total_revenue DESC
    """.format(CATALOG_NAME=CATALOG_NAME),
    "Product performance within each category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ ANALYTICAL VIEWS CREATED SUCCESSFULLY!")
print("="*60)

# List all views
views = [
    "v_sales_summary",
    "v_sales_by_category",
    "v_sales_by_month",
    "v_sales_by_quarter",
    "v_top_customers",
    "v_top_products",
    "v_customer_order_history",
    "v_product_sales_trend",
    "v_daily_sales",
    "v_category_performance",
    "v_order_status_summary",
    "v_payment_type_analysis",
    "v_customer_segmentation",
    "v_weekly_sales",
    "v_product_category_matrix"
]

print(f"\n📊 Created {len(views)} analytical views:")
print(f"\n{'View Name':<35} {'Description'}")
print("-" * 80)

view_descriptions = {
    "v_sales_summary": "Overall sales summary with key metrics",
    "v_sales_by_category": "Sales performance by product category",
    "v_sales_by_month": "Monthly sales trends and metrics",
    "v_sales_by_quarter": "Quarterly sales performance",
    "v_top_customers": "Top customers by total spending",
    "v_top_products": "Top products by revenue",
    "v_customer_order_history": "Detailed order history per customer",
    "v_product_sales_trend": "Monthly sales trends per product",
    "v_daily_sales": "Daily sales metrics",
    "v_category_performance": "Comprehensive category performance metrics",
    "v_order_status_summary": "Sales summary by order status",
    "v_payment_type_analysis": "Payment method analysis and preferences",
    "v_customer_segmentation": "Customer segmentation by age and spending",
    "v_weekly_sales": "Weekly sales aggregation",
    "v_product_category_matrix": "Product performance within each category"
}

for view in views:
    desc = view_descriptions.get(view, "")
    print(f"{view:<35} {desc}")

print(f"\n📦 All views are in: {CATALOG_NAME}.{GOLD_SCHEMA}.*")
print(f"\n💡 Usage Example:")
print(f"   SELECT * FROM {CATALOG_NAME}.{GOLD_SCHEMA}.v_top_customers LIMIT 10;")
print(f"\n📈 Ready for analytics and reporting!")
print("\n" + "="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Queries

# MAGIC %md
# MAGIC ### Top 10 Customers
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM ecommerce_data.gold.v_top_customers 
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC ### Sales by Category (Last 3 Months)
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM ecommerce_data.gold.v_sales_by_category
# MAGIC ORDER BY total_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC ### Monthly Sales Trend
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM ecommerce_data.gold.v_sales_by_month
# MAGIC ORDER BY year DESC, month DESC
# MAGIC LIMIT 12;
# MAGIC ```
# MAGIC
# MAGIC ### VIP Customers
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM ecommerce_data.gold.v_customer_segmentation
# MAGIC WHERE customer_tier = 'VIP'
# MAGIC ORDER BY total_spent DESC;
# MAGIC ```

