# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog for Medallion Architecture
# MAGIC 
# MAGIC This notebook creates the Unity Catalog structure for the Medallion Architecture:
# MAGIC - **Catalog**: `ecommerce_data`
# MAGIC - **Schemas**: `bronze`, `silver`, `gold`
# MAGIC 
# MAGIC Run this notebook **once** before generating bronze data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_data
# MAGIC COMMENT 'E-commerce data catalog for Medallion Architecture practice';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the catalog
# MAGIC USE CATALOG ecommerce_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas for Each Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schemas for each layer
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Bronze layer: Raw data with quality issues';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Silver layer: Cleaned and validated data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Gold layer: Business-level aggregations and star schema';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all schemas in the catalog
# MAGIC SHOW SCHEMAS IN ecommerce_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ **Catalog created**: `ecommerce_data`
# MAGIC 
# MAGIC ✅ **Schemas created**:
# MAGIC - `ecommerce_data.bronze` - Raw data layer
# MAGIC - `ecommerce_data.silver` - Cleaned data layer
# MAGIC - `ecommerce_data.gold` - Business data layer
# MAGIC 
# MAGIC **Next step**: Run `01_generate_bronze_data.py` to populate the bronze layer.

