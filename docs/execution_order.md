# Execution Order - Medallion Architecture Pipeline

This document describes the execution order of notebooks for both initial setup and daily scheduled runs.

## 📋 Notebooks Overview

| Notebook | Purpose | Frequency | Mode |
|----------|---------|-----------|------|
| `00_setup_unity_catalog.py` | Create catalog and schemas | **One-time** | Setup |
| `01_generate_bronze_data.py` | Generate initial bronze data | **One-time** | Setup |
| `01b_incremental_bronze_data.py` | Generate incremental bronze data | **Daily** | Scheduled |
| `02_silver_layer_cleaning.py` | Clean and validate data | **Daily** | Scheduled |
| `03_gold_layer_star_schema.py` | Build star schema | **Daily** | Scheduled |

---

## 🚀 Initial Setup (One-Time)

Execute these notebooks **once** to set up the environment:

### Step 1: Setup Unity Catalog
```
00_setup_unity_catalog.py
```
- Creates catalog: `ecommerce_data`
- Creates schemas: `bronze`, `silver`, `gold`
- **Run once** before any data generation

### Step 2: Generate Initial Bronze Data
```
01_generate_bronze_data.py
```
- Generates initial datasets (1000 customers, 250 products, 2500 orders, etc.)
- Saves to `ecommerce_data.bronze.*`
- **Run once** to populate initial data

---

## 📅 Daily Scheduled Pipeline

After initial setup, schedule these notebooks to run **daily** in the following order:

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────┐
│  Daily Scheduled Pipeline (Run in this order)          │
└─────────────────────────────────────────────────────────┘

1. 01b_incremental_bronze_data.py
   │
   ├─► Reads existing bronze data
   ├─► Generates new incremental data
   └─► Appends to bronze layer (customers, products, orders, order_items)
   
2. 02_silver_layer_cleaning.py
   │
   ├─► Reads from bronze layer
   ├─► Cleans and validates data
   ├─► Removes duplicates, standardizes formats
   └─► Saves to silver layer
   
3. 03_gold_layer_star_schema.py
   │
   ├─► Reads from silver layer
   ├─► Builds dimension tables (SCD Type 2)
   ├─► Creates fact table (fato_vendas)
   └─► Saves to gold layer (star schema)
```

### Detailed Execution Order

#### 1️⃣ **01b_incremental_bronze_data.py** (Bronze - Incremental Load)
- **When:** Daily (e.g., 00:00 UTC)
- **What it does:**
  - Reads existing bronze tables to get max IDs
  - Generates new customers, products, orders, order_items
  - Appends new data to bronze layer (maintains referential integrity)
  - Includes data quality issues (duplicates, nulls, format inconsistencies)
- **Output:** New records in `ecommerce_data.bronze.*` tables
- **Dependencies:** None (can run independently)

#### 2️⃣ **02_silver_layer_cleaning.py** (Silver - Data Cleaning)
- **When:** Daily (e.g., 01:00 UTC) - **After bronze incremental load**
- **What it does:**
  - Reads ALL data from bronze layer (including new incremental data)
  - Removes duplicates
  - Standardizes string formats (trim, case)
  - Parses dates (handles multiple formats)
  - Converts types (strings to numbers)
  - Validates referential integrity
  - Saves cleaned data to silver layer
- **Output:** Cleaned data in `ecommerce_data.silver.*` tables
- **Dependencies:** Requires `01b_incremental_bronze_data.py` to complete first

#### 3️⃣ **03_gold_layer_star_schema.py** (Gold - Business Layer)
- **When:** Daily (e.g., 02:00 UTC) - **After silver cleaning**
- **What it does:**
  - Reads cleaned data from silver layer
  - Creates dimension tables with SCD Type 2 (customer, product)
  - Creates dimension tables with SCD Type 1 (category, order)
  - Creates time dimension
  - Builds fact table (fato_vendas) with star schema
  - Saves to gold layer
- **Output:** Star schema in `ecommerce_data.gold.*` tables
- **Dependencies:** Requires `02_silver_layer_cleaning.py` to complete first

---

## ⏰ Recommended Schedule

### Option 1: Sequential (Recommended)
Run notebooks one after another with dependencies:

```
00:00 UTC - 01b_incremental_bronze_data.py
01:00 UTC - 02_silver_layer_cleaning.py (depends on bronze)
02:00 UTC - 03_gold_layer_star_schema.py (depends on silver)
```

### Option 2: Job Chain (Databricks Jobs)
Create a Databricks Job with task dependencies:

```
Job: Daily Medallion Pipeline
├─ Task 1: 01b_incremental_bronze_data.py
│  └─ On Success → Task 2
├─ Task 2: 02_silver_layer_cleaning.py
│  └─ On Success → Task 3
└─ Task 3: 03_gold_layer_star_schema.py
```

### Option 3: Single Job with Multiple Notebooks
Create a single job that runs all notebooks sequentially:

```python
# In Databricks Jobs UI:
Notebook 1: notebooks/01b_incremental_bronze_data.py
Notebook 2: notebooks/02_silver_layer_cleaning.py
Notebook 3: notebooks/03_gold_layer_star_schema.py
```

---

## 🔄 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │customers│  │products  │  │ orders   │  │order_items│   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │              │              │          │
│       └─────────────┴──────────────┴──────────────┘          │
│                          │                                     │
│                          ▼                                     │
│             01b_incremental_bronze_data.py                    │
│                    (Daily - Append)                           │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │customers │  │products  │  │ orders   │  │order_items│  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │              │              │          │
│       └─────────────┴──────────────┴──────────────┘          │
│                          │                                     │
│                          ▼                                     │
│              02_silver_layer_cleaning.py                       │
│                    (Daily - Overwrite)                        │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Business)                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │dim_      │  │dim_      │  │dim_      │  │dim_      │   │
│  │customer  │  │product   │  │category  │  │time      │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │              │              │          │
│       └─────────────┴──────────────┴──────────────┘          │
│                          │                                     │
│                          ▼                                     │
│                   ┌──────────────┐                            │
│                   │ fato_vendas  │                            │
│                   │ (Fact Table)│                            │
│                   └──────────────┘                            │
│                          │                                     │
│                          ▼                                     │
│              03_gold_layer_star_schema.py                     │
│                    (Daily - Overwrite)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 📝 Execution Notes

### Bronze Layer (Incremental)
- **Mode:** `append` - New data is added to existing tables
- **Data Quality:** Includes issues (duplicates, nulls, format inconsistencies)
- **Frequency:** Daily (simulates continuous data ingestion)

### Silver Layer (Cleaned)
- **Mode:** `overwrite` - Rebuilds entire table from bronze
- **Data Quality:** Cleaned, validated, standardized
- **Frequency:** Daily (processes all bronze data, including new)

### Gold Layer (Business)
- **Mode:** `overwrite` - Rebuilds entire star schema
- **Data Quality:** Business-ready, star schema structure
- **Frequency:** Daily (rebuilds from silver)

---

## ⚠️ Important Considerations

1. **Execution Time:** Ensure each notebook completes before the next starts
2. **Error Handling:** Configure retry logic in Databricks Jobs
3. **Monitoring:** Set up alerts for job failures
4. **Backup:** Consider backing up gold layer before overwrite (if needed)
5. **Idempotency:** Silver and Gold layers use `overwrite` mode, so re-running is safe

---

## 🎯 Quick Start Checklist

- [ ] Run `00_setup_unity_catalog.py` (one-time)
- [ ] Run `01_generate_bronze_data.py` (one-time)
- [ ] Schedule `01b_incremental_bronze_data.py` (daily)
- [ ] Schedule `02_silver_layer_cleaning.py` (daily, after bronze)
- [ ] Schedule `03_gold_layer_star_schema.py` (daily, after silver)

---

## 📚 Related Documentation

- [README.md](../README.md) - Project overview
- [docs/star_schema_diagram.md](../docs/star_schema_diagram.md) - Star schema design

