# DLT Bronze Layer - Data Loading Pipeline

## 🎯 Purpose

This module loads extracted F1 data from JSON files into Databricks Delta tables (bronze layer). It's the bridge between your raw data extraction and dbt transformations.

---

## 📋 Table of Contents

1. [Quick Start](#quick-start)
2. [What This Does](#what-this-does)
3. [File Structure](#file-structure)
4. [How to Use](#how-to-use)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)

---

## 🚀 Quick Start

### Prerequisites
```bash
# 1. Ensure you have data from extraction
ls ~/Documents/openf1_data_analytics/local_test_data/*.json

# 2. Set Databricks credentials
export DBT_DATABRICKS_TOKEN=your_token
export DBT_DATABRICKS_HOST=your_host.cloud.databricks.com

# 3. Activate virtual environment
cd ~/Documents/openf1_data_analytics
source .venv/bin/activate
```

### Run Bronze Load
```bash
cd openf1_api_data_extractor/dlt_bronze_load
python run_batch_load.py
```

**That's it!** Your bronze tables will be ready in 5-10 minutes.

---

## 🔍 What This Does

### Input
- JSON files from `databricks_extractor.py` in `local_test_data/`:
  - `sessions_*.json` (session metadata)
  - `drivers_*.json` (driver information)
  - `laps_*.json` (lap timing data)
  - `locations_*.json` (GPS telemetry data)

### Process
1. **Reads** all JSON files from local directory
2. **Combines** data from multiple files per entity
3. **Transforms** to Spark DataFrames
4. **Loads** to Delta tables in Databricks
5. **Verifies** record counts

### Output
- Delta tables in `dev_f1_data_analytics.bronze_raw`:
  - `bronze_sessions`
  - `bronze_drivers`
  - `bronze_laps`
  - `bronze_locations`

---

## 📁 File Structure

```
dlt_bronze_load/
├── README.md                          ← You are here
├── config.py                          ← Configuration (paths, catalogs)
├── schemas.py                         ← Data schemas (for DLT streaming)
├── table_definitions.py               ← DLT table definitions (for streaming)
├── load_to_bronze.py                  ← Core loading logic
├── run_batch_load.py                  ← Main execution script
├── BRONZE_VERIFICATION_QUERIES.sql    ← SQL queries for verification
├── QUICKSTART.md                      ← 5-minute getting started guide
├── START_HERE.md                      ← First-time user guide
├── INDEX.md                           ← Documentation index
└── ARCHITECTURE.md                    ← Technical architecture details
```

---

## 📄 File Descriptions

### 🔧 Core Files

#### `config.py`
**Purpose:** Central configuration for all bronze layer operations

**What it contains:**
- Databricks catalog and schema names
- File paths (local and Databricks volumes)
- Table properties and optimization settings
- Batch processing configuration

**Key settings:**
```python
BRONZE_CATALOG = "dev_f1_data_analytics"
BRONZE_SCHEMA = "bronze_raw"
LOCAL_DATA_DIR = "~/Documents/openf1_data_analytics/local_test_data"
BATCH_SIZE = 100000  # Records per batch for locations
```

**When to edit:**
- Changing output catalog/schema
- Using different local data directory
- Adjusting batch size for performance

---

#### `load_to_bronze.py`
**Purpose:** Core loading logic with batch processing and error handling

**What it does:**
- Loads JSON files and combines data
- Converts to pandas/Spark DataFrames
- Writes to Delta tables with retry logic
- Handles large datasets with batching (locations table)

**Key functions:**
```python
class BronzeLoader:
    def _load_entity_with_pandas(entity)      # Load sessions/drivers/laps
    def load_locations_batched()              # Load locations with batching
    def load_all_entities_pandas()            # Load all entities
    def verify_tables()                       # Verify tables created
```

**Features:**
- ✅ Automatic retry on concurrent update errors
- ✅ Batched loading for large datasets (locations)
- ✅ Schema evolution support
- ✅ Progress tracking
- ✅ Ingestion timestamp metadata

**When you need it:**
- Understanding how data is loaded
- Customizing load logic
- Debugging load issues
- Extending to new entities

---

#### `run_batch_load.py`
**Purpose:** Main execution script - this is what you run!

**What it does:**
1. Checks data directory exists
2. Counts available JSON files
3. Initializes Databricks connection
4. Loads all entities (sessions, drivers, laps, locations)
5. Verifies tables were created successfully
6. Displays record counts

**Usage:**
```bash
python run_batch_load.py
```

**Output example:**
```
============================================================
Bronze Layer Batch Load
============================================================

Data Directory: ~/Documents/openf1_data_analytics/local_test_data
Found 60 JSON files

  sessions: 15 files
  drivers: 15 files
  laps: 15 files
  locations: 15 files

Initializing Bronze Loader...
Creating schema dev_f1_data_analytics.bronze_raw...

Loading bronze_sessions table...
  ✅ Created with 15 records

Loading bronze_drivers table...
  ✅ Created with 300 records

Loading bronze_laps table...
  ✅ Created with 7,747 records

Loading bronze_locations table (batched)...
  Loaded batch 1/61: 100000 records
  ...
  Loaded batch 61/61: 97599 records
  ✅ Created with 6,097,599 records (100% complete)

============================================================
Bronze tables loaded successfully!
============================================================
```

**When to run:**
- After extracting data with `databricks_extractor.py`
- When you have new data files to load
- To reload/refresh bronze tables

---

### 📊 Supporting Files

#### `schemas.py`
**Purpose:** Define data schemas for DLT streaming pipeline

**What it contains:**
- Column definitions for each entity
- Data types and nullability
- Used by DLT streaming (table_definitions.py)

**When you need it:**
- Using DLT streaming instead of batch
- Adding new columns
- Schema evolution

---

#### `table_definitions.py`
**Purpose:** DLT (Delta Live Tables) definitions for streaming ingestion

**What it does:**
- Defines streaming tables for production
- Uses Auto Loader (CloudFiles) for incremental loads
- Alternative to batch loading with `run_batch_load.py`

**When to use:**
- Production environment with continuous data flow
- Want automatic incremental processing
- Need to process files as they arrive

**How to use:**
```python
# In Databricks DLT pipeline
# Point to this file as your pipeline definition
# DLT will create and manage tables automatically
```

---

#### `BRONZE_VERIFICATION_QUERIES.sql`
**Purpose:** Comprehensive SQL queries to verify bronze layer data

**What it contains:**
- Table existence checks
- Record count validation
- Sample data queries
- Data quality checks
- Relationship validation

**Sections:**
1. Table existence & structure
2. Record counts
3. Sample data
4. Data quality checks
5. Data distribution & statistics
6. Comprehensive summary
7. Relationship validation
8. Performance checks
9. Quick validation (run this first!)

**How to use:**
```sql
-- In Databricks SQL Editor
-- Run quick health check first
SELECT * FROM (
  SELECT 'bronze_sessions' as table_name, COUNT(*) as count
  FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
  UNION ALL
  SELECT 'bronze_drivers', COUNT(*)
  FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
  -- ... etc
);
```

---

### 📚 Documentation Files

#### `START_HERE.md`
- First document to read
- 3-step quick start
- Links to other documentation

#### `QUICKSTART.md`
- 5-minute getting started guide
- Step-by-step instructions
- Common issues and solutions

#### `INDEX.md`
- Documentation navigation
- Organized by use case
- Links to all documents

#### `ARCHITECTURE.md`
- Technical architecture details
- Design decisions
- Data flow diagrams

---

## 🎬 How to Use

### Step 1: Extract Data (If Not Already Done)

```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/src/ingestion

# Extract latest sessions
python databricks_extractor.py --mode latest --num-sessions 10

# Or extract full year
python databricks_extractor.py --mode year --year 2024
```

**Result:** JSON files in `~/Documents/openf1_data_analytics/local_test_data/`

---

### Step 2: Load to Bronze Layer

```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/dlt_bronze_load

# Run the batch load
python run_batch_load.py
```

**What happens:**
1. Connects to Databricks
2. Creates `dev_f1_data_analytics.bronze_raw` schema (if not exists)
3. Loads sessions, drivers, laps, locations tables
4. Displays progress and results

**Duration:** 5-10 minutes (depending on data volume)

---

### Step 3: Verify Bronze Tables

**Option A: Using SQL (Databricks SQL Editor)**

```sql
-- Quick health check
SELECT 'bronze_sessions' as table_name, COUNT(*) as count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT 'bronze_drivers', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
UNION ALL
SELECT 'bronze_laps', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_laps
UNION ALL
SELECT 'bronze_locations', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_locations;
```

**Expected results (for 15 sessions):**
- bronze_sessions: 15
- bronze_drivers: 300
- bronze_laps: ~7,747
- bronze_locations: ~6,097,599

**Option B: Using provided SQL file**

1. Open `BRONZE_VERIFICATION_QUERIES.sql`
2. Copy queries to Databricks SQL Editor
3. Run section by section

---

### Step 4: Proceed to dbt

Once bronze tables are verified:

```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics
dbt build
```

This will create silver and gold layer tables using dbt transformations.

---

## ✅ Verification Checklist

### After Running `run_batch_load.py`:

- [ ] Script completed without errors
- [ ] Saw "Bronze tables loaded successfully!" message
- [ ] All 4 tables show record counts in output
- [ ] Locations table shows "100% complete"

### In Databricks SQL Editor:

- [ ] All 4 bronze tables exist
- [ ] Record counts match expected values
- [ ] Sample data looks correct
- [ ] No orphaned records (relationships intact)
- [ ] `_ingestion_timestamp` column populated

### Quick Verification SQL:

```sql
-- 1. Check tables exist
SHOW TABLES IN dev_f1_data_analytics.bronze_raw;

-- 2. Check record counts
SELECT
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions) as sessions,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_drivers) as drivers,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_laps) as laps,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_locations) as locations;

-- 3. View sample data
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions LIMIT 10;
```

---

## 🔧 Troubleshooting

### Issue: "Data directory not found"

**Error:**
```
Error: Data directory not found: /path/to/local_test_data
```

**Solution:**
```bash
# Check if data directory exists
ls ~/Documents/openf1_data_analytics/local_test_data/

# If empty, run extraction first
cd ../src/ingestion
python databricks_extractor.py --mode latest --num-sessions 5
```

---

### Issue: "databricks.connect module not found"

**Solution:**
```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Install databricks-connect if missing
pip install "databricks-connect==17.2.*"
```

---

### Issue: "Schema does not exist"

**Solution:**
The script creates the schema automatically. If it fails:

```sql
-- Manually create schema in Databricks
CREATE SCHEMA IF NOT EXISTS dev_f1_data_analytics.bronze_raw;
```

---

### Issue: "Concurrent update error" / "MetadataChangedException"

**Status:** ✅ FIXED (retry logic added)

The script now automatically retries with exponential backoff. If you still see issues:

```bash
# Just rerun the script - it will work
python run_batch_load.py
```

---

### Issue: "Table already exists"

**Solution:**
The script uses `overwrite` mode, so existing tables are replaced. This is intentional and safe.

If you want to preserve existing data, modify `load_to_bronze.py`:
```python
# Change mode from 'overwrite' to 'append'
.mode("append")  # Instead of .mode("overwrite")
```

---

### Issue: "Out of memory"

**Solution:**
The batch size is already optimized (100K records per batch). If still having issues:

Edit `config.py`:
```python
BATCH_SIZE = 50000  # Reduce from 100000
```

---

### Issue: "No location files found"

**Cause:** Location data not extracted yet

**Solution:**
```bash
# Extract location data
cd ../src/ingestion
python databricks_extractor.py --mode year --year 2024

# Then rerun bronze load
cd ../dlt_bronze_load
python run_batch_load.py
```

---

## 📊 Expected Data Volumes

### For 15 Sessions (Typical):
- **Sessions:** 15 records (~1 KB)
- **Drivers:** 300 records (~5 KB)
- **Laps:** ~7,747 records (~200 KB)
- **Locations:** ~6,097,599 records (~500 MB in Delta format)

### For 123 Sessions (Full 2024):
- **Sessions:** 123 records (~10 KB)
- **Drivers:** ~2,460 records (~50 KB)
- **Laps:** ~110,000 records (~2 MB)
- **Locations:** ~50,000,000 records (~4 GB in Delta format)

---

## ⚡ Performance Tips

### 1. Batch Size
```python
# In config.py
BATCH_SIZE = 100000  # Good for most cases
BATCH_SIZE = 50000   # If memory issues
BATCH_SIZE = 200000  # If you have lots of memory
```

### 2. Parallel Processing
Currently processes entities sequentially. For faster loading, you could parallelize sessions/drivers/laps (but locations batching already optimizes this).

### 3. Delta Optimization
Tables auto-optimize with these settings in `config.py`:
```python
"delta.autoOptimize.optimizeWrite": "true"
"delta.autoOptimize.autoCompact": "true"
```

---

## 🔄 Data Flow

```
┌─────────────────────────────────────────────┐
│  Step 1: Extract Data                       │
│  databricks_extractor.py                    │
│  ↓ Creates JSON files                       │
└─────────────────────────────────────────────┘
             ↓
┌─────────────────────────────────────────────┐
│  Local Storage                              │
│  ~/Documents/.../local_test_data/           │
│  ├── sessions_*.json                        │
│  ├── drivers_*.json                         │
│  ├── laps_*.json                            │
│  └── locations_*.json                       │
└─────────────────────────────────────────────┘
             ↓
┌─────────────────────────────────────────────┐
│  Step 2: Load to Bronze (THIS MODULE)       │
│  run_batch_load.py                          │
│  ↓ Uses load_to_bronze.py                   │
└─────────────────────────────────────────────┘
             ↓
┌─────────────────────────────────────────────┐
│  Bronze Layer (Delta Tables)                │
│  dev_f1_data_analytics.bronze_raw           │
│  ├── bronze_sessions                        │
│  ├── bronze_drivers                         │
│  ├── bronze_laps                            │
│  └── bronze_locations                       │
└─────────────────────────────────────────────┘
             ↓
┌─────────────────────────────────────────────┐
│  Step 3: Transform with dbt                 │
│  dbt build                                  │
│  ↓ Creates silver and gold layers           │
└─────────────────────────────────────────────┘
```

---

## 🎯 Next Steps

After successful bronze load:

1. **Verify tables** using SQL queries
2. **Run dbt transformations:**
   ```bash
   cd ../../dbt_f1_data_analytics
   dbt build
   ```
3. **Create visualizations** from gold layer tables
4. **Set up incremental loads** for new data

---

## 📚 Additional Resources

- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute getting started guide
- **[BRONZE_VERIFICATION_QUERIES.sql](BRONZE_VERIFICATION_QUERIES.sql)** - SQL verification queries
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Technical architecture details
- **[INDEX.md](INDEX.md)** - Documentation index

---

## 🆘 Support

If you encounter issues:

1. Check this README's troubleshooting section
2. Review QUICKSTART.md for common issues
3. Verify Databricks connection and permissions
4. Check that data files exist in local_test_data/
5. Ensure virtual environment is activated

---

## 📝 Summary

**Purpose:** Load extracted F1 data JSON files into Databricks Delta bronze tables

**Main Script:** `run_batch_load.py`

**Input:** JSON files from `databricks_extractor.py`

**Output:** 4 Delta tables in `dev_f1_data_analytics.bronze_raw`

**Duration:** 5-10 minutes

**Next Step:** Run `dbt build` to create silver and gold layers

---

**Version:** 1.0.0
**Last Updated:** 2025-11-04
**Status:** ✅ Production Ready
