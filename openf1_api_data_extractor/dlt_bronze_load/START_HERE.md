# 👋 Welcome to DLT Bronze Layer!

## You're in the right place!

This module loads your extracted F1 data into Databricks Delta tables (bronze layer).

---

## 🚀 Quick Start - Choose Your Approach

### Approach A: Local Files (Simpler, Development)

**Step 1:** Extract Data (If Not Done)
```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/src/ingestion
python databricks_extractor.py --mode latest --num-sessions 10
```

**Step 2:** Load to Bronze
```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/dlt_bronze_load
python run_batch_load.py
```

**Step 3:** Verify
```sql
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions LIMIT 10;
```

### Approach B: Volume-Based (Recommended, Production)

**Step 1:** Upload data to Databricks volume

**Step 2:** Load from volume
```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/dlt_bronze_load

# Create NEW tables (recommended - keeps existing tables safe)
python load_from_volume.py --separate-tables
```

**Step 3:** Verify
```sql
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions_v2 LIMIT 10;
```

📚 **See [VOLUME_LOADING_GUIDE.md](VOLUME_LOADING_GUIDE.md) for complete volume-based setup**

**✅ Done! Your bronze tables are ready for dbt transformations!**

---

## 📋 What Just Happened?

```
1. Script read JSON files from local_test_data/
2. Combined data from multiple files
3. Loaded to 4 Delta tables:
   ✅ bronze_sessions   (session metadata)
   ✅ bronze_drivers    (driver information)
   ✅ bronze_laps       (lap timing data)
   ✅ bronze_locations  (GPS telemetry)
```

---

## 📚 Documentation

| Read This | If You Want To |
|-----------|----------------|
| **[README.md](README.md)** | Complete guide with troubleshooting |
| **[QUICKSTART.md](QUICKSTART.md)** | 5-minute step-by-step guide |
| **[BRONZE_VERIFICATION_QUERIES.sql](BRONZE_VERIFICATION_QUERIES.sql)** | SQL queries to verify data |
| **[INDEX.md](INDEX.md)** | Navigate all documentation |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Technical architecture details |

---

## 🎯 Expected Results

### For 15 Sessions:
```
bronze_sessions:   15 records
bronze_drivers:    300 records
bronze_laps:       ~7,747 records
bronze_locations:  ~6,097,599 records
```

### For 123 Sessions (Full 2024):
```
bronze_sessions:   123 records
bronze_drivers:    ~2,460 records
bronze_laps:       ~110,000 records
bronze_locations:  ~50,000,000 records
```

---

## ✅ Verification Queries

### Quick Health Check
```sql
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

### View Sample Data
```sql
-- Latest sessions
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
ORDER BY date_start DESC LIMIT 10;

-- Drivers from latest session
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
WHERE session_key = (SELECT MAX(session_key) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions)
ORDER BY driver_number;
```

---

## 🔧 File Overview

### Main Files You'll Use:
- **`run_batch_load.py`** - Run this to load data! ⭐
- **`config.py`** - Edit paths and settings here
- **`BRONZE_VERIFICATION_QUERIES.sql`** - Use these to verify data

### Supporting Files:
- **`load_to_bronze.py`** - Core loading logic
- **`schemas.py`** - Data schemas
- **`table_definitions.py`** - For DLT streaming (advanced)

---

## 🆘 Common Issues

### "Data directory not found"
```bash
# Check if data exists
ls ~/Documents/openf1_data_analytics/local_test_data/*.json

# If empty, run extraction first
cd ../src/ingestion
python databricks_extractor.py --mode latest --num-sessions 5
```

### "Module not found: databricks.connect"
```bash
# Activate virtual environment
source .venv/bin/activate
```

### "No location files"
Location data needs to be extracted first. See: `../LOCATION_FIX_SUMMARY.md`

---

## 🎉 What's Next?

After bronze tables are loaded:

### Run dbt Transformations
```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics
dbt build
```

This creates:
- **Silver layer** - Cleaned and deduplicated data
- **Gold layer** - Business-ready analytics tables

---

## 💡 Key Features

✅ **Automatic batching** - Handles large datasets (6M+ location records)
✅ **Retry logic** - Recovers from concurrent update errors
✅ **Progress tracking** - See real-time batch progress
✅ **Schema evolution** - Handles data structure changes
✅ **Delta tables** - ACID transactions, time travel
✅ **Metadata tracking** - `_ingestion_timestamp` on all records

---

## 📊 Tables Created

```
dev_f1_data_analytics.bronze_raw/
├── bronze_sessions     Session metadata (dates, locations, circuits)
├── bronze_drivers      Driver info (names, teams, numbers)
├── bronze_laps         Lap timing (sector times, lap durations)
└── bronze_locations    GPS telemetry (x, y, z coordinates)
```

---

## ⏱️ How Long Does It Take?

- **Sessions, Drivers, Laps:** 30-60 seconds
- **Locations (15 sessions):** 4-6 minutes
- **Locations (123 sessions):** 30-40 minutes

**Total for 15 sessions:** ~5-7 minutes
**Total for 123 sessions:** ~35-45 minutes

---

## 🔍 Data Flow

```
Extract          Load            Transform        Analyze
───────         ─────           ──────────       ────────
OpenF1 API  →   JSON    →   Bronze Tables  →   dbt    →   Gold Layer
               (Local)      (Delta in DB)      Silver      (Analytics)
```

**You are here: Load to Bronze** ⬆️

---

## 📖 Need More Help?

1. **Read [README.md](README.md)** - Comprehensive guide
2. **Check [QUICKSTART.md](QUICKSTART.md)** - Step-by-step walkthrough
3. **Use [BRONZE_VERIFICATION_QUERIES.sql](BRONZE_VERIFICATION_QUERIES.sql)** - Verify your data
4. **Review troubleshooting section** in README.md

---

**Ready?** Run `python run_batch_load.py` and watch the magic happen! ✨

---

**Pro Tip:** Keep this terminal open to see progress. The script shows batch-by-batch updates for locations (61 batches of 100K records each).
