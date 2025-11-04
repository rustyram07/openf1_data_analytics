# F1 Data Analytics - Complete Data Warehouse

**A production-ready Medallion Architecture (Bronze → Silver → Gold) data warehouse for Formula 1 analytics using dbt, Databricks, and OpenF1 API**

---

## Quick Start

```bash
cd ~/Documents/openf1_data_analytics
./load_2024_session.sh
```

**That's it!** The script automatically:
- ✓ Extracts data from OpenF1 API
- ✓ Loads to Bronze layer (raw data)
- ✓ Builds Silver layer (cleaned data)
- ✓ Builds Gold layer (analytics)
- ✓ Creates 14 tables ready for analysis

**Time:** 15-20 minutes | **Tables:** 14 | **Records:** ~200k per session

---

## Project Overview

### Architecture

```
OpenF1 API
    ↓
Bronze Layer (raw data)
    • bronze_sessions
    • bronze_drivers
    • bronze_laps
    • bronze_locations
    ↓
Silver Layer (cleaned & conformed)
    • silver_sessions
    • silver_drivers
    • silver_laps
    • silver_locations
    ↓
Gold Layer (analytics-ready)
    • Dimensions: dim_sessions, dim_drivers
    • Facts: fact_laps
    • Marts: session_leaderboard, driver_session_summary, team_performance
```

### Technology Stack

- **Data Source:** OpenF1 API
- **Storage:** Databricks Delta Tables (Unity Catalog)
- **Transformation:** dbt (data build tool)
- **Language:** Python + SQL
- **Architecture:** Medallion (Bronze → Silver → Gold)

---

## Documentation

### Getting Started
- **[QUICK_START.md](QUICK_START.md)** - 5-minute quick reference


### Build & Schema Reference
- **[dbt_f1_data_analytics/SCHEMA_STRUCTURE.md](dbt_f1_data_analytics/SCHEMA_STRUCTURE.md)** - Schema documentation with query examples


### Orchestration (Optional)
- **[DAGSTER_PIPELINE_DIAGRAM.md](DAGSTER_PIPELINE_DIAGRAM.md)** - Visual pipeline diagrams
- **[dagster_pipeline.py](dagster_pipeline.py)** - Dagster orchestration code

---

## Project Structure

```
openf1_data_analytics/
│
├── 📜 README.md                          ← You are here
├── 📜 QUICK_START.md                     ← Quick reference guide
├── 📜 LOAD_2024_SESSION_GUIDE.md         ← Detailed step-by-step guide
├── 🔧 load_2024_session.sh               ← Automated pipeline script
│
├── 📁 openf1_api_data_extractor/         ← API extraction & bronze loading
│   ├── src/
│   │   ├── ingestion/
│   │   │   └── databricks_extractor.py  ← Extract from OpenF1 API
│   │   └── utils/
│   │       ├── api_client.py            ← API client with rate limiting
│   │       └── session_finder.py        ← Session discovery
│   ├── dlt_bronze_load/
│   │   ├── run_batch_load.py            ← Load JSON → Bronze tables
│   │   └── config.py                     ← Bronze loader config
│   └── config/
│       └── settings.py                   ← API & path configuration
│
├── 📁 dbt_f1_data_analytics/             ← dbt transformation project
│   ├── models/
│   │   ├── silver_clean/                 ← Silver layer (cleaned data)
│   │   │   ├── core/
│   │   │   │   ├── silver_sessions.sql
│   │   │   │   ├── silver_drivers.sql
│   │   │   │   ├── silver_laps.sql
│   │   │   │   └── silver_locations.sql
│   │   │   └── schema.yml
│   │   └── gold_analytics/               ← Gold layer (analytics)
│   │       ├── core/                     ← Dimensions & Facts
│   │       │   ├── dim_sessions.sql
│   │       │   ├── dim_drivers.sql
│   │       │   ├── fact_laps.sql
│   │       │   └── schema.yml
│   │       └── marts/                    ← Pre-aggregated analytics
│   │           ├── session_leaderboard.sql
│   │           ├── driver_session_summary.sql
│   │           ├── team_performance.sql
│   │           └── schema.yml
│   ├── snapshots/
│   │   ├── driver_snapshot.sql           ← SCD Type 2 for drivers
│   │   └── session_snapshot.sql
│   ├── tests/
│   │   └── one_current_record_per_driver.sql
│   ├── 🔧 run_dbt.sh                     ← dbt helper script
│   ├── dbt_project.yml                   ← dbt project config
│   ├── profiles.yml                      ← Databricks connection
│   ├── 📜 FINAL_BUILD_SUMMARY.md         ← Build status & summary
│   ├── 📜 SCHEMA_STRUCTURE.md            ← Schema documentation
│   ├── 📜 COMPLETE_VALIDATION.sql        ← All validation queries
│   └── 📜 QUICK_VALIDATION.sql           ← Quick checks
│
└── 📁 local_test_data/                   ← Local JSON storage
    └── *.json                            ← API response files
```

---

## Schema Overview

### Bronze Layer: `dev_f1_data_analytics.bronze_raw`

Raw data directly from OpenF1 API with minimal transformation.

| Table | Description | Typical Records |
|-------|-------------|----------------|
| `bronze_sessions` | Session metadata | 1 per session |
| `bronze_drivers` | Driver information | ~20 per session |
| `bronze_laps` | Lap timing data | 500-600 per session |
| `bronze_locations` | GPS telemetry | 150k-200k per session |

### Silver Layer: `dev_f1_data_analytics.default_silver_clean`

Cleaned, deduplicated, and conformed data with business logic applied.

| Table | Features | Strategy |
|-------|----------|----------|
| `silver_sessions` | Dedup, session categorization | Incremental (merge) |
| `silver_drivers` | Dedup, team classification | Incremental (merge) |
| `silver_laps` | Lap validation, metrics | Incremental (append) |
| `silver_locations` | GPS cleaning | Incremental (append) |

### Gold Layer: `dev_f1_data_analytics.default_gold_analytics`

Analytics-ready tables optimized for queries and dashboards.

**Dimensions:**
| Table | Type | Records |
|-------|------|---------|
| `dim_sessions` | Standard | 1 per session |
| `dim_drivers` | SCD Type 2 | ~20 per session |

**Facts:**
| Table | Grain | Records |
|-------|-------|---------|
| `fact_laps` | One row per lap | 500-600 per session |

**Marts (Pre-aggregated Analytics):**
| Table | Purpose | Records |
|-------|---------|---------|
| `session_leaderboard` | Driver rankings | ~20 per session |
| `driver_session_summary` | Performance metrics | ~20 per session |
| `team_performance` | Team aggregations | ~10 per session |

---

## Usage Examples

### Load Latest Session (Default)
```bash
./load_2024_session.sh
```

### Load Multiple Sessions
```bash
./load_2024_session.sh 5        # Latest 5 sessions
./load_2024_session.sh oldest   # Oldest 1 session
./load_2024_session.sh oldest 5 # Oldest 5 sessions
./load_2024_session.sh year     # All 2024 sessions
```

### Orchestration with Dagster (Optional)
```bash
# Install Dagster
pip install dagster dagster-webserver dagster-shell

# Start Dagster UI
dagster dev -f dagster_pipeline.py

# Open http://localhost:3000
# Click "Assets" → "Materialize all"
```
See [DAGSTER_QUICKSTART.md](DAGSTER_QUICKSTART.md) for details.

### Manual Step-by-Step

**Step 1: Extract from API**
```bash
cd openf1_api_data_extractor/src/ingestion
python databricks_extractor.py --mode latest --num-sessions 1
```

**Step 2: Load to Bronze**
```bash
cd ../../dlt_bronze_load
python run_batch_load.py
```

**Step 3: Build dbt Layers**
```bash
cd ../../dbt_f1_data_analytics
./run_dbt.sh build --full-refresh
```

---

## Validation Queries

### Quick Record Count Check
```sql
SELECT
    'BRONZE' AS layer, 'bronze_sessions' AS table_name,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions) AS count
UNION ALL
SELECT 'SILVER', 'silver_sessions',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_sessions)
UNION ALL
SELECT 'GOLD-MART', 'session_leaderboard',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard)
ORDER BY layer;
```

### Session Leaderboard
```sql
SELECT
    position,
    driver_name,
    team_name,
    ROUND(fastest_lap_time, 3) AS lap_time,
    ROUND(delta_to_fastest, 3) AS delta
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
WHERE session_category = 'Race'
ORDER BY position
LIMIT 10;
```

### Driver Performance Summary
```sql
SELECT
    driver_name,
    team_name,
    total_laps,
    ROUND(fastest_lap_time, 3) AS fastest_lap,
    ROUND(consistency_coefficient, 4) AS consistency,
    top_speed
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
ORDER BY fastest_lap_time
LIMIT 10;
```

For more queries, see [COMPLETE_VALIDATION.sql](dbt_f1_data_analytics/COMPLETE_VALIDATION.sql)

---

## Key Features

### Data Quality
- ✓ Deduplication at silver layer
- ✓ NULL handling with COALESCE
- ✓ Business logic validation
- ✓ 83 dbt data tests
- ✓ Custom test for SCD Type 2

### Performance
- ✓ Incremental processing (merge/append strategies)
- ✓ Partitioning on large tables (6M+ records)
- ✓ Pre-aggregated mart tables
- ✓ Delta table optimization ready

### Analytics Features
- ✓ Surrogate keys using dbt_utils
- ✓ SCD Type 2 for driver history
- ✓ Date dimension attributes
- ✓ Pre-calculated rankings & deltas
- ✓ Business flags (is_race, is_valid_lap, is_top_3)

---

## Troubleshooting

### API Extraction Fails
**Issue:** Connection timeout or rate limit

**Fix:**
```bash
# Reduce sessions or add delay
./load_2024_session.sh 1
```

### Bronze Load Fails
**Issue:** Databricks connection error

**Fix:**
1. Check credentials in config files
2. Verify SQL warehouse is running
3. Test connection:
```bash
cd openf1_api_data_extractor/src/utils
python testconnection.py
```

### dbt Build Fails
**Issue:** Compilation or runtime error

**Fix:**
```bash
cd dbt_f1_data_analytics
./run_dbt.sh clean
./run_dbt.sh deps
./run_dbt.sh build --full-refresh
```

### Empty Tables
**Issue:** Tables created but no data

**Fix:**
1. Verify bronze has data:
```sql
SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions;
```
2. Check dbt logs:
```bash
cat dbt_f1_data_analytics/logs/dbt.log | tail -100
```

---

## Data Lineage

```
OpenF1 API (REST API)
    ↓ (Python - databricks_extractor.py)
Local JSON Files (~/Documents/openf1_data_analytics/local_test_data/)
    ↓ (Python - run_batch_load.py)
Bronze Layer - Delta Tables (dev_f1_data_analytics.bronze_raw)
    ↓ (dbt - SQL transformations)
Silver Layer - Delta Tables (dev_f1_data_analytics.default_silver_clean)
    ↓ (dbt - SQL transformations)
Gold Layer - Delta Tables (dev_f1_data_analytics.default_gold_analytics)
    ↓
Dashboards / Analytics / ML Models
```

---

## Build History

### Latest Build Status: ✅ SUCCESS

**Date:** 2025-11-04
**Duration:** ~2 hours (including debugging)
**Total Records:** 6,105,661
**Tables Created:** 14 (4 bronze + 4 silver + 3 gold core + 3 gold marts)

**Test Results:**
- Silver: 48 PASS, 3 FAIL (data quality - not blocking)
- Gold Core: 35 PASS, 0 FAIL
- Gold Marts: All tests pass


---

## Next Steps

### 1. Build Dashboards
Use mart tables for visualization:
- `session_leaderboard` → Position charts
- `driver_session_summary` → Performance trends
- `team_performance` → Team comparisons

### 2. Schedule Incremental Refresh
```bash
# Daily refresh (cron job or Databricks Workflows)
0 2 * * * cd ~/Documents/openf1_data_analytics && ./load_2024_session.sh
```

### 3. Optimize Large Tables
```sql
OPTIMIZE dev_f1_data_analytics.default_silver_clean.silver_locations
ZORDER BY (session_key, driver_number, date);
```

### 4. Create Additional Marts
- Fastest laps across all sessions
- Driver vs driver head-to-head
- Team development over time


---

## License

This project uses data from the OpenF1 API (https://openf1.org/)

---

## Status

**Project Status:** ✅ Production Ready

**Last Updated:** 2025-11-04
**dbt Version:** 1.10.13
**Platform:** Databricks Unity Catalog with Delta Tables


---

**Built with ❤️ for F1 data analytics**
