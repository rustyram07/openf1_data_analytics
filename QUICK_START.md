# F1 Data Pipeline - Quick Start Guide

**One-command solution to load 2024 F1 session data end-to-end**

---

## TL;DR - Fastest Way to Load Data

```bash
cd ~/Documents/openf1_data_analytics
./load_2024_session.sh
```

That's it! The script automatically runs all 5 steps:
1. ✓ Extract from OpenF1 API
2. ✓ Load to Bronze layer
3. ✓ Build Silver layer
4. ✓ Build Gold layer
5. ✓ Generate validation SQL

**Time:** 15-20 minutes | **Records:** ~200k

---

## Command Options

### Load Latest 1 Session (Default)
```bash
./load_2024_session.sh
```

### Load Latest 5 Sessions
```bash
./load_2024_session.sh 5
```

### Load Oldest 1 Session
```bash
./load_2024_session.sh oldest
```

### Load Oldest 5 Sessions
```bash
./load_2024_session.sh oldest 5
```

### Load ALL 2024 Sessions (~24 races)
```bash
./load_2024_session.sh year
```

---

## What Gets Created

### Bronze Layer (`bronze_raw`)
- `bronze_sessions` - Raw session metadata
- `bronze_drivers` - Raw driver information
- `bronze_laps` - Raw lap timing data
- `bronze_locations` - Raw GPS telemetry

### Silver Layer (`default_silver_clean`)
- `silver_sessions` - Cleaned sessions with categorization
- `silver_drivers` - Cleaned drivers with team classification
- `silver_laps` - Validated laps with calculated metrics
- `silver_locations` - Cleaned GPS data

### Gold Layer (`default_gold_analytics`)
**Dimensions:**
- `dim_sessions` - Session dimension with date attributes
- `dim_drivers` - Driver dimension (SCD Type 2)

**Facts:**
- `fact_laps` - Lap-level fact table with rankings

**Marts:**
- `session_leaderboard` - Driver rankings per session
- `driver_session_summary` - Performance metrics per driver
- `team_performance` - Team aggregations

**Total: 14 tables**

---

## Validation Queries

### Check All Record Counts
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
ORDER BY layer, table_name;
```

### View Session Leaderboard
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

### View Driver Performance
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

---

## Manual Step-by-Step (If Automation Script Fails)

### Step 1: Extract from API
```bash
cd ~/Documents/openf1_data_analytics
source .venv/bin/activate
cd openf1_api_data_extractor/src/ingestion
python databricks_extractor.py --mode latest --num-sessions 1
```

### Step 2: Load to Bronze
```bash
cd ~/Documents/openf1_data_analytics/openf1_api_data_extractor/dlt_bronze_load
python run_batch_load.py
```

### Step 3: Build Silver
```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics
./run_dbt.sh build --select silver_clean --full-refresh
```

### Step 4: Build Gold
```bash
./run_dbt.sh build --select gold_analytics.core --full-refresh
./run_dbt.sh run --select session_leaderboard driver_session_summary team_performance --full-refresh
```

---

## Troubleshooting

### Script Fails at Step 1 (API Extraction)
**Issue:** API timeout or rate limiting

**Fix:**
```bash
# Reduce number of sessions
./load_2024_session.sh 1
```

### Script Fails at Step 2 (Bronze Load)
**Issue:** Databricks connection error

**Fix:**
1. Check Databricks credentials in `.env` or config files
2. Verify SQL warehouse is running in Databricks
3. Test connection:
```bash
cd openf1_api_data_extractor/src/utils
python testconnection.py
```

### Script Fails at Step 3/4 (dbt Build)
**Issue:** dbt compilation error

**Fix:**
```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics
./run_dbt.sh clean
./run_dbt.sh deps
./run_dbt.sh build --full-refresh
```

### No Data in Tables
**Issue:** Tables created but empty

**Fix:**
1. Verify bronze tables have data:
```sql
SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions;
```
2. If bronze is empty, re-run Step 1 and Step 2
3. If bronze has data but silver is empty, check dbt logs:
```bash
cat dbt_f1_data_analytics/logs/dbt.log | tail -100
```

---

## Common Use Cases

### Daily Incremental Refresh
After initial full load, use incremental mode to only process new data:
```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics
./run_dbt.sh build  # No --full-refresh flag
```

### Rebuild Specific Layer
```bash
# Only rebuild silver layer
./run_dbt.sh build --select silver_clean --full-refresh

# Only rebuild gold marts
./run_dbt.sh run --select session_leaderboard driver_session_summary team_performance --full-refresh
```

### Load Specific Year
```bash
# Extract all 2023 sessions
cd openf1_api_data_extractor/src/ingestion
python databricks_extractor.py --mode year --year 2023

# Then load to bronze and build dbt layers
cd ~/Documents/openf1_data_analytics
./load_2024_session.sh  # Skip extraction, just build layers
```

---

## Project Structure

```
openf1_data_analytics/
├── load_2024_session.sh          ← AUTOMATED SCRIPT (USE THIS!)
├── QUICK_START.md                ← This file
├── LOAD_2024_SESSION_GUIDE.md    ← Detailed step-by-step guide
│
├── openf1_api_data_extractor/
│   ├── src/ingestion/
│   │   └── databricks_extractor.py  ← API extraction
│   └── dlt_bronze_load/
│       └── run_batch_load.py        ← Bronze loader
│
└── dbt_f1_data_analytics/
    ├── run_dbt.sh                   ← dbt helper script
    ├── models/
    │   ├── silver_clean/            ← Silver layer models
    │   └── gold_analytics/          ← Gold layer models
    │       ├── core/                ← Dimensions & Facts
    │       └── marts/               ← Analytics tables
    ├── FINAL_BUILD_SUMMARY.md       ← Complete build summary
    ├── SCHEMA_STRUCTURE.md          ← Schema reference
    └── COMPLETE_VALIDATION.sql      ← All validation queries
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `load_2024_session.sh` | **Main automation script** - runs entire pipeline |
| `QUICK_START.md` | Quick reference (this file) |
| `LOAD_2024_SESSION_GUIDE.md` | Detailed step-by-step guide |
| `dbt_f1_data_analytics/run_dbt.sh` | dbt helper with env vars |
| `dbt_f1_data_analytics/FINAL_BUILD_SUMMARY.md` | Complete project status |
| `dbt_f1_data_analytics/SCHEMA_STRUCTURE.md` | Schema documentation |
| `dbt_f1_data_analytics/COMPLETE_VALIDATION.sql` | All validation queries |

---

## FAQ

### Q: How long does it take?
**A:** 15-20 minutes for 1 session. About 2-3 hours for all 2024 sessions.

### Q: How much data is processed?
**A:** ~200k records per session (1 session + 20 drivers + 500-600 laps + 150-200k GPS points)

### Q: Can I run this daily?
**A:** Yes! Use incremental mode instead of full refresh:
```bash
cd dbt_f1_data_analytics
./run_dbt.sh build  # Incremental mode
```

### Q: What if I only want to rebuild Gold layer?
**A:** Skip earlier steps:
```bash
cd dbt_f1_data_analytics
./run_dbt.sh build --select gold_analytics --full-refresh
```

### Q: Where are the actual tables in Databricks?
**A:**
- Bronze: `dev_f1_data_analytics.bronze_raw.*`
- Silver: `dev_f1_data_analytics.default_silver_clean.*`
- Gold: `dev_f1_data_analytics.default_gold_analytics.*`

### Q: Why "default_" prefix in silver and gold schemas?
**A:** dbt combines base schema ("default" from profiles.yml) + custom schema ("silver_clean" from dbt_project.yml) = "default_silver_clean"

---

## Next Steps

1. **Run the automated script:**
   ```bash
   cd ~/Documents/openf1_data_analytics
   ./load_2024_session.sh
   ```

2. **Validate results** in Databricks SQL Editor (queries above)

3. **Build dashboards** using the mart tables:
   - `session_leaderboard` → Position charts
   - `driver_session_summary` → Performance trends
   - `team_performance` → Team comparisons

4. **Set up scheduling** for daily/weekly refreshes

5. **Explore data** with the validation queries in `COMPLETE_VALIDATION.sql`

---

**Created:** 2025-11-04
**Version:** 1.0
**Status:** Production Ready ✓

For detailed information, see [LOAD_2024_SESSION_GUIDE.md](LOAD_2024_SESSION_GUIDE.md)
