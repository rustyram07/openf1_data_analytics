# Getting Started Guide

Complete guide to set up and run the OpenF1 Data Analytics project locally.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start (5 minutes)](#quick-start-5-minutes)
3. [Detailed Setup](#detailed-setup)
4. [Test with Sample Data](#test-with-sample-data)
5. [Run Your First Session](#run-your-first-session)
6. [Verify the Pipeline](#verify-the-pipeline)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software

| Software | Minimum Version | Purpose | Installation |
|----------|----------------|---------|--------------|
| **Python** | 3.11+ | Run ingestion scripts and dbt | [python.org](https://www.python.org/downloads/) |
| **Git** | 2.x+ | Clone repository | [git-scm.com](https://git-scm.com/downloads) |
| **GitHub CLI** (optional) | 2.x+ | Trigger workflows | [cli.github.com](https://cli.github.com/) |

### Required Accounts

1. **GitHub Account** - To clone the repository
2. **Databricks Account** - For data warehouse and storage
   - Community Edition (free) or Standard workspace
   - SQL Warehouse (serverless or classic)
   - Unity Catalog enabled

### Required Credentials

You'll need these from your Databricks workspace:

```bash
DATABRICKS_HOST         # e.g., https://adb-1234567890.8.azuredatabricks.net
DATABRICKS_TOKEN        # Personal Access Token
DATABRICKS_HTTP_PATH    # e.g., /sql/1.0/warehouses/abcd1234
```

**How to get these:**
1. **DATABRICKS_HOST**: Your workspace URL (from browser address bar)
2. **DATABRICKS_TOKEN**:
   - Go to Settings → Developer → Access Tokens
   - Click "Generate New Token"
   - Copy and save securely
3. **DATABRICKS_HTTP_PATH**:
   - Go to SQL Warehouses
   - Click on your warehouse → Connection Details
   - Copy "HTTP Path"

---

## Quick Start (5 minutes)

```bash
# 1. Clone repository
git clone https://github.com/rustyram07/openf1_data_analytics.git
cd openf1_data_analytics

# 2. Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Set up environment variables
cp .env.example .env
# Edit .env with your Databricks credentials

# 5. Test connection
cd openf1_api_data_extractor/src/utils
python testconnection.py

# 6. Run test with sample data (see below)
cd ../../../
python scripts/test_pipeline_local.py
```

---

## Detailed Setup

### Step 1: Clone Repository

```bash
# HTTPS
git clone https://github.com/rustyram07/openf1_data_analytics.git

# OR SSH (if configured)
git clone git@github.com:rustyram07/openf1_data_analytics.git

cd openf1_data_analytics
```

### Step 2: Create Virtual Environment

**macOS/Linux:**
```bash
python3.11 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Verify activation:**
```bash
which python  # Should show path to venv/bin/python
python --version  # Should show Python 3.11+
```

### Step 3: Install Python Dependencies

```bash
# Upgrade pip first
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt

# Verify key packages
pip list | grep -E "dbt|databricks|pandas"
```

**Expected output:**
```
databricks-connect       17.2.0
databricks-sdk           0.63.0
databricks-sql-connector 4.0.5
dbt-core                 1.10.13
dbt-databricks           1.10.14
pandas                   2.2.3
```

### Step 4: Configure Environment Variables

**Create `.env` file:**

```bash
# Copy example file
cp .env.example .env

# Edit with your credentials
nano .env  # or vim, code, etc.
```

**`.env` contents:**
```bash
# Databricks Configuration
DATABRICKS_HOST=https://adb-1234567890.azuredatabricks.net
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcd1234efgh5678

# Optional: Catalog configuration
BRONZE_CATALOG=bronze_f1_data_analytics
STAGE_CATALOG=stage_f1_data_analytics
PROD_CATALOG=prod_f1_data_analytics

# Optional: Local data directory
LOCAL_DATA_DIR=./test_data
```

**Security Note:** Never commit `.env` to git! It's already in `.gitignore`.

### Step 5: Verify Databricks Connection

```bash
cd openf1_api_data_extractor/src/utils
python testconnection.py
```

**Expected output:**
```
Testing Databricks Connection...
✅ Successfully connected to Databricks!
Workspace: https://adb-1234567890.azuredatabricks.net
Warehouse: /sql/1.0/warehouses/abcd1234efgh5678
```

**If connection fails:**
- Verify credentials in `.env`
- Check SQL Warehouse is running
- Verify network access (VPN, firewall)

### Step 6: Configure dbt Profile

**Create/edit `~/.dbt/profiles.yml`:**

```yaml
dbt_f1_data_analytics:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: dev_f1_data_analytics
      schema: default
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4

    stage:
      type: databricks
      catalog: stage_f1_data_analytics
      schema: default
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4

    prod:
      type: databricks
      catalog: prod_f1_data_analytics
      schema: default
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
```

**Test dbt connection:**
```bash
cd dbt_f1_data_analytics
dbt debug
```

**Expected output:**
```
Running with dbt=1.10.13
dbt version: 1.10.13
...
All checks passed!
```

---

## Test with Sample Data

We've included sample data from the **2024 Bahrain Grand Prix** for testing.

### Option 1: Use Pre-loaded Test Data (Fastest)

```bash
# Run the test script
python scripts/test_pipeline_local.py

# This will:
# 1. Load sample JSON files from test_data/
# 2. Create bronze tables in your dev catalog
# 3. Run dbt to create silver tables
# 4. Display results
```

### Option 2: Test with Real API (One Session)

```bash
# Extract latest session from OpenF1 API
cd openf1_api_data_extractor/src/ingestion
python extract_sessions.py --num-sessions 1

# Load to bronze
cd ../../dbt_bronze_load
python run_batch_load.py

# Run dbt transformations
cd ../../../dbt_f1_data_analytics
dbt build --target dev
```

---

## Run Your First Session

### Full Pipeline End-to-End

```bash
# 1. Extract data from OpenF1 API
cd openf1_api_data_extractor/src/ingestion
python extract_sessions.py --num-sessions 1

# Output: JSON files in landing volume
# - sessions_XXXXX.json
# - drivers_XXXXX.json
# - laps_XXXXX.json
# - locations_XXXXX.json

# 2. Load to bronze tables
cd ../../dlt_bronze_load
python run_batch_load.py

# Output: Bronze Delta tables created
# - bronze.bronze_sessions
# - bronze.bronze_drivers
# - bronze.bronze_laps
# - bronze.bronze_locations

# 3. Transform with dbt
cd ../../../dbt_f1_data_analytics
dbt build --target dev

# Output: Silver and Gold tables created
# - silver_clean.silver_sessions
# - silver_clean.silver_drivers
# - silver_clean.silver_laps
# - gold_analytics.dim_sessions
# - gold_analytics.fact_laps
# ... etc
```

### Monitoring Progress

**Check bronze tables:**
```sql
-- In Databricks SQL Editor
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_sessions;
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_drivers;
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_laps;
```

**Check silver tables:**
```sql
SELECT COUNT(*) FROM stage_silver_clean.silver_sessions;
SELECT COUNT(*) FROM stage_silver_clean.silver_drivers;
SELECT COUNT(*) FROM stage_silver_clean.silver_laps;
```

**Check gold tables:**
```sql
SELECT * FROM stage_gold_analytics.dim_sessions LIMIT 10;
SELECT * FROM stage_gold_analytics.fact_laps LIMIT 10;
```

---

## Verify the Pipeline

### 1. Check dbt Models

```bash
cd dbt_f1_data_analytics

# List all models
dbt ls

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # Opens in browser at http://localhost:8080
```

### 2. Verify Data Quality

**Run specific tests:**
```bash
# Test silver layer only
dbt test --select silver_clean.*

# Test specific model
dbt test --select silver_sessions

# Test with warnings
dbt test --warn-error
```

### 3. Query Sample Data

**Fastest lap per driver:**
```sql
SELECT
    driver_name,
    MIN(lap_duration) as fastest_lap
FROM stage_gold_analytics.fact_laps
WHERE is_valid_lap = TRUE
GROUP BY driver_name
ORDER BY fastest_lap
LIMIT 10;
```

**Session summary:**
```sql
SELECT
    session_name,
    location,
    year,
    session_category
FROM stage_gold_analytics.dim_sessions
ORDER BY date_start DESC;
```

---

## Troubleshooting

### Issue 1: Python Version Mismatch

**Error:** `python: command not found` or `Python 3.11 required`

**Solution:**
```bash
# Check Python version
python --version
python3 --version
python3.11 --version

# Use specific version
python3.11 -m venv venv
```

### Issue 2: pip install fails

**Error:** `Could not find a version that satisfies the requirement...`

**Solution:**
```bash
# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install with verbose output
pip install -v -r requirements.txt

# Try individual packages
pip install dbt-databricks>=1.10.0
pip install databricks-connect>=17.2.0
```

### Issue 3: Databricks connection fails

**Error:** `Failed to connect to Databricks`

**Solutions:**

1. **Verify credentials:**
```bash
# Check environment variables are loaded
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
echo $DATABRICKS_HTTP_PATH

# If empty, load .env manually
export $(cat .env | xargs)
```

2. **Check SQL Warehouse status:**
   - Go to Databricks workspace → SQL Warehouses
   - Ensure warehouse is **Running** (not Stopped)
   - Click "Start" if needed

3. **Verify network access:**
   - Check firewall/VPN
   - Try accessing Databricks UI in browser
   - Verify IP allowlist in Databricks settings

### Issue 4: dbt debug fails

**Error:** `Compilation Error` or `Could not find profile`

**Solutions:**

1. **Check profiles.yml location:**
```bash
ls -la ~/.dbt/profiles.yml
```

2. **Verify profile name matches:**
```bash
# In dbt_project.yml
grep "profile:" dbt_f1_data_analytics/dbt_project.yml

# Should output:
# profile: 'dbt_f1_data_analytics'
```

3. **Test with explicit profile:**
```bash
dbt debug --profiles-dir ~/.dbt
```

### Issue 5: No data in bronze tables

**Error:** `Table is empty` or `0 rows returned`

**Solutions:**

1. **Check JSON files exist:**
```bash
ls -la data/  # or your LOCAL_DATA_DIR
```

2. **Re-run extraction:**
```bash
cd openf1_api_data_extractor/src/ingestion
python extract_sessions.py --num-sessions 1
```

3. **Check landing volume:**
```sql
LIST '/Volumes/your_catalog/your_schema/landing/';
```

### Issue 6: dbt tests failing

**Error:** `FAIL` or `ERROR` in dbt test output

**Expected warnings:**
- `lap_duration_reasonable` - WARN (574 records) - OK
- `max_speed > 0` - WARN (14 records) - OK
- `team_category` - WARN (3 records) - OK

**Actual errors to fix:**
```bash
# View specific test
dbt test --select <test_name>

# Run with verbose logging
dbt test --debug

# Check compiled SQL
cat target/compiled/dbt_f1_data_analytics/models/...
```

---

## Next Steps

### 1. Explore the Data

```bash
# Generate and view dbt docs
cd dbt_f1_data_analytics
dbt docs generate
dbt docs serve
```

Browse to http://localhost:8080 to explore:
- Data lineage diagrams
- Model documentation
- Column descriptions
- Test results

### 2. Run More Sessions

```bash
# Extract latest 5 sessions
cd openf1_api_data_extractor/src/ingestion
python extract_sessions.py --num-sessions 5

# Or extract entire 2024 season
python extract_sessions.py --mode year --year 2024
```

### 3. Set up CI/CD

See [GitHub Actions Setup](./CI_CD_SETUP.md) for:
- Configuring GitHub secrets
- Running automated tests
- Deploying to production

### 4. Customize for Your Needs

- Add new dbt models in `dbt_f1_data_analytics/models/`
- Create custom tests in `schema.yml`
- Build dashboards with your BI tool
- Export data for ML models

---

## Useful Commands Reference

### Data Ingestion
```bash
# Test connection
python openf1_api_data_extractor/src/utils/testconnection.py

# Extract latest session
python openf1_api_data_extractor/src/ingestion/extract_sessions.py --num-sessions 1

# Load to bronze
python openf1_api_data_extractor/dlt_bronze_load/run_batch_load.py
```

### dbt Commands
```bash
cd dbt_f1_data_analytics

# Run all models
dbt build

# Run specific model
dbt run --select silver_sessions

# Test data quality
dbt test

# Full refresh (rebuild from scratch)
dbt build --full-refresh

# Generate docs
dbt docs generate && dbt docs serve
```

### GitHub Workflows
```bash
# Trigger data ingestion
gh workflow run data_ingestion.yml \
  --field mode=latest \
  --field num_sessions=1

# View workflow runs
gh run list

# Watch current run
gh run watch
```

---

## Getting Help

- **Documentation:** [docs/](../docs/)
- **Issues:** [GitHub Issues](https://github.com/rustyram07/openf1_data_analytics/issues)
- **OpenF1 API Docs:** [openf1.org](https://openf1.org/)
- **dbt Docs:** [docs.getdbt.com](https://docs.getdbt.com/)
- **Databricks Docs:** [docs.databricks.com](https://docs.databricks.com/)

---

**Happy Racing! 🏎️💨**
