# F1 Data Ingestion Workflow Guide

## Overview

The data ingestion workflow (`data_ingestion.yml`) handles the complete pipeline from OpenF1 API to Bronze tables in Databricks.

```
┌─────────────┐      ┌─────────────────┐      ┌──────────────┐
│  OpenF1 API │  →   │ Landing Volume  │  →   │ Bronze Tables│
└─────────────┘      └─────────────────┘      └──────────────┘
     Step 1                Step 2                  Step 3
```

---

## Pipeline Stages

### Stage 1: API Extraction → Landing Volume
- **Script:** `openf1_api_data_extractor/src/ingestion/extract_sessions.py`
- **What it does:**
  - Fetches session data from OpenF1 API
  - Downloads drivers, laps, locations, sessions data
  - Saves JSON files to Databricks Volume (landing layer)

### Stage 2: Landing Volume → Bronze Tables
- **Script:** `openf1_api_data_extractor/dlt_bronze_load/run_batch_load.py`
- **What it does:**
  - Reads JSON files from landing volume
  - Loads data into bronze Delta tables
  - Adds ingestion metadata (`_ingestion_timestamp`)

### Stage 3: Trigger dbt Transformation (Optional)
- **Workflow:** `dbt_ci_cd.yml`
- **What it does:**
  - Transforms bronze → silver → gold layers
  - Runs data quality tests
  - Creates analytics-ready tables

---

## Usage

### 1. Scheduled Runs (Automatic)

The workflow runs automatically every 2 hours on **race weekends** (Friday-Sunday):

```yaml
# Friday 00:00, 02:00, 04:00, ..., 22:00
# Saturday 00:00, 02:00, 04:00, ..., 22:00
# Sunday 00:00, 02:00, 04:00, ..., 22:00
```

**No action required** - it runs automatically.

---

### 2. Manual Runs (GitHub UI)

Go to **Actions** → **F1 Data Ingestion Pipeline** → **Run workflow**

#### Example 1: Extract latest session
```
Mode: latest
Number of sessions: 1
Trigger dbt: ✅ Yes
```

#### Example 2: Extract latest 5 sessions
```
Mode: latest
Number of sessions: 5
Trigger dbt: ✅ Yes
```

#### Example 3: Extract all 2024 sessions
```
Mode: year
Year: 2024
Trigger dbt: ✅ Yes
```

#### Example 4: Backfill oldest 10 sessions
```
Mode: oldest
Number of sessions: 10
Trigger dbt: ❌ No  (manual dbt run later)
```

---

### 3. Manual Runs (GitHub CLI)

```bash
# Extract latest session
gh workflow run data_ingestion.yml \
  --field mode=latest \
  --field num_sessions=1 \
  --field trigger_dbt=true

# Extract all 2024 sessions
gh workflow run data_ingestion.yml \
  --field mode=year \
  --field year=2024 \
  --field trigger_dbt=true

# Extract oldest 5 sessions (backfill)
gh workflow run data_ingestion.yml \
  --field mode=oldest \
  --field num_sessions=5 \
  --field trigger_dbt=false
```

---

### 4. Manual Runs (API)

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/rustyram07/openf1_data_analytics/actions/workflows/data_ingestion.yml/dispatches \
  -d '{
    "ref": "main",
    "inputs": {
      "mode": "latest",
      "num_sessions": "1",
      "trigger_dbt": "true"
    }
  }'
```

---

## Configuration

### Required GitHub Secrets

Add these secrets in **Settings** → **Secrets and variables** → **Actions**:

| Secret Name | Description | Example |
|------------|-------------|---------|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://adb-xxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal access token | `dapi...` |
| `DATABRICKS_HTTP_PATH` | SQL warehouse path | `/sql/1.0/warehouses/xxx` |

### Environment Variables (Optional)

You can customize these in the workflow file:

```yaml
env:
  PYTHON_VERSION: '3.11'
  LANDING_VOLUME: '/Volumes/catalog/schema/landing'
  BRONZE_CATALOG: 'bronze_f1_data_analytics'
```

---

## Workflow Outputs

### Success Output

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏁 F1 Data Ingestion Pipeline Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mode: latest
Sessions extracted: 1
Bronze load status: success
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ dbt workflow triggered successfully
```

### Artifacts Created

**Landing Volume:**
```
/Volumes/your_catalog/landing/
  ├── sessions_12345.json
  ├── drivers_12345.json
  ├── laps_12345.json
  └── locations_12345.json
```

**Bronze Tables:**
```
bronze_f1_data_analytics.bronze_sessions
bronze_f1_data_analytics.bronze_drivers
bronze_f1_data_analytics.bronze_laps
bronze_f1_data_analytics.bronze_locations
```

---

## Monitoring

### View Workflow Status

**GitHub UI:**
1. Go to **Actions** tab
2. Click **F1 Data Ingestion Pipeline**
3. View recent runs and logs

**GitHub CLI:**
```bash
# List recent runs
gh run list --workflow=data_ingestion.yml

# Watch a specific run
gh run watch <run-id>

# View logs
gh run view <run-id> --log
```

### Key Metrics to Monitor

| Metric | Command | Expected Value |
|--------|---------|----------------|
| Sessions extracted | Check logs | 1-25 sessions |
| Bronze rows loaded | Check Databricks | Thousands-Millions |
| Pipeline duration | Check Actions | 5-15 minutes |
| Failure rate | Check Actions | <5% |

---

## Troubleshooting

### Issue 1: API Rate Limiting

**Error:** `429 Too Many Requests`

**Solution:**
```python
# Update config/settings.py
API_RATE_LIMIT_DELAY = 2.0  # Increase delay between requests
```

### Issue 2: Databricks Connection Failed

**Error:** `Failed to connect to Databricks`

**Solution:**
1. Verify secrets are set correctly
2. Check token hasn't expired
3. Verify SQL warehouse is running

```bash
# Test connection locally
cd openf1_api_data_extractor/src/utils
python testconnection.py
```

### Issue 3: Bronze Load Failed

**Error:** `Table not found` or `Schema mismatch`

**Solution:**
```python
# Run bronze loader manually with verbose logging
cd openf1_api_data_extractor/dlt_bronze_load
python run_batch_load.py
```

### Issue 4: dbt Trigger Failed

**Error:** `Failed to trigger dbt workflow`

**Solution:**
- Ensure workflow has `contents: read` and `actions: write` permissions
- Verify `dbt_ci_cd.yml` exists and is valid
- Check that target branch (`main`) exists

---

## Best Practices

### 1. Race Weekend Coverage

Enable scheduled runs on race weekends to capture:
- FP1, FP2, FP3 (Friday-Saturday)
- Qualifying (Saturday)
- Sprint (if applicable)
- Race (Sunday)

### 2. Backfilling Historical Data

```bash
# Extract all sessions for 2023
gh workflow run data_ingestion.yml \
  --field mode=year \
  --field year=2023 \
  --field trigger_dbt=false

# After completion, run dbt manually with full refresh
cd dbt_f1_data_analytics
dbt build --full-refresh --target prod
```

### 3. Incremental Updates

For regular updates, use `mode=latest` with `num_sessions=1`:
```bash
gh workflow run data_ingestion.yml \
  --field mode=latest \
  --field num_sessions=1 \
  --field trigger_dbt=true
```

### 4. Cost Optimization

- Use scheduled runs only during race weekends
- Set `trigger_dbt=false` for backfills
- Run dbt transformations once per day instead of per ingestion

---

## Integration with dbt Pipeline

### Automatic Triggering

When `trigger_dbt=true`, the workflow automatically triggers:

```
Ingestion Complete
      ↓
dbt_ci_cd.yml (target: prod)
      ↓
Bronze → Silver → Gold
```

### Manual dbt Run

If you set `trigger_dbt=false`, run dbt manually:

```bash
# Trigger via GitHub Actions
gh workflow run dbt_ci_cd.yml --field target=prod

# Or run locally
cd dbt_f1_data_analytics
dbt build --target prod
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    GitHub Actions Runner                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │  Job 1: ingest_f1_data                                 │    │
│  │                                                         │    │
│  │  [Extract Sessions] → [Load Bronze] → [Verify]        │    │
│  │         ↓                    ↓              ↓           │    │
│  │    Landing Volume      Bronze Tables    Logs          │    │
│  └────────────────────────────────────────────────────────┘    │
│                            ↓                                     │
│  ┌────────────────────────────────────────────────────────┐    │
│  │  Job 2: trigger_dbt_transformation (if enabled)        │    │
│  │                                                         │    │
│  │  [Trigger dbt_ci_cd.yml workflow]                      │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                            ↓
                ┌──────────────────────┐
                │ Databricks Workspace  │
                │                      │
                │  Landing Volume      │
                │  Bronze Tables       │
                │  Silver Tables (dbt) │
                │  Gold Tables (dbt)   │
                └──────────────────────┘
```

---

## Next Steps

1. ✅ Commit and push the workflow file
2. ✅ Add required secrets to GitHub repository
3. ✅ Test with manual run: `mode=latest`, `num_sessions=1`
4. ✅ Enable scheduled runs for next race weekend
5. ✅ Monitor workflow runs and adjust as needed

---

## Support

For issues or questions:
- Check workflow logs in GitHub Actions
- Review Databricks workspace for data issues
- See main [README.md](../README.md) for project overview
