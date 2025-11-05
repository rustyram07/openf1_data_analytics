# Quick Reference Card

One-page reference for common tasks.

---

## 🚀 Setup (First Time)

```bash
git clone https://github.com/rustyram07/openf1_data_analytics.git
cd openf1_data_analytics
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

---

## ✅ Test Pipeline (Sample Data)

```bash
python scripts/test_pipeline_local.py
```

---

## 🔄 Run Full Pipeline (Real Data)

### Extract Latest Session
```bash
cd openf1_api_data_extractor/src/ingestion
python extract_sessions.py --num-sessions 1
```

### Load to Bronze
```bash
cd ../../dlt_bronze_load
python run_batch_load.py
```

### Transform with dbt
```bash
cd ../../../dbt_f1_data_analytics
dbt build --target dev
```

---

## 📊 dbt Commands

```bash
cd dbt_f1_data_analytics

# Install packages
dbt deps

# Test connection
dbt debug

# Run all models
dbt build

# Run specific model
dbt run --select silver_sessions

# Run tests
dbt test

# Full refresh
dbt build --full-refresh

# Generate docs
dbt docs generate && dbt docs serve
```

---

## 🔧 GitHub Workflows

### Trigger Data Ingestion
```bash
gh workflow run data_ingestion.yml \
  --field mode=latest \
  --field num_sessions=1 \
  --field trigger_dbt=true
```

### View Workflow Runs
```bash
gh run list
gh run watch
```

---

## 🗄️ Useful SQL Queries

### Check Data Counts
```sql
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_sessions;
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_drivers;
SELECT COUNT(*) FROM bronze_f1_data_analytics.bronze_laps;
```

### Fastest Laps
```sql
SELECT
    driver_number,
    MIN(lap_duration) as fastest_lap
FROM bronze_f1_data_analytics.bronze_laps
GROUP BY driver_number
ORDER BY fastest_lap
LIMIT 10;
```

### Session Summary
```sql
SELECT
    session_name,
    location,
    year,
    COUNT(DISTINCT driver_number) as drivers
FROM stage_gold_analytics.dim_sessions s
LEFT JOIN stage_silver_clean.silver_drivers d
    ON s.session_key = d.session_key
GROUP BY session_name, location, year
ORDER BY year DESC;
```

---

## 🐛 Troubleshooting

### Connection Issues
```bash
cd openf1_api_data_extractor/src/utils
python testconnection.py
```

### Check Environment
```bash
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
echo $DATABRICKS_HTTP_PATH
```

### Load Environment from .env
```bash
export $(cat .env | xargs)
```

### dbt Debug
```bash
cd dbt_f1_data_analytics
dbt debug --target dev
```

---

## 📁 Project Structure

```
openf1_data_analytics/
├── .github/workflows/
│   ├── dbt_ci_cd.yml          # dbt transformations CI/CD
│   └── data_ingestion.yml     # API → Bronze ingestion
├── dbt_f1_data_analytics/
│   ├── models/
│   │   ├── bronze/            # Sources
│   │   ├── silver_clean/      # Cleaned data
│   │   └── gold_analytics/    # Analytics
│   └── dbt_project.yml
├── openf1_api_data_extractor/
│   ├── src/ingestion/         # API extraction
│   └── dlt_bronze_load/       # Bronze loading
├── test_data/                 # Sample test data
├── scripts/
│   └── test_pipeline_local.py # Local testing
├── docs/
│   ├── GETTING_STARTED.md     # Full setup guide
│   ├── INGESTION_WORKFLOW.md  # Ingestion docs
│   └── QUICK_REFERENCE.md     # This file
└── README.md
```

---

## 🔗 Important Links

- **Repo:** https://github.com/rustyram07/openf1_data_analytics
- **OpenF1 API:** https://openf1.org/
- **dbt Docs:** https://docs.getdbt.com/
- **Databricks:** https://docs.databricks.com/

---

## 🆘 Need Help?

1. Check [GETTING_STARTED.md](GETTING_STARTED.md) for detailed setup
2. See [INGESTION_WORKFLOW.md](INGESTION_WORKFLOW.md) for API usage
3. Open an issue on GitHub
4. Review dbt docs: `dbt docs generate && dbt docs serve`

---

**Last Updated:** 2025-01-05
