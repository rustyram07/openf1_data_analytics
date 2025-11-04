# Best Practices & Operations Guide

**Comprehensive guide for orchestration, ingestion, transformation, testing, monitoring, scaling, and governance**

---

## Table of Contents

1. [Orchestration](#orchestration)
2. [Data Ingestion](#data-ingestion)
3. [Transformation & Storage](#transformation--storage)
4. [Testing & Monitoring](#testing--monitoring)
5. [Scaling & Performance](#scaling--performance)
6. [Governance & Security](#governance--security)
7. [Operational Procedures](#operational-procedures)
8. [Idiosyncrasies & Gotchas](#idiosyncrasies--gotchas)

---

## Orchestration

### Current Implementation

#### Manual Orchestration
```bash
# Single command to run entire pipeline
./load_2024_session.sh

# What it does:
# 1. Extract from OpenF1 API
# 2. Load to Bronze Delta tables
# 3. Run dbt transformations (Bronze → Silver → Gold)
# 4. Run data quality tests
```

#### GitHub Actions CI/CD

**Automated Triggers**:
- PR to `dev` → Test against `stage_f1_data_analytics`
- PR to `main` → Test against `prod_f1_data_analytics`

**Workflow**:
```yaml
jobs:
  ci_test_stage:
    steps:
      - Install dbt
      - dbt deps
      - dbt debug --target stage
      - dbt compile --target stage
      - dbt build --target stage  # Models + tests
      - Upload artifacts
      - Post PR comment
```

### Dagster Implementation (Optional)

**Asset-Based Pipeline**:

```python
# dagster_pipeline.py

from dagster import asset, AssetExecutionContext
from dagster_shell import execute_shell_command

@asset
def extract_session_data(context: AssetExecutionContext):
    """Extract data from OpenF1 API"""
    cmd = """
    cd openf1_api_data_extractor/src/ingestion && \
    python extract_sessions.py --order latest --num-sessions 1
    """
    result = execute_shell_command(cmd, output_logging="STREAM")
    return result.output

@asset(deps=[extract_session_data])
def load_bronze_tables(context: AssetExecutionContext):
    """Load JSON to Bronze Delta tables"""
    cmd = """
    cd openf1_api_data_extractor/dlt_bronze_load && \
    python run_batch_load.py
    """
    return execute_shell_command(cmd, output_logging="STREAM")

@asset(deps=[load_bronze_tables])
def build_silver_layer(context: AssetExecutionContext):
    """Run dbt for Silver layer"""
    cmd = "cd dbt_f1_data_analytics && ./run_dbt.sh build --select silver_clean"
    return execute_shell_command(cmd, output_logging="STREAM")

@asset(deps=[build_silver_layer])
def build_gold_layer(context: AssetExecutionContext):
    """Run dbt for Gold layer"""
    cmd = "cd dbt_f1_data_analytics && ./run_dbt.sh build --select gold_analytics"
    return execute_shell_command(cmd, output_logging="STREAM")
```

**Run Dagster**:
```bash
dagster dev -f dagster_pipeline.py
# Access UI: http://localhost:3000
```

### Best Practices

#### DO
✅ Use asset-based thinking (Dagster) over task-based (Airflow)
✅ Make pipelines idempotent (can re-run safely)
✅ Include data quality checks in every run
✅ Log execution metadata (duration, row counts)
✅ Separate extraction from transformation
✅ Use incremental processing where beneficial
✅ Implement retry logic with exponential backoff
✅ Monitor pipeline SLAs

#### DON'T
❌ Don't hardcode timestamps or dates
❌ Don't mix orchestration with business logic
❌ Don't skip testing in CI/CD
❌ Don't run full refresh if incremental works
❌ Don't ignore failed tests
❌ Don't process already processed data
❌ Don't deploy without testing in stage first

---

## Data Ingestion

### API Extraction Pattern

```python
class OpenF1DataExtractor:
    """Best practice API client"""

    def __init__(self):
        self.base_url = "https://api.openf1.org/v1"
        self.rate_limiter = RateLimiter(100, 1)  # 100/sec
        self.retry_config = {
            'max_attempts': 3,
            'backoff_factor': 2,
            'status_forcelist': [429, 500, 502, 503, 504]
        }

    @retry(**retry_config)
    def get_with_rate_limit(self, endpoint, params=None):
        """Rate-limited GET with retry"""
        self.rate_limiter.wait()
        response = requests.get(f"{self.base_url}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def extract_session(self, session_key):
        """Extract all data for a session"""
        # Parallel requests for different entities
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                'drivers': executor.submit(self.get_drivers, session_key),
                'laps': executor.submit(self.get_laps, session_key),
                'locations': executor.submit(self.get_locations, session_key)
            }
            return {k: f.result() for k, f in futures.items()}

    def save_to_json(self, data, entity, session_key):
        """Save with timestamp and metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{entity}_{session_key}_{timestamp}.json"

        metadata = {
            'extracted_at': datetime.now().isoformat(),
            'session_key': session_key,
            'entity': entity,
            'record_count': len(data)
        }

        output = {'metadata': metadata, 'data': data}
        with open(f"local_test_data/{filename}", 'w') as f:
            json.dump(output, f, indent=2)
```

### Loading to Bronze

```python
def load_bronze_table(entity: str):
    """Load JSON to Bronze with best practices"""

    # 1. Get all unprocessed files
    files = get_unprocessed_files(entity)

    # 2. Read with schema inference
    df = spark.read.json(files)

    # 3. Add ingestion metadata
    df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
           .withColumn("_source_file", input_file_name()) \
           .withColumn("_ingestion_batch_id", lit(generate_batch_id()))

    # 4. Write to Delta (append-only)
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \  # Allow schema evolution
        .partitionBy("batch_day") \  # For large tables
        .saveAsTable(f"bronze_raw.bronze_{entity}")

    # 5. Mark files as processed
    mark_files_processed(files)

    # 6. Log metrics
    log_ingestion_metrics(entity, df.count(), files)
```

### Best Practices

#### API Extraction
✅ **Rate Limiting**: Respect API limits (100/sec for OpenF1)
✅ **Retry Logic**: Exponential backoff for transient errors
✅ **Parallel Requests**: Use ThreadPoolExecutor for multiple endpoints
✅ **Incremental Extraction**: Only fetch new/changed data
✅ **Metadata Capture**: Record extraction timestamp, counts
✅ **Error Handling**: Graceful degradation (skip failed sessions)
✅ **Validation**: Check response structure before saving

#### Bronze Loading
✅ **Append-Only**: Never update/delete bronze data
✅ **Schema Evolution**: Enable mergeSchema for flexibility
✅ **Partition Large Tables**: Use batch_day for > 1M rows
✅ **Idempotency**: Track processed files to avoid duplicates
✅ **Atomic Operations**: Delta Lake ACID guarantees
✅ **Metadata**: Add _ingestion_timestamp, _source_file

#### File Management
```bash
# Directory structure
local_test_data/
├── sessions_20241104_120000.json
├── drivers_9158_20241104_120100.json
├── laps_9158_20241104_120200.json
├── locations_9158_20241104_120300.json
└── _processed/  # Move after successful load
    └── archive/
```

---

## Transformation & Storage

### dbt Best Practices

#### Model Organization
```
models/
├── silver_clean/
│   ├── core/
│   │   ├── silver_sessions.sql
│   │   ├── silver_drivers.sql
│   │   ├── silver_laps.sql
│   │   ├── silver_locations.sql
│   │   └── sources.yml  # Bronze source definitions
│   └── schema.yml  # Silver tests & docs
├── gold_analytics/
│   ├── core/
│   │   ├── dim_sessions.sql
│   │   ├── dim_drivers.sql
│   │   ├── fact_laps.sql
│   │   └── schema.yml
│   └── marts/
│       ├── session_leaderboard.sql
│       ├── driver_session_summary.sql
│       ├── team_performance.sql
│       └── schema.yml
└── snapshots/
    ├── driver_snapshot.sql
    └── session_snapshot.sql
```

#### Model Configuration

**Silver (Incremental)**:
```sql
{{
    config(
        materialized='incremental',
        unique_key=['session_key', 'driver_number'],
        incremental_strategy='merge',
        merge_update_columns=['team_name', 'team_colour', 'updated_at'],
        partition_by=['batch_day'],  # For large tables
        on_schema_change='append_new_columns'
    )
}}
```

**Gold (Full Refresh)**:
```sql
{{
    config(
        materialized='table',
        file_format='delta'
    )
}}
```

#### Testing Strategy

**Required Tests**:
```yaml
# schema.yml
models:
  - name: silver_sessions
    columns:
      - name: session_key
        tests:
          - not_null
          - unique

      - name: session_name
        tests:
          - not_null

      - name: session_category
        tests:
          - accepted_values:
              values: ['Race', 'Qualifying', 'Practice', 'Sprint', 'Other']
```

**Custom Tests**:
```sql
-- tests/one_current_record_per_driver.sql
-- Ensures SCD Type 2 integrity
SELECT
    driver_number,
    COUNT(*) as current_count
FROM {{ ref('dim_drivers') }}
WHERE is_current = TRUE
GROUP BY driver_number
HAVING COUNT(*) > 1
```

### Storage Optimization

#### Delta Lake Configuration

```sql
-- Enable auto-optimize
ALTER TABLE bronze_raw.bronze_locations
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Configure retention
ALTER TABLE gold_analytics.fact_laps
SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 90 days'
);
```

#### Z-Ordering

```sql
-- Optimize for common query patterns
OPTIMIZE bronze_raw.bronze_locations
ZORDER BY (session_key, driver_number, date);

OPTIMIZE gold_analytics.fact_laps
ZORDER BY (session_key, driver_number);
```

#### Vacuum

```sql
-- Reclaim storage (run monthly)
VACUUM bronze_raw.bronze_locations RETAIN 30 HOURS;
VACUUM bronze_raw.bronze_laps RETAIN 30 HOURS;

-- Dry run first
VACUUM bronze_raw.bronze_locations RETAIN 30 HOURS DRY RUN;
```

### Best Practices

✅ **Incremental > Full Refresh**: For large tables (silver layer)
✅ **Full Refresh > Incremental**: For small tables (gold layer)
✅ **Partition Strategically**: Only when queries filter by partition key
✅ **Z-Order**: On columns in WHERE/JOIN clauses
✅ **Vacuum Regularly**: Monthly for large tables
✅ **Test Everything**: 117 tests across all layers
✅ **Document Models**: Descriptions + column-level docs
✅ **Version Control**: All dbt code in Git
✅ **CI/CD**: Test before merging (stage → prod)

---

## Testing & Monitoring

### Testing Layers

#### 1. Source Data Tests (Bronze)
```yaml
sources:
  - name: bronze
    tables:
      - name: bronze_sessions
        columns:
          - name: session_key
            tests:
              - not_null
              - unique
```

#### 2. Transformation Tests (Silver)
```yaml
models:
  - name: silver_laps
    tests:
      # Row count validation
      - dbt_utils.expression_is_true:
          expression: "count(*) > 0"

    columns:
      - name: is_valid_lap
        tests:
          - not_null
          - accepted_values:
              values: [TRUE, FALSE]

      - name: lap_duration
        tests:
          - dbt_utils.expression_is_true:
              expression: "lap_duration >= 0"
```

#### 3. Business Logic Tests (Gold)
```sql
-- tests/leaderboard_position_sequential.sql
-- Ensures positions are 1, 2, 3... without gaps
WITH position_check AS (
    SELECT
        session_key,
        position,
        LAG(position) OVER (PARTITION BY session_key ORDER BY position) as prev_position
    FROM {{ ref('session_leaderboard') }}
)
SELECT *
FROM position_check
WHERE position != COALESCE(prev_position + 1, 1)
```

#### 4. Snapshot Integrity Tests
```sql
-- tests/one_current_record_per_driver.sql
SELECT
    driver_number,
    COUNT(*) as current_count
FROM {{ ref('dim_drivers') }}
WHERE is_current = TRUE
GROUP BY driver_number
HAVING COUNT(*) > 1
```

### Monitoring

#### Databricks System Tables

```sql
-- Query history (last 24 hours)
SELECT
    query_id,
    query_text,
    user_name,
    start_time,
    end_time,
    execution_status,
    rows_produced,
    total_time_ms / 1000 as duration_sec
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 1 DAY
    AND query_text LIKE '%f1_data_analytics%'
ORDER BY start_time DESC;
```

```sql
-- Table audit logs
SELECT
    event_time,
    user_identity.email,
    request_params.full_name_arg as table_name,
    action_name,
    request_params.operation as operation_type
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%f1_data_analytics%'
    AND event_date >= current_date() - INTERVAL 7 DAY
ORDER BY event_time DESC;
```

#### Custom Monitoring

```python
# monitoring/pipeline_monitor.py

def check_data_freshness():
    """Alert if data is stale"""
    query = """
    SELECT
        MAX(_ingestion_timestamp) as last_ingestion,
        DATEDIFF(HOUR, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP()) as hours_stale
    FROM bronze_raw.bronze_sessions
    """
    result = spark.sql(query).collect()[0]

    if result.hours_stale > 48:  # Alert if > 48 hours
        send_alert(f"Data is {result.hours_stale} hours stale!")

def check_row_counts():
    """Validate expected row counts"""
    checks = {
        'bronze_sessions': {'min': 1, 'max': 100},
        'bronze_drivers': {'min': 18, 'max': 24},
        'bronze_laps': {'min': 400, 'max': 800},
    }

    for table, expected in checks.items():
        count = spark.table(f"bronze_raw.{table}").count()
        if not (expected['min'] <= count <= expected['max']):
            send_alert(f"{table} has {count} rows (expected {expected})")
```

#### dbt Cloud Monitoring (if using)

```yaml
# dbt_project.yml
on-run-end:
  - "{{ log_run_metrics() }}"  # Custom macro to log metrics

# macros/log_run_metrics.sql
{% macro log_run_metrics() %}
    {% if execute %}
        INSERT INTO monitoring.dbt_run_log
        VALUES (
            '{{ run_started_at }}',
            '{{ invocation_id }}',
            '{{ target.name }}',
            {{ results | length }},
            {{ results | selectattr("status", "equalto", "error") | list | length }}
        )
    {% endif %}
{% endmacro %}
```

### Best Practices

✅ **Test at Every Layer**: Source, Silver, Gold, Snapshots
✅ **Custom Tests**: Business-specific validations
✅ **Automated Monitoring**: Check freshness, counts, quality
✅ **Alert on Failures**: Email/Slack when tests fail
✅ **Query History**: Track performance, identify slow queries
✅ **Audit Logs**: Track who accessed what
✅ **SLA Monitoring**: Track pipeline execution time
✅ **Cost Monitoring**: Track warehouse usage

---

## Scaling & Performance

### Current Scale

| Metric | Current | Future (2 Seasons) |
|--------|---------|-------------------|
| **Sessions** | ~24/year | 48 |
| **Total Records** | 6M | 12M |
| **Bronze Storage** | ~5 GB | ~10 GB |
| **Gold Storage** | ~100 MB | ~200 MB |
| **Query Latency** | < 5 sec | Target < 10 sec |
| **Load Time** | 15-20 min | Target < 30 min |

### Performance Optimization

#### 1. Partitioning

**When to Partition**:
- Table > 1M rows
- Queries filter by partition key
- Time-series data

**Example**:
```sql
CREATE TABLE bronze_raw.bronze_locations (
    ...
)
USING DELTA
PARTITIONED BY (batch_day);
```

#### 2. Z-Ordering

**When to Z-Order**:
- Columns in WHERE/JOIN clauses
- High cardinality columns
- Run monthly or after major loads

```sql
OPTIMIZE bronze_raw.bronze_locations
ZORDER BY (session_key, driver_number);

-- Check effectiveness
DESCRIBE HISTORY bronze_raw.bronze_locations;
```

#### 3. File Compaction

```sql
-- Auto-compaction (recommended)
ALTER TABLE bronze_raw.bronze_laps
SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Manual optimize (if needed)
OPTIMIZE bronze_raw.bronze_laps;
```

#### 4. Photon Acceleration

**Enable in SQL Warehouse**:
- Go to SQL Warehouse settings
- Enable Photon
- 2-3x faster queries on large tables

#### 5. Caching

```sql
-- Cache frequently accessed tables
CACHE SELECT * FROM gold_analytics.session_leaderboard;

-- Check cache status
SHOW TABLES IN gold_analytics;
```

#### 6. Materialized Views (Future)

```sql
-- For frequently queried aggregations
CREATE MATERIALIZED VIEW mv_driver_season_stats AS
SELECT
    driver_number,
    driver_name,
    year,
    COUNT(DISTINCT session_key) as sessions,
    AVG(fastest_lap_time) as avg_fastest_lap,
    SUM(CASE WHEN position <= 3 THEN 1 ELSE 0 END) as podiums
FROM gold_analytics.session_leaderboard
GROUP BY driver_number, driver_name, year;
```

### Scaling Strategies

#### Vertical Scaling (SQL Warehouse)

| Workload | Size | Clusters |
|----------|------|----------|
| Development | X-Small | 1 |
| Staging | Small | 1-2 |
| Production (current) | Medium | 2-4 |
| Production (future 5 years) | Large | 2-8 |

#### Horizontal Scaling (Data Distribution)

```python
# For very large datasets, consider:

# 1. Read partitions in parallel
df = spark.read.format("delta") \
    .option("partitionKey", "batch_day") \
    .load("bronze_raw.bronze_locations")

# 2. Repartition for balanced processing
df = df.repartition(200, "session_key", "driver_number")

# 3. Optimize shuffle
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Best Practices

✅ **Monitor Query Performance**: Use query history to find slow queries
✅ **Partition Large Tables**: > 1M rows, time-series data
✅ **Z-Order Selectively**: On filter/join columns only
✅ **Enable Photon**: 2-3x speedup for analytics
✅ **Use Incremental Processing**: Silver layer only processes new data
✅ **Optimize File Sizes**: Target 128 MB - 1 GB per file
✅ **Cache Hot Tables**: Frequently accessed gold layer tables
✅ **Right-Size Warehouse**: Match compute to workload

---

## Governance & Security

### Unity Catalog Governance

#### Catalog Structure

```
Metastore
├── dev_f1_data_analytics
│   ├── bronze_raw (read_files, write_files)
│   ├── silver_clean (data_engineers)
│   └── gold_analytics (data_analysts)
├── stage_f1_data_analytics
│   └── [same structure]
└── prod_f1_data_analytics
    └── [same structure]
```

#### Role-Based Access Control

```sql
-- Data Engineers (full access to dev, read to stage/prod)
GRANT ALL PRIVILEGES ON CATALOG dev_f1_data_analytics TO data_engineers;
GRANT SELECT ON CATALOG stage_f1_data_analytics TO data_engineers;
GRANT SELECT ON CATALOG prod_f1_data_analytics TO data_engineers;

-- Data Analysts (read gold only)
GRANT SELECT ON SCHEMA prod_f1_data_analytics.gold_analytics TO data_analysts;
GRANT SELECT ON SCHEMA stage_f1_data_analytics.gold_analytics TO data_analysts;

-- Service Account (CI/CD)
GRANT ALL PRIVILEGES ON CATALOG stage_f1_data_analytics TO dbt_service_account;
GRANT ALL PRIVILEGES ON CATALOG prod_f1_data_analytics TO dbt_service_account;

-- Admin (full access)
GRANT ALL PRIVILEGES ON METASTORE TO admin_group;
```

#### Column-Level Security (if needed)

```sql
-- Mask sensitive data
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(LEFT(email, 3), '***@***.com');

-- Apply to specific columns
ALTER TABLE gold_analytics.dim_drivers
ALTER COLUMN contact_email SET MASK mask_email;
```

#### Row-Level Security (if needed)

```sql
-- Filter by team
CREATE OR REPLACE FUNCTION team_filter()
RETURNS STRING
RETURN CASE
    WHEN is_member('team_redbull') THEN "team_name = 'Red Bull Racing'"
    WHEN is_member('team_ferrari') THEN "team_name = 'Ferrari'"
    ELSE "1=1"  -- Admins see all
END;

ALTER TABLE gold_analytics.driver_session_summary
SET ROW FILTER team_filter();
```

### Data Quality & Lineage

#### Data Quality Rules

```yaml
# dbt schema.yml
models:
  - name: fact_laps
    meta:
      data_quality:
        freshness: 24 hours
        completeness: > 95%
        accuracy: lap_duration > 0
        consistency: calculated_lap_duration ~= lap_duration

    columns:
      - name: lap_duration
        meta:
          valid_range: [50, 300]  # seconds
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "lap_duration BETWEEN 50 AND 300"
```

#### Lineage Tracking

```sql
-- dbt automatically tracks lineage
-- View in dbt docs: dbt docs generate && dbt docs serve

-- Or query Unity Catalog lineage
SELECT
    source_table_full_name,
    target_table_full_name,
    source_type,
    created_at
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '%f1_data_analytics%'
ORDER BY created_at DESC;
```

### Best Practices

✅ **Least Privilege**: Grant minimum required permissions
✅ **Service Accounts**: Use for CI/CD, not personal tokens
✅ **Rotate Credentials**: Every 90 days
✅ **Audit Access**: Monitor system.access.audit table
✅ **Data Classification**: Tag sensitive data
✅ **Backup Strategy**: Unity Catalog handles versioning
✅ **Disaster Recovery**: Test restore procedures
✅ **Compliance**: Document data retention policies

---

## Operational Procedures

### Daily Operations

```bash
# 1. Check pipeline health
./check_pipeline_health.sh

# 2. Monitor data freshness
databricks sql -e "SELECT MAX(_ingestion_timestamp) FROM bronze_raw.bronze_sessions"

# 3. Review failed tests
cd dbt_f1_data_analytics && ./run_dbt.sh test

# 4. Check warehouse usage
# Go to Databricks SQL → Query History
```

### Weekly Operations

```bash
# 1. Review query performance
# SQL: SELECT * FROM system.query.history WHERE start_time >= current_date() - 7

# 2. Check storage growth
# DESCRIBE DETAIL bronze_raw.bronze_locations

# 3. Optimize large tables
databricks sql -e "OPTIMIZE bronze_raw.bronze_locations"

# 4. Review audit logs
# SQL: SELECT * FROM system.access.audit WHERE event_date >= current_date() - 7
```

### Monthly Operations

```bash
# 1. Vacuum old data
databricks sql -e "VACUUM bronze_raw.bronze_locations RETAIN 30 HOURS"
databricks sql -e "VACUUM bronze_raw.bronze_laps RETAIN 30 HOURS"

# 2. Review and adjust warehouse sizing
# Databricks SQL → Warehouses → View metrics

# 3. Z-order large tables
databricks sql -e "OPTIMIZE bronze_raw.bronze_locations ZORDER BY (session_key, driver_number)"

# 4. Review costs
# Account Console → Usage → SQL Warehouses

# 5. Update documentation
# Run: dbt docs generate && deploy to wiki
```

### Incident Response

```bash
# Problem: Pipeline failure

# 1. Check logs
cat dbt_f1_data_analytics/logs/dbt.log | tail -100

# 2. Check GitHub Actions
open https://github.com/rustyram07/openf1_data_analytics/actions

# 3. Validate source data
databricks sql -e "SELECT COUNT(*) FROM bronze_raw.bronze_sessions"

# 4. Re-run failed steps
cd dbt_f1_data_analytics
./run_dbt.sh build --select silver_laps+  # Re-run from failed model

# 5. If data corruption, use time travel
databricks sql -e "SELECT * FROM bronze_raw.bronze_laps VERSION AS OF 123"
```

---

## Idiosyncrasies & Gotchas

### Databricks-Specific

#### 1. Unity Catalog Schema Naming

**Issue**: Default schema names can be confusing

```sql
-- DON'T: Rely on default schema
USE dev_f1_data_analytics;
SELECT * FROM bronze_sessions;  -- Which schema?

-- DO: Always use fully qualified names
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions;
```

#### 2. Delta Lake Time Travel

**Issue**: Old versions consume storage

```sql
-- Check versions
DESCRIBE HISTORY bronze_raw.bronze_locations;

-- View old version
SELECT * FROM bronze_raw.bronze_locations VERSION AS OF 10;

-- But remember: VACUUM removes old versions!
VACUUM bronze_raw.bronze_locations RETAIN 30 HOURS;
-- ⚠️ Versions older than 30 hours are GONE
```

#### 3. Incremental Models Can Drift

**Issue**: Incremental state can become inconsistent

```bash
# Symptom: Duplicate records in silver layer

# Fix: Full refresh
./run_dbt.sh build --select silver_drivers --full-refresh

# Prevention: Test for duplicates
# tests/no_duplicates_silver_drivers.sql
```

#### 4. Partition Pruning

**Issue**: Partitions not pruned if filter uses function

```sql
-- BAD: No partition pruning
SELECT * FROM bronze_raw.bronze_laps
WHERE DATE(batch_day) = '2024-11-01';

-- GOOD: Partition pruning works
SELECT * FROM bronze_raw.bronze_laps
WHERE batch_day = '2024-11-01';
```

### dbt-Specific

#### 1. Sources Must Be Defined

**Issue**: Can't reference bronze tables without sources.yml

```sql
-- WON'T WORK
SELECT * FROM dev_f1_data_analytics.bronze_raw.bronze_sessions

-- MUST USE
SELECT * FROM {{ source('bronze', 'bronze_sessions') }}
```

#### 2. Incremental Strategy Matters

**Issue**: Wrong strategy = slow builds or data loss

```sql
-- APPEND: Fast, but no updates (use for immutable data)
materialized='incremental',
incremental_strategy='append'

-- MERGE: Slower, but handles updates (use for changing data)
materialized='incremental',
incremental_strategy='merge',
unique_key=['session_key', 'driver_number']
```

#### 3. Tests Run AFTER Build

**Issue**: Failing tests don't prevent model creation

```bash
# Models are built EVEN IF tests fail
./run_dbt.sh build

# Solution: Check return code in CI/CD
if [ $? -ne 0 ]; then
    echo "Tests failed!"
    exit 1
fi
```

### OpenF1 API-Specific

#### 1. Rate Limiting

**Issue**: 429 errors if exceed 100/sec

```python
# BAD: No rate limiting
for session in sessions:
    data = requests.get(f"https://api.openf1.org/v1/laps?session_key={session}")

# GOOD: Rate limited
rate_limiter = RateLimiter(100, 1)  # 100 per second
for session in sessions:
    rate_limiter.wait()
    data = requests.get(...)
```

#### 2. Location Data Size

**Issue**: 150K-200K records per session = slow transfers

```python
# DON'T: Load all at once
locations = requests.get("/locations?session_key=9158").json()  # Times out!

# DO: Use pagination or streaming
for chunk in paginate("/locations", session_key=9158, page_size=10000):
    process_chunk(chunk)
```

#### 3. Inconsistent Data Quality

**Issue**: Some sessions have missing sector times

```sql
-- Always check sector_completeness
SELECT
    sector_completeness,
    COUNT(*) as lap_count
FROM silver_clean.silver_laps
GROUP BY sector_completeness;

-- Results:
-- Complete: 400
-- Partial: 50
-- No Sectors: 100
```

### General Gotchas

#### 1. GitHub Secrets are Immutable

**Issue**: Can't update secrets, only recreate

```bash
# Can't edit existing secret
# Must delete and recreate:
# 1. Go to repo settings → Secrets
# 2. Delete old secret
# 3. Create new with same name
```

#### 2. Databricks Tokens Expire

**Issue**: CI/CD fails after 90 days

```bash
# Set calendar reminder to rotate tokens!
# Or use shorter-lived tokens for testing
```

#### 3. dbt Artifacts Can Conflict

**Issue**: Stale artifacts cause compilation errors

```bash
# Symptom: "Model not found" errors

# Fix: Clean and rebuild
cd dbt_f1_data_analytics
./run_dbt.sh clean
./run_dbt.sh deps
./run_dbt.sh build
```

---

## Summary Checklist

### Pre-Deployment
- [ ] All tests passing (117 tests)
- [ ] Documentation generated (`dbt docs generate`)
- [ ] Code reviewed and approved
- [ ] Tested in stage environment
- [ ] Secrets configured in GitHub
- [ ] Databricks catalogs created (dev/stage/prod)

### Production Operations
- [ ] Daily: Check pipeline health, data freshness
- [ ] Weekly: Review query performance, storage
- [ ] Monthly: Vacuum, optimize, Z-order
- [ ] Quarterly: Review costs, resize warehouses
- [ ] Rotate credentials every 90 days

### Monitoring
- [ ] Pipeline SLA: < 30 minutes
- [ ] Data freshness: < 48 hours
- [ ] Query latency: < 10 seconds (gold layer)
- [ ] Test pass rate: 100%
- [ ] Storage growth: < 10 GB/month

---

**Document Version**: 1.0
**Last Updated**: 2025-11-04
**Related**: [architecture.md](architecture.md), [logical_data_model.md](logical_data_model.md)
