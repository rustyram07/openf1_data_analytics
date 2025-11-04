# F1 Data Warehouse Architecture

**Comprehensive architecture documentation for the OpenF1 Data Analytics platform on Databricks**

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Technology Stack](#technology-stack)
4. [Data Architecture](#data-architecture)
5. [Storage Architecture](#storage-architecture)
6. [Processing Architecture](#processing-architecture)
7. [Security Architecture](#security-architecture)
8. [Deployment Architecture](#deployment-architecture)
9. [Integration Points](#integration-points)
10. [Design Decisions & Rationale](#design-decisions--rationale)

---

## Executive Summary

### Purpose
This document describes the architecture of the F1 Data Analytics platform - a production-grade data warehouse built on Databricks for analyzing Formula 1 telemetry and race data from the OpenF1 API.

### Key Characteristics

| Aspect | Implementation |
|--------|----------------|
| **Architecture Pattern** | Medallion (Bronze → Silver → Gold) |
| **Platform** | Databricks with Unity Catalog |
| **Storage Format** | Delta Lake (ACID transactions) |
| **Transformation Engine** | dbt (data build tool) |
| **Orchestration** | GitHub Actions + Dagster (optional) |
| **Data Volume** | ~200K records per session, 6M+ total |
| **Update Frequency** | On-demand (race weekends) |
| **Deployment Model** | Multi-environment (Dev/Stage/Prod) |

### Business Value

- **Real-time Insights**: Analyze race performance within minutes of session completion
- **Historical Analysis**: Track driver/team performance across seasons
- **Data Quality**: Automated testing and validation at every layer
- **Scalability**: Designed to handle full F1 season (24+ races)
- **Governance**: RBAC, audit logs, data lineage tracking

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   OpenF1 REST API          │
                    │   https://api.openf1.org   │
                    └─────────────┬──────────────┘
                                  │
┌─────────────────────────────────▼─────────────────────────────────┐
│                         INGESTION LAYER                            │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  Python API Extractor                                      │   │
│  │  • Rate limiting (100 calls/sec)                           │   │
│  │  • Session discovery & filtering                           │   │
│  │  • JSON file output                                        │   │
│  └────────────────────┬───────────────────────────────────────┘   │
│                       │                                             │
│  ┌────────────────────▼───────────────────────────────────────┐   │
│  │  DLT Batch Loader                                           │   │
│  │  • Batch JSON processing                                    │   │
│  │  • Schema inference                                         │   │
│  │  • Delta table creation                                     │   │
│  └────────────────────┬───────────────────────────────────────┘   │
└───────────────────────┼─────────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────────┐
│                    STORAGE LAYER (Databricks)                        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Unity Catalog: dev_f1_data_analytics                        │   │
│  │                 stage_f1_data_analytics                      │   │
│  │                 prod_f1_data_analytics                       │   │
│  │                                                              │   │
│  │  ┌────────────────────────────────────────────────────────┐ │   │
│  │  │ Bronze Layer (bronze_raw schema)                       │ │   │
│  │  │ • bronze_sessions    (1 per session)                   │ │   │
│  │  │ • bronze_drivers     (~20 per session)                 │ │   │
│  │  │ • bronze_laps        (500-600 per session)             │ │   │
│  │  │ • bronze_locations   (150K-200K per session)           │ │   │
│  │  │ Format: Delta Tables, Unoptimized                      │ │   │
│  │  └────────────────────────────────────────────────────────┘ │   │
│  │                                                              │   │
│  │  ┌────────────────────────────────────────────────────────┐ │   │
│  │  │ Silver Layer (silver_clean schema)                     │ │   │
│  │  │ • silver_sessions    (deduplicated)                    │ │   │
│  │  │ • silver_drivers     (cleaned, categorized)            │ │   │
│  │  │ • silver_laps        (validated, enriched)             │ │   │
│  │  │ • silver_locations   (GPS cleaned)                     │ │   │
│  │  │ Strategy: Incremental (merge/append)                   │ │   │
│  │  └────────────────────────────────────────────────────────┘ │   │
│  │                                                              │   │
│  │  ┌────────────────────────────────────────────────────────┐ │   │
│  │  │ Gold Layer (gold_analytics schema)                     │ │   │
│  │  │ Core Dimensions:                                       │ │   │
│  │  │ • dim_sessions       (session metadata)                │ │   │
│  │  │ • dim_drivers        (SCD Type 2)                      │ │   │
│  │  │                                                        │ │   │
│  │  │ Facts:                                                 │ │   │
│  │  │ • fact_laps          (lap-level metrics)               │ │   │
│  │  │                                                        │ │   │
│  │  │ Marts (Pre-aggregated):                               │ │   │
│  │  │ • session_leaderboard    (rankings)                   │ │   │
│  │  │ • driver_session_summary (performance)                │ │   │
│  │  │ • team_performance       (aggregations)               │ │   │
│  │  │ Strategy: Full refresh, optimized for queries         │ │   │
│  │  └────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼─────────────────────────────────┐
│                    TRANSFORMATION LAYER (dbt)                      │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  dbt Models (SQL)                                          │   │
│  │  • Jinja templating                                        │   │
│  │  • Incremental materialization                             │   │
│  │  │  • Data quality tests (117 tests)                        │   │
│  │  • Documentation & lineage                                 │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  dbt Snapshots (SCD Type 2)                                │   │
│  │  • driver_snapshot      (track driver changes)             │   │
│  │  • session_snapshot     (track session metadata)           │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼─────────────────────────────────┐
│                    ORCHESTRATION & CI/CD                           │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  GitHub Actions                                            │   │
│  │  • CI: Test on stage (PR to dev)                           │   │
│  │  • CD: Test on prod (PR to main)                           │   │
│  │  • Automated testing & validation                          │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  Dagster (Optional)                                        │   │
│  │  • Asset-based orchestration                               │   │
│  │  • Schedule-based runs                                     │   │
│  │  • Dependency management                                   │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼─────────────────────────────────┐
│                        CONSUMPTION LAYER                           │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  BI Tools                                                  │   │
│  │  • Databricks SQL Dashboards                               │   │
│  │  • Tableau / Power BI                                      │   │
│  │  • Custom dashboards                                       │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  ML/Analytics                                              │   │
│  │  • Databricks notebooks                                    │   │
│  │  • MLflow experiments                                      │   │
│  │  • Predictive models                                       │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  APIs & Applications                                       │   │
│  │  • REST APIs (read-only)                                   │   │
│  │  • Custom applications                                     │   │
│  │  • Data exports                                            │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Core Technologies

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Cloud Platform** | Databricks | Latest | Unified analytics platform |
| **Storage** | Delta Lake | 3.x | ACID-compliant data lake |
| **Catalog** | Unity Catalog | Latest | Centralized governance |
| **Transformation** | dbt | 1.10.13 | SQL-based transformations |
| **Python Runtime** | Python | 3.11 | API extraction & processing |
| **Orchestration** | GitHub Actions | Latest | CI/CD automation |
| **Optional Orchestration** | Dagster | Latest | Asset-based pipelines |
| **Version Control** | Git/GitHub | Latest | Source control |

### Python Libraries

```python
# Core dependencies
dbt-databricks>=1.10.0      # dbt adapter for Databricks
dbt-utils                   # dbt utility macros
requests>=2.31.0            # HTTP client
pandas>=2.0.0               # Data manipulation
databricks-sdk              # Databricks API client

# Data loading
dlt>=0.4.0                  # Data Load Tool

# Orchestration (optional)
dagster>=1.5.0              # Pipeline orchestration
dagster-webserver           # Dagster UI
dagster-shell               # Shell command ops
```

### SQL Warehouse Configuration

**Development:**
- Type: Serverless
- Size: X-Small
- Auto-stop: 10 minutes
- Scaling: 1-2 clusters

**Production:**
- Type: Pro
- Size: Medium
- Auto-stop: 30 minutes
- Scaling: 2-8 clusters
- Photon enabled

---

## Data Architecture

### Medallion Architecture Implementation

#### Bronze Layer: Raw Data (Landing Zone)

**Purpose**: Store raw data exactly as received from source with minimal transformation

**Characteristics**:
- Schema-on-read (flexible schema)
- Full history retained
- Immutable (append-only)
- Includes metadata (_ingestion_timestamp)

**Tables**:

| Table | Records/Session | Update Pattern | Partitioning |
|-------|----------------|----------------|--------------|
| `bronze_sessions` | 1 | Append | None |
| `bronze_drivers` | ~20 | Append | None |
| `bronze_laps` | 500-600 | Append | batch_day |
| `bronze_locations` | 150K-200K | Append | batch_day |

**Design Decisions**:
- No data quality checks at bronze
- Preserve source data types
- Include all source fields
- Add ingestion metadata only

#### Silver Layer: Cleaned & Conformed

**Purpose**: Business-level data quality and conformance

**Characteristics**:
- Deduplication applied
- NULL handling with COALESCE
- Business rules enforced
- Incremental processing
- Type 1 SCD (overwrite)

**Tables**:

| Table | Strategy | Unique Key | Merge Columns |
|-------|----------|------------|---------------|
| `silver_sessions` | Incremental (merge) | session_key | session_name, location, updated_at |
| `silver_drivers` | Incremental (merge) | [session_key, driver_number] | team_name, team_colour, updated_at |
| `silver_laps` | Incremental (append) | [session_key, driver_number, lap_number] | N/A |
| `silver_locations` | Incremental (append) | [session_key, driver_number, date] | N/A |

**Data Quality Rules**:
- Remove duplicate records (ROW_NUMBER partitioning)
- Validate required fields (NOT NULL tests)
- Apply business logic (e.g., valid lap indicators)
- Standardize naming (team categorization)
- Calculate derived fields (lap_duration deltas)

#### Gold Layer: Analytics-Ready

**Purpose**: Optimized for analytical queries and reporting

**Characteristics**:
- Star/Snowflake schema
- Pre-aggregated where beneficial
- Surrogate keys for dimensions
- Type 2 SCD for history tracking
- Optimized for query performance

**Core Tables**:

**Dimensions:**
| Table | Type | Grain | Records/Session |
|-------|------|-------|-----------------|
| `dim_sessions` | Standard | One row per session | 1 |
| `dim_drivers` | SCD Type 2 | One row per driver change | ~20 |

**Facts:**
| Table | Grain | Records/Session | Measures |
|-------|-------|-----------------|----------|
| `fact_laps` | One row per lap | 500-600 | lap_duration, sector_times, speeds, rankings |

**Marts (Aggregated):**
| Table | Purpose | Records/Session | Refresh |
|-------|---------|-----------------|---------|
| `session_leaderboard` | Driver rankings | ~20 | Full |
| `driver_session_summary` | Performance metrics | ~20 | Full |
| `team_performance` | Team aggregations | ~10 | Full |

---

## Storage Architecture

### Delta Lake Configuration

#### Table Properties

**Bronze Tables:**
```sql
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
)
```

**Silver Tables:**
```sql
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver'
)
```

**Gold Tables:**
```sql
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'quality' = 'gold'
)
```

#### Partitioning Strategy

| Table | Partition Key | Reasoning |
|-------|---------------|-----------|
| `bronze_laps` | batch_day | Large table, time-based queries common |
| `bronze_locations` | batch_day | Largest table (6M+ rows), essential for performance |
| `silver_laps` | batch_day | Inherited from bronze, supports time travel |
| `silver_locations` | batch_day | Inherited from bronze, query optimization |

#### Z-Ordering

Planned for future optimization:

```sql
OPTIMIZE prod_f1_data_analytics.bronze_raw.bronze_locations
ZORDER BY (session_key, driver_number, date);

OPTIMIZE prod_f1_data_analytics.silver_clean.silver_laps
ZORDER BY (session_key, driver_number, lap_number);
```

### Unity Catalog Structure

```
Unity Catalog Metastore
│
├── dev_f1_data_analytics (Catalog)
│   ├── bronze_raw (Schema)
│   │   ├── bronze_sessions
│   │   ├── bronze_drivers
│   │   ├── bronze_laps
│   │   └── bronze_locations
│   ├── silver_clean (Schema)
│   │   ├── silver_sessions
│   │   ├── silver_drivers
│   │   ├── silver_laps
│   │   ├── silver_locations
│   │   ├── driver_snapshot
│   │   └── session_snapshot
│   └── gold_analytics (Schema)
│       ├── dim_sessions
│       ├── dim_drivers
│       ├── fact_laps
│       ├── session_leaderboard
│       ├── driver_session_summary
│       └── team_performance
│
├── stage_f1_data_analytics (Catalog)
│   └── [Same structure as dev]
│
└── prod_f1_data_analytics (Catalog)
    └── [Same structure as dev]
```

---

## Processing Architecture

### Data Flow

#### 1. Extraction (Python → JSON)

```python
# API Client with rate limiting
class OpenF1Client:
    def __init__(self):
        self.base_url = "https://api.openf1.org/v1"
        self.rate_limiter = RateLimiter(100, 1)  # 100 calls/sec

    def get_sessions(self, year=2024):
        # Returns list of session metadata

    def get_session_data(self, session_key):
        # Parallel requests for:
        # - drivers
        # - laps
        # - locations
```

**Output**: JSON files in `local_test_data/`
- `sessions_YYYYMMDD_HHMMSS.json`
- `drivers_sessionkey_YYYYMMDD_HHMMSS.json`
- `laps_sessionkey_YYYYMMDD_HHMMSS.json`
- `locations_sessionkey_YYYYMMDD_HHMMSS.json`

#### 2. Loading (JSON → Bronze Delta)

```python
# DLT Batch Loader
def load_bronze_tables():
    for entity in ['sessions', 'drivers', 'laps', 'locations']:
        json_files = get_json_files(entity)
        df = spark.read.json(json_files)
        df = df.withColumn("_ingestion_timestamp", current_timestamp())

        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(f"bronze_raw.bronze_{entity}")
```

#### 3. Transformation (dbt Bronze → Silver → Gold)

**dbt Execution Graph**:
```
sources:bronze_sessions
    ↓
silver_sessions
    ↓
dim_sessions ← ─ ─ ┐
    ↓              │
session_leaderboard│
                   │
sources:bronze_drivers
    ↓              │
silver_drivers     │
    ↓              │
dim_drivers ← ─ ─ ─┘
    ↓
driver_session_summary
    ↓
team_performance

sources:bronze_laps
    ↓
silver_laps
    ↓
fact_laps
    ↓
[connects to marts above]
```

**Execution Order**:
1. Sources validation
2. Silver layer (parallel where possible)
3. Gold dimensions
4. Gold facts
5. Gold marts
6. Snapshots
7. Tests

---

## Security Architecture

### Authentication & Authorization

#### Unity Catalog RBAC

**Roles**:

| Role | Permissions | Use Case |
|------|-------------|----------|
| **data_engineers** | ALL on dev catalog, READ on stage/prod | Development & testing |
| **data_analysts** | READ on gold schemas (all catalogs) | Analytics & reporting |
| **dbt_service_account** | ALL on all catalogs | CI/CD automation |
| **admin** | ALL on metastore | Administration |

**Grants Example**:
```sql
-- Data Engineers
GRANT ALL PRIVILEGES ON CATALOG dev_f1_data_analytics TO data_engineers;
GRANT SELECT ON CATALOG stage_f1_data_analytics TO data_engineers;
GRANT SELECT ON CATALOG prod_f1_data_analytics TO data_engineers;

-- Data Analysts
GRANT SELECT ON SCHEMA prod_f1_data_analytics.gold_analytics TO data_analysts;

-- Service Account (CI/CD)
GRANT ALL PRIVILEGES ON CATALOG stage_f1_data_analytics TO dbt_service_account;
GRANT ALL PRIVILEGES ON CATALOG prod_f1_data_analytics TO dbt_service_account;
```

### Secrets Management

#### GitHub Secrets (CI/CD)

Stored in repository settings:
- `DATABRICKS_HOST` - Workspace URL
- `DATABRICKS_HTTP_PATH` - SQL Warehouse endpoint
- `DATABRICKS_TOKEN` - Service account token

#### Databricks Secrets

For production ETL jobs:
```bash
# Create secret scope
databricks secrets create-scope --scope openf1_api

# Add API credentials if needed
databricks secrets put --scope openf1_api --key api_token
```

### Data Access Patterns

**Development Environment**:
- Direct access via SQL Warehouse
- dbt runs from local machine
- Personal access tokens

**Production Environment**:
- Service account authentication only
- No direct user access to raw data
- All changes via CI/CD pipeline

---

## Deployment Architecture

### Environment Strategy

| Environment | Catalog | Branch | Purpose | Data |
|------------|---------|--------|---------|------|
| **Development** | `dev_f1_data_analytics` | `dev` | Feature development | Sample/test data |
| **Staging** | `stage_f1_data_analytics` | `dev` (CI) | Integration testing | Copy of prod |
| **Production** | `prod_f1_data_analytics` | `main` | Production workloads | Full dataset |

### CI/CD Pipeline

```
Developer → Feature Branch → PR to dev
                               ↓
                          CI Pipeline
                               ↓
                    dbt build --target stage
                               ↓
                         Tests Pass?
                               ↓
                          Merge to dev
                               ↓
                   Ready for Production?
                               ↓
                        PR dev → main
                               ↓
                          CD Pipeline
                               ↓
                    dbt build --target prod
                               ↓
                         Tests Pass?
                               ↓
                          Merge to main
                               ↓
                    Production Deployed!
```

### Deployment Workflow

**Daily Development:**
1. Developer creates feature branch
2. Makes dbt model changes
3. Tests locally: `./run_dbt.sh build`
4. Commits and pushes
5. Creates PR to dev
6. **Automated**: CI runs against stage
7. **Automated**: Results posted as PR comment
8. Code review + approval
9. Merge to dev

**Production Deployment:**
1. Create PR from dev to main
2. **Automated**: CD runs against prod
3. Manual review of test results
4. Final approval
5. Merge to main
6. Production updated

---

## Integration Points

### External Systems

#### OpenF1 API

**Endpoint**: `https://api.openf1.org/v1`

**Rate Limits**: 100 requests/second

**Key Endpoints**:
- `/sessions` - List all sessions
- `/sessions/{session_key}` - Session details
- `/drivers?session_key={key}` - Drivers in session
- `/laps?session_key={key}` - Lap data
- `/location?session_key={key}` - GPS telemetry

**Authentication**: None (public API)

**Error Handling**:
- Exponential backoff on rate limits
- Retry on transient errors (3 attempts)
- Graceful degradation (skip session on failure)

#### Databricks SQL Warehouse

**Connection**:
- Protocol: HTTPS
- Authentication: Personal Access Token
- Driver: databricks-sql-connector

**Configuration**:
```yaml
type: databricks
host: dbc-6095b2ae-cb87.cloud.databricks.com
http_path: /sql/1.0/warehouses/b4c47257d66c0fe9
catalog: {dev|stage|prod}_f1_data_analytics
schema: default
```

### Internal Components

#### dbt → Databricks

**Adapter**: `dbt-databricks`

**Connection Flow**:
1. dbt reads `profiles.yml`
2. Connects to SQL Warehouse
3. Executes compiled SQL
4. Materializes models as Delta tables
5. Runs tests
6. Generates documentation

#### GitHub Actions → Databricks

**Workflow**:
1. Checkout code
2. Install dbt
3. Set environment variables from secrets
4. Run dbt commands
5. Upload artifacts
6. Post PR comments

---

## Design Decisions & Rationale

### Why Medallion Architecture?

**Decision**: Implement Bronze → Silver → Gold layers

**Rationale**:
- ✅ Separation of concerns (raw vs. cleaned vs. analytics)
- ✅ Reprocessing capability (recreate silver/gold from bronze)
- ✅ Performance optimization at each layer
- ✅ Clear data quality boundaries
- ✅ Standard Databricks pattern

**Alternatives Considered**:
- Single layer: Too simple, no reprocessing
- Lambda architecture: Too complex for batch workloads

### Why Delta Lake?

**Decision**: Use Delta Lake for all storage

**Rationale**:
- ✅ ACID transactions (data consistency)
- ✅ Time travel (audit & debugging)
- ✅ Schema evolution (flexible changes)
- ✅ Compaction & optimization
- ✅ Native Databricks integration

**Alternatives Considered**:
- Parquet: No ACID, no time travel
- Iceberg: Not native to Databricks

### Why dbt?

**Decision**: Use dbt for transformations

**Rationale**:
- ✅ SQL-based (accessible to analysts)
- ✅ Built-in testing framework
- ✅ Documentation generation
- ✅ Incremental materialization
- ✅ Version control friendly
- ✅ Large community & packages

**Alternatives Considered**:
- Spark SQL scripts: No testing, no docs
- Databricks SQL: Limited orchestration
- Airflow: Over-engineered for this use case

### Why Incremental Processing (Silver)?

**Decision**: Use incremental merge for sessions/drivers, append for laps/locations

**Rationale**:
- ✅ Performance: Only process new data
- ✅ Cost: Fewer compute resources
- ✅ Scalability: Handles growing datasets
- ✅ Flexibility: Full refresh available if needed

**Trade-offs**:
- ❌ Complexity: More logic than full refresh
- ❌ State management: Must track processed data

### Why Full Refresh (Gold)?

**Decision**: Full refresh gold layer on each run

**Rationale**:
- ✅ Simplicity: No complex merge logic
- ✅ Consistency: Always correct aggregations
- ✅ Performance: Gold tables are small
- ✅ Reliability: No incremental bugs

**When to Switch to Incremental**:
- Gold tables exceed 10M rows
- Query performance degrades
- Full refresh takes > 30 minutes

### Why Type 2 SCD for Drivers?

**Decision**: Implement slowly changing dimension for driver history

**Rationale**:
- ✅ Track team changes mid-season
- ✅ Historical analysis accuracy
- ✅ Audit trail for changes

**Implementation**:
```sql
-- dbt snapshot
{% snapshot driver_snapshot %}
    {{
        config(
            target_schema='silver_clean',
            unique_key='driver_number',
            strategy='timestamp',
            updated_at='updated_at'
        )
    }}
    SELECT * FROM {{ ref('silver_drivers') }}
{% endsnapshot %}
```

### Why Partitioning on batch_day?

**Decision**: Partition large tables by batch_day

**Rationale**:
- ✅ Query performance (time-based filters)
- ✅ Maintenance (VACUUM old partitions)
- ✅ Cost (scan less data)

**Tables Partitioned**:
- bronze_laps (600 rows/session × 24 races = 14K rows)
- bronze_locations (200K rows/session × 24 races = 4.8M rows)

**When to Add More Partitions**:
- Multiple seasons in warehouse
- Queries always filter by season/year
- Partition pruning shows benefits

---

## Next Steps & Future Enhancements

### Phase 2 Enhancements

1. **Real-time Streaming**
   - Implement DLT streaming pipelines
   - Process location data in real-time during races
   - Near real-time dashboard updates

2. **Advanced Analytics**
   - Predictive models (race outcomes)
   - Driver performance clustering
   - Strategy optimization

3. **Additional Data Sources**
   - Weather data integration
   - Tire strategy data
   - Historical F1 results

4. **Performance Optimization**
   - Implement Z-ordering
   - Optimize large table queries
   - Add materialized views

5. **Enhanced Governance**
   - Data quality metrics dashboard
   - Automated data profiling
   - PII detection & masking

---

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [OpenF1 API Documentation](https://openf1.org/)

---

**Document Version**: 1.0
**Last Updated**: 2025-11-04
**Next Review**: 2025-12-01
