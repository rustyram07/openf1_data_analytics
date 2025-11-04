# Databricks Schema Structure - F1 Data Analytics

## Catalog and Schema Naming Convention

**Important:** dbt combines the base schema from `profiles.yml` with the `+schema` config from `dbt_project.yml`:

```
Catalog: dev_f1_data_analytics
Base Schema: default (from profiles.yml)
Custom Schema: silver_clean or gold_analytics (from dbt_project.yml)
Result: default_silver_clean or default_gold_analytics
```

---

## Complete Table Structure

### Bronze Layer (Raw Data)
**Schema:** `dev_f1_data_analytics.bronze_raw`

| Table | Records | Description |
|-------|---------|-------------|
| `bronze_sessions` | 15 | Raw session data from OpenF1 API |
| `bronze_drivers` | 300 | Raw driver data from OpenF1 API |
| `bronze_laps` | 7,747 | Raw lap timing data from OpenF1 API |
| `bronze_locations` | 6,097,599 | Raw GPS telemetry from OpenF1 API |

---

### Silver Layer (Cleaned & Conformed)
**Schema:** `dev_f1_data_analytics.default_silver_clean`

| Table | Records | Materialization | Description |
|-------|---------|-----------------|-------------|
| `silver_sessions` | 15 | Incremental (merge) | Cleaned session data with business logic |
| `silver_drivers` | 300 | Incremental (merge) | Cleaned driver data with team categorization |
| `silver_laps` | 7,747 | Incremental (append) | Cleaned lap timing with calculated metrics |
| `silver_locations` | 6,097,599 | Incremental (append) | Cleaned GPS telemetry data |

**Key Features:**
- Deduplication using ROW_NUMBER()
- NULL handling with COALESCE
- Business logic (session categories, lap validation)
- Incremental processing (merge/append strategy)
- Metadata tracking (_ingestion_timestamp, updated_at)

---

### Gold Layer - Dimensions
**Schema:** `dev_f1_data_analytics.default_gold_analytics`

| Table | Records | Type | Description |
|-------|---------|------|-------------|
| `dim_sessions` | 15 | Dimension | Session master with enriched date dimensions |
| `dim_drivers` | ~20 | Dimension (SCD Type 2) | Driver master with team history tracking |

**dim_sessions** columns:
- session_dim_key (surrogate key)
- session_key, session_name, session_type, session_category
- date_start, date_end, session_duration_minutes
- location, country_code, country_name, circuit_key
- session_date, session_year, session_month, session_day_of_week, session_week_of_year, session_quarter
- is_race, is_qualifying, is_practice, is_sprint

**dim_drivers** columns:
- driver_key (surrogate key)
- driver_number, full_name, broadcast_name, name_acronym
- team_name, team_colour, team_category
- first_name, last_name, headshot_url, country_code
- valid_from, valid_to, is_current (SCD Type 2 fields)

---

### Gold Layer - Facts
**Schema:** `dev_f1_data_analytics.default_gold_analytics`

| Table | Records | Grain | Description |
|-------|---------|-------|-------------|
| `fact_laps` | 7,747 | One row per lap | Lap-level telemetry and timing data |

**fact_laps** columns:
- lap_key (surrogate key)
- session_dim_key, driver_key (foreign keys)
- session_key, meeting_key, driver_number, lap_number
- date_start, lap_duration
- duration_sector_1, duration_sector_2, duration_sector_3
- calculated_lap_duration, sector_duration_delta
- is_pit_out_lap, is_valid_lap, is_personal_best_lap
- intermediate_1_speed, intermediate_2_speed, speed_trap_speed, max_speed
- segments_sector_1, segments_sector_2, segments_sector_3, sector_completeness
- driver_lap_rank, session_lap_time_rank, driver_best_lap_time, delta_to_personal_best
- batch_day, session_name, updated_at

---

### Gold Layer - Marts (Analytics)
**Schema:** `dev_f1_data_analytics.default_gold_analytics`

| Table | Records | Description |
|-------|---------|-------------|
| `session_leaderboard` | ~300 | Driver rankings per session by fastest lap |
| `driver_session_summary` | ~300 | Aggregated driver performance per session |
| `team_performance` | ~100 | Team-level performance metrics per session |

#### session_leaderboard
Pre-aggregated leaderboard for each session showing driver rankings.

**Key Columns:**
- leaderboard_key (surrogate key)
- session_key, driver_number, driver_name, team_name
- position, team_position (rankings)
- fastest_lap_time, fastest_lap_number
- delta_to_fastest, gap_percentage
- is_fastest, is_top_3, is_top_10, is_fastest_in_team (boolean flags)

**Use Case:** Dashboard leaderboards, position analysis

#### driver_session_summary
Complete performance summary for each driver in each session.

**Key Columns:**
- summary_key (surrogate key)
- session_key, driver_number, driver_name, team_name
- total_laps, valid_laps, pit_out_laps, valid_lap_percentage
- fastest_lap_time, slowest_lap_time, avg_lap_time, lap_time_stddev, lap_time_range
- avg_sector_1_time, avg_sector_2_time, avg_sector_3_time
- best_sector_1_time, best_sector_2_time, best_sector_3_time, theoretical_best_lap
- top_speed, avg_speed
- complete_sector_laps, consistency_coefficient

**Use Case:** Driver performance analysis, consistency metrics

#### team_performance
Team-level aggregations across all drivers.

**Key Columns:**
- team_performance_key (surrogate key)
- session_key, team_name (team_category)
- drivers_count
- team_total_laps, team_valid_laps, team_valid_lap_percentage
- team_fastest_lap, team_avg_fastest_lap, intra_team_gap
- team_top_speed, team_avg_top_speed
- team_avg_consistency, team_avg_lap_stddev
- team_rank (position in session)

**Use Case:** Team comparisons, constructor championship analysis

---

## Query Examples

### Count all records in each layer

```sql
-- Bronze Layer
SELECT 'bronze_sessions' AS table_name, COUNT(*) AS count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT 'bronze_drivers', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
UNION ALL
SELECT 'bronze_laps', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_laps
UNION ALL
SELECT 'bronze_locations', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_locations
UNION ALL

-- Silver Layer
SELECT 'silver_sessions', COUNT(*)
FROM dev_f1_data_analytics.default_silver_clean.silver_sessions
UNION ALL
SELECT 'silver_drivers', COUNT(*)
FROM dev_f1_data_analytics.default_silver_clean.silver_drivers
UNION ALL
SELECT 'silver_laps', COUNT(*)
FROM dev_f1_data_analytics.default_silver_clean.silver_laps
UNION ALL
SELECT 'silver_locations', COUNT(*)
FROM dev_f1_data_analytics.default_silver_clean.silver_locations
UNION ALL

-- Gold Dimensions
SELECT 'dim_sessions', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions
UNION ALL
SELECT 'dim_drivers', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.dim_drivers
UNION ALL

-- Gold Facts
SELECT 'fact_laps', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.fact_laps
UNION ALL

-- Gold Marts
SELECT 'session_leaderboard', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
UNION ALL
SELECT 'driver_session_summary', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
UNION ALL
SELECT 'team_performance', COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.team_performance
ORDER BY table_name;
```

### Get session leaderboard for a specific race

```sql
SELECT
    position,
    driver_name,
    team_name,
    fastest_lap_time,
    delta_to_fastest,
    gap_percentage,
    is_top_3,
    is_fastest_in_team
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
WHERE session_name = 'Race' -- Adjust to actual session name
  AND session_category = 'Race'
ORDER BY position ASC;
```

### Get driver performance summary

```sql
SELECT
    session_name,
    session_date,
    driver_name,
    team_name,
    total_laps,
    valid_laps,
    fastest_lap_time,
    avg_lap_time,
    consistency_coefficient,
    top_speed
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
WHERE driver_name LIKE '%Verstappen%'  -- Adjust to actual driver
ORDER BY session_date DESC;
```

### Get team rankings

```sql
SELECT
    session_name,
    session_date,
    team_rank,
    team_name,
    drivers_count,
    team_fastest_lap,
    intra_team_gap,
    team_avg_consistency
FROM dev_f1_data_analytics.default_gold_analytics.team_performance
WHERE session_category = 'Race'
ORDER BY session_date DESC, team_rank ASC;
```

### Join dimensions with facts

```sql
SELECT
    s.session_name,
    s.location,
    s.session_date,
    d.full_name AS driver_name,
    d.team_name,
    f.lap_number,
    f.lap_duration,
    f.is_valid_lap,
    f.max_speed
FROM dev_f1_data_analytics.default_gold_analytics.fact_laps f
INNER JOIN dev_f1_data_analytics.default_gold_analytics.dim_sessions s
    ON f.session_dim_key = s.session_dim_key
INNER JOIN dev_f1_data_analytics.default_gold_analytics.dim_drivers d
    ON f.driver_key = d.driver_key
WHERE f.is_valid_lap = TRUE
  AND s.session_category = 'Qualifying'
ORDER BY f.lap_duration ASC
LIMIT 10;
```

---

## Data Lineage Flow

```
OpenF1 API
    ↓
Bronze Layer (bronze_raw schema)
    ├── bronze_sessions
    ├── bronze_drivers
    ├── bronze_laps
    └── bronze_locations
    ↓
Silver Layer (default_silver_clean schema)
    ├── silver_sessions ← dedup, business logic
    ├── silver_drivers ← dedup, team categorization
    ├── silver_laps ← lap validation, metrics
    └── silver_locations ← GPS data cleaning
    ↓
Gold Dimensions (default_gold_analytics schema)
    ├── dim_sessions ← enriched with date dimensions
    └── dim_drivers ← SCD Type 2 with history
    ↓
Gold Facts (default_gold_analytics schema)
    └── fact_laps ← with rankings and deltas
    ↓
Gold Marts (default_gold_analytics schema)
    ├── session_leaderboard ← pre-aggregated rankings
    ├── driver_session_summary ← performance metrics
    └── team_performance ← team aggregations
```

---

## dbt Commands

```bash
cd ~/Documents/openf1_data_analytics/dbt_f1_data_analytics

# Build everything
./run_dbt.sh build

# Build only silver layer
./run_dbt.sh build --select silver_clean

# Build only gold layer
./run_dbt.sh build --select gold_analytics

# Build only marts
./run_dbt.sh run --select session_leaderboard driver_session_summary team_performance

# Run tests
./run_dbt.sh test

# Generate documentation
./run_dbt.sh docs generate
./run_dbt.sh docs serve
```

---

## Schema Reference for Python/SQL

When querying from Python or writing SQL:

```python
# Correct schema references
BRONZE_SCHEMA = "dev_f1_data_analytics.bronze_raw"
SILVER_SCHEMA = "dev_f1_data_analytics.default_silver_clean"
GOLD_SCHEMA = "dev_f1_data_analytics.default_gold_analytics"

# Example query
query = f"""
SELECT *
FROM {GOLD_SCHEMA}.session_leaderboard
WHERE session_category = 'Race'
ORDER BY position
"""
```

---

**Last Updated:** 2025-11-04
**dbt Version:** 1.10.13
**Databricks:** Unity Catalog with Delta Tables
