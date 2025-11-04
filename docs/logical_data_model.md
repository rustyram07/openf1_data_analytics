# Logical Data Model

**Comprehensive data model documentation for the F1 Data Warehouse**

---

## Table of Contents

1. [Overview](#overview)
2. [Entity Relationship Diagrams](#entity-relationship-diagrams)
3. [Data Dictionary](#data-dictionary)
4. [Relationships & Joins](#relationships--joins)
5. [Data Types & Constraints](#data-types--constraints)
6. [Slowly Changing Dimensions](#slowly-changing-dimensions)
7. [Derived Attributes](#derived-attributes)

---

## Overview

### Modeling Approach

| Aspect | Implementation |
|--------|----------------|
| **Schema Type** | Star Schema with some snowflaking |
| **Grain** | Lap-level for facts, session/driver for dimensions |
| **Surrogate Keys** | Yes (dbt_utils.generate_surrogate_key) |
| **SCD Type** | Type 2 for dim_drivers, Type 1 for others |
| **Normalization** | Denormalized for query performance |

### Layer Models

```
Bronze (4 tables) → Silver (4 tables) → Gold (6 tables + 2 snapshots)
       Raw              Cleaned            Analytics-Ready
```

---

## Entity Relationship Diagrams

### Gold Layer - Star Schema

```
                              ┌──────────────────────┐
                              │   dim_sessions       │
                              ├──────────────────────┤
                              │ PK: session_dim_key  │
                              │     session_key      │
                              │     session_name     │
                              │     session_type     │
                              │     location         │
                              │     date_start       │
                              │     circuit_key      │
                              │     country_code     │
                              │     is_race          │
                              │     is_qualifying    │
                              └──────────────────────┘
                                        │
                                        │ 1
                                        │
                                        │ M
                              ┌──────────────────────┐
                              │    fact_laps         │
                              ├──────────────────────┤
                              │ PK: lap_key          │
                              │ FK: session_dim_key  │
                              │ FK: driver_key       │
                              │     session_key      │
                              │     driver_number    │
                              │     lap_number       │
                              │     lap_duration     │
                              │     sector_1/2/3     │
                              │     speeds           │
                              │     is_valid_lap     │
                              │     rankings         │
                              └──────────────────────┘
                                        │
                                        │ M
                                        │
                                        │ 1
                              ┌──────────────────────┐
                              │   dim_drivers        │
                              ├──────────────────────┤
                              │ PK: driver_key       │
                              │     driver_number    │
                              │     full_name        │
                              │     team_name        │
                              │     team_colour      │
                              │     country_code     │
                              │     valid_from       │
                              │     valid_to         │
                              │     is_current       │
                              └──────────────────────┘


            ┌─────────────────────────────────────┐
            │    Mart: session_leaderboard        │
            ├─────────────────────────────────────┤
            │ From: fact_laps + dimensions        │
            │      Position ranking per session   │
            │      Pre-aggregated for BI          │
            └─────────────────────────────────────┘

            ┌─────────────────────────────────────┐
            │    Mart: driver_session_summary     │
            ├─────────────────────────────────────┤
            │ From: fact_laps + dimensions        │
            │      Driver metrics per session     │
            │      Performance indicators         │
            └─────────────────────────────────────┘

            ┌─────────────────────────────────────┐
            │    Mart: team_performance           │
            ├─────────────────────────────────────┤
            │ From: driver_session_summary        │
            │      Team-level aggregations        │
            │      Comparative metrics            │
            └─────────────────────────────────────┘
```

### Data Lineage Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
│   OpenF1    │───▶│   Bronze    │───▶│   Silver    │───▶│     Gold     │
│     API     │    │     Raw     │    │   Cleaned   │    │  Analytics   │
└─────────────┘    └─────────────┘    └─────────────┘    └──────────────┘
                           │                   │                   │
                           │                   │                   │
                    ┌──────┴──────┐    ┌───────┴──────┐   ┌───────┴───────┐
                    │ 4 tables    │    │  4 tables    │   │   6 tables    │
                    │ Append-only │    │  Incremental │   │   Full refresh│
                    │ Immutable   │    │  Validated   │   │   Optimized   │
                    └─────────────┘    └──────────────┘   └───────────────┘
```

---

## Data Dictionary

### Bronze Layer

#### bronze_sessions

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| session_key | BIGINT | NO | Unique identifier for session |
| session_name | STRING | YES | Name (e.g., "Race", "Qualifying") |
| session_type | STRING | YES | Type of session |
| date_start | TIMESTAMP | YES | Session start time |
| date_end | TIMESTAMP | YES | Session end time |
| gmt_offset | STRING | YES | GMT offset for location |
| meeting_key | BIGINT | YES | Meeting identifier |
| location | STRING | YES | Circuit location |
| country_key | BIGINT | YES | Country identifier |
| country_code | STRING | YES | ISO country code |
| country_name | STRING | YES | Full country name |
| circuit_key | BIGINT | YES | Circuit identifier |
| circuit_short_name | STRING | YES | Short circuit name |
| year | BIGINT | YES | Year of session |
| _ingestion_timestamp | TIMESTAMP | NO | When data was ingested |

**Primary Key**: session_key
**Partitioning**: None
**Avg Records/Load**: 1

#### bronze_drivers

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| session_key | BIGINT | NO | Session identifier |
| meeting_key | BIGINT | YES | Meeting identifier |
| driver_number | BIGINT | NO | Driver's racing number |
| broadcast_name | STRING | YES | Name for broadcasting |
| full_name | STRING | YES | Driver's full name |
| name_acronym | STRING | YES | Three-letter acronym (e.g., "VER") |
| team_name | STRING | YES | Team name |
| team_colour | STRING | YES | Team color (hex) |
| first_name | STRING | YES | Driver's first name |
| last_name | STRING | YES | Driver's last name |
| headshot_url | STRING | YES | URL to driver photo |
| country_code | STRING | YES | Driver's country |
| _ingestion_timestamp | TIMESTAMP | NO | When data was ingested |

**Composite Key**: (session_key, driver_number)
**Partitioning**: None
**Avg Records/Load**: 20

#### bronze_laps

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| session_key | BIGINT | NO | Session identifier |
| meeting_key | BIGINT | YES | Meeting identifier |
| driver_number | BIGINT | NO | Driver's racing number |
| lap_number | BIGINT | NO | Lap number in session |
| date_start | TIMESTAMP | YES | Lap start timestamp |
| lap_duration | DOUBLE | YES | Total lap time (seconds) |
| is_pit_out_lap | BOOLEAN | YES | Pit out lap indicator |
| duration_sector_1 | DOUBLE | YES | Sector 1 time (seconds) |
| duration_sector_2 | DOUBLE | YES | Sector 2 time (seconds) |
| duration_sector_3 | DOUBLE | YES | Sector 3 time (seconds) |
| i1_speed | DOUBLE | YES | Intermediate 1 speed (km/h) |
| i2_speed | DOUBLE | YES | Intermediate 2 speed (km/h) |
| st_speed | DOUBLE | YES | Speed trap speed (km/h) |
| segments_sector_1 | ARRAY<INT> | YES | Segment data for sector 1 |
| segments_sector_2 | ARRAY<INT> | YES | Segment data for sector 2 |
| segments_sector_3 | ARRAY<INT> | YES | Segment data for sector 3 |
| batch_day | DATE | NO | Partition key |
| session_name | STRING | YES | Session name |
| _ingestion_timestamp | TIMESTAMP | NO | When data was ingested |

**Composite Key**: (session_key, driver_number, lap_number)
**Partitioning**: batch_day
**Avg Records/Load**: 500-600

#### bronze_locations

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| session_key | BIGINT | NO | Session identifier |
| meeting_key | BIGINT | YES | Meeting identifier |
| driver_number | BIGINT | NO | Driver's racing number |
| date | TIMESTAMP | NO | GPS point timestamp |
| x | DOUBLE | NO | X coordinate |
| y | DOUBLE | NO | Y coordinate |
| z | DOUBLE | YES | Z coordinate (elevation) |
| batch_day | DATE | NO | Partition key |
| session_name | STRING | YES | Session name |
| _ingestion_timestamp | TIMESTAMP | NO | When data was ingested |

**Composite Key**: (session_key, driver_number, date)
**Partitioning**: batch_day
**Avg Records/Load**: 150,000-200,000

---

### Silver Layer

#### silver_sessions

| Column | Type | Nullable | Description | Transformation |
|--------|------|----------|-------------|----------------|
| session_key | BIGINT | NO | Unique identifier | From bronze |
| session_name | STRING | NO | Session name | COALESCE with 'Unknown' |
| session_type | STRING | NO | Session type | COALESCE with session_name |
| date_start | TIMESTAMP | YES | Start timestamp | From bronze |
| date_end | TIMESTAMP | YES | End timestamp | From bronze |
| session_duration_minutes | DOUBLE | YES | Duration in minutes | **CALCULATED**: (date_end - date_start) / 60 |
| session_category | STRING | NO | Categorized type | **CALCULATED**: CASE statement |
| gmt_offset | STRING | YES | GMT offset | From bronze |
| meeting_key | BIGINT | YES | Meeting identifier | From bronze |
| location | STRING | NO | Location name | COALESCE with 'Unknown' |
| country_key | BIGINT | YES | Country identifier | From bronze |
| country_code | STRING | YES | Country code | From bronze |
| country_name | STRING | NO | Country name | COALESCE with 'Unknown' |
| circuit_key | BIGINT | YES | Circuit identifier | From bronze |
| circuit_short_name | STRING | YES | Circuit short name | From bronze |
| year | BIGINT | YES | Year | From bronze |
| _ingestion_timestamp | TIMESTAMP | NO | Ingestion time | From bronze |
| updated_at | TIMESTAMP | NO | Last update time | CURRENT_TIMESTAMP() |

**Business Rules**:
- Deduplicate using ROW_NUMBER() by session_key
- session_category derived from session_name pattern matching
- All critical fields have COALESCE defaults

#### silver_drivers

| Column | Type | Nullable | Description | Transformation |
|--------|------|----------|-------------|----------------|
| session_key | BIGINT | NO | Session identifier | From bronze |
| meeting_key | BIGINT | YES | Meeting identifier | From bronze |
| driver_number | BIGINT | NO | Racing number | From bronze |
| broadcast_name | STRING | NO | Display name | COALESCE(broadcast_name, full_name, CONCAT) |
| full_name | STRING | NO | Full name | COALESCE(full_name, broadcast_name) |
| name_acronym | STRING | YES | Three-letter code | From bronze |
| team_name | STRING | NO | Team name | COALESCE with 'Unknown Team' |
| team_colour | STRING | NO | Team color | COALESCE with '000000' |
| team_category | STRING | NO | Standardized team | **CALCULATED**: CASE statement |
| first_name | STRING | YES | First name | From bronze |
| last_name | STRING | YES | Last name | From bronze |
| headshot_url | STRING | YES | Photo URL | From bronze |
| country_code | STRING | YES | Country | From bronze |
| _ingestion_timestamp | TIMESTAMP | NO | Ingestion time | From bronze |
| updated_at | TIMESTAMP | NO | Last update | CURRENT_TIMESTAMP() |

**Business Rules**:
- Deduplicate using ROW_NUMBER() by (session_key, driver_number)
- team_category standardizes team names for reporting
- broadcast_name has fallback logic for display

#### silver_laps

| Column | Type | Nullable | Description | Transformation |
|--------|------|----------|-------------|----------------|
| session_key | BIGINT | NO | Session identifier | From bronze |
| meeting_key | BIGINT | YES | Meeting identifier | From bronze |
| driver_number | BIGINT | NO | Racing number | From bronze |
| lap_number | BIGINT | NO | Lap number | From bronze |
| date_start | TIMESTAMP | YES | Lap start time | From bronze |
| lap_duration | DOUBLE | YES | Total lap time | From bronze |
| is_pit_out_lap | BOOLEAN | NO | Pit out indicator | COALESCE with FALSE |
| duration_sector_1 | DOUBLE | YES | Sector 1 time | From bronze |
| duration_sector_2 | DOUBLE | YES | Sector 2 time | From bronze |
| duration_sector_3 | DOUBLE | YES | Sector 3 time | From bronze |
| intermediate_1_speed | DOUBLE | YES | I1 speed | Renamed from i1_speed |
| intermediate_2_speed | DOUBLE | YES | I2 speed | Renamed from i2_speed |
| speed_trap_speed | DOUBLE | YES | ST speed | Renamed from st_speed |
| segments_sector_1 | ARRAY<INT> | YES | Sector 1 segments | From bronze |
| segments_sector_2 | ARRAY<INT> | YES | Sector 2 segments | From bronze |
| segments_sector_3 | ARRAY<INT> | YES | Sector 3 segments | From bronze |
| calculated_lap_duration | DOUBLE | YES | Sum of sectors | **CALCULATED**: SUM(sector_1/2/3) |
| sector_duration_delta | DOUBLE | YES | Lap vs sectors delta | **CALCULATED**: lap_duration - calculated |
| is_valid_lap | BOOLEAN | NO | Lap validity flag | **CALCULATED**: Complex CASE |
| sector_completeness | STRING | NO | Sector data status | **CALCULATED**: 'Complete'/'Partial'/'No Sectors' |
| max_speed | DOUBLE | YES | Maximum speed | **CALCULATED**: GREATEST(i1, i2, st) |
| batch_day | DATE | NO | Partition key | From bronze |
| session_name | STRING | YES | Session name | From bronze |
| _ingestion_timestamp | TIMESTAMP | NO | Ingestion time | From bronze |
| updated_at | TIMESTAMP | NO | Last update | CURRENT_TIMESTAMP() |

**Business Rules**:
- is_valid_lap = FALSE if: lap_duration NULL, is_pit_out_lap TRUE, duration < 10s or > 300s
- Calculates sector sum and compares to reported lap_duration
- All records appended (no deduplication)

#### silver_locations

| Column | Type | Nullable | Description | Transformation |
|--------|------|----------|-------------|----------------|
| session_key | BIGINT | NO | Session identifier | From bronze |
| meeting_key | BIGINT | YES | Meeting identifier | From bronze |
| driver_number | BIGINT | NO | Racing number | From bronze |
| date | TIMESTAMP | NO | GPS timestamp | From bronze |
| position_x | DOUBLE | NO | X coordinate | Renamed from x |
| position_y | DOUBLE | NO | Y coordinate | Renamed from y |
| position_z | DOUBLE | NO | Z coordinate | COALESCE(z, 0) |
| distance_from_origin | DOUBLE | YES | Distance calc | **CALCULATED**: SQRT(x² + y²) |
| batch_day | DATE | NO | Partition key | From bronze |
| session_name | STRING | YES | Session name | From bronze |
| _ingestion_timestamp | TIMESTAMP | NO | Ingestion time | From bronze |
| updated_at | TIMESTAMP | NO | Last update | CURRENT_TIMESTAMP() |

**Business Rules**:
- position_z defaults to 0 if NULL
- distance_from_origin calculated for track position analysis
- All records appended (no deduplication)

---

### Gold Layer - Core

#### dim_sessions

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| session_dim_key | STRING | NO | Surrogate key (PK) | **CALCULATED**: hash(session_key) |
| session_key | BIGINT | NO | Natural key | From silver_sessions |
| session_name | STRING | NO | Session name | From silver_sessions |
| session_type | STRING | NO | Session type | From silver_sessions |
| session_category | STRING | NO | Categorized type | From silver_sessions |
| date_start | TIMESTAMP | YES | Start time | From silver_sessions |
| date_end | TIMESTAMP | YES | End time | From silver_sessions |
| session_duration_minutes | DOUBLE | YES | Duration | From silver_sessions |
| session_date | DATE | YES | Date portion | **CALCULATED**: DATE(date_start) |
| session_year | INT | YES | Year | **CALCULATED**: YEAR(date_start) |
| session_month | INT | YES | Month | **CALCULATED**: MONTH(date_start) |
| session_day_of_week | INT | YES | Day of week | **CALCULATED**: DAYOFWEEK(date_start) |
| session_week_of_year | INT | YES | Week number | **CALCULATED**: WEEKOFYEAR(date_start) |
| session_quarter | INT | YES | Quarter | **CALCULATED**: QUARTER(date_start) |
| is_race | BOOLEAN | NO | Race flag | **CALCULATED**: session_category = 'Race' |
| is_qualifying | BOOLEAN | NO | Quali flag | **CALCULATED**: session_category = 'Qualifying' |
| is_practice | BOOLEAN | NO | Practice flag | **CALCULATED**: session_category = 'Practice' |
| is_sprint | BOOLEAN | NO | Sprint flag | **CALCULATED**: session_category = 'Sprint' |
| gmt_offset | STRING | YES | GMT offset | From silver_sessions |
| meeting_key | BIGINT | YES | Meeting ID | From silver_sessions |
| location | STRING | NO | Location | From silver_sessions |
| country_key | BIGINT | YES | Country ID | From silver_sessions |
| country_code | STRING | YES | Country code | From silver_sessions |
| country_name | STRING | NO | Country name | From silver_sessions |
| circuit_key | BIGINT | YES | Circuit ID | From silver_sessions |
| circuit_short_name | STRING | YES | Circuit name | From silver_sessions |
| year | BIGINT | YES | Year | From silver_sessions |
| updated_at | TIMESTAMP | NO | Last update | From silver_sessions |

**Primary Key**: session_dim_key
**Natural Key**: session_key
**Refresh Strategy**: Full refresh

#### dim_drivers

| Column | Type | Nullable | Description | SCD Type |
|--------|------|----------|-------------|----------|
| driver_key | STRING | NO | Surrogate key (PK) | Generated |
| driver_number | BIGINT | NO | Racing number (NK) | From silver_drivers |
| full_name | STRING | NO | Full name | From silver_drivers |
| broadcast_name | STRING | NO | Display name | From silver_drivers |
| name_acronym | STRING | YES | Three letters | From silver_drivers |
| team_name | STRING | NO | Team | From silver_drivers - **SCD Tracked** |
| team_colour | STRING | NO | Color | From silver_drivers - **SCD Tracked** |
| team_category | STRING | NO | Standard team | From silver_drivers - **SCD Tracked** |
| first_name | STRING | YES | First name | From silver_drivers |
| last_name | STRING | YES | Last name | From silver_drivers |
| headshot_url | STRING | YES | Photo URL | From silver_drivers |
| country_code | STRING | YES | Country | From silver_drivers |
| valid_from | TIMESTAMP | NO | SCD start date | **SCD**: From updated_at |
| valid_to | TIMESTAMP | YES | SCD end date | **SCD**: NULL for current |
| is_current | BOOLEAN | NO | Current record flag | **SCD**: TRUE for current |
| updated_at | TIMESTAMP | NO | Last update | From silver_drivers |

**Primary Key**: driver_key
**Natural Key**: driver_number
**SCD Type**: Type 2 (history tracking)
**Tracked Attributes**: team_name, team_colour, team_category
**Refresh Strategy**: Latest record per driver_number

#### fact_laps

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| lap_key | STRING | NO | Surrogate key (PK) | **CALCULATED**: hash(session_key, driver_number, lap_number) |
| session_dim_key | STRING | NO | FK to dim_sessions | **CALCULATED**: hash(session_key) |
| driver_key | STRING | NO | FK to dim_drivers | **CALCULATED**: hash(driver_number) |
| session_key | BIGINT | NO | Degenerate dimension | From silver_laps |
| meeting_key | BIGINT | YES | Degenerate dimension | From silver_laps |
| driver_number | BIGINT | NO | Degenerate dimension | From silver_laps |
| lap_number | BIGINT | NO | Lap sequence | From silver_laps |
| date_start | TIMESTAMP | YES | Lap start time | From silver_laps |
| lap_duration | DOUBLE | YES | Total time | From silver_laps |
| duration_sector_1 | DOUBLE | YES | Sector 1 time | From silver_laps |
| duration_sector_2 | DOUBLE | YES | Sector 2 time | From silver_laps |
| duration_sector_3 | DOUBLE | YES | Sector 3 time | From silver_laps |
| calculated_lap_duration | DOUBLE | YES | Sum of sectors | From silver_laps |
| sector_duration_delta | DOUBLE | YES | Discrepancy | From silver_laps |
| is_pit_out_lap | BOOLEAN | NO | Pit out flag | From silver_laps |
| is_valid_lap | BOOLEAN | NO | Validity flag | From silver_laps |
| intermediate_1_speed | DOUBLE | YES | I1 speed | From silver_laps |
| intermediate_2_speed | DOUBLE | YES | I2 speed | From silver_laps |
| speed_trap_speed | DOUBLE | YES | ST speed | From silver_laps |
| max_speed | DOUBLE | YES | Max speed | From silver_laps |
| segments_sector_1 | ARRAY<INT> | YES | Sector 1 data | From silver_laps |
| segments_sector_2 | ARRAY<INT> | YES | Sector 2 data | From silver_laps |
| segments_sector_3 | ARRAY<INT> | YES | Sector 3 data | From silver_laps |
| sector_completeness | STRING | NO | Completeness | From silver_laps |
| driver_lap_rank | BIGINT | YES | Lap sequence rank | **CALCULATED**: ROW_NUMBER() OVER session/driver |
| session_lap_time_rank | BIGINT | YES | Session fastest rank | **CALCULATED**: ROW_NUMBER() OVER session BY lap_duration |
| driver_best_lap_time | DOUBLE | YES | Driver's best | **CALCULATED**: MIN(lap_duration) OVER session/driver |
| delta_to_personal_best | DOUBLE | YES | Delta to best | **CALCULATED**: lap_duration - driver_best_lap_time |
| is_personal_best_lap | BOOLEAN | NO | Best lap flag | **CALCULATED**: lap_duration = driver_best_lap_time |
| batch_day | DATE | NO | Partition key | From silver_laps |
| session_name | STRING | YES | Session name | From silver_laps |
| updated_at | TIMESTAMP | NO | Last update | From silver_laps |

**Primary Key**: lap_key
**Foreign Keys**: session_dim_key, driver_key
**Grain**: One row per lap
**Refresh Strategy**: Full refresh
**Partitioning**: batch_day

---

### Gold Layer - Marts

#### session_leaderboard

**Purpose**: Final session standings with rankings

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| session_key | BIGINT | Session ID | From fact_laps |
| session_name | STRING | Session name | From dim_sessions |
| session_category | STRING | Category | From dim_sessions |
| location | STRING | Circuit | From dim_sessions |
| session_date | DATE | Date | From dim_sessions |
| driver_number | BIGINT | Driver number | From fact_laps |
| driver_name | STRING | Driver name | From dim_drivers |
| team_name | STRING | Team | From dim_drivers |
| team_colour | STRING | Color | From dim_drivers |
| position | BIGINT | Final position | **CALCULATED**: RANK fastest_lap_time |
| fastest_lap_time | DOUBLE | Fastest lap | **CALCULATED**: MIN(lap_duration) WHERE is_valid_lap |
| total_laps | BIGINT | Lap count | **CALCULATED**: COUNT(*) |
| valid_laps | BIGINT | Valid lap count | **CALCULATED**: COUNT WHERE is_valid_lap |
| delta_to_fastest | DOUBLE | Gap to P1 | **CALCULATED**: fastest - session_fastest |
| is_top_3 | BOOLEAN | Top 3 flag | **CALCULATED**: position <= 3 |
| batch_day | DATE | Partition | From fact_laps |

**Refresh**: Full
**Grain**: One row per driver per session

#### driver_session_summary

**Purpose**: Detailed driver performance metrics

| Column | Type | Description | Calculation |
|--------|------|-------------|-------------|
| session_key | BIGINT | Session ID | From fact_laps |
| session_name | STRING | Session | From dim_sessions |
| session_category | STRING | Category | From dim_sessions |
| driver_number | BIGINT | Driver | From fact_laps |
| driver_name | STRING | Name | From dim_drivers |
| team_name | STRING | Team | From dim_drivers |
| total_laps | BIGINT | Total laps | COUNT(*) |
| valid_laps | BIGINT | Valid laps | COUNT WHERE is_valid |
| fastest_lap_time | DOUBLE | Fastest lap | MIN(lap_duration) WHERE is_valid |
| avg_lap_time | DOUBLE | Average lap | AVG(lap_duration) WHERE is_valid |
| consistency_coefficient | DOUBLE | Std dev / avg | STDDEV(lap_duration) / AVG WHERE is_valid |
| top_speed | DOUBLE | Max speed | MAX(max_speed) |
| sector_1_best | DOUBLE | Best S1 | MIN(duration_sector_1) WHERE NOT NULL |
| sector_2_best | DOUBLE | Best S2 | MIN(duration_sector_2) WHERE NOT NULL |
| sector_3_best | DOUBLE | Best S3 | MIN(duration_sector_3) WHERE NOT NULL |
| batch_day | DATE | Partition | From fact_laps |

**Refresh**: Full
**Grain**: One row per driver per session

#### team_performance

**Purpose**: Team-level aggregations

| Column | Type | Description | Calculation |
|--------|------|-------------|-------------|
| session_key | BIGINT | Session ID | From driver_session_summary |
| session_name | STRING | Session | From driver_session_summary |
| team_name | STRING | Team | From driver_session_summary |
| driver_count | BIGINT | Drivers | COUNT(DISTINCT driver_number) |
| total_laps | BIGINT | Team laps | SUM(total_laps) |
| avg_fastest_lap | DOUBLE | Avg fastest | AVG(fastest_lap_time) |
| best_team_lap | DOUBLE | Team best | MIN(fastest_lap_time) |
| avg_consistency | DOUBLE | Avg consistency | AVG(consistency_coefficient) |
| total_valid_laps | BIGINT | Valid laps | SUM(valid_laps) |
| batch_day | DATE | Partition | From driver_session_summary |

**Refresh**: Full
**Grain**: One row per team per session

---

## Relationships & Joins

### Primary Join Paths

**Session Analysis**:
```sql
FROM fact_laps f
JOIN dim_sessions s ON f.session_dim_key = s.session_dim_key
JOIN dim_drivers d ON f.driver_key = d.driver_key AND d.is_current = TRUE
WHERE s.is_race = TRUE
```

**Driver History**:
```sql
FROM fact_laps f
JOIN dim_drivers d ON f.driver_key = d.driver_key
  AND f.date_start BETWEEN d.valid_from AND COALESCE(d.valid_to, '9999-12-31')
```

**Mart Queries**:
```sql
-- Use marts directly (pre-joined, pre-aggregated)
SELECT * FROM session_leaderboard WHERE session_category = 'Race';
SELECT * FROM driver_session_summary WHERE driver_name = 'Max Verstappen';
SELECT * FROM team_performance WHERE team_name LIKE '%Red Bull%';
```

### Cardinality

```
dim_sessions (1) ────< fact_laps (M)
dim_drivers (1) ─────< fact_laps (M)

fact_laps (M) ────> session_leaderboard (aggregated)
fact_laps (M) ────> driver_session_summary (aggregated)
driver_session_summary (M) ────> team_performance (aggregated)
```

---

## Data Types & Constraints

### Key Constraints

| Table | Primary Key | Foreign Keys | Unique Constraints |
|-------|-------------|--------------|-------------------|
| dim_sessions | session_dim_key | None | session_key |
| dim_drivers | driver_key | None | None (SCD Type 2) |
| fact_laps | lap_key | session_dim_key, driver_key | None |

### Data Type Standards

| Domain | Type | Example |
|--------|------|---------|
| **Identifiers** | BIGINT | session_key, driver_number |
| **Surrogate Keys** | STRING (MD5) | session_dim_key, driver_key |
| **Timestamps** | TIMESTAMP | date_start, updated_at |
| **Durations** | DOUBLE (seconds) | lap_duration, sector times |
| **Speeds** | DOUBLE (km/h) | max_speed, trap speeds |
| **Flags** | BOOLEAN | is_valid_lap, is_current |
| **Categories** | STRING | session_category, team_category |
| **Arrays** | ARRAY<INT> | segments_sector_1/2/3 |
| **Dates** | DATE | batch_day, session_date |

---

## Slowly Changing Dimensions

### SCD Type 2: dim_drivers

**Purpose**: Track driver team changes over time

**Implementation**:
```sql
-- dbt snapshot configuration
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

**Example**: Max Verstappen team change
| driver_key | driver_number | full_name | team_name | valid_from | valid_to | is_current |
|------------|---------------|-----------|-----------|------------|----------|------------|
| abc123 | 1 | Max Verstappen | Red Bull Racing | 2023-01-01 | 2024-12-31 | FALSE |
| def456 | 1 | Max Verstappen | New Team | 2025-01-01 | NULL | TRUE |

**Query Pattern**:
```sql
-- Get current team
SELECT * FROM dim_drivers WHERE driver_number = 1 AND is_current = TRUE;

-- Get historical team at specific date
SELECT * FROM dim_drivers
WHERE driver_number = 1
  AND '2024-06-15' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31');
```

---

## Derived Attributes

### Calculated Fields Summary

| Field | Formula | Purpose |
|-------|---------|---------|
| **session_duration_minutes** | (date_end - date_start) / 60 | Session length |
| **session_category** | CASE session_name LIKE patterns | Standardized type |
| **team_category** | CASE team_name LIKE patterns | Standardized team |
| **calculated_lap_duration** | SUM(sector_1/2/3) | Validation check |
| **sector_duration_delta** | lap_duration - calculated | Data quality metric |
| **is_valid_lap** | Complex CASE | Business rule |
| **max_speed** | GREATEST(i1, i2, st) | Performance metric |
| **distance_from_origin** | SQRT(x² + y²) | Track position |
| **driver_lap_rank** | ROW_NUMBER() OVER | Lap sequence |
| **session_lap_time_rank** | ROW_NUMBER() OVER ORDER BY time | Fastest lap rank |
| **driver_best_lap_time** | MIN() OVER | Personal best |
| **delta_to_personal_best** | lap_duration - best | Performance delta |
| **consistency_coefficient** | STDDEV / AVG | Driver consistency |

---

**Document Version**: 1.0
**Last Updated**: 2025-11-04
**Related**: [architecture.md](architecture.md)
