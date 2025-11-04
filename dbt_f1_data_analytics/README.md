# dbt F1 Data Analytics

A dbt project for analyzing Formula 1 race data using the medallion architecture (Silver-Gold layers) on Databricks.

## Project Structure

```
dbt_f1_data_analytics/
├── models/
│   ├── silver_clean/         # Cleaned & conformed data (incremental, Delta)
│   │   └── core/             # Core silver models: drivers, sessions, laps, locations
│   └── gold_analytics/       # Business logic & aggregations (tables, Delta)
│       ├── core/             # Dimensional models: dim_drivers, dim_sessions, fact_laps
│       └── marts/            # Analytics marts: leaderboards, summaries, performance
├── macros/                   # Custom generic tests & utilities
├── tests/                    # Data quality tests
├── snapshots/                # SCD Type 2 snapshots
└── seeds/                    # Static reference data
```

## Layer Overview

### Silver Layer (`silver_clean`)
- **Materialization**: Incremental (Delta format)
- **Purpose**: Cleaned, conformed source data ready for analytics
- **Models**: `silver_drivers`, `silver_sessions`, `silver_laps`, `silver_locations`

### Gold Layer (`gold_analytics`)
- **Materialization**: Tables (Delta format)
- **Purpose**: Business-ready dimensional models and analytics marts
- **Core Models**: `dim_drivers` (SCD2), `dim_sessions`, `fact_laps`
- **Marts**: `session_leaderboard`, `driver_session_summary`, `team_performance`

## Getting Started

1. Configure Databricks connection in [profiles.yml](profiles.yml)
2. Install dependencies: `dbt deps`
3. Run models: `dbt run` or use [run_dbt.sh](run_dbt.sh)
4. Run tests: `dbt test`

## Custom Tests

Custom generic tests in [macros/](macros/):
- `test_no_future_dates` - Validates date fields
- `test_positive_values` - Ensures numeric fields are positive
- `test_reasonable_lap_time` - Validates lap time ranges
- `test_valid_percentage` - Checks percentage bounds

## Documentation

- [SCHEMA_STRUCTURE.md](SCHEMA_STRUCTURE.md) - Detailed schema documentation
- [VALIDATION_QUERIES.sql](VALIDATION_QUERIES.sql) - Data validation queries