# Test Data

Sample F1 data for local testing and development.

## Dataset Information

**Session:** 2024 Bahrain Grand Prix - Race
**Session Key:** 9158
**Date:** March 2, 2024
**Location:** Bahrain International Circuit, Sakhir

## Files Included

| File | Records | Description |
|------|---------|-------------|
| `sessions_9158.json` | 1 | Session metadata (race details, location, timing) |
| `drivers_9158.json` | 5 | Driver information (Verstappen, Perez, Leclerc, Sainz, Hamilton) |
| `laps_9158.json` | 4 | Lap timing data (2 laps each for VER and LEC) |
| `locations_9158.json` | 5 | GPS telemetry samples |

## Data Structure

### sessions_9158.json
```json
{
  "session_key": 9158,
  "session_name": "Race",
  "session_type": "Race",
  "location": "Sakhir",
  "country_name": "Bahrain",
  "date_start": "2024-03-02T13:00:00+00:00",
  "date_end": "2024-03-02T16:00:00+00:00",
  "year": 2024
  ...
}
```

### drivers_9158.json
```json
{
  "driver_number": 1,
  "full_name": "Max VERSTAPPEN",
  "team_name": "Red Bull Racing",
  "team_colour": "3671C6",
  "session_key": 9158
  ...
}
```

### laps_9158.json
```json
{
  "session_key": 9158,
  "driver_number": 1,
  "lap_number": 1,
  "lap_duration": 92.692,
  "duration_sector_1": 28.234,
  "duration_sector_2": 38.567,
  "duration_sector_3": 25.891,
  "st_speed": 305
  ...
}
```

### locations_9158.json
```json
{
  "session_key": 9158,
  "driver_number": 1,
  "date": "2024-03-02T13:32:15.123000+00:00",
  "x": 1234,
  "y": 5678,
  "z": 10
}
```

## Usage

### Option 1: Use with Test Script (Recommended)

```bash
# From project root
python scripts/test_pipeline_local.py
```

This will:
1. Load all JSON files into bronze tables
2. Run dbt transformations
3. Verify data quality
4. Show summary

### Option 2: Manual Load to Bronze

```bash
# Set environment variable
export LOCAL_DATA_DIR=./test_data

# Load to bronze
cd openf1_api_data_extractor/dlt_bronze_load
python run_batch_load.py
```

### Option 3: Custom Testing

```python
import json
import pandas as pd

# Load sessions
with open('test_data/sessions_9158.json') as f:
    sessions = json.load(f)

df_sessions = pd.DataFrame(sessions)
print(df_sessions)

# Load drivers
with open('test_data/drivers_9158.json') as f:
    drivers = json.load(f)

df_drivers = pd.DataFrame(drivers)
print(df_drivers)
```

## Expected Results

After running the test pipeline, you should see:

**Bronze Tables:**
- `bronze_sessions`: 1 row
- `bronze_drivers`: 5 rows
- `bronze_laps`: 4 rows
- `bronze_locations`: 5 rows

**Silver Tables:**
- `silver_sessions`: 1 row
- `silver_drivers`: 5 rows
- `silver_laps`: 4 rows
- `silver_locations`: 5 rows

**Gold Tables:**
- `dim_sessions`: 1 row
- `dim_drivers`: Depends on SCD2 logic
- `fact_laps`: 4 rows
- Analytics aggregations

## Sample Queries

After loading the test data, try these queries:

```sql
-- View session details
SELECT * FROM bronze_f1_data_analytics.bronze_sessions;

-- Fastest lap by driver
SELECT
    driver_number,
    MIN(lap_duration) as fastest_lap
FROM bronze_f1_data_analytics.bronze_laps
GROUP BY driver_number;

-- Driver standings
SELECT
    d.full_name,
    d.team_name,
    COUNT(l.lap_number) as laps_completed
FROM bronze_f1_data_analytics.bronze_drivers d
LEFT JOIN bronze_f1_data_analytics.bronze_laps l
    ON d.driver_number = l.driver_number
    AND d.session_key = l.session_key
GROUP BY d.full_name, d.team_name;
```

## Limitations

This is minimal test data for development purposes:
- Only 1 session (out of ~25 per season)
- Only 5 drivers (out of 20)
- Only 4 laps (races have 50-70 laps)
- Only 5 location points (actual data has millions)

For comprehensive testing, use real data from OpenF1 API:

```bash
python openf1_api_data_extractor/src/ingestion/extract_sessions.py --num-sessions 1
```

## Data Source

This test data is derived from the [OpenF1 API](https://openf1.org/) with representative but simplified values for testing purposes.

**Note:** This is synthetic/sample data for testing only. For analysis, use real data from the API.
