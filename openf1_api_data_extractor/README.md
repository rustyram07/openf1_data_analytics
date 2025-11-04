# OpenF1 Data Extractor

A modular Python application for extracting Formula 1 data from the [OpenF1 API](https://openf1.org/) and loading it to Databricks.

## Features

✅ **Flexible Extraction Modes**
- Latest sessions (default)
- Specific year
- Manual date selection

✅ **Thread-Safe Rate Limiting**
- Respects OpenF1 API limits (3 req/sec)
- Automatic 429 error retry with exponential backoff
- Coordinates requests across parallel workers

✅ **Comprehensive Data Extraction**
- Sessions metadata
- Driver information
- Lap timing data
- Location telemetry (with pagination)

✅ **Dual Output**
- Local JSON files for development
- Databricks volumes for production

✅ **Production-Ready**
- Incremental loading support
- Error handling and retry logic
- Configurable via environment variables

---

## Quick Start

### Installation

```bash
# Clone repository
cd openf1_data_analytics

# Install dependencies (using uv)
uv pip install -r requirements.txt

# Or with pip
pip install -r requirements.txt
```

### Basic Usage

```bash
# Extract latest 10 sessions (default)
python openf1_api_data_extractor/src/ingestion/databricks_extractor.py

# Extract latest 5 sessions
python openf1_api_data_extractor/src/ingestion/databricks_extractor.py --mode latest --num-sessions 5

# Extract all sessions from 2024
python openf1_api_data_extractor/src/ingestion/databricks_extractor.py --mode year --year 2024
```

---

## Extraction Modes

### Mode 1: Latest Sessions (DEFAULT)

Extract the N most recent sessions.

```bash
# Default: 10 most recent sessions
python databricks_extractor.py

# Custom number
python databricks_extractor.py --mode latest --num-sessions 15
```

**Use cases:** Incremental loading, daily updates, development/testing

---

### Mode 2: Year Mode

Extract all sessions from a specific year.

```bash
python databricks_extractor.py --mode year --year 2024
python databricks_extractor.py --mode year --year 2023
```

**Use cases:** Historical backfill, season analysis, data warehouse initialization

---

### Mode 3: Manual Mode

Extract specific dates configured in `config/settings.py`.

```bash
# Configure dates in settings.py first
python databricks_extractor.py --mode manual
```

**Configuration in `config/settings.py`:**

```python
manual_batch_days = [
    '2024-12-08',  # Abu Dhabi GP
    '2024-12-07',
    '2024-11-24',  # Las Vegas GP
    # Add more dates...
]
```

**Use cases:** Specific race extraction, gap filling, data quality fixes

---

## Project Structure

```
openf1_api_data_extractor/
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuration management
├── src/
│   ├── ingestion/
│   │   └── databricks_extractor.py  # Main extraction script
│   └── utils/
│       ├── api_client.py    # OpenF1 API client with rate limiting
│       ├── file_handler.py  # File operations & Databricks upload
│       └── session_finder.py # Session discovery utilities
├── EXTRACTION_MODES.md      # Detailed mode documentation
└── README.md                # This file
```

---

## Configuration

### Environment Variables

Required for Databricks connection:

```bash
export DBT_DATABRICKS_HOST=your-host.cloud.databricks.com
export DBT_DATABRICKS_TOKEN=your-token-here
```

### Configuration File: `config/settings.py`

```python
@dataclass
class ExtractionConfig:
    # Default mode
    fetch_mode: str = 'latest'  # 'latest', 'year', or 'manual'

    # Latest mode settings
    num_latest_sessions: int = 10

    # Year mode settings
    fetch_year: int = 2024

    # Data options
    fetch_locations: bool = True
    fetch_laps: bool = True
    fetch_drivers: bool = True
    fetch_sessions: bool = True

    # Performance
    use_pagination_for_locations: bool = True
    location_chunk_minutes: int = 15
    max_location_chunks: int = 12
```

---

## Output

### Local Storage (Development)

```
~/Documents/openf1_data_analytics/local_test_data/
├── sessions_2024-12-08_Yas_Island_Race.json
├── drivers_2024-12-08_Yas_Island_Race.json
├── laps_2024-12-08_Yas_Island_Race.json
└── locations_2024-12-08_Yas_Island_Race.json
```

### Databricks Volume (Production)

```
/Volumes/dev_f1_data_analytics/raw_data/landing/
├── sessions_*.json
├── drivers_*.json
├── laps_*.json
└── locations_*.json
```

---

## Data Flow

```
OpenF1 API
    ↓
Thread-Safe Rate Limiter (2.8 req/sec)
    ↓
Parallel Workers (3 concurrent)
    ↓
Data Transformation & Enrichment
    ↓
┌─────────────────┬─────────────────────┐
│  Local Storage  │  Databricks Volume  │
│  (JSON files)   │  (JSON files)       │
└─────────────────┴─────────────────────┘
                    ↓
            dbt Bronze Layer
                    ↓
            dbt Silver Layer
                    ↓
            dbt Gold Layer
```

---

## Features in Detail

### Thread-Safe Rate Limiting

```python
class RateLimiter:
    """Ensures no more than 2.8 requests/second globally"""

    def wait_if_needed(self):
        with self.lock:
            # Thread-safe coordination
            # Prevents 429 errors across all workers
```

**Benefits:**
- ✅ Coordinates all parallel workers
- ✅ Prevents API rate limit errors
- ✅ Automatic 429 retry with backoff

### Location Data Pagination

Large location datasets are automatically chunked:

```python
# Splits session into 15-minute chunks
# Prevents "too much data" API errors
# Up to 12 chunks per driver (3 hours)
```

### Error Handling

- **API Errors:** Automatic retry with exponential backoff
- **Rate Limits:** Thread-safe coordination + 429 handling
- **Network Issues:** Configurable timeout and retry logic
- **Data Quality:** Metadata tracking for auditing

---

## Advanced Usage

### Programmatic Usage

```python
from src.ingestion.databricks_extractor import extract_to_databricks

# Extract latest 10 sessions
extract_to_databricks(mode='latest', num_sessions=10)

# Extract full year
extract_to_databricks(mode='year', year=2024)
```

### Scheduled Runs

```bash
# Daily cron job (2 AM) - Latest 5 sessions
0 2 * * * cd /path/to/project && .venv/bin/python openf1_api_data_extractor/src/ingestion/databricks_extractor.py --mode latest --num-sessions 5

# Weekly full year refresh (Sunday 3 AM)
0 3 * * 0 cd /path/to/project && .venv/bin/python openf1_api_data_extractor/src/ingestion/databricks_extractor.py --mode year --year 2024
```

### Databricks Workflows Integration

```python
# In Databricks Notebook
%python
import sys
sys.path.insert(0, '/Workspace/Repos/production/openf1_data_analytics/openf1_api_data_extractor')

from src.ingestion.databricks_extractor import extract_to_databricks

# Run extraction
extract_to_databricks(mode='latest', num_sessions=10)
```

---

## Performance

### Extraction Times (Approximate)

| Mode | Sessions | Duration | API Calls |
|------|----------|----------|-----------|
| Latest 1 | 1 session | ~2-3 min | ~24 requests |
| Latest 10 | 10 sessions | ~15-20 min | ~240 requests |
| Year 2024 | ~120 sessions | ~3-4 hours | ~2,880 requests |

### Optimizations

- ✅ Parallel processing (3 workers)
- ✅ Thread-safe rate limiting
- ✅ Location data pagination
- ✅ Delta format for Databricks
- ✅ Incremental loading support

---

## Troubleshooting

### Connection Issues

**Error:** `Could not connect to Databricks`

**Solution:**
```bash
# Verify environment variables
echo $DBT_DATABRICKS_HOST
echo $DBT_DATABRICKS_TOKEN

# Test connection
python -c "from databricks.connect import DatabricksSession; spark = DatabricksSession.builder.remote(serverless=True).getOrCreate(); print('Connected!')"
```

### Rate Limit Errors

**Symptom:** Many 429 errors

**Solution:** Already handled automatically with:
- Thread-safe rate limiter
- Automatic retry with exponential backoff
- 2.8 req/sec coordination

If persistent:
```python
# In config/settings.py, reduce workers
max_workers: int = 2  # Down from 3
```

### No Sessions Found

**Error:** `No sessions found for this criteria`

**Solution:**
```bash
# Check if year has data
python databricks_extractor.py --mode year --year 2023  # Known good year

# Use latest mode instead
python databricks_extractor.py --mode latest
```

---

## Development

### Running Tests

```bash
# Test API connection
python -c "import sys; sys.path.insert(0, 'openf1_api_data_extractor'); from src.utils import api_client; print('Health:', api_client.check_health())"

# Test configuration
python -c "import sys; sys.path.insert(0, 'openf1_api_data_extractor'); from config import config; print('Mode:', config.extraction.fetch_mode)"

# Test single session extraction
python databricks_extractor.py --mode latest --num-sessions 1
```

### Debugging

Enable verbose logging:

```bash
# Set environment variable
export DBT_LOG_LEVEL=DEBUG

# Run with output
python databricks_extractor.py --mode latest --num-sessions 1 2>&1 | tee extraction.log
```

---

## Integration with dbt

After extraction completes, data flows to dbt:

```
Bronze Layer (bronze_raw schema)
    ↓
dbt Silver Layer (silver_clean schema)
    - Deduplication
    - Type casting
    - Business logic
    ↓
dbt Gold Layer (gold_analytics schema)
    - Dimensional models
    - Aggregations
    - Analytics-ready tables
```

See `../dbt_f1_data_analytics/` for dbt project.

---

## API Reference

### OpenF1 API

- **Base URL:** https://api.openf1.org/v1
- **Rate Limit:** 3 requests per second
- **Data Limit:** 4MB per 10 seconds
- **Documentation:** https://openf1.org/

### Endpoints Used

- `/sessions` - Session metadata
- `/drivers` - Driver information
- `/laps` - Lap timing data
- `/location` - Telemetry data (paginated)

---

## Best Practices

### Development
```bash
# Quick test
python databricks_extractor.py --mode latest --num-sessions 1
```

### Production Daily Run
```bash
# Incremental: Latest 10 sessions
python databricks_extractor.py --mode latest --num-sessions 10
```

### Initial Setup
```bash
# Backfill: Full year
python databricks_extractor.py --mode year --year 2024
```

### Gap Filling
```bash
# Edit settings.py with specific dates
python databricks_extractor.py --mode manual
```

---

## Documentation

- **[EXTRACTION_MODES.md](EXTRACTION_MODES.md)** - Detailed mode documentation
- **[../DBT_PROJECT_REVIEW.md](../DBT_PROJECT_REVIEW.md)** - Full project review
- **[../dbt_f1_data_analytics/CICD_SETUP.md](../dbt_f1_data_analytics/CICD_SETUP.md)** - CI/CD setup guide

---

## Support

For issues:
1. Check extraction logs
2. Verify Databricks connection
3. Test API health: `python -c "import requests; print(requests.get('https://api.openf1.org/v1/sessions').status_code)"`
4. Review [EXTRACTION_MODES.md](EXTRACTION_MODES.md)

---

## License

This project is for educational and analytical purposes using publicly available OpenF1 API data.

---

## Acknowledgments

- **OpenF1 API** - https://openf1.org/
- **Databricks** - Data lakehouse platform
- **dbt** - Data transformation tool

---

**Version:** 1.0.0
**Last Updated:** 2025-11-04
