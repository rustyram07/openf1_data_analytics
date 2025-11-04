#!/bin/bash
################################################################################
# Load 2024 F1 Session - Automated End-to-End Script
#
# This script automates the complete pipeline:
#   1. Extract data from OpenF1 API
#   2. Load to Bronze layer
#   3. Build Silver layer (full refresh)
#   4. Build Gold layer (full refresh)
#   5. Validate results
#
# Usage:
#   ./load_2024_session.sh                # Load latest 1 session
#   ./load_2024_session.sh 5              # Load latest 5 sessions
#   ./load_2024_session.sh year           # Load all 2024 sessions
#   ./load_2024_session.sh oldest         # Load oldest 1 session
#   ./load_2024_session.sh oldest 5       # Load oldest 5 sessions
#
################################################################################

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="$HOME/Documents/openf1_data_analytics"

# Parse arguments
MODE="latest"
NUM_SESSIONS=1
SORT_ORDER="DESC"  # Default: newest first (latest)

if [ "$1" = "year" ]; then
    MODE="year"
    YEAR=2024
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Loading ALL 2024 F1 Sessions${NC}"
    echo -e "${BLUE}========================================${NC}"
elif [ "$1" = "oldest" ]; then
    MODE="oldest"
    SORT_ORDER="ASC"  # Oldest first
    if [ -n "$2" ]; then
        NUM_SESSIONS=$2
        echo -e "${BLUE}========================================${NC}"
        echo -e "${BLUE}Loading Oldest $NUM_SESSIONS Session(s)${NC}"
        echo -e "${BLUE}========================================${NC}"
    else
        NUM_SESSIONS=1
        echo -e "${BLUE}========================================${NC}"
        echo -e "${BLUE}Loading Oldest 1 Session${NC}"
        echo -e "${BLUE}========================================${NC}"
    fi
elif [ -n "$1" ]; then
    NUM_SESSIONS=$1
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Loading Latest $NUM_SESSIONS Session(s)${NC}"
    echo -e "${BLUE}========================================${NC}"
else
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Loading Latest 1 Session (Default)${NC}"
    echo -e "${BLUE}========================================${NC}"
fi

echo ""
echo -e "${YELLOW}Pipeline Steps:${NC}"
echo "  1. Extract data from OpenF1 API"
echo "  2. Load to Bronze layer"
echo "  3. Build Silver layer (full refresh)"
echo "  4. Build Gold layer (full refresh)"
echo "  5. Validate results"
echo ""

# Function to print step header
print_step() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

# Function to print error
print_error() {
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}ERROR: $1${NC}"
    echo -e "${RED}========================================${NC}"
    echo ""
}

# Function to print success
print_success() {
    echo ""
    echo -e "${GREEN}✓ $1${NC}"
    echo ""
}

# Check if virtual environment exists
if [ ! -d "$PROJECT_ROOT/.venv" ]; then
    print_error "Virtual environment not found at $PROJECT_ROOT/.venv"
    echo "Please create virtual environment first:"
    echo "  cd $PROJECT_ROOT"
    echo "  python -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
    exit 1
fi

################################################################################
# STEP 1: Extract Data from OpenF1 API
################################################################################

print_step "STEP 1/5: Extracting Data from OpenF1 API"

cd "$PROJECT_ROOT"
source .venv/bin/activate

cd openf1_api_data_extractor/src/ingestion

if [ "$MODE" = "year" ]; then
    echo "Fetching all sessions from year $YEAR..."
    python extract_sessions.py --mode year --year $YEAR
elif [ "$MODE" = "oldest" ]; then
    echo "Fetching oldest $NUM_SESSIONS session(s)..."
    python extract_sessions.py --order oldest --num-sessions $NUM_SESSIONS
else
    echo "Fetching latest $NUM_SESSIONS session(s)..."
    python extract_sessions.py --order latest --num-sessions $NUM_SESSIONS
fi

if [ $? -ne 0 ]; then
    print_error "API extraction failed"
    exit 1
fi

print_success "API extraction completed"

################################################################################
# STEP 2: Load to Bronze Layer
################################################################################

print_step "STEP 2/5: Loading Data to Bronze Layer"

cd "$PROJECT_ROOT/openf1_api_data_extractor/dlt_bronze_load"

# Check if data files exist
DATA_DIR="$PROJECT_ROOT/local_test_data"
FILE_COUNT=$(ls -1 "$DATA_DIR"/*.json 2>/dev/null | wc -l)

if [ $FILE_COUNT -eq 0 ]; then
    print_error "No JSON files found in $DATA_DIR"
    exit 1
fi

echo "Found $FILE_COUNT JSON files in $DATA_DIR"
echo ""

python run_batch_load.py

if [ $? -ne 0 ]; then
    print_error "Bronze layer load failed"
    exit 1
fi

print_success "Bronze layer loaded successfully"

################################################################################
# STEP 3: Build Silver Layer (Full Refresh)
################################################################################

print_step "STEP 3/5: Building Silver Layer (Full Refresh)"

cd "$PROJECT_ROOT/dbt_f1_data_analytics"

./run_dbt.sh build --select silver_clean --full-refresh

if [ $? -ne 0 ]; then
    print_error "Silver layer build failed"
    exit 1
fi

print_success "Silver layer built successfully"

################################################################################
# STEP 4: Build Gold Layer (Full Refresh)
################################################################################

print_step "STEP 4/5: Building Gold Layer (Full Refresh)"

echo "Building Gold Core (Dimensions & Facts)..."
./run_dbt.sh build --select gold_analytics.core --full-refresh

if [ $? -ne 0 ]; then
    print_error "Gold core build failed"
    exit 1
fi

echo ""
echo "Building Gold Marts (Analytics Tables)..."
./run_dbt.sh run --select session_leaderboard driver_session_summary team_performance --full-refresh

if [ $? -ne 0 ]; then
    print_error "Gold marts build failed"
    exit 1
fi

print_success "Gold layer built successfully"

################################################################################
# STEP 5: Validate Results
################################################################################

print_step "STEP 5/5: Validation Summary"

echo "Generating validation report..."
echo ""

# Create validation SQL file
VALIDATION_FILE="/tmp/validate_2024_session.sql"

cat > "$VALIDATION_FILE" << 'EOF'
-- Quick validation query
SELECT
    'BRONZE' AS layer,
    'bronze_sessions' AS table_name,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions) AS record_count
UNION ALL
SELECT 'BRONZE', 'bronze_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_drivers)
UNION ALL
SELECT 'BRONZE', 'bronze_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_laps)
UNION ALL
SELECT 'BRONZE', 'bronze_locations',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_locations)
UNION ALL
SELECT 'SILVER', 'silver_sessions',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_sessions)
UNION ALL
SELECT 'SILVER', 'silver_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_drivers)
UNION ALL
SELECT 'SILVER', 'silver_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_laps)
UNION ALL
SELECT 'SILVER', 'silver_locations',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_locations)
UNION ALL
SELECT 'GOLD-DIM', 'dim_sessions',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions)
UNION ALL
SELECT 'GOLD-DIM', 'dim_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_drivers)
UNION ALL
SELECT 'GOLD-FACT', 'fact_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.fact_laps)
UNION ALL
SELECT 'GOLD-MART', 'session_leaderboard',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard)
UNION ALL
SELECT 'GOLD-MART', 'driver_session_summary',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary)
UNION ALL
SELECT 'GOLD-MART', 'team_performance',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.team_performance)
ORDER BY layer, table_name;
EOF

echo "Validation SQL created at: $VALIDATION_FILE"
echo ""
echo "To validate in Databricks SQL Editor, run:"
echo "  cat $VALIDATION_FILE"
echo ""

################################################################################
# FINAL SUMMARY
################################################################################

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ PIPELINE COMPLETED SUCCESSFULLY${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Summary:${NC}"
echo "  ✓ Step 1: API extraction completed"
echo "  ✓ Step 2: Bronze layer loaded"
echo "  ✓ Step 3: Silver layer built (full refresh)"
echo "  ✓ Step 4: Gold layer built (full refresh)"
echo "  ✓ Step 5: Validation SQL created"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Run validation query in Databricks SQL Editor:"
echo "     cat $VALIDATION_FILE"
echo ""
echo "  2. View session leaderboard:"
echo "     Open Databricks → SQL Editor → Run:"
echo "     SELECT * FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard"
echo "     ORDER BY position LIMIT 10;"
echo ""
echo "  3. View detailed guide:"
echo "     cat $PROJECT_ROOT/LOAD_2024_SESSION_GUIDE.md"
echo ""
echo -e "${YELLOW}Files Created:${NC}"
echo "  • Bronze: bronze_raw.* (4 tables)"
echo "  • Silver: default_silver_clean.* (4 tables)"
echo "  • Gold Core: default_gold_analytics.dim_*, fact_* (3 tables)"
echo "  • Gold Marts: default_gold_analytics.*_performance, *_summary, *_leaderboard (3 tables)"
echo ""
echo -e "${GREEN}Total: 14 tables created across Bronze/Silver/Gold layers${NC}"
echo ""
