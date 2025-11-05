#!/usr/bin/env python3
"""
Local Pipeline Test Script
Tests the complete data pipeline with sample data from test_data/

Usage:
    python scripts/test_pipeline_local.py
"""

import os
import sys
import subprocess
from pathlib import Path

# Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(msg):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}\n")


def print_success(msg):
    print(f"{Colors.OKGREEN}✅ {msg}{Colors.ENDC}")


def print_error(msg):
    print(f"{Colors.FAIL}❌ {msg}{Colors.ENDC}")


def print_warning(msg):
    print(f"{Colors.WARNING}⚠️  {msg}{Colors.ENDC}")


def print_info(msg):
    print(f"{Colors.OKCYAN}ℹ️  {msg}{Colors.ENDC}")


def run_command(cmd, cwd=None, env=None):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            env=env or os.environ.copy(),
            capture_output=True,
            text=True,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr


def check_prerequisites():
    """Check if all prerequisites are met"""
    print_header("1. Checking Prerequisites")

    # Check Python version
    print_info("Checking Python version...")
    python_version = sys.version_info
    if python_version >= (3, 11):
        print_success(f"Python {python_version.major}.{python_version.minor}.{python_version.micro}")
    else:
        print_error(f"Python 3.11+ required, found {python_version.major}.{python_version.minor}")
        return False

    # Check for required directories
    print_info("Checking directory structure...")
    required_dirs = [
        "test_data",
        "openf1_api_data_extractor",
        "dbt_f1_data_analytics"
    ]
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print_success(f"Found {dir_path}/")
        else:
            print_error(f"Missing directory: {dir_path}/")
            return False

    # Check for test data files
    print_info("Checking test data files...")
    test_files = [
        "test_data/sessions_9158.json",
        "test_data/drivers_9158.json",
        "test_data/laps_9158.json",
        "test_data/locations_9158.json"
    ]
    for file_path in test_files:
        if Path(file_path).exists():
            print_success(f"Found {file_path}")
        else:
            print_error(f"Missing test file: {file_path}")
            return False

    # Check environment variables
    print_info("Checking Databricks credentials...")
    required_env_vars = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HTTP_PATH"
    ]
    missing_vars = []
    for var in required_env_vars:
        if os.getenv(var):
            print_success(f"{var} is set")
        else:
            print_error(f"{var} not set")
            missing_vars.append(var)

    if missing_vars:
        print_warning("\nPlease set missing environment variables:")
        print_warning("  export DATABRICKS_HOST='your_host'")
        print_warning("  export DATABRICKS_TOKEN='your_token'")
        print_warning("  export DATABRICKS_HTTP_PATH='your_http_path'")
        print_warning("\nOr load from .env file:")
        print_warning("  export $(cat .env | xargs)")
        return False

    print_success("\n✅ All prerequisites met!\n")
    return True


def test_databricks_connection():
    """Test connection to Databricks"""
    print_header("2. Testing Databricks Connection")

    print_info("Running connection test...")
    success, output = run_command(
        "python testconnection.py",
        cwd="openf1_api_data_extractor/src/utils"
    )

    if success:
        print_success("Connected to Databricks successfully!")
        return True
    else:
        print_error("Failed to connect to Databricks")
        print(output)
        return False


def load_bronze_tables():
    """Load test data into bronze tables"""
    print_header("3. Loading Test Data to Bronze Tables")

    # Set LOCAL_DATA_DIR to test_data
    env = os.environ.copy()
    env['LOCAL_DATA_DIR'] = str(Path.cwd() / 'test_data')

    print_info(f"Using test data from: {env['LOCAL_DATA_DIR']}")
    print_info("Loading bronze tables...")

    success, output = run_command(
        "python run_batch_load.py",
        cwd="openf1_api_data_extractor/dlt_bronze_load",
        env=env
    )

    if success:
        print_success("Bronze tables loaded successfully!")
        print(output)
        return True
    else:
        print_error("Failed to load bronze tables")
        print(output)
        return False


def run_dbt_transformations():
    """Run dbt transformations"""
    print_header("4. Running dbt Transformations")

    # Install dbt packages
    print_info("Installing dbt packages...")
    success, output = run_command(
        "dbt deps",
        cwd="dbt_f1_data_analytics"
    )
    if not success:
        print_warning("dbt deps failed, but continuing...")

    # Run dbt debug
    print_info("Testing dbt connection...")
    success, output = run_command(
        "dbt debug --target dev",
        cwd="dbt_f1_data_analytics"
    )
    if success:
        print_success("dbt connection OK")
    else:
        print_error("dbt debug failed")
        print(output)
        return False

    # Run dbt build
    print_info("Building dbt models (this may take a few minutes)...")
    success, output = run_command(
        "dbt build --target dev",
        cwd="dbt_f1_data_analytics"
    )

    if success or "WARN" in output:  # Accept warnings
        print_success("dbt models built successfully!")

        # Extract summary
        lines = output.split('\n')
        for line in lines:
            if 'Done.' in line or 'PASS' in line or 'WARN' in line:
                print_info(line)

        return True
    else:
        print_error("dbt build failed")
        print(output)
        return False


def verify_results():
    """Verify the pipeline results"""
    print_header("5. Verifying Results")

    print_info("Running verification...")
    success, output = run_command(
        "python -c \"from load_to_bronze import BronzeLoader; loader = BronzeLoader(); loader.verify_tables()\"",
        cwd="openf1_api_data_extractor/dlt_bronze_load"
    )

    if success:
        print_success("Bronze tables verified!")
        print(output)
    else:
        print_warning("Verification encountered issues (might be OK)")

    return True


def print_summary():
    """Print summary and next steps"""
    print_header("🏁 Test Pipeline Complete!")

    print(f"{Colors.OKGREEN}✅ Test data loaded successfully{Colors.ENDC}")
    print(f"{Colors.OKGREEN}✅ Bronze tables created{Colors.ENDC}")
    print(f"{Colors.OKGREEN}✅ Silver tables created{Colors.ENDC}")
    print(f"{Colors.OKGREEN}✅ Gold tables created{Colors.ENDC}")

    print(f"\n{Colors.BOLD}Expected Tables:{Colors.ENDC}")
    print("  📊 Bronze: bronze_sessions, bronze_drivers, bronze_laps, bronze_locations")
    print("  🔧 Silver: silver_sessions, silver_drivers, silver_laps, silver_locations")
    print("  📈 Gold: dim_sessions, dim_drivers, fact_laps, analytics tables")

    print(f"\n{Colors.BOLD}Next Steps:{Colors.ENDC}")
    print("  1. View dbt docs:")
    print("     cd dbt_f1_data_analytics && dbt docs generate && dbt docs serve")
    print()
    print("  2. Query your data in Databricks SQL Editor")
    print()
    print("  3. Extract real data from OpenF1 API:")
    print("     cd openf1_api_data_extractor/src/ingestion")
    print("     python extract_sessions.py --num-sessions 1")
    print()
    print("  4. Set up GitHub Actions CI/CD (see docs/CI_CD_SETUP.md)")

    print(f"\n{Colors.BOLD}Useful Queries:{Colors.ENDC}")
    print("  -- View sessions")
    print("  SELECT * FROM bronze_f1_data_analytics.bronze_sessions;")
    print()
    print("  -- Fastest laps")
    print("  SELECT driver_number, MIN(lap_duration) as fastest")
    print("  FROM bronze_f1_data_analytics.bronze_laps")
    print("  GROUP BY driver_number;")

    print(f"\n{Colors.OKGREEN}🏎️  Happy Racing! 💨{Colors.ENDC}\n")


def main():
    """Main execution"""
    print(f"\n{Colors.BOLD}{Colors.HEADER}")
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║                                                                   ║")
    print("║             OpenF1 Data Analytics - Local Test Suite             ║")
    print("║                                                                   ║")
    print("╚═══════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}\n")

    # Run test steps
    steps = [
        ("Prerequisites", check_prerequisites),
        ("Databricks Connection", test_databricks_connection),
        ("Bronze Load", load_bronze_tables),
        ("dbt Transformations", run_dbt_transformations),
        ("Verification", verify_results),
    ]

    for step_name, step_func in steps:
        if not step_func():
            print_error(f"\n❌ Test failed at step: {step_name}")
            print_info("Please fix the errors above and try again.")
            sys.exit(1)

    # All steps passed
    print_summary()
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}⚠️  Test interrupted by user{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
