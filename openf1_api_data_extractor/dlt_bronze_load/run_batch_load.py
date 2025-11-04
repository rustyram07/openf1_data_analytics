"""
Run Batch Load to Bronze Layer
Main script to execute batch loading of JSON files to bronze Delta tables.
Use this for initial data loads or when you have new data files to process.
"""

import os
import glob
from load_to_bronze import BronzeLoader
from config import config


def main():
    """Main execution function"""
    print("="*60)
    print("Bronze Layer Batch Load")
    print("="*60)
    print()

    # Check if data directory exists
    if not os.path.exists(config.LOCAL_DATA_DIR):
        print(f"Error: Data directory not found: {config.LOCAL_DATA_DIR}")
        print("Please update config.py with the correct LOCAL_DATA_DIR path")
        return

    # Count available files
    all_files = glob.glob(os.path.join(config.LOCAL_DATA_DIR, "*.json"))
    print(f"Data Directory: {config.LOCAL_DATA_DIR}")
    print(f"Found {len(all_files)} JSON files")
    print()

    # Show file breakdown
    for entity in ["sessions", "drivers", "laps", "locations"]:
        entity_files = config.get_local_files(entity)
        print(f"  {entity}: {len(entity_files)} files")
    print()

    # Initialize loader
    print("Initializing Bronze Loader...")
    loader = BronzeLoader()
    print()

    try:
        # Load all entities
        # Using pandas approach as it handles schema variations better
        loader.load_all_entities_pandas()

        # Verify tables
        print()
        loader.verify_tables()
        print()

        print("="*60)
        print("Bronze tables loaded successfully!")
        print("="*60)
        print()
        print("Next steps:")
        print("  1. Run 'dbt build' to build your silver and gold layers")
        print("  2. Or use the DLT pipeline for streaming: table_definitions.py")
        print()

    except Exception as e:
        print(f"\nError loading bronze tables: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
