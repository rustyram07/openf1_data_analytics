from dotenv import load_dotenv
import os
from databricks.connect import DatabricksSession

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

if not host or not token:
    raise ValueError(
        "Missing required environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN\n"
        "Please set them before running this script:\n"
        "  export DATABRICKS_HOST='XXXXX'\n"
        "  export DATABRICKS_TOKEN='XXXXXX'\n"
        "Or create a .env file and load it."
    )

# Create Databricks session with serverless compute
spark = DatabricksSession.builder.remote(
    host=host,
    token=token,
    serverless=True
).getOrCreate()

# Test the connection
print("Connection successful!")
print(f"Spark version: {spark.version}")

# Simple test query
df = spark.sql("SELECT 'Ready Use Databricks!' as message")
df.show()