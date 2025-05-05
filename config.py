import os

# Authentication Configuration
# Path to service account credentials file
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

# Organization Configuration
# If ORG_ID is empty or not set, we will not list projects from the organization.
# Instead, we will use MANUAL_PROJECT_LIST.
ORG_ID = os.environ.get("ORG_ID", "")

# The BigQuery table to insert suggestions into (will be created if it doesn't exist).
# Format: "project.dataset.table"
OUTPUT_TABLE = os.environ.get("OUTPUT_TABLE", "finops360-dev-2025.cloudact_dev.optimization_suggestions")

# Parse the output table into its components for table creation if needed
OUTPUT_PROJECT, OUTPUT_DATASET, OUTPUT_TABLE_NAME = OUTPUT_TABLE.split(".")

# If ORG_ID is provided and you want to filter projects, specify the query:
# For example, "parent.type:organization parent.id:ORG_ID state:ACTIVE"
PROJECT_QUERY = os.environ.get("PROJECT_QUERY", f"parent.type:organization parent.id:{ORG_ID} state:ACTIVE")

# If ORG_ID is not provided, we use this manual list of project IDs
MANUAL_PROJECT_LIST = [
    # Add your project IDs here
    "finops360-dev-2025",
]

# Default location for resources
LOCATION = os.environ.get("LOCATION", "US")

# Table schema for optimization suggestions
TABLE_SCHEMA = [
    {"name": "project_id", "type": "STRING", "mode": "REQUIRED", "description": "The project ID"},
    {"name": "dataset_id", "type": "STRING", "mode": "REQUIRED", "description": "The dataset ID"},
    {"name": "table_id", "type": "STRING", "mode": "REQUIRED", "description": "The table ID"},
    {"name": "suggestions", "type": "STRING", "mode": "REQUIRED", "description": "JSON array of optimization suggestions"},
    {"name": "table_size_bytes", "type": "INTEGER", "mode": "NULLABLE", "description": "Size of the table in bytes"},
    {"name": "table_size_gb", "type": "FLOAT", "mode": "NULLABLE", "description": "Size of the table in GB"},
    {"name": "row_count", "type": "INTEGER", "mode": "NULLABLE", "description": "Number of rows in the table"},
    {"name": "is_partitioned", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table is partitioned"},
    {"name": "partition_type", "type": "STRING", "mode": "NULLABLE", "description": "Type of partitioning (if applicable)"},
    {"name": "is_clustered", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table is clustered"},
    {"name": "clustering_fields", "type": "STRING", "mode": "NULLABLE", "description": "Fields used for clustering (if applicable)"},
    {"name": "last_modified", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "When the table was last modified"},
    {"name": "last_modified_days", "type": "INTEGER", "mode": "NULLABLE", "description": "Days since the table was last modified"},
    {"name": "has_expiration", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table has an expiration date"},
    {"name": "expiration_date", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "When the table will expire (if applicable)"},
    {"name": "column_count", "type": "INTEGER", "mode": "NULLABLE", "description": "Number of columns in the table"},
    {"name": "has_nested_schema", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table has nested fields"},
    {"name": "storage_billing_model", "type": "STRING", "mode": "NULLABLE", "description": "Storage billing model (LOGICAL, PHYSICAL, etc.)"},
    {"name": "creation_time", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "When the table was created"},
    {"name": "has_streaming_buffer", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table has a streaming buffer"},
    {"name": "table_type", "type": "STRING", "mode": "NULLABLE", "description": "Type of table (TABLE, VIEW, EXTERNAL, etc.)"},
    {"name": "has_labels", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table has labels"},
    {"name": "has_description", "type": "BOOLEAN", "mode": "NULLABLE", "description": "Whether the table has a description"},
    {"name": "analyzed_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "When this analysis was performed"}
]

# Default table settings
TABLE_SETTINGS = {
    "partition_field": "analyzed_at",
    "partition_type": "DAY",
    "expiration_days": 365,  # Set table expiration to 1 year
    "description": "BigQuery table optimization suggestions and metadata"
}