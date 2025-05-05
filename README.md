# BigQuery Optimization Analysis Tool

This tool analyzes BigQuery tables across your organization's projects to provide data-driven cost optimization suggestions and collects detailed metadata about table usage and configuration. It helps identify cost-saving opportunities and performance improvements in your BigQuery environment.

## Features

- Automatic table creation with bq command line tool
- Comprehensive metadata collection and analysis
- Detailed cost optimization recommendations with estimated savings
- Support for organization-wide scanning or manual project list
- CSV output for data analysis
- Well-defined optimization criteria based on industry best practices

## Prerequisites

- Python 3.7+
- Google Cloud SDK installed with bq command line tool
- Service account with BigQuery access
- Required Python packages (see requirements.txt)

## Installation

1. Install required packages:

```bash
pip install -r requirements.txt
```

2. Ensure you have a service account key file with appropriate permissions.

## Configuration

Configure the tool by editing the `config.py` file or setting environment variables:

- `ORG_ID`: Your Google Cloud organization ID (if applicable)
- `OUTPUT_TABLE`: BigQuery table for storing results (format: "project.dataset.table")
- `PROJECT_QUERY`: Custom query for filtering projects (if using organization scanning)
- `MANUAL_PROJECT_LIST`: List of project IDs to scan (used if no ORG_ID is provided)

## Usage

The tool is split into multiple scripts for greater flexibility:

### 1. Setup Output Table

```bash
./setup_table.sh
```

This script creates the BigQuery dataset and table if they don't exist, using the bq command line tool.

### 2. Run Analysis

```bash
./run_analysis.sh --key-file PATH_TO_KEY_FILE
```

Options:
- `--key-file FILE`: Path to service account key file (required)
- `--project PROJECT`: Override output project ID
- `--dataset DATASET`: Override output dataset
- `--table TABLE`: Override output table name
- `--help`: Show help message

### 3. All-in-One Script

For convenience, you can run the entire workflow:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/your-key-file.json
./run_analysis.sh
```

Results will be saved to a timestamped CSV file in the current directory.

## Analysis Example

Here's an example of analyzing a BigQuery project with several tables:

```bash
$ ./run_analysis.sh --key-file service-account.json

Using service account key: service-account.json

Running BigQuery optimization analysis

Analysis complete!
Results saved to CSV file: optimization_suggestions_20250504_123045.csv
```

### Example Output

The analysis produces a CSV file with detailed table metadata and optimization suggestions. Here's a sample excerpt:

| project_id | dataset_id | table_id | table_size_gb | row_count | is_partitioned | is_clustered | suggestions |
|------------|------------|----------|--------------|-----------|----------------|--------------|-------------|
| finops-project | analytics | transactions | 15.6 | 45000000 | false | false | ["Consider partitioning/clustering large table (20-50% cost reduction).", "Very large table; prune unused columns or apply filters (10-30% cost saving).", "Consider materialized views for frequent queries (30-70% cost/performance improvement)."] |
| finops-project | analytics | customer_events | 8.2 | 12500000 | true | false | ["Cluster large partitioned table (20-40% query performance improvement).", "Require partition filters on partitioned table (30-90% cost reduction)."] |
| finops-project | reporting | daily_summary | 0.5 | 365 | false | false | ["No description; add for discoverability (reduces accidental queries ~10%).", "No labels; add labels for governance (indirect cost control benefits)."] |

### Interpretation

For the `transactions` table:
1. It's a large table (15.6GB) without partitioning or clustering
2. Three optimization suggestions are provided:
   - Partitioning would reduce costs by 20-50%
   - Pruning unused columns could save 10-30%
   - Using materialized views could improve performance by 30-70%
   
For the `customer_events` table:
1. It's already partitioned (good), but not clustered
2. Two optimization suggestions:
   - Adding clustering would improve performance by 20-40%
   - Requiring partition filters would reduce costs by 30-90%

For the `daily_summary` table:
1. It's a smaller table with governance recommendations:
   - Adding descriptions would improve discoverability
   - Adding labels would help with governance

After collecting this data, review the `RECOMMENDATIONS.md` file for detailed explanations and implementation guidance for each suggestion.

## Optimization Suggestions

The tool analyzes tables against a comprehensive set of optimization criteria, producing actionable recommendations with estimated savings. For detailed explanation of each recommendation, see [RECOMMENDATIONS.md](RECOMMENDATIONS.md).

### Cost Optimization Criteria
- **Partitioning opportunities** - Large tables that should be partitioned (20-50% savings)
- **Sharded table patterns** - Legacy date-sharded tables that should be converted (up to 80% savings)
- **Partition filter requirements** - Partitioned tables should require filters (30-90% savings)
- **Storage optimizations** - Table expiration and lifecycle policies (10-40% storage savings)
- **Batch vs. streaming** - Converting streaming to batch loads (50%+ cost reduction)

### Performance Optimization Criteria
- **Clustering opportunities** - Tables that would benefit from clustering (20-40% improvement)
- **Materialized view candidates** - Frequently queried aggregations (30-70% improvement)
- **Schema optimizations** - Nested schema and column count analysis (10-25% improvement)
- **BI Engine candidates** - Tables used for dashboards (up to 50% performance gain)
- **Incremental load patterns** - Detecting large, frequently updated tables (20-40% improvement)

### Governance Criteria
- **Labeling** - Tables missing cost attribution and governance labels
- **Documentation** - Tables without proper descriptions
- **Naming conventions** - Tables with non-descriptive names

Each suggestion includes an estimated potential cost reduction or performance improvement, based on typical observed values in production environments.

## Output Schema

The tool collects and stores the following metadata for each table:

- Basic identifiers (project, dataset, table)
- Size information (bytes, GB, row count)
- Partitioning and clustering details
- Age and modification information
- Configuration details
- Schema information
- Usage patterns

## Troubleshooting

If you encounter authentication errors:
1. Make sure your service account key file exists and has the correct permissions
2. Use the `--key-file` option to specify the key file path
3. Check if the service account has access to the projects you're trying to analyze

For error messages:
1. Check the logs for specific error messages
2. Verify the output CSV file exists and contains data
3. For BigQuery access issues, make sure the service account has BigQuery Admin or appropriate permissions

## License

Copyright (c) 2025 FinOps360

## Customization for Your Environment

The BigQuery Optimization Analysis Tool can be easily customized for different projects and environments. Here's how to adapt the tool for your specific needs:

### Changing Target Projects

To analyze different BigQuery projects:

1. Edit `config.py` and update the `MANUAL_PROJECT_LIST`:

```python
# Example: Replace with your project IDs
MANUAL_PROJECT_LIST = [
    "my-analytics-project",
    "my-data-warehouse",
    "my-marketing-data"
]
```

2. Or use organization-wide scanning by setting an environment variable:

```bash
export ORG_ID="123456789012"  # Your Google Cloud Organization ID
```

### Changing Output Location

To change where optimization suggestions are stored:

1. Edit `config.py` and update the `OUTPUT_TABLE` variable:

```python
# Example: Replace with your desired output location
OUTPUT_TABLE = "my-project.analytics_admin.bq_optimizations"
# Format: "project.dataset.table"
```

2. Or set an environment variable when running the script:

```bash
export OUTPUT_TABLE="my-project.analytics_admin.bq_optimizations"
./run_analysis.sh
```

### Customizing Analysis Criteria

To adjust the optimization thresholds:

1. Edit `optimization_criteria.py` and modify the threshold values:

```python
# Example: Change the threshold for large tables from 1GB to 5GB
SIZE_CRITERIA = {
    "large_unpartitioned": {
        "threshold": 5,  # GB (changed from 1GB to 5GB)
        # ... rest of the configuration
    },
    # ... other criteria
}
```

### Demo Setup Example

For a quick demo setup:

```bash
# 1. Clone the repository
git clone https://github.com/example/bigquery-optimization.git
cd bigquery-optimization

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure for demo environment (using example values)
cat > config.py << EOF
import os

# Authentication Configuration
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

# Organization Configuration
ORG_ID = os.environ.get("ORG_ID", "")

# Output table (modify this for your demo)
OUTPUT_TABLE = os.environ.get("OUTPUT_TABLE", "demo-project-123.analytics.optimization_suggestions")

# Parse the output table into its components
OUTPUT_PROJECT, OUTPUT_DATASET, OUTPUT_TABLE_NAME = OUTPUT_TABLE.split(".")

# Project query (if using organization-based scanning)
PROJECT_QUERY = os.environ.get("PROJECT_QUERY", f"parent.type:organization parent.id:{ORG_ID} state:ACTIVE")

# Manual project list (modify these for your demo)
MANUAL_PROJECT_LIST = [
    "demo-project-123",
    "demo-analytics-456"
]

# Rest of the configuration remains unchanged
EOF

# 4. Run the analysis (assuming service account key is already set up)
export GOOGLE_APPLICATION_CREDENTIALS=~/demo-project-key.json
./run_analysis.sh
```

This will analyze the specified demo projects and save results to a CSV file in the current directory.