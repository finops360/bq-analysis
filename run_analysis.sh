#!/bin/bash
# run_analysis.sh - Run the BigQuery optimization analysis

# Default key file
KEY_FILE="$HOME/finops360-dev-2025-8fe770ea99a8.json"
CSV_OUTPUT=1  # Default to CSV output for reliability

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --key-file) KEY_FILE="$2"; shift ;;
        --project) PROJECT="$2"; shift ;;
        --dataset) DATASET="$2"; shift ;;
        --table) TABLE="$2"; shift ;;
        --help) SHOW_HELP=1 ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Show help if requested
if [[ -n "$SHOW_HELP" ]]; then
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --key-file FILE    Path to the service account key file (default: $KEY_FILE)"
    echo "  --project PROJECT  Override output project ID"
    echo "  --dataset DATASET  Override output dataset"
    echo "  --table TABLE      Override output table name"
    echo "  --help             Show this help message"
    exit 0
fi

# Check if key file exists
if [[ ! -f "$KEY_FILE" ]]; then
    echo "Error: Service account key file not found: $KEY_FILE"
    echo "Please provide a valid key file with --key-file"
    exit 1
fi

# Export credentials for Google API
export GOOGLE_APPLICATION_CREDENTIALS="$KEY_FILE"
echo "Using service account key: $KEY_FILE"

# Override output table if specified
if [[ -n "$PROJECT" && -n "$DATASET" && -n "$TABLE" ]]; then
    export OUTPUT_TABLE="${PROJECT}.${DATASET}.${TABLE}"
    echo "Using custom output table: $OUTPUT_TABLE"
fi

# Generate timestamp for CSV output
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_FILE="optimization_suggestions_${TIMESTAMP}.csv"

# Run the analyzer with CSV output for reliability
echo -e "\nRunning BigQuery optimization analysis"
python3 bq_analyzer.py --csv-output "$CSV_FILE"

echo -e "\nAnalysis complete!"
echo "Results saved to CSV file: $CSV_FILE"