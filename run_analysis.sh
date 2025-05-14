#!/bin/bash
# BigQuery Optimization Analysis Runner
# This script runs the BigQuery Optimizer

set -e

# Default configuration
CONFIG_FILE="bigquery_optimizer/config/config.yaml"
VERBOSE=false
SKIP_METADATA=false
SKIP_QUERIES=false
SKIP_VECTOR_DB=false  # Using vector DB by default now that we've fixed the issues
QUERY_LIMIT=10

# Display usage information
function show_usage {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  -p, --project PROJECT_ID   Override GCP Project ID"
  echo "  -d, --days DAYS            Override number of days of query history to analyze"
  echo "  -o, --output FILE          Override output file for recommendations"
  echo "  -c, --config FILE          Use alternate config file (default: config.yaml)"
  echo "  -n, --no-llm               Disable LLM-based recommendations"
  echo "  -v, --verbose              Enable verbose logging"
  echo
  echo "  --skip-metadata            Skip collecting table metadata (use existing data)"
  echo "  --skip-queries             Skip collecting query history (use existing data)"
  echo "  --skip-vector-db           Skip using vector database for schema storage"
  echo "  --query-limit NUM          Maximum number of queries to analyze with LLM (default: 10)"
  echo "  -h, --help                 Show this help message"
  echo
  echo "Examples:"
  echo "  $0 --project my-project --days 15 --no-llm"
  echo "  $0 --skip-metadata --skip-queries  # Use existing data files only"
  echo "  $0 --skip-vector-db  # Skip vector database steps but still use LLM"
}

# Parse command line arguments
PARAMS=""
while (( "$#" )); do
  case "$1" in
    -p|--project)
      PROJECT_ID="$2"
      shift 2
      ;;
    -d|--days)
      LOOKBACK_DAYS="$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    -c|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -n|--no-llm)
      NO_LLM=true
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    --skip-metadata)
      SKIP_METADATA=true
      shift
      ;;
    --skip-queries)
      SKIP_QUERIES=true
      shift
      ;;
    --skip-vector-db)
      SKIP_VECTOR_DB=true
      shift
      ;;
    --query-limit)
      QUERY_LIMIT="$2"
      shift 2
      ;;
    -h|--help)
      show_usage
      exit 0
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      show_usage
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;
  esac
done

# Set positional arguments in their proper place
eval set -- "$PARAMS"

# Build the command
CMD="python3 -m bigquery_optimizer.main --config $CONFIG_FILE"

# Add optional arguments
if [ ! -z "$PROJECT_ID" ]; then
  CMD="$CMD --project-id $PROJECT_ID"
fi

if [ ! -z "$LOOKBACK_DAYS" ]; then
  CMD="$CMD --lookback-days $LOOKBACK_DAYS"
fi

if [ ! -z "$OUTPUT_FILE" ]; then
  CMD="$CMD --output-file $OUTPUT_FILE"
fi

if [ "$NO_LLM" = true ]; then
  CMD="$CMD --no-llm"
fi

if [ "$VERBOSE" = true ]; then
  CMD="$CMD --verbose"
fi

# Add optional stage control arguments
if [ "$SKIP_METADATA" = true ]; then
  CMD="$CMD --skip-metadata"
fi

if [ "$SKIP_QUERIES" = true ]; then
  CMD="$CMD --skip-queries"
fi

if [ "$SKIP_VECTOR_DB" = true ]; then
  CMD="$CMD --skip-vector-db"
fi

if [ ! -z "$QUERY_LIMIT" ]; then
  CMD="$CMD --query-limit $QUERY_LIMIT"
fi

# Print configuration
echo "BigQuery Optimizer Configuration:"
echo "  Config file: $CONFIG_FILE"
if [ ! -z "$PROJECT_ID" ]; then
  echo "  Project ID: $PROJECT_ID"
fi
if [ ! -z "$LOOKBACK_DAYS" ]; then
  echo "  Lookback days: $LOOKBACK_DAYS"
fi
if [ ! -z "$OUTPUT_FILE" ]; then
  echo "  Output file: $OUTPUT_FILE"
fi
if [ "$NO_LLM" = true ]; then
  echo "  LLM analysis: Disabled"
else
  echo "  LLM analysis: Enabled"
fi

# Print optional stages configuration
echo "Optional stages:"
if [ "$SKIP_METADATA" = true ]; then
  echo "  Metadata collection: Skipped (using existing data)"
else
  echo "  Metadata collection: Enabled"
fi
if [ "$SKIP_QUERIES" = true ]; then
  echo "  Query history collection: Skipped (using existing data)"
else
  echo "  Query history collection: Enabled"
fi
if [ "$SKIP_VECTOR_DB" = true ]; then
  echo "  Vector database: Skipped"
else
  echo "  Vector database: Enabled"
fi
echo "  Query limit for LLM analysis: $QUERY_LIMIT"
echo

# Run the command
echo "Running: $CMD"
echo
eval $CMD

echo
echo "BigQuery Optimizer analysis complete!"