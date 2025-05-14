#!/bin/bash
# BigQuery Optimization Analysis Runner
# This script runs the BigQuery Optimizer

set -e

# Default configuration
CONFIG_FILE="bigquery_optimizer/config/config.yaml"
VERBOSE=false

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
  echo "  -h, --help                 Show this help message"
  echo
  echo "Example:"
  echo "  $0 --project my-project --days 15 --no-llm"
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
CMD="python -m bigquery_optimizer.main --config $CONFIG_FILE"

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
echo

# Run the command
echo "Running: $CMD"
echo
eval $CMD

echo
echo "BigQuery Optimizer analysis complete!"