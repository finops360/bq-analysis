#!/bin/bash
# setup_table.sh - Script to create the output table for BigQuery optimization analysis

# Clean up function to remove temporary files
cleanup() {
    rm -f schema.json setup_env.sh
}

# Register cleanup function to run on exit
trap cleanup EXIT

# Exit on errors but allow error handling
set -e

# Load configuration
# Run Python script to export configuration
python3 -c "
import json
import os
from config import OUTPUT_PROJECT, OUTPUT_DATASET, OUTPUT_TABLE_NAME, TABLE_SCHEMA, TABLE_SETTINGS, LOCATION

# Output variables to environment
with open('setup_env.sh', 'w') as f:
    f.write(f'export PROJECT=\"{OUTPUT_PROJECT}\"\n')
    f.write(f'export DATASET=\"{OUTPUT_DATASET}\"\n')
    f.write(f'export TABLE=\"{OUTPUT_TABLE_NAME}\"\n')
    f.write(f'export LOCATION=\"{LOCATION}\"\n')
    f.write(f'export PARTITION_FIELD=\"{TABLE_SETTINGS[\"partition_field\"]}\"\n')
    f.write(f'export PARTITION_TYPE=\"{TABLE_SETTINGS[\"partition_type\"]}\"\n')
    f.write(f'export EXPIRATION_DAYS=\"{TABLE_SETTINGS[\"expiration_days\"]}\"\n')
    f.write(f'export DESCRIPTION=\"{TABLE_SETTINGS[\"description\"]}\"\n')

# Convert schema to JSON for use with bq command
with open('schema.json', 'w') as f:
    json.dump(TABLE_SCHEMA, f)
"

# Source the generated environment file
source setup_env.sh

echo "Creating output table for BigQuery optimization suggestions"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Table: $TABLE"

# Check if dataset exists, create if it doesn't
echo "Checking if dataset $DATASET exists..."
bq ls -d "$PROJECT:$DATASET" &>/dev/null
if [ $? -eq 0 ]; then
    echo "Dataset $DATASET already exists."
else
    echo "Dataset $DATASET does not exist. Creating it..."
    bq --location=$LOCATION mk \
        --dataset \
        --description "Dataset for BigQuery optimization analysis" \
        "$PROJECT:$DATASET" || echo "Dataset may already exist. Continuing..."
fi

# Check if table exists, create if it doesn't
echo "Checking if table $TABLE exists..."
bq ls "$PROJECT:$DATASET.$TABLE" &>/dev/null
if [ $? -eq 0 ]; then
    echo "Table $TABLE already exists."
else
    echo "Table $TABLE does not exist. Creating it..."
    
    # Create the table using the schema.json
    bq --location=$LOCATION mk \
        --table \
        --time_partitioning_field "$PARTITION_FIELD" \
        --time_partitioning_type "$PARTITION_TYPE" \
        --expiration "$EXPIRATION_DAYS" \
        --description "$DESCRIPTION" \
        "$PROJECT:$DATASET.$TABLE" \
        schema.json || echo "Error creating table. It may already exist or there could be a schema issue."
    
    # Verify if table was created
    if bq ls "$PROJECT:$DATASET.$TABLE" &>/dev/null; then
        echo "Table created or already exists."
    else
        echo "WARNING: Table may not have been created properly."
    fi
fi

echo "Setup complete."