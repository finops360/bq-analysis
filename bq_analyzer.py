#!/usr/bin/env python3
"""
BigQuery Table Analyzer - Analyzes BigQuery tables to provide optimization suggestions
"""

import json
import re
import os
import sys
import argparse
import logging
import csv
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery
from google.cloud.resourcemanager_v3 import ProjectsClient
from tabulate import tabulate
from google.cloud.exceptions import NotFound

# Import configuration from config.py
from config import (
    ORG_ID, OUTPUT_TABLE, PROJECT_QUERY, MANUAL_PROJECT_LIST,
    OUTPUT_PROJECT, OUTPUT_DATASET, OUTPUT_TABLE_NAME,
    TABLE_SCHEMA, TABLE_SETTINGS, CREDENTIALS_FILE
)

# Import optimization criteria
from optimization_criteria import ALL_CRITERIA, RECOMMENDATION_ORDER, get_recommendations_by_id

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def human_readable_bytes(num_bytes):
    """Convert a byte count into a human-readable string (KB, MB, GB, etc.)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024


def suggest_optimizations(table_obj):
    """
    Provide data-driven suggestions for BigQuery cost optimization and governance,
    along with approximate outcomes (cost savings or performance improvements).
    Uses the criteria defined in optimization_criteria.py.

    Returns a list of suggestion strings.
    """
    suggestions = []
    now = datetime.now(timezone.utc)
    
    # Extract metadata from table object
    table_size_bytes = table_obj.num_bytes or 0
    table_size_gb = table_size_bytes / (1024**3) if table_size_bytes else 0
    is_partitioned = table_obj.time_partitioning is not None
    is_clustered = table_obj.clustering_fields is not None
    modified_time = table_obj.modified
    last_modified_age_days = (now - modified_time).days if modified_time else None
    table_id = table_obj.table_id
    schema = table_obj.schema
    column_count = len(schema) if schema else 0
    
    # Detect sharded tables (pattern like 'table_20220101')
    shards_pattern = re.compile(r'\d{8}')
    is_sharded = bool(shards_pattern.search(table_id))
    
    # Build a dictionary of table properties for criteria checking
    table_data = {
        "table_size_bytes": table_size_bytes,
        "table_size_gb": table_size_gb,
        "row_count": table_obj.num_rows or 0,
        "is_partitioned": is_partitioned,
        "is_clustered": is_clustered,
        "last_modified_days": last_modified_age_days,
        "has_expiration": table_obj.expires is not None,
        "column_count": column_count,
        "has_streaming_buffer": table_obj.streaming_buffer is not None,
        "table_type": table_obj.table_type,
        "has_labels": bool(table_obj.labels),
        "has_description": bool(table_obj.description),
        "table_id": table_id,
        "is_sharded": is_sharded,
        "has_nested_schema": any(f.fields for f in schema) if schema else False,
        "require_partition_filter": getattr(table_obj, 'require_partition_filter', False)
    }
    
    # Get a mapping of recommendation IDs to criteria
    recommendations_by_id = get_recommendations_by_id()
    
    # Track which recommendations we'll include
    recommendation_ids = set()
    
    # Check each criterion against the table data
    for criteria_id, criteria in ALL_CRITERIA.items():
        try:
            if criteria["check"](table_data):
                recommendation_ids.add(criteria["recommendation_id"])
        except (KeyError, TypeError, Exception) as e:
            # Skip criteria that can't be evaluated
            continue
    
    # Build suggestions in the recommended order
    for rec_id in RECOMMENDATION_ORDER:
        if rec_id in recommendation_ids and rec_id in recommendations_by_id:
            criteria = recommendations_by_id[rec_id]
            suggestion = f"{criteria['description']} ({criteria['savings']})."
            suggestions.append(suggestion)
    
    # If no suggestions, add a default message
    if not suggestions:
        suggestions.append("No specific optimization suggestions.")
    
    return suggestions


def list_projects_in_org(org_id, query):
    """
    List all active projects within an organization using the Resource Manager API.

    :param org_id: Organization ID as a string.
    :param query: Query string to filter projects.
    :return: A list of project IDs.
    """
    from google.cloud.resourcemanager_v3.types import SearchProjectsRequest
    
    client = ProjectsClient()
    request = SearchProjectsRequest(query=query)
    projects = []
    for response in client.search_projects(request=request):
        if response.state.name == "ACTIVE":
            projects.append(response.project_id)
    return projects


def process_project(project_id):
    """
    For the given project, list datasets and tables, gather optimization suggestions, and return rows to insert.
    """
    rows_to_insert = []
    bq_client = bigquery.Client(project=project_id)
    datasets = list(bq_client.list_datasets())
    now = datetime.now(timezone.utc)

    if not datasets:
        logger.info(f"No datasets found in project {project_id}.")
        return rows_to_insert

    for dataset in datasets:
        dataset_id = f"{project_id}.{dataset.dataset_id}"
        tables = list(bq_client.list_tables(dataset_id))

        if not tables:
            logger.info(f"No tables found in dataset {dataset_id}.")
            continue

        logger.info(f"Processing dataset: {dataset_id}")
        for table in tables:
            table_ref = f"{project_id}.{dataset.dataset_id}.{table.table_id}"
            try:
                table_obj = bq_client.get_table(table_ref)
            except Exception as e:
                logger.error(f"Error getting table {table_ref}: {e}")
                continue

            # Extract metadata from table object
            table_size_bytes = table_obj.num_bytes or 0
            table_size_gb = table_size_bytes / (1024**3) if table_size_bytes else 0
            row_count = table_obj.num_rows or 0
            
            is_partitioned = table_obj.time_partitioning is not None
            partition_type = table_obj.time_partitioning.type_ if is_partitioned and hasattr(table_obj.time_partitioning, 'type_') else None
            
            is_clustered = table_obj.clustering_fields is not None
            clustering_fields = json.dumps(table_obj.clustering_fields) if is_clustered else None
            
            modified_time = table_obj.modified
            last_modified_days = (now - modified_time).days if modified_time else None
            
            has_expiration = table_obj.expires is not None
            expiration_date = table_obj.expires
            
            schema = table_obj.schema
            column_count = len(schema) if schema else 0
            has_nested_schema = any(f.fields for f in schema) if schema else False
            
            storage_billing_model = getattr(table_obj, 'storage_billing_model', None)
            creation_time = table_obj.created
            has_streaming_buffer = table_obj.streaming_buffer is not None
            table_type = table_obj.table_type
            has_labels = bool(table_obj.labels)
            has_description = bool(table_obj.description)

            # Generate optimization suggestions
            suggestions = suggest_optimizations(table_obj)
            suggestion_json = json.dumps(suggestions) if suggestions else json.dumps(["No specific optimization suggestions."])

            # Create a row with all collected metadata
            row = {
                "project_id": project_id,
                "dataset_id": dataset.dataset_id,
                "table_id": table.table_id,
                "suggestions": suggestion_json,
                "table_size_bytes": table_size_bytes,
                "table_size_gb": table_size_gb,
                "row_count": row_count,
                "is_partitioned": is_partitioned,
                "partition_type": partition_type,
                "is_clustered": is_clustered,
                "clustering_fields": clustering_fields,
                "last_modified": modified_time,
                "last_modified_days": last_modified_days,
                "has_expiration": has_expiration,
                "expiration_date": expiration_date,
                "column_count": column_count,
                "has_nested_schema": has_nested_schema,
                "storage_billing_model": storage_billing_model,
                "creation_time": creation_time,
                "has_streaming_buffer": has_streaming_buffer,
                "table_type": table_type,
                "has_labels": has_labels,
                "has_description": has_description,
                "analyzed_at": now
            }
            
            rows_to_insert.append(row)
            logger.info(f"Processed table: {table_ref} - Size: {human_readable_bytes(table_size_bytes)}, Rows: {row_count}")

    return rows_to_insert


def save_to_csv(rows, filename="optimization_suggestions.csv"):
    """
    Save the rows to a CSV file.
    :param rows: A list of dictionaries representing the rows.
    :param filename: The name of the CSV file to write to.
    """
    if not rows:
        logger.info("No rows to save.")
        return

    try:
        # Get all field names from the first row
        fieldnames = sorted(rows[0].keys())
        
        with open(filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                # Convert any complex types to strings
                for k, v in row.items():
                    if isinstance(v, (dict, list)):
                        row[k] = json.dumps(v)
                writer.writerow(row)
        
        logger.info(f"Successfully saved {len(rows)} rows to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False


def insert_suggestions(rows):
    """
    Insert the given rows into the BigQuery suggestions table.
    :param rows: A list of dictionaries matching the schema of OUTPUT_TABLE.
    """
    if not rows:
        logger.info("No rows to insert.")
        return

    client = bigquery.Client()
    errors = client.insert_rows_json(OUTPUT_TABLE, rows)
    if errors:
        logger.error(f"Errors occurred while inserting rows: {errors}")
        return False
    else:
        logger.info("Successfully inserted optimization suggestions.")
        return True


def analyze_projects():
    """
    Main analysis function that processes projects and returns collected data.
    """
    try:
        if ORG_ID:
            logger.info(f"Organization ID: {ORG_ID}")
            # List projects under organization
            projects = list_projects_in_org(ORG_ID, PROJECT_QUERY)
            if not projects:
                logger.warning(f"No active projects found under organization {ORG_ID} with query: {PROJECT_QUERY}")
                return []
            logger.info(f"Found {len(projects)} projects. Processing each...")
        else:
            # If no ORG_ID provided, use the manual project list
            logger.info("No ORG_ID provided. Using manual project list.")
            projects = MANUAL_PROJECT_LIST
            logger.info(f"Processing {len(projects)} manually specified projects.")
    except Exception as e:
        logger.error(f"Error getting projects: {e}")
        logger.info("Falling back to manual project list.")
        projects = MANUAL_PROJECT_LIST
        logger.info(f"Processing {len(projects)} manually specified projects.")

    all_rows = []
    for project_id in projects:
        logger.info(f"Processing project: {project_id}")
        try:
            rows = process_project(project_id)
            if rows:
                all_rows.extend(rows)
                logger.info(f"Processed {len(rows)} tables in project {project_id}")
            else:
                logger.info(f"No tables processed in project {project_id}")
        except Exception as e:
            logger.error(f"Error processing project {project_id}: {e}")
            logger.error("Continuing with next project...")
            continue
            
    return all_rows


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="BigQuery optimization analysis tool")
    parser.add_argument("--csv-output", dest="csv_output", 
                        help="Save results to this CSV file instead of BigQuery")
    return parser.parse_args()


def main():
    """
    Main function that runs the analysis and saves results.
    """
    # Parse command line arguments
    args = parse_args()
    
    logger.info("Starting BigQuery optimization suggestion analysis.")
    
    # Run the analysis
    all_rows = analyze_projects()
    
    # Insert or save results
    if all_rows:
        # If CSV output is specified, only save to CSV
        if args.csv_output:
            logger.info(f"Saving {len(all_rows)} rows to CSV file as requested")
            csv_filename = args.csv_output
            save_to_csv(all_rows, csv_filename)
            logger.info(f"Results saved to {csv_filename}")
        else:
            # Otherwise try BigQuery insertion with CSV fallback
            try:
                logger.info(f"Inserting {len(all_rows)} rows of table metadata and suggestions")
                success = insert_suggestions(all_rows)
                
                if success:
                    logger.info(f"Successfully inserted {len(all_rows)} rows of data to BigQuery")
                else:
                    logger.warning("Failed to insert data into BigQuery, saving to CSV instead")
                    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"optimization_suggestions_{current_time}.csv"
                    save_to_csv(all_rows, csv_filename)
                    logger.info(f"Results saved to {csv_filename}")
            except Exception as e:
                logger.error(f"Error inserting data into BigQuery: {e}")
                logger.warning("Saving results to CSV file instead")
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_filename = f"optimization_suggestions_{current_time}.csv"
                save_to_csv(all_rows, csv_filename)
                logger.info(f"Results saved to {csv_filename}")
    else:
        logger.warning("No table data collected. No rows to insert.")
        
    logger.info("Completed BigQuery optimization suggestion analysis.")


if __name__ == "__main__":
    main()