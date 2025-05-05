import json
import re
import os
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
    TABLE_SCHEMA, TABLE_SETTINGS
)

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
    Provide heuristic-based suggestions for BigQuery cost optimization and governance,
    along with approximate outcomes (cost savings or performance improvements).

    Returns a list of suggestion strings.
    """
    suggestions = []
    now = datetime.now(timezone.utc)
    table_size_bytes = table_obj.num_bytes
    table_size_gb = table_size_bytes / (1024**3)
    is_partitioned = table_obj.time_partitioning is not None
    is_clustered = table_obj.clustering_fields is not None
    modified_time = table_obj.modified
    last_modified_age_days = (now - modified_time).days if modified_time else None
    table_id = table_obj.table_id
    schema = table_obj.schema
    num_columns = len(schema)

    shards_pattern = re.compile(r'\d{8}')
    is_sharded = bool(shards_pattern.search(table_id))

    if table_size_gb > 1 and not is_partitioned and not is_clustered:
        suggestions.append("Consider partitioning/clustering large table (20-50% cost reduction).")

    if last_modified_age_days is not None and last_modified_age_days > 90:
        suggestions.append("Table old (>90 days since modification); apply expiration/archive (20-40% storage saving).")

    if is_sharded:
        suggestions.append("Date-sharded pattern detected; use a partitioned table (up to 80% scan cost reduction).")

    if table_size_gb > 10:
        suggestions.append("Very large table; prune unused columns or apply filters (10-30% cost saving).")

    if table_size_gb > 5:
        suggestions.append("Consider materialized views for frequent queries (30-70% cost/performance improvement).")

    if table_obj.expires is None and last_modified_age_days and last_modified_age_days > 180:
        suggestions.append("Long-unused table with no expiration; add expiration (20%+ storage cost saving).")

    if num_columns > 50:
        suggestions.append("Many columns (>50); reduce or split columns (10-20% efficiency gain).")

    if is_partitioned and not table_obj.require_partition_filter:
        suggestions.append("Require partition filters on partitioned table (30-90% cost reduction).")

    if table_obj.streaming_buffer:
        suggestions.append("Streaming buffer in use; consider batch loads (50%+ cheaper than streaming).")

    if table_obj.table_type == "EXTERNAL":
        suggestions.append("External table; ensure data pruning (20-40% cost saving).")

    if table_obj.table_type == "VIEW":
        suggestions.append("View detected; consider materialized views (30-60% repeated query savings).")

    if is_partitioned and "time" in table_id.lower() and is_sharded:
        suggestions.append("Reassess partitioning strategy alignment (20-50% scan reduction).")

    if table_size_gb > 5 and is_partitioned and not is_clustered:
        suggestions.append("Cluster large partitioned table (20-40% query performance improvement).")

    if not table_obj.labels:
        suggestions.append("No labels; add labels for governance (indirect cost control benefits).")

    nested_fields = any(f.fields for f in schema)
    if nested_fields and table_size_gb > 1:
        suggestions.append("Nested schema; consider flattening (10-25% query cost reduction).")

    if last_modified_age_days is not None and last_modified_age_days < 1 and table_size_gb > 5:
        suggestions.append("Large, frequently updated table; incremental loads/partitioning (20-40% cost saving).")

    if table_obj.expires is None and table_size_gb > 2:
        suggestions.append("Set table expiration for large table (10-30% long-term storage savings).")

    if not table_obj.description:
        suggestions.append("No description; add for discoverability (reduces accidental queries ~10%).")

    if not re.search('[a-zA-Z]', table_id):
        suggestions.append("Non-descriptive name; rename for clarity (indirect cost avoidance).")

    if table_size_gb > 1:
        suggestions.append("Consider BI Engine for dashboards (up to 50% performance gain).")

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
            
            storage_billing_model = table_obj.storage_billing_model
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


def ensure_output_table_exists():
    """
    Ensure the output table exists. If it doesn't, create it with the defined schema.
    """
    client = bigquery.Client(project=OUTPUT_PROJECT)
    
    # Check if the dataset exists, create it if it doesn't
    dataset_id = f"{OUTPUT_PROJECT}.{OUTPUT_DATASET}"
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        logger.info(f"Dataset {dataset_id} not found. Creating it...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Set the location to your preferred location
        dataset = client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_id} created.")
    
    # Check if the table exists, create it if it doesn't
    table_id = f"{dataset_id}.{OUTPUT_TABLE_NAME}"
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists.")
    except NotFound:
        logger.info(f"Table {table_id} not found. Creating it...")
        
        schema = [bigquery.SchemaField(field["name"], field["type"], field["mode"], field["description"]) 
                  for field in TABLE_SCHEMA]
        
        table = bigquery.Table(table_id, schema=schema)
        table.description = TABLE_SETTINGS["description"]
        
        # Set up time partitioning
        if TABLE_SETTINGS.get("partition_field"):
            table.time_partitioning = bigquery.TimePartitioning(
                type_=TABLE_SETTINGS["partition_type"],
                field=TABLE_SETTINGS["partition_field"]
            )
        
        # Set up table expiration
        if TABLE_SETTINGS.get("expiration_days"):
            table.expires = datetime.now(timezone.utc) + timedelta(days=TABLE_SETTINGS["expiration_days"])
        
        table = client.create_table(table)
        logger.info(f"Table {table_id} created successfully")
    
    return True


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

    # Ensure table exists before inserting
    ensure_output_table_exists()

    client = bigquery.Client()
    errors = client.insert_rows_json(OUTPUT_TABLE, rows)
    if errors:
        logger.error(f"Errors occurred while inserting rows: {errors}")
        return False
    else:
        logger.info("Successfully inserted optimization suggestions.")
        return True


def main():
    logger.info("Starting BigQuery optimization suggestion analysis.")
    
    try:
        # Ensure output table exists before beginning analysis
        ensure_output_table_exists()
        logger.info(f"Output table confirmed: {OUTPUT_TABLE}")
    except Exception as e:
        logger.error(f"Error creating/accessing output table: {e}")
        logger.error("Make sure you have authenticated with Google Cloud. Run the following command:")
        logger.error("gcloud auth application-default login")
        return

    try:
        if ORG_ID:
            logger.info(f"Organization ID: {ORG_ID}")
            # List projects under organization
            projects = list_projects_in_org(ORG_ID, PROJECT_QUERY)
            if not projects:
                logger.warning(f"No active projects found under organization {ORG_ID} with query: {PROJECT_QUERY}")
                return
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

    # Insert all collected suggestions into BigQuery
    if all_rows:
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
