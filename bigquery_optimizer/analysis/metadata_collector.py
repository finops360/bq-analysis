"""
BigQuery Metadata Collector

Collects metadata about BigQuery tables and query history.
"""

import json
import logging
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

def collect_table_metadata(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Collect metadata about BigQuery tables in the project
    
    Args:
        config: Application configuration
        
    Returns:
        List[Dict]: List of table metadata
    """
    project_id = config['project_id']
    output_file = config['output_metadata_file']
    
    logger.info(f"Collecting table metadata for project {project_id}")
    client = bigquery.Client(project=project_id)
    
    metadata = []
    
    try:
        # List all datasets in the project
        datasets = list(client.list_datasets())
        logger.info(f"Found {len(datasets)} datasets in project {project_id}")
        
        if not datasets:
            logger.warning(f"No datasets found in project {project_id}")
            return []
            
        # Process each dataset
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logger.info(f"Processing dataset: {dataset_id}")
            
            # List tables in this dataset
            tables = list(client.list_tables(f"{project_id}.{dataset_id}"))
            logger.info(f"Found {len(tables)} tables in dataset {dataset_id}")
            
            # Process each table
            for table_ref in tables:
                table_id = f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}"
                logger.info(f"Processing table: {table_id}")
                
                try:
                    # Get table details
                    table = client.get_table(table_id)
                    
                    # Extract basic info
                    size_bytes = table.num_bytes or 0
                    size_gb = size_bytes / (1024**3) if size_bytes else 0
                    row_count = table.num_rows or 0
                    
                    # Extract partitioning info
                    is_partitioned = table.time_partitioning is not None
                    partition_field = table.time_partitioning.field if is_partitioned and table.time_partitioning else None
                    partition_type = table.time_partitioning.type_ if is_partitioned and table.time_partitioning else None
                    
                    # Extract clustering info
                    is_clustered = table.clustering_fields is not None
                    clustering_fields = json.dumps(table.clustering_fields) if is_clustered else None
                    
                    # Extract schema
                    schema_fields = [{
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": field.description
                    } for field in table.schema]
                    
                    schema_json = json.dumps(schema_fields)
                    
                    # Check for table expiration
                    has_expiration = table.expires is not None
                    expiration_date = table.expires.isoformat() if has_expiration else None
                    
                    # Check for labels and description
                    has_labels = bool(table.labels)
                    has_description = bool(table.description)
                    
                    # Create metadata record
                    metadata.append({
                        "table_id": table_id,
                        "dataset_id": dataset_id,
                        "table_name": table_ref.table_id,
                        "size_bytes": size_bytes,
                        "size_gb": size_gb,
                        "row_count": row_count,
                        "is_partitioned": is_partitioned,
                        "partition_field": partition_field,
                        "partition_type": partition_type,
                        "is_clustered": is_clustered,
                        "clustering_fields": clustering_fields,
                        "last_modified": table.modified.isoformat() if table.modified else None,
                        "days_since_modified": (datetime.now(table.modified.tzinfo) - table.modified).days if table.modified else None,
                        "table_type": table.table_type,
                        "schema": schema_json,
                        "has_expiration": has_expiration,
                        "expiration_date": expiration_date,
                        "column_count": len(schema_fields),
                        "has_nested_schema": any(f.get("type") == "RECORD" for f in schema_fields),
                        "storage_billing_model": getattr(table, "storage_billing_model", None),
                        "creation_time": table.created.isoformat() if table.created else None,
                        "has_streaming_buffer": getattr(table, "streaming_buffer", None) is not None,
                        "has_labels": has_labels,
                        "has_description": has_description
                    })
                    
                except Exception as e:
                    logger.warning(f"Error processing table {table_id}: {e}")
                    continue
        
        logger.info(f"Collected metadata for {len(metadata)} tables")
        
        # Save metadata to CSV file
        save_to_csv(metadata, output_file)
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error collecting table metadata: {e}")
        return []

def collect_query_history(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Collect query history from BigQuery
    
    Args:
        config: Application configuration
        
    Returns:
        List[Dict]: List of query history records
    """
    project_id = config['project_id']
    days = config['lookback_days']
    output_file = config['output_queries_file']
    
    logger.info(f"Collecting query history for the last {days} days")
    client = bigquery.Client(project=project_id)
    
    try:
        # Calculate the start date
        start_date = datetime.now() - timedelta(days=days)
        
        # Query INFORMATION_SCHEMA.JOBS to get query history
        query = f"""
        SELECT
            job_id,
            creation_time,
            user_email,
            query,
            total_bytes_processed,
            total_slot_ms,
            state AS status,
            error_result,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
            (SELECT ARRAY_AGG(DISTINCT table_id)
             FROM UNNEST(referenced_tables) AS t) AS referenced_tables
        FROM
            `region-us`.INFORMATION_SCHEMA.JOBS
        WHERE
            creation_time >= TIMESTAMP('{start_date.strftime("%Y-%m-%d")}')
            AND project_id = '{project_id}'
            AND job_type = 'QUERY'
            AND query NOT LIKE '%INFORMATION_SCHEMA%'
            AND query IS NOT NULL
        ORDER BY
            creation_time DESC
        LIMIT 1000
        """
        
        query_job = client.query(query)
        results = list(query_job.result())
        
        query_history = []
        for row in results:
            # Build query history record
            history = {
                "job_id": row.job_id,
                "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                "user_email": row.user_email,
                "query_text": row.query,
                "total_bytes_processed": row.total_bytes_processed,
                "total_slot_ms": row.total_slot_ms,
                "referenced_tables": str(row.referenced_tables),
                "status": row.status,
                "duration_ms": row.duration_ms
            }
            query_history.append(history)
        
        logger.info(f"Collected {len(query_history)} query history records")
        
        # Save query history to CSV file
        save_to_csv(query_history, output_file)
        
        return query_history
        
    except Exception as e:
        logger.error(f"Error collecting query history: {e}")
        return []

def save_to_csv(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save data to CSV file
    
    Args:
        data: List of dictionaries to save
        filename: Output file path
    """
    if not data:
        logger.info(f"No data to save to {filename}")
        return
    
    try:
        fieldnames = data[0].keys()
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            
        logger.info(f"Saved {len(data)} records to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")