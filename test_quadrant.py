#!/usr/bin/env python3
"""
Test script for Quadrant connectivity and operations

This script verifies that Quadrant is accessible and working properly with the UUIDs.
"""

import requests
import json
import uuid
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Configuration
QUADRANT_ENDPOINT = "http://localhost:6333"
COLLECTION_NAME = "bigquery_schemas_test"
VECTOR_DIM = 768

def test_quadrant_connection():
    """Test basic connectivity to Quadrant"""
    try:
        logger.info("Testing Quadrant connection...")
        resp = requests.get(f"{QUADRANT_ENDPOINT}/collections")
        
        if resp.status_code == 200:
            logger.info("✅ Successfully connected to Quadrant")
            collections = resp.json().get('result', {}).get('collections', [])
            logger.info(f"Found {len(collections)} collections")
            return True
        else:
            logger.error(f"❌ Failed to connect to Quadrant: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Error connecting to Quadrant: {e}")
        return False

def create_test_collection():
    """Create a test collection"""
    try:
        logger.info(f"Creating test collection: {COLLECTION_NAME}")
        create_resp = requests.put(
            f"{QUADRANT_ENDPOINT}/collections/{COLLECTION_NAME}",
            json={
                "vectors": {
                    "size": VECTOR_DIM,
                    "distance": "Cosine"
                }
            }
        )
        
        if create_resp.status_code in (200, 201):
            logger.info(f"✅ Successfully created collection {COLLECTION_NAME}")
            return True
        else:
            logger.error(f"❌ Failed to create collection: {create_resp.status_code} - {create_resp.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Error creating collection: {e}")
        return False

def test_point_operations():
    """Test point operations with UUIDs"""
    try:
        # Create test points with UUIDs
        logger.info("Testing point operations with UUIDs...")
        
        # Generate a test UUID
        test_table_id = "project.dataset.test_table"
        test_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, test_table_id))
        
        logger.info(f"Test UUID: {test_uuid} for table: {test_table_id}")
        
        # Create a test point
        points = [{
            "id": test_uuid,
            "vector": [0.1] * VECTOR_DIM,
            "payload": {
                "table_id": test_table_id,
                "point_id": test_uuid,
                "schema_text": "Test schema"
            }
        }]
        
        # Store the point
        logger.info("Storing test point...")
        resp = requests.put(
            f"{QUADRANT_ENDPOINT}/collections/{COLLECTION_NAME}/points",
            json={"points": points}
        )
        
        if resp.status_code in (200, 201):
            logger.info("✅ Successfully stored test point")
        else:
            logger.error(f"❌ Failed to store point: {resp.status_code} - {resp.text}")
            return False
        
        # Retrieve the point by ID
        logger.info("Retrieving point by ID...")
        get_resp = requests.get(
            f"{QUADRANT_ENDPOINT}/collections/{COLLECTION_NAME}/points/{test_uuid}"
        )
        
        if get_resp.status_code == 200:
            logger.info("✅ Successfully retrieved point by ID")
        else:
            logger.error(f"❌ Failed to retrieve point by ID: {get_resp.status_code} - {get_resp.text}")
            return False
        
        # Search for the point by payload
        logger.info("Searching for point by payload...")
        search_resp = requests.post(
            f"{QUADRANT_ENDPOINT}/collections/{COLLECTION_NAME}/points/scroll",
            json={
                "filter": {
                    "must": [
                        {
                            "key": "payload.table_id",
                            "match": {
                                "value": test_table_id
                            }
                        }
                    ]
                },
                "limit": 1
            }
        )
        
        if search_resp.status_code == 200:
            points = search_resp.json().get("result", {}).get("points", [])
            if points:
                logger.info("✅ Successfully found point by payload search")
            else:
                logger.error("❌ Point not found in payload search")
                return False
        else:
            logger.error(f"❌ Failed to search points: {search_resp.status_code} - {search_resp.text}")
            return False
        
        # Clean up - delete the test collection
        logger.info("Cleaning up test collection...")
        delete_resp = requests.delete(
            f"{QUADRANT_ENDPOINT}/collections/{COLLECTION_NAME}"
        )
        
        if delete_resp.status_code == 200:
            logger.info("✅ Successfully deleted test collection")
        else:
            logger.warning(f"⚠️ Failed to delete test collection: {delete_resp.status_code} - {delete_resp.text}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in point operations: {e}")
        return False

def main():
    """Main function"""
    logger.info("Starting Quadrant connectivity test")
    
    # Test connection
    if not test_quadrant_connection():
        logger.error("Failed to connect to Quadrant. Make sure it's running at http://localhost:6333")
        return False
    
    # Create test collection
    if not create_test_collection():
        logger.error("Failed to create test collection")
        return False
    
    # Test point operations
    if not test_point_operations():
        logger.error("Failed to perform point operations")
        return False
    
    logger.info("✅ All tests passed! Quadrant is working properly with UUIDs.")
    logger.info("You can now run the BigQuery Optimizer with:")
    logger.info("  ./run_analysis.sh")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)