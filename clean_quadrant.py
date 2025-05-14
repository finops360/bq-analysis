#!/usr/bin/env python3
"""
Utility script for cleaning and resetting Quadrant collections

This script helps reset the Quadrant vector database by deleting and recreating collections.
"""

import argparse
import requests
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Clean and reset Quadrant collections")
    
    parser.add_argument("--endpoint", default="http://localhost:6333",
                       help="Quadrant API endpoint (default: http://localhost:6333)")
    parser.add_argument("--collection", default="bigquery_schemas",
                       help="Collection name to reset (default: bigquery_schemas)")
    parser.add_argument("--vector-dim", type=int, default=768,
                       help="Vector dimension for new collection (default: 768)")
    parser.add_argument("--list-only", action="store_true",
                       help="Only list collections, don't delete anything")
    
    return parser.parse_args()

def list_collections(endpoint):
    """List all collections in Quadrant"""
    try:
        logger.info(f"Listing collections at {endpoint}")
        resp = requests.get(f"{endpoint}/collections")
        
        if resp.status_code == 200:
            collections = resp.json().get('result', {}).get('collections', [])
            if collections:
                logger.info(f"Found {len(collections)} collections:")
                for i, coll in enumerate(collections):
                    logger.info(f"  {i+1}. {coll.get('name')}")
            else:
                logger.info("No collections found")
            return collections
        else:
            logger.error(f"Failed to list collections: {resp.status_code} - {resp.text}")
            return []
    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        return []

def delete_collection(endpoint, collection_name):
    """Delete a collection from Quadrant"""
    try:
        logger.info(f"Deleting collection: {collection_name}")
        resp = requests.delete(f"{endpoint}/collections/{collection_name}")
        
        if resp.status_code == 200:
            logger.info(f"Successfully deleted collection: {collection_name}")
            return True
        else:
            logger.error(f"Failed to delete collection: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Error deleting collection: {e}")
        return False

def create_collection(endpoint, collection_name, vector_dim):
    """Create a new collection in Quadrant"""
    try:
        logger.info(f"Creating collection: {collection_name}")
        create_resp = requests.put(
            f"{endpoint}/collections/{collection_name}",
            json={
                "vectors": {
                    "size": vector_dim,
                    "distance": "Cosine"
                }
            }
        )
        
        if create_resp.status_code in (200, 201):
            logger.info(f"Successfully created collection: {collection_name}")
            return True
        else:
            logger.error(f"Failed to create collection: {create_resp.status_code} - {create_resp.text}")
            return False
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        return False

def main():
    """Main function"""
    args = parse_args()
    
    # Connect to Quadrant
    try:
        # Test connection
        resp = requests.get(f"{args.endpoint}/collections")
        if resp.status_code != 200:
            logger.error(f"Failed to connect to Quadrant at {args.endpoint}")
            return 1
    except Exception as e:
        logger.error(f"Error connecting to Quadrant: {e}")
        return 1
    
    # List collections
    collections = list_collections(args.endpoint)
    
    # If list-only mode, exit here
    if args.list_only:
        return 0
    
    # Check if the target collection exists
    collection_exists = any(c.get('name') == args.collection for c in collections)
    
    # Delete collection if it exists
    if collection_exists:
        if not delete_collection(args.endpoint, args.collection):
            logger.error("Failed to delete collection, cannot continue")
            return 1
    else:
        logger.info(f"Collection '{args.collection}' does not exist, creating it")
    
    # Create new collection
    if not create_collection(args.endpoint, args.collection, args.vector_dim):
        logger.error("Failed to create collection")
        return 1
    
    logger.info(f"Collection '{args.collection}' was successfully reset")
    logger.info("You can now run the BigQuery Optimizer which will use the clean collection")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())