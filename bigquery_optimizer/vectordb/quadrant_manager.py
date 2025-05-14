"""
Quadrant Vector Database Manager

Provides integration with Quadrant vector database for schema storage and similarity search.
"""

import json
import logging
import hashlib
import struct
import array
import requests
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class QuadrantManager:
    """
    Manages interaction with Quadrant vector database
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Quadrant manager with the provided configuration
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.endpoint = config['quadrant_endpoint']
        self.collection = config['quadrant_collection']
        self.vector_dim = config['vector_dimension']
        self.ollama_endpoint = config['ollama_endpoint']
        self.ollama_model = config['ollama_model']
    
    def initialize_collection(self) -> bool:
        """
        Initialize Quadrant collection for schema storage
        
        Returns:
            bool: Success status
        """
        logger.info(f"Initializing Quadrant collection: {self.collection}")
        
        try:
            # Check if collection exists
            resp = requests.get(f"{self.endpoint}/collections")
            if resp.status_code != 200:
                logger.error(f"Failed to connect to Quadrant at {self.endpoint}")
                return False
                
            collections = resp.json().get('result', {}).get('collections', [])
            collection_exists = any(c.get('name') == self.collection for c in collections)
            
            # Create collection if it doesn't exist
            if not collection_exists:
                logger.info(f"Creating new collection: {self.collection}")
                create_resp = requests.put(
                    f"{self.endpoint}/collections/{self.collection}",
                    json={
                        "vectors": {
                            "size": self.vector_dim,
                            "distance": "Cosine"
                        }
                    }
                )
                
                if create_resp.status_code not in (200, 201):
                    logger.error(f"Failed to create collection: {create_resp.text}")
                    return False
                    
                logger.info(f"Created collection {self.collection}")
            else:
                logger.info(f"Collection {self.collection} already exists")
                
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Quadrant: {e}")
            return False
    
    def store_schemas(self, table_metadata: List[Dict[str, Any]]) -> bool:
        """
        Store schema information in Quadrant
        
        Args:
            table_metadata: List of table metadata dictionaries
            
        Returns:
            bool: Success status
        """
        logger.info(f"Storing {len(table_metadata)} schemas in Quadrant")
        
        try:
            points = []
            
            for table in table_metadata:
                # Create a text representation of the schema
                table_id = table['table_id']
                schema_text = f"Table: {table_id}\n"
                schema_text += f"Size: {table['size_gb']:.2f} GB\n"
                schema_text += f"Rows: {table['row_count']}\n"
                schema_text += f"Partitioned: {table['is_partitioned']}\n"
                schema_text += f"Clustered: {table['is_clustered']}\n\n"
                schema_text += "Schema:\n"
                
                try:
                    schema = json.loads(table['schema'])
                    for field in schema:
                        schema_text += f"- {field['name']} ({field['type']}, {field['mode']})\n"
                except:
                    schema_text += "[Schema parsing error]\n"
                    
                # Generate embedding
                embedding = self.generate_embedding(schema_text)
                if not embedding:
                    logger.warning(f"Failed to generate embedding for {table_id}")
                    continue
                    
                # Create point
                points.append({
                    "id": table_id.replace(".", "_"),
                    "vector": embedding,
                    "payload": {
                        "table_id": table_id,
                        "schema_text": schema_text,
                        "metadata": table
                    }
                })
                
            # Store points in batches
            if points:
                logger.info(f"Storing {len(points)} points in Quadrant")
                resp = requests.put(
                    f"{self.endpoint}/collections/{self.collection}/points",
                    json={"points": points}
                )
                
                if resp.status_code not in (200, 201):
                    logger.error(f"Failed to store points: {resp.text}")
                    return False
                    
                logger.info(f"Successfully stored {len(points)} schema points in Quadrant")
                return True
            else:
                logger.warning("No points to store in Quadrant")
                return False
                
        except Exception as e:
            logger.error(f"Error storing schemas in Quadrant: {e}")
            return False
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate a deterministic embedding for text
        
        Args:
            text: Text to generate embedding for
            
        Returns:
            List[float]: Embedding vector
        """
        try:
            # Try calling Ollama for embeddings
            if self.ollama_endpoint:
                try:
                    response = requests.post(
                        self.ollama_endpoint,
                        json={
                            "model": self.ollama_model,
                            "prompt": text,
                            "options": {"embedding": True}
                        }
                    )
                    
                    if response.status_code == 200:
                        embedding = response.json().get("embedding")
                        if embedding and isinstance(embedding, list) and len(embedding) > 0:
                            # Ensure we have the correct dimension
                            if len(embedding) >= self.vector_dim:
                                return embedding[:self.vector_dim]
                            else:
                                # Pad with zeros if needed
                                return embedding + [0.0] * (self.vector_dim - len(embedding))
                except Exception as e:
                    logger.warning(f"Error getting embedding from Ollama: {e}, falling back to hash-based approach")
            
            # Fallback to hash-based approach if Ollama fails or isn't configured
            # Create a deterministic embedding from hash of content
            hash_obj = hashlib.sha256(text.encode())
            hash_bytes = hash_obj.digest()
            
            # Convert hash to a list of float values (-1 to 1)
            # Repeat the hash as needed to get enough bytes
            repeats = (self.vector_dim * 4 + len(hash_bytes) - 1) // len(hash_bytes)
            extended_hash = hash_bytes * repeats
            
            # Convert to float values between -1 and 1
            floats = array.array('f')
            for i in range(0, min(self.vector_dim * 4, len(extended_hash)), 4):
                if i + 4 <= len(extended_hash):
                    # Create a float from 4 bytes
                    val = struct.unpack('f', extended_hash[i:i+4])[0]
                    # Ensure the value is between -1 and 1
                    val = max(min(val, 1.0), -1.0)
                    floats.append(val)
            
            # Ensure we have exactly vector_dim values
            embedding = list(floats)[:self.vector_dim]
            while len(embedding) < self.vector_dim:
                embedding.append(0.0)
                
            return embedding
                
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            # Return a zero vector as fallback
            return [0.0] * self.vector_dim
    
    def get_schema_by_table_id(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get schema information for a specific table
        
        Args:
            table_id: Table ID to retrieve
            
        Returns:
            Dict containing schema information or None
        """
        try:
            # Replace dots with underscores for Quadrant ID format
            point_id = table_id.replace(".", "_")
            
            resp = requests.post(
                f"{self.endpoint}/collections/{self.collection}/points/scroll",
                json={
                    "filter": {
                        "must": [
                            {
                                "key": "id",
                                "match": {
                                    "value": point_id
                                }
                            }
                        ]
                    },
                    "limit": 1
                }
            )
            
            if resp.status_code != 200:
                logger.error(f"Error retrieving schema: {resp.text}")
                return None
                
            points = resp.json().get("result", {}).get("points", [])
            if not points:
                logger.warning(f"No schema found for table {table_id}")
                return None
                
            return points[0].get("payload", {})
            
        except Exception as e:
            logger.error(f"Error retrieving schema: {e}")
            return None
    
    def get_relevant_schemas(self, query_text: str, table_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get schemas relevant to a query
        
        Args:
            query_text: SQL query text
            table_ids: List of referenced table IDs
            
        Returns:
            List[Dict]: Relevant schema information
        """
        schemas = []
        
        try:
            # First, get the referenced tables
            for table_id in table_ids:
                schema = self.get_schema_by_table_id(table_id)
                if schema:
                    schemas.append(schema)
            
            # If no schemas found or we need more context, search by query similarity
            if not schemas or len(schemas) < 3:
                # Generate embedding for query
                query_embedding = self.generate_embedding(query_text)
                if query_embedding:
                    # Search for similar schemas
                    search_resp = requests.post(
                        f"{self.endpoint}/collections/{self.collection}/points/search",
                        json={
                            "vector": query_embedding,
                            "limit": 3,
                            "with_payload": True
                        }
                    )
                    
                    if search_resp.status_code == 200:
                        search_results = search_resp.json().get("result", [])
                        for result in search_results:
                            # Avoid duplicates
                            payload = result.get("payload", {})
                            if payload and not any(s.get("table_id") == payload.get("table_id") for s in schemas):
                                schemas.append(payload)
            
            return schemas
            
        except Exception as e:
            logger.error(f"Error retrieving schemas: {e}")
            return []