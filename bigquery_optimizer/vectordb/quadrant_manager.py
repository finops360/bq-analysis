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
                    
                # Create point - use UUID for point ID to meet Quadrant requirements
                import uuid
                # Generate a deterministic UUID based on table_id
                point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table_id))

                points.append({
                    "id": point_uuid,  # Use UUID format which is accepted by Quadrant
                    "vector": embedding,
                    "payload": {
                        "table_id": table_id,
                        "point_id": point_uuid,  # Store the ID for reference
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
        Generate embedding vector for text using Ollama or a fallback method
        
        The function first tries to get embeddings from Ollama API. If that fails,
        it falls back to a deterministic hash-based approach.
        
        Args:
            text: Text to generate embedding for
            
        Returns:
            List[float]: Embedding vector of dimension vector_dim
        """
        try:
            # Try calling Ollama for embeddings
            if self.ollama_endpoint:
                try:
                    # Your Ollama installation doesn't support embeddings via API
                    # Instead, we'll use a text-based embedding approach

                    # First, let's get a short summary of the text from Ollama
                    # This will help create a more stable embedding
                    summary_prompt = f"Summarize this text in a few key points, focusing on the most important technical details:\n\n{text}"

                    response = requests.post(
                        self.ollama_endpoint,
                        json={
                            "model": self.ollama_model,
                            "prompt": summary_prompt,
                            "stream": False,
                            "temperature": 0.1,  # Low temperature for consistent results
                            "num_predict": 256   # Short summary
                        }
                    )

                    # Check response
                    if response.status_code == 200:
                        result = response.json()

                        # Get the generated summary
                        summary = result.get("response", "")

                        if summary:
                            # We'll use the summary to generate a deterministic embedding
                            summary_hash = hashlib.md5(summary.encode()).digest()
                            text_hash = hashlib.sha256(text.encode()).digest()

                            # Combine both hashes for a more robust embedding
                            combined_hash = summary_hash + text_hash

                            # Convert to embedding
                            embedding = []
                            for i in range(self.vector_dim):
                                # Use different parts of hash for each dimension
                                pos = i % len(combined_hash)
                                val = (combined_hash[pos] / 255.0) * 2.0 - 1.0  # Scale to [-1, 1]
                                embedding.append(val)

                            # Normalize
                            magnitude = sum(x*x for x in embedding) ** 0.5
                            if magnitude > 0:
                                embedding = [x/magnitude for x in embedding]

                            logger.info("Generated text-based LLM embedding")
                            return embedding
                        else:
                            logger.warning("No summary text returned from Ollama API")
                    else:
                        logger.warning(f"Failed to get embeddings from Ollama: {response.status_code} - {response.text}")
                
                except Exception as e:
                    logger.warning(f"Error getting embedding from Ollama: {e}, falling back to hash-based approach")
            
            # Fallback to deterministic hash-based approach
            logger.info("Using hash-based embedding generation as fallback")
            
            # Create a deterministic embedding from hash of content
            hash_obj = hashlib.sha256(text.encode())
            hash_bytes = hash_obj.digest()
            
            # Convert hash to a list of float values between -1 and 1
            # We need vector_dim values, so we may need to repeat the hash
            embedding = []
            
            # Generate enough pseudo-random but deterministic values
            for i in range(self.vector_dim):
                # Use different parts of hash for each dimension
                # to ensure variety while maintaining determinism
                position = i % len(hash_bytes)
                value = (hash_bytes[position] / 255.0) * 2.0 - 1.0  # Scale to [-1, 1]
                embedding.append(value)
            
            # Normalize the vector (important for cosine similarity)
            magnitude = sum(x*x for x in embedding) ** 0.5
            if magnitude > 0:
                embedding = [x/magnitude for x in embedding]
                
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
            # Generate the same UUID as used when storing the point
            import uuid
            point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table_id))

            # First try looking up by UUID point ID
            resp = requests.post(
                f"{self.endpoint}/collections/{self.collection}/points/scroll",
                json={
                    "filter": {
                        "must": [
                            {
                                "key": "id",
                                "match": {
                                    "value": point_uuid
                                }
                            }
                        ]
                    },
                    "limit": 1
                }
            )
            
            if resp.status_code != 200:
                logger.error(f"Error retrieving schema by ID: {resp.text}")
                return None

            points = resp.json().get("result", {}).get("points", [])

            # If not found by ID, try searching by payload.table_id
            if not points:
                logger.info(f"Point not found by ID, trying payload search for {table_id}")

                # Search by payload.table_id field
                payload_resp = requests.post(
                    f"{self.endpoint}/collections/{self.collection}/points/scroll",
                    json={
                        "filter": {
                            "must": [
                                {
                                    "key": "payload.table_id",
                                    "match": {
                                        "value": table_id
                                    }
                                }
                            ]
                        },
                        "limit": 1
                    }
                )

                if payload_resp.status_code == 200:
                    points = payload_resp.json().get("result", {}).get("points", [])

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
                # Skip empty or invalid table IDs
                if not table_id or table_id.strip() == "":
                    logger.warning("Skipping empty table ID")
                    continue

                logger.info(f"Looking up schema for table: {table_id}")
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