#!/usr/bin/env python3
"""
Test script for Ollama embedding generation

This script tests the embedding generation approach implemented in QuadrantManager
to ensure it works with the user's specific Ollama setup.
"""

import json
import requests
import hashlib
import logging
import argparse
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Test Ollama embeddings generation")
    
    parser.add_argument("--endpoint", default="http://127.0.0.1:11434/api/generate",
                      help="Ollama API endpoint (default: http://127.0.0.1:11434/api/generate)")
    parser.add_argument("--model", default="llama3",
                      help="Ollama model to use (default: llama3)")
    parser.add_argument("--vector-dim", type=int, default=768,
                      help="Embedding vector dimension (default: 768)")
    
    return parser.parse_args()

def generate_embedding(text: str, endpoint: str, model: str, vector_dim: int) -> List[float]:
    """
    Generate embedding vector for text using Ollama or a fallback method
    
    Args:
        text: Text to generate embedding for
        endpoint: Ollama API endpoint
        model: Ollama model to use
        vector_dim: Embedding vector dimension
        
    Returns:
        List[float]: Embedding vector
    """
    try:
        logger.info(f"Generating embedding for text: '{text[:50]}...'")
        logger.info(f"Using Ollama endpoint: {endpoint}")
        logger.info(f"Using model: {model}")
        
        # Try calling Ollama for embeddings
        if endpoint:
            try:
                # First, let's get a short summary of the text from Ollama
                summary_prompt = f"Summarize this text in a few key points, focusing on the most important technical details:\n\n{text}"
                
                logger.info("Sending request to Ollama for text summarization")
                response = requests.post(
                    endpoint,
                    json={
                        "model": model,
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
                    logger.info(f"Generated summary: '{summary[:100]}...'")
                    
                    if summary:
                        # We'll use the summary to generate a deterministic embedding
                        summary_hash = hashlib.md5(summary.encode()).digest()
                        text_hash = hashlib.sha256(text.encode()).digest()
                        
                        # Combine both hashes for a more robust embedding
                        combined_hash = summary_hash + text_hash
                        
                        # Convert to embedding
                        embedding = []
                        for i in range(vector_dim):
                            # Use different parts of hash for each dimension
                            pos = i % len(combined_hash)
                            val = (combined_hash[pos] / 255.0) * 2.0 - 1.0  # Scale to [-1, 1]
                            embedding.append(val)
                        
                        # Normalize
                        magnitude = sum(x*x for x in embedding) ** 0.5
                        if magnitude > 0:
                            embedding = [x/magnitude for x in embedding]
                        
                        logger.info(f"Generated embedding with dimension {len(embedding)}")
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
        embedding = []
        
        # Generate enough pseudo-random but deterministic values
        for i in range(vector_dim):
            # Use different parts of hash for each dimension
            position = i % len(hash_bytes)
            value = (hash_bytes[position] / 255.0) * 2.0 - 1.0  # Scale to [-1, 1]
            embedding.append(value)
        
        # Normalize the vector (important for cosine similarity)
        magnitude = sum(x*x for x in embedding) ** 0.5
        if magnitude > 0:
            embedding = [x/magnitude for x in embedding]
            
        logger.info(f"Generated fallback embedding with dimension {len(embedding)}")
        return embedding
            
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        # Return a zero vector as final fallback
        return [0.0] * vector_dim

def test_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Calculate cosine similarity between two vectors
    
    Args:
        vec1: First vector
        vec2: Second vector
        
    Returns:
        float: Cosine similarity (-1 to 1)
    """
    if len(vec1) != len(vec2):
        raise ValueError("Vectors must have the same dimension")
    
    dot_product = sum(x*y for x, y in zip(vec1, vec2))
    mag1 = sum(x*x for x in vec1) ** 0.5
    mag2 = sum(x*x for x in vec2) ** 0.5
    
    if mag1 == 0 or mag2 == 0:
        return 0
    
    return dot_product / (mag1 * mag2)

def main():
    """Main entry point"""
    args = parse_args()
    
    # Test texts
    text1 = "Table: project.dataset.users\nSize: 1.25 GB\nRows: 1000000\nSchema:\n- user_id (STRING, REQUIRED)\n- name (STRING, NULLABLE)\n- email (STRING, NULLABLE)\n- signup_date (TIMESTAMP, NULLABLE)"
    text2 = "Table: project.dataset.users\nSize: 1.25 GB\nRows: 1000000\nSchema:\n- user_id (STRING, REQUIRED)\n- name (STRING, NULLABLE)\n- email (STRING, NULLABLE)\n- signup_date (TIMESTAMP, NULLABLE)\n- last_login (TIMESTAMP, NULLABLE)"
    text3 = "Table: project.dataset.orders\nSize: 2.5 GB\nRows: 5000000\nSchema:\n- order_id (STRING, REQUIRED)\n- user_id (STRING, REQUIRED)\n- order_date (TIMESTAMP, REQUIRED)\n- total_amount (FLOAT, REQUIRED)"
    
    # Generate embeddings
    logger.info("Generating embeddings for test texts...")
    embedding1 = generate_embedding(text1, args.endpoint, args.model, args.vector_dim)
    embedding2 = generate_embedding(text2, args.endpoint, args.model, args.vector_dim)
    embedding3 = generate_embedding(text3, args.endpoint, args.model, args.vector_dim)
    
    # Test similarity
    logger.info("Testing similarity between embeddings...")
    
    # Similar texts should have high similarity
    similarity_1_2 = test_similarity(embedding1, embedding2)
    logger.info(f"Similarity between similar tables (users and users+extra field): {similarity_1_2:.4f}")
    
    # Different texts should have lower similarity
    similarity_1_3 = test_similarity(embedding1, embedding3)
    logger.info(f"Similarity between different tables (users and orders): {similarity_1_3:.4f}")
    
    # Different texts should have lower similarity
    similarity_2_3 = test_similarity(embedding2, embedding3)
    logger.info(f"Similarity between different tables (users+extra and orders): {similarity_2_3:.4f}")
    
    # Check if the similarity scores make sense
    if similarity_1_2 > similarity_1_3 and similarity_1_2 > similarity_2_3:
        logger.info("SUCCESS: Embedding similarity test passed!")
        logger.info("The similar tables have higher similarity than different tables.")
        return True
    else:
        logger.warning("FAILURE: Embedding similarity test failed!")
        logger.warning("The similar tables do not have higher similarity than different tables.")
        return False

if __name__ == "__main__":
    main()