"""
Configuration Utilities

Handles loading and managing configuration settings.
"""

import os
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    # Project Settings
    "project_id": "finops360-dev-2025",
    "lookback_days": 30,
    
    # LLM Settings
    "use_llm": True,
    "ollama_endpoint": "http://127.0.0.1:11434/api/generate",
    "ollama_model": "llama3",
    "temperature": 0.2,
    "max_tokens": 4096,
    
    # Quadrant Settings
    "quadrant_endpoint": "http://localhost:6333",
    "quadrant_collection": "bigquery_schemas",
    "vector_dimension": 768,
    
    # Output Settings
    "output_metadata_file": "table_metadata.csv",
    "output_queries_file": "query_history.csv",
    "output_recommendations_file": "query_recommendations.csv",
    
    # Analysis Settings
    "table_size_threshold": 0.01,  # GB, very low to include all tables
    "min_query_count": 0,  # No minimum query count
    "recommendation_limit": 100,  # Maximum recommendations to return
}

def load_config(config_file: str = 'config.yaml') -> Dict[str, Any]:
    """
    Load configuration from YAML file with fallback to defaults
    
    Args:
        config_file: Path to YAML configuration file
        
    Returns:
        Dict containing configuration settings
    """
    config = DEFAULT_CONFIG.copy()
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    config.update(file_config)
            logger.info(f"Loaded configuration from {config_file}")
        else:
            logger.warning(f"Config file {config_file} not found, using defaults")
    except Exception as e:
        logger.error(f"Error loading config from {config_file}: {e}")
        logger.info("Using default configuration")
    
    return config