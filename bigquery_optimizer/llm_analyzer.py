"""
LLM-Based Query Analyzer

Analyzes BigQuery queries using LLMs to provide optimization recommendations.
"""

import json
import logging
import requests
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class LLMAnalyzer:
    """
    Implements LLM-based analysis for BigQuery optimization recommendations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the analyzer with the provided configuration
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.ollama_endpoint = config['ollama_endpoint']
        self.model = config['ollama_model']
        self.temperature = config['temperature']
        self.max_tokens = config['max_tokens']
    
    def analyze_query(self, query: Dict[str, Any], relevant_schemas: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Analyze a query using LLM and schema information
        
        Args:
            query: Query information
            relevant_schemas: Relevant schema information
            
        Returns:
            Dict: Analysis results
        """
        try:
            # Build the schema context
            schema_context = ""
            for schema in relevant_schemas:
                if "schema_text" in schema:
                    schema_context += schema["schema_text"] + "\n\n"
                elif "metadata" in schema and "schema_text" in schema["metadata"]:
                    schema_context += schema["metadata"]["schema_text"] + "\n\n"
            
            # Create the prompt
            prompt = f"""You are an expert in BigQuery optimization. Analyze the following SQL query and suggest optimizations based on the table schemas provided.

SQL QUERY:
```sql
{query['query_text']}
```

PERFORMANCE METRICS:
- Bytes processed: {query.get('total_bytes_processed', 'N/A')}
- Duration: {query.get('duration_ms', 'N/A')} ms
- Tables referenced: {query.get('referenced_tables', 'N/A')}

TABLE SCHEMAS:
{schema_context}

Provide a detailed analysis including:
1. Partitioning recommendations (if applicable)
2. Clustering recommendations (if applicable)
3. Query structure improvements
4. Any other optimization suggestions

For each recommendation, provide:
- A clear explanation of the problem
- Specific implementation suggestions with SQL examples
- Expected benefits (performance improvement, cost savings)

Format your response as a JSON object with the following structure:
{{
  "recommendation_type": "One of: PARTITION, CLUSTER, QUERY_OPTIMIZATION, MATERIALIZED_VIEW, INDEX, CACHE, TABLE_STRUCTURE",
  "recommendation": "A concise recommendation",
  "justification": "Detailed explanation",
  "implementation": "Specific implementation details or SQL",
  "estimated_savings_pct": A number between 0-100,
  "priority": "One of: HIGH, MEDIUM, LOW"
}}
"""
            
            # Call Ollama
            response = requests.post(
                self.ollama_endpoint,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": self.temperature,
                        "num_predict": self.max_tokens
                    }
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Error from Ollama: {response.status_code} - {response.text}")
                return None
                
            llm_response = response.json().get("response", "")
            
            # Extract JSON from response
            try:
                # Find JSON in the response
                start_idx = llm_response.find("{")
                end_idx = llm_response.rfind("}") + 1
                
                if start_idx >= 0 and end_idx > start_idx:
                    json_str = llm_response[start_idx:end_idx]
                    recommendation = json.loads(json_str)
                    
                    # Add query info
                    recommendation["query_id"] = query["job_id"]
                    recommendation["query_text"] = query["query_text"]
                    recommendation["query_created_at"] = query["creation_time"]
                    
                    return recommendation
                else:
                    logger.error("No JSON found in LLM response")
                    return None
                    
            except Exception as e:
                logger.error(f"Error parsing LLM response: {e}")
                logger.debug(f"LLM response: {llm_response}")
                return None
            
        except Exception as e:
            logger.error(f"Error analyzing query with LLM: {e}")
            return None
    
    def analyze_queries(self, query_history: List[Dict[str, Any]], schema_manager) -> List[Dict[str, Any]]:
        """
        Analyze all queries in the history
        
        Args:
            query_history: List of query history records
            schema_manager: Quadrant schema manager instance
            
        Returns:
            List[Dict]: List of recommendations
        """
        recommendations = []
        
        for i, query in enumerate(query_history):
            logger.info(f"Analyzing query {i+1}/{len(query_history)}: {query['job_id']}")
            
            # Parse referenced tables
            referenced_tables = []
            if query['referenced_tables'] and query['referenced_tables'] != "None":
                referenced_tables = query['referenced_tables'].replace("[", "").replace("]", "").replace("'", "").split(", ")
            
            # Get relevant schemas
            relevant_schemas = schema_manager.get_relevant_schemas(query['query_text'], referenced_tables)
            
            # Analyze query with LLM
            recommendation = self.analyze_query(query, relevant_schemas)
            if recommendation:
                recommendations.append(recommendation)
        
        logger.info(f"Generated {len(recommendations)} LLM-based recommendations")
        return recommendations