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
    
    def _extract_recommendation_manually(self, json_str: str, referenced_tables=None) -> Optional[Dict[str, Any]]:
        """
        Manually extract recommendation fields from improperly formatted JSON
        
        Args:
            json_str: Potentially malformed JSON string
            referenced_tables: List of referenced table IDs (optional)
            
        Returns:
            Dict containing extracted fields or None
        """
        import re
        
        # Initialize default recommendation
        recommendation = {
            "recommendation_type": "QUERY_OPTIMIZATION",
            "recommendation": "Optimize query structure",
            "justification": "Extracted from LLM response",
            "implementation": "See detailed recommendations",
            "estimated_savings_pct": 10,
            "priority": "MEDIUM"
        }
        
        # Try to extract each field using regex patterns
        try:
            # Extract recommendation type
            rec_type_match = re.search(r'"recommendation_type"\s*:\s*"([^"]+)"', json_str)
            if rec_type_match:
                recommendation["recommendation_type"] = rec_type_match.group(1).strip()
                
            # Extract recommendation
            rec_match = re.search(r'"recommendation"\s*:\s*"([^"]+)"', json_str)
            if rec_match:
                recommendation["recommendation"] = rec_match.group(1).strip()
                
            # Extract justification - might contain newlines
            just_match = re.search(r'"justification"\s*:\s*"(.*?)"(?=\s*,\s*")', json_str, re.DOTALL)
            if just_match:
                recommendation["justification"] = just_match.group(1).strip()
                
            # Extract implementation - might contain newlines and code
            impl_match = re.search(r'"implementation"\s*:\s*"(.*?)"(?=\s*,\s*")', json_str, re.DOTALL)
            if impl_match:
                recommendation["implementation"] = impl_match.group(1).strip()
                
            # Extract estimated savings percentage
            savings_match = re.search(r'"estimated_savings_pct"\s*:\s*(\d+)', json_str)
            if savings_match:
                recommendation["estimated_savings_pct"] = int(savings_match.group(1))
                
            # Extract priority
            priority_match = re.search(r'"priority"\s*:\s*"([^"]+)"', json_str)
            if priority_match:
                recommendation["priority"] = priority_match.group(1).strip()
            
            # Set table_id from referenced tables if available
            if referenced_tables and len(referenced_tables) > 0:
                recommendation["table_id"] = referenced_tables[0]
            else:
                recommendation["table_id"] = "unknown_table"
                
            # If we successfully extracted at least a few fields, return the recommendation
            extracted_fields = sum(1 for m in [rec_type_match, rec_match, just_match, impl_match, savings_match, priority_match] if m)
            if extracted_fields >= 2:  # At least two fields successfully extracted
                return recommendation
                
            return None
                
        except Exception as e:
            logger.error(f"Error manually extracting recommendation: {e}")
            return None
    
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
                    
                    # Clean the JSON string to handle control characters and escape sequences
                    import re
                    # Remove control characters
                    json_str = re.sub(r'[\x00-\x1F\x7F]', '', json_str)
                    # Fix escaped quotes and backslashes
                    json_str = json_str.replace('\\"', '"').replace('\\\\', '\\')
                    
                    try:
                        recommendation = json.loads(json_str)
                    except json.JSONDecodeError as e:
                        # Try different approaches to fix JSON
                        logger.warning(f"Initial JSON parsing failed: {e}")
                        
                        # Try fixing common JSON issues
                        if "control character" in str(e):
                            # More aggressive cleanup for control characters
                            cleaned_json = ''.join(ch for ch in json_str if ord(ch) >= 32 or ch == '\n')
                            try:
                                recommendation = json.loads(cleaned_json)
                                logger.info("JSON parsing succeeded after control character cleanup")
                            except:
                                logger.warning("JSON parsing failed after control character cleanup")
                                
                                # Try manual extraction as a last resort
                                logger.warning("Trying manual field extraction")
                                recommendation = self._extract_recommendation_manually(json_str, referenced_tables)
                        else:
                            # Try manual extraction directly
                            logger.warning("Trying manual field extraction")
                            recommendation = self._extract_recommendation_manually(json_str, referenced_tables)
                            
                        if not recommendation:
                            # Last resort: create a generic recommendation
                            logger.warning("Creating generic recommendation as fallback")
                            recommendation = {
                                "recommendation_type": "QUERY_OPTIMIZATION",
                                "recommendation": "Optimize query structure",
                                "justification": "JSON parsing failed, but query analysis was attempted",
                                "implementation": "Review query for optimization opportunities",
                                "estimated_savings_pct": 5,
                                "priority": "MEDIUM"
                            }
                    
                    # Add query info
                    recommendation["query_id"] = query["job_id"]
                    recommendation["query_text"] = query["query_text"]
                    recommendation["query_created_at"] = query["creation_time"]
                    
                    # Set table_id carefully to avoid format issues
                    if referenced_tables and len(referenced_tables) > 0:
                        # Use the first referenced table as table_id
                        recommendation["table_id"] = referenced_tables[0]
                    else:
                        # Fallback to a default table ID
                        recommendation["table_id"] = "unknown_table"
                    
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
            
            # Try to extract from referenced_tables field if available
            if query.get('referenced_tables') and query['referenced_tables'] not in ("None", "[]", ""):
                try:
                    # Try to handle different formats of referenced_tables
                    ref_tables_str = query['referenced_tables']
                    
                    # If it looks like a string representation of a list
                    if ref_tables_str.startswith('[') and ref_tables_str.endswith(']'):
                        # Remove brackets and split by commas
                        tables_str = ref_tables_str[1:-1]
                        if tables_str.strip():  # Only process if there's content
                            # Handle quoted and unquoted formats
                            if "'" in tables_str or '"' in tables_str:
                                # With quotes - handle properly
                                import re
                                # Match strings inside quotes
                                matches = re.findall(r'[\'"]([^\'"]*)[\'"]', tables_str)
                                referenced_tables = [table.strip() for table in matches if table.strip()]
                            else:
                                # No quotes - simple split
                                referenced_tables = [table.strip() for table in tables_str.split(',') if table.strip()]
                    else:
                        # Just a single table name or comma-separated list
                        referenced_tables = [table.strip() for table in ref_tables_str.split(',') if table.strip()]
                    
                    logger.info(f"Parsed referenced tables: {referenced_tables}")
                except Exception as e:
                    logger.warning(f"Error parsing referenced tables: {e}")
                    # Fallback to simple approach
                    referenced_tables = query['referenced_tables'].replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(",")
                    referenced_tables = [table.strip() for table in referenced_tables if table.strip()]
            
            # If still no referenced tables, try to extract from query text
            if not referenced_tables and query.get('query_text'):
                try:
                    # Simple regex pattern to extract table names from common SQL patterns
                    import re
                    # Look for FROM, JOIN patterns
                    sql = query['query_text'].upper()
                    # Replace all newlines and extra whitespace
                    sql = re.sub(r'\s+', ' ', sql)
                    
                    # Common patterns: FROM table, JOIN table, FROM project.dataset.table
                    from_matches = re.findall(r'FROM\s+([^\s,;()]+)', sql)
                    join_matches = re.findall(r'JOIN\s+([^\s,;()]+)', sql)
                    
                    # Combine and clean up
                    extracted_tables = []
                    for table in from_matches + join_matches:
                        # Remove any backticks or brackets
                        table = table.replace('`', '').replace('[', '').replace(']', '')
                        extracted_tables.append(table)
                    
                    # Add to referenced tables if any found
                    if extracted_tables:
                        logger.info(f"Extracted tables from query text: {extracted_tables}")
                        referenced_tables.extend(extracted_tables)
                        
                except Exception as e:
                    logger.warning(f"Error extracting tables from query text: {e}")
            
            # Get relevant schemas if schema manager is available
            relevant_schemas = []
            if schema_manager:
                relevant_schemas = schema_manager.get_relevant_schemas(query['query_text'], referenced_tables)
            
            # Analyze query with LLM
            recommendation = self.analyze_query(query, relevant_schemas)
            if recommendation:
                recommendations.append(recommendation)
        
        logger.info(f"Generated {len(recommendations)} LLM-based recommendations")
        return recommendations