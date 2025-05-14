"""
BigQuery Heuristic Analyzer

Provides heuristic analysis to identify BigQuery optimization opportunities.
Implements multiple optimization strategies based on table metadata and query patterns.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class HeuristicAnalyzer:
    """
    Implements heuristic-based analysis for BigQuery optimization recommendations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the analyzer with the provided configuration
        
        Args:
            config: Application configuration 
        """
        self.config = config
        self.table_size_threshold = config.get('table_size_threshold', 0.01)  # GB
        self.min_query_count = config.get('min_query_count', 0)
        
    def analyze_data(self, table_metadata: List[Dict[str, Any]], 
                    query_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze metadata and query history to identify optimization opportunities
        
        Args:
            table_metadata: List of table metadata dictionaries
            query_history: List of query history dictionaries
            
        Returns:
            List[Dict]: List of recommendations
        """
        logger.info("Analyzing data for optimization opportunities")
        
        recommendations = []
        
        # Convert metadata to dictionary for easier lookup
        tables_by_id = {table["table_id"]: table for table in table_metadata}
        
        # Extract query patterns
        table_query_counts, bytes_processed_by_table = self._analyze_query_patterns(query_history)
        
        # Analyze each table for optimization opportunities
        for table_id, table in tables_by_id.items():
            size_gb = table.get("size_gb", 0)
            row_count = table.get("row_count", 0)
            is_partitioned = table.get("is_partitioned", False)
            is_clustered = table.get("is_clustered", False)
            query_count = table_query_counts.get(table_id, 0)
            
            # Skip tables smaller than threshold
            if size_gb < self.table_size_threshold and row_count < 1000:
                logger.debug(f"Skipping small table: {table_id}")
                continue
                
            # Parse the schema
            schema_fields = self._parse_schema(table)
            if not schema_fields:
                logger.warning(f"Could not parse schema for table {table_id}")
                continue
                
            # Find potential columns for partitioning and clustering
            potential_partition_columns = self._find_partition_candidates(schema_fields)
            potential_cluster_columns = self._find_cluster_candidates(schema_fields)
            
            # Generate recommendations
            table_recommendations = []
            
            # 1. Partitioning recommendations
            if not is_partitioned and potential_partition_columns:
                partition_rec = self._generate_partition_recommendation(
                    table_id, table, potential_partition_columns, size_gb, query_count
                )
                if partition_rec:
                    table_recommendations.append(partition_rec)
            
            # 2. Clustering recommendations
            if is_partitioned and not is_clustered and potential_cluster_columns:
                cluster_rec = self._generate_cluster_recommendation(
                    table_id, table, potential_cluster_columns, size_gb, query_count
                )
                if cluster_rec:
                    table_recommendations.append(cluster_rec)
            
            # 3. Combined partitioning and clustering
            if not is_partitioned and not is_clustered and potential_partition_columns and potential_cluster_columns:
                combined_rec = self._generate_combined_recommendation(
                    table_id, table, potential_partition_columns, potential_cluster_columns, size_gb, query_count
                )
                if combined_rec:
                    table_recommendations.append(combined_rec)
            
            # 4. Query optimization recommendations
            bytes_processed = bytes_processed_by_table.get(table_id, 0)
            table_size = table.get("size_bytes", 0)
            if query_count > 0 and table_size > 0 and bytes_processed > 0:
                query_rec = self._generate_query_optimization_recommendation(
                    table_id, table, schema_fields, bytes_processed, table_size, query_count
                )
                if query_rec:
                    table_recommendations.append(query_rec)
            
            # 5. Materialized view recommendations
            if query_count > 0 or size_gb > 0.1:
                view_rec = self._generate_materialized_view_recommendation(
                    table_id, table, schema_fields, query_count, size_gb
                )
                if view_rec:
                    table_recommendations.append(view_rec)
                    
            # 6. Column and data type recommendations
            column_recs = self._generate_column_recommendations(table_id, table, schema_fields)
            if column_recs:
                table_recommendations.extend(column_recs)
                
            # 7. Table lifecycle recommendations
            lifecycle_rec = self._generate_lifecycle_recommendation(table_id, table)
            if lifecycle_rec:
                table_recommendations.append(lifecycle_rec)
                
            # Add all recommendations for this table
            recommendations.extend(table_recommendations)
        
        # Sort recommendations by priority and estimated savings
        recommendations.sort(key=lambda x: (
            -self._priority_to_value(x.get("priority", "LOW")), 
            -x.get("estimated_savings_pct", 0)
        ))
        
        # Limit number of recommendations if configured
        limit = self.config.get('recommendation_limit', 100)
        if limit > 0 and len(recommendations) > limit:
            recommendations = recommendations[:limit]
        
        logger.info(f"Generated {len(recommendations)} recommendations")
        return recommendations
    
    def _analyze_query_patterns(self, query_history: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Analyze query patterns to count references and bytes processed by table
        
        Args:
            query_history: List of query history dictionaries
            
        Returns:
            Tuple of table_query_counts and bytes_processed_by_table dictionaries
        """
        table_query_counts = {}
        bytes_processed_by_table = {}
        
        for query in query_history:
            if not query.get("referenced_tables") or query["referenced_tables"] == "None":
                continue
            
            # Parse the referenced tables string
            referenced_tables = query["referenced_tables"].replace("[", "").replace("]", "").replace("'", "").split(", ")
            bytes_processed = query.get("total_bytes_processed", 0) or 0
            
            # Distribute bytes processed evenly among referenced tables
            bytes_per_table = bytes_processed / len(referenced_tables) if referenced_tables else 0
            
            for table_id in referenced_tables:
                if table_id:
                    # Update query count
                    if table_id not in table_query_counts:
                        table_query_counts[table_id] = 0
                    table_query_counts[table_id] += 1
                    
                    # Update bytes processed
                    if table_id not in bytes_processed_by_table:
                        bytes_processed_by_table[table_id] = 0
                    bytes_processed_by_table[table_id] += bytes_per_table
        
        return table_query_counts, bytes_processed_by_table
    
    def _parse_schema(self, table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse the schema field from the table metadata
        
        Args:
            table: Table metadata dictionary
            
        Returns:
            List of schema field dictionaries
        """
        try:
            schema = json.loads(table.get("schema", "[]"))
            return schema
        except Exception as e:
            logger.warning(f"Error parsing schema: {e}")
            return []
    
    def _find_partition_candidates(self, schema_fields: List[Dict[str, Any]]) -> List[str]:
        """
        Find potential columns suitable for partitioning
        
        Args:
            schema_fields: List of schema field dictionaries
            
        Returns:
            List of potential partition column names
        """
        potential_columns = []
        
        for field in schema_fields:
            field_type = field.get("type", "")
            field_name = field.get("name", "")
            
            # Look for date/timestamp type fields
            if field_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                # Priority for date-related columns
                score = 0
                
                # Prefer columns with date in the name
                if any(keyword in field_name.lower() for keyword in 
                       ["date", "time", "day", "month", "year", "created", "modified", "updated"]):
                    score += 2
                
                # Add field with score (for sorting)
                potential_columns.append((field_name, score))
        
        # Sort by score (descending)
        potential_columns.sort(key=lambda x: x[1], reverse=True)
        
        # Return column names only
        return [col[0] for col in potential_columns]
    
    def _find_cluster_candidates(self, schema_fields: List[Dict[str, Any]]) -> List[str]:
        """
        Find potential columns suitable for clustering
        
        Args:
            schema_fields: List of schema field dictionaries
            
        Returns:
            List of potential cluster column names
        """
        potential_columns = []
        
        for field in schema_fields:
            field_type = field.get("type", "")
            field_name = field.get("name", "")
            
            # Look for suitable clustering column types
            if field_type in ["STRING", "INTEGER", "BOOL"]:
                # Calculate a preference score
                score = 0
                
                # Prefer specific column types
                if field_type == "STRING":
                    score += 1
                if field_type == "INTEGER":
                    score += 0.5
                
                # Boost score for likely high-cardinality columns
                if any(keyword in field_name.lower() for keyword in 
                       ["id", "key", "code", "category", "type", "status", "region", "country"]):
                    score += 2
                
                potential_columns.append((field_name, score))
        
        # Sort by score (descending)
        potential_columns.sort(key=lambda x: x[1], reverse=True)
        
        # Return column names only
        return [col[0] for col in potential_columns]
    
    def _generate_partition_recommendation(
        self, table_id: str, table: Dict[str, Any], 
        potential_columns: List[str], size_gb: float, query_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a partitioning recommendation for a table
        
        Args:
            table_id: The table ID
            table: Table metadata
            potential_columns: List of potential partition columns
            size_gb: Table size in GB
            query_count: Number of queries referencing this table
            
        Returns:
            Recommendation dictionary or None
        """
        if not potential_columns:
            return None
            
        recommended_column = potential_columns[0]
        
        # Determine priority based on table size and query count
        if size_gb > 10 or query_count > 10:
            priority = "HIGH"
            estimated_savings = 25
        elif size_gb > 1 or query_count > 5:
            priority = "MEDIUM"
            estimated_savings = 15
        else:
            priority = "LOW"
            estimated_savings = 5
            
        # Create SQL implementation example
        implementation = f"""
-- To partition this table, you can create a new partitioned table and copy data:
CREATE OR REPLACE TABLE `{table_id}_partitioned`
PARTITION BY DATE({recommended_column})
AS SELECT * FROM `{table_id}`;

-- Then you can drop the old table and rename the new one:
DROP TABLE `{table_id}`;
ALTER TABLE `{table_id}_partitioned` RENAME TO `{table_id.split('.')[-1]}`;
"""
        
        # Estimate potential query improvement
        query_improvement = "20-90% (depending on query patterns)"
        if size_gb > 100:
            cost_impact = "Significant cost reduction expected"
        elif size_gb > 10:
            cost_impact = "Moderate cost reduction expected"
        else:
            cost_impact = "Minor cost reduction possible"
            
        # Detailed justification
        justification = (
            f"Table is {size_gb:.2f} GB and has {len(potential_columns)} potential date/timestamp fields "
            f"for partitioning. Partitioning can improve query performance and reduce costs by limiting "
            f"the amount of data scanned. {query_count} queries referenced this table in the analysis period."
        )
            
        return {
            "table_id": table_id,
            "recommendation_type": "PARTITION",
            "recommendation": f"Partition table on {recommended_column}",
            "justification": justification,
            "potential_columns": ", ".join(potential_columns),
            "implementation": implementation.strip(),
            "estimated_savings_pct": estimated_savings,
            "query_improvement": query_improvement,
            "cost_impact": cost_impact,
            "priority": priority
        }
    
    def _generate_cluster_recommendation(
        self, table_id: str, table: Dict[str, Any], 
        potential_columns: List[str], size_gb: float, query_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a clustering recommendation for a partitioned table
        
        Args:
            table_id: The table ID
            table: Table metadata
            potential_columns: List of potential clustering columns
            size_gb: Table size in GB
            query_count: Number of queries referencing this table
            
        Returns:
            Recommendation dictionary or None
        """
        if not potential_columns:
            return None
            
        # Limit to top 3 potential clustering columns (BigQuery limit)
        top_cluster_columns = potential_columns[:3]
        
        # Determine priority
        if size_gb > 5 or query_count > 10:
            priority = "HIGH"
            estimated_savings = 20
        elif size_gb > 0.5 or query_count > 5:
            priority = "MEDIUM"
            estimated_savings = 10
        else:
            priority = "LOW"
            estimated_savings = 3
            
        # Create SQL implementation example
        # Get partition field if available
        partition_field = table.get("partition_field", "")
        partition_clause = f"PARTITION BY {partition_field}" if partition_field else "-- Maintain existing partitioning"
            
        implementation = f"""
-- To add clustering to this partitioned table:
CREATE OR REPLACE TABLE `{table_id}_clustered`
{partition_clause}
CLUSTER BY {', '.join(top_cluster_columns)}
AS SELECT * FROM `{table_id}`;

-- Then you can drop the old table and rename the new one:
DROP TABLE `{table_id}`;
ALTER TABLE `{table_id}_clustered` RENAME TO `{table_id.split('.')[-1]}`;
"""
        
        # Estimate potential query improvement
        query_improvement = "10-40% (depending on query patterns)"
        if size_gb > 50:
            cost_impact = "Moderate cost reduction expected"
        elif size_gb > 5:
            cost_impact = "Minor cost reduction expected"
        else:
            cost_impact = "Minimal cost impact"
            
        # Detailed justification
        partition_field_str = f" on {table.get('partition_field', '')}" if table.get("partition_field") else ""
        justification = (
            f"Table is already partitioned{partition_field_str} but not clustered. Clustering on "
            f"high-cardinality columns can further improve query performance by co-locating related data. "
            f"This is especially effective when queries filter on the clustering columns."
        )
            
        return {
            "table_id": table_id,
            "recommendation_type": "CLUSTER",
            "recommendation": f"Cluster this partitioned table on {', '.join(top_cluster_columns)}",
            "justification": justification,
            "potential_columns": ", ".join(top_cluster_columns),
            "implementation": implementation.strip(),
            "estimated_savings_pct": estimated_savings,
            "query_improvement": query_improvement,
            "cost_impact": cost_impact,
            "priority": priority
        }
    
    def _generate_combined_recommendation(
        self, table_id: str, table: Dict[str, Any],
        partition_columns: List[str], cluster_columns: List[str],
        size_gb: float, query_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a combined partitioning and clustering recommendation
        
        Args:
            table_id: The table ID
            table: Table metadata
            partition_columns: List of potential partition columns
            cluster_columns: List of potential clustering columns
            size_gb: Table size in GB
            query_count: Number of queries referencing this table
            
        Returns:
            Recommendation dictionary or None
        """
        if not partition_columns or not cluster_columns:
            return None
            
        partition_col = partition_columns[0]
        cluster_cols = cluster_columns[:3]  # BigQuery limit of 4 clustering columns
        
        # Determine priority
        if size_gb > 5 or query_count > 10:
            priority = "HIGH"
            estimated_savings = 35
        elif size_gb > 1 or query_count > 5:
            priority = "MEDIUM"
            estimated_savings = 20
        else:
            priority = "LOW"
            estimated_savings = 7
            
        # Create SQL implementation example
        implementation = f"""
-- To add both partitioning and clustering to this table:
CREATE OR REPLACE TABLE `{table_id}_optimized`
PARTITION BY DATE({partition_col})
CLUSTER BY {', '.join(cluster_cols)}
AS SELECT * FROM `{table_id}`;

-- Then you can drop the old table and rename the new one:
DROP TABLE `{table_id}`;
ALTER TABLE `{table_id}_optimized` RENAME TO `{table_id.split('.')[-1]}`;
"""
        
        # Estimate potential query improvement
        query_improvement = "30-90% (depending on query patterns)"
        if size_gb > 50:
            cost_impact = "Significant cost reduction expected"
        elif size_gb > 5:
            cost_impact = "Moderate cost reduction expected"
        else:
            cost_impact = "Minor cost reduction possible"
            
        # Detailed justification
        justification = (
            f"Table has both partitioning and clustering potential. Implementing both can significantly "
            f"improve query performance and reduce costs by limiting data scanned. This table is {size_gb:.2f} GB "
            f"and was referenced by {query_count} queries in the analysis period."
        )
            
        return {
            "table_id": table_id,
            "recommendation_type": "PARTITION_AND_CLUSTER",
            "recommendation": f"Partition on {partition_col} and cluster on {', '.join(cluster_cols)}",
            "justification": justification,
            "partition_column": partition_col,
            "cluster_columns": ", ".join(cluster_cols),
            "implementation": implementation.strip(),
            "estimated_savings_pct": estimated_savings,
            "query_improvement": query_improvement,
            "cost_impact": cost_impact,
            "priority": priority
        }
    
    def _generate_query_optimization_recommendation(
        self, table_id: str, table: Dict[str, Any],
        schema_fields: List[Dict[str, Any]],
        bytes_processed: int, table_size: int, query_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Generate query optimization recommendations based on scan ratio
        
        Args:
            table_id: The table ID
            table: Table metadata
            schema_fields: Table schema fields
            bytes_processed: Total bytes processed for this table
            table_size: Table size in bytes
            query_count: Number of queries referencing this table
            
        Returns:
            Recommendation dictionary or None
        """
        if table_size == 0 or query_count == 0:
            return None
            
        # Calculate scan ratio (how much of the table is being scanned on average)
        scan_ratio = bytes_processed / (table_size * query_count)
        
        # Only recommend if scan ratio is high
        if scan_ratio < 0.5:
            return None
            
        # Find columns that could benefit from filtering
        filtering_columns = []
        for field in schema_fields:
            field_type = field.get("type", "")
            field_name = field.get("name", "")
            
            if field_type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"]:
                filtering_columns.append(field_name)
        
        if not filtering_columns:
            return None
            
        # Determine priority
        if scan_ratio > 0.9 and (query_count > 10 or bytes_processed > 10 * (1024**3)):  # > 10GB processed
            priority = "HIGH"
            estimated_savings = 25
        elif scan_ratio > 0.7 and (query_count > 5 or bytes_processed > 1 * (1024**3)):  # > 1GB processed
            priority = "MEDIUM"
            estimated_savings = 15
        else:
            priority = "LOW"
            estimated_savings = 5
            
        # Example optimized query
        implementation = f"""
-- Example of query optimization by adding filters and column pruning:

-- BEFORE:
SELECT * FROM `{table_id}`

-- AFTER (add filters and select specific columns):
SELECT
  -- Select only needed columns instead of SELECT *
  {', '.join(filtering_columns[:5]) + (', ...' if len(filtering_columns) > 5 else '')}
FROM 
  `{table_id}`
WHERE
  -- Add filters on these high-cardinality columns when possible
  {filtering_columns[0]} = 'value'
  -- Consider adding a date range filter if applicable
  {f"AND {filtering_columns[1]} BETWEEN start_date AND end_date" if len(filtering_columns) > 1 else ""}
"""
        
        # Detailed justification
        avg_bytes = bytes_processed / query_count if query_count > 0 else 0
        avg_gb = avg_bytes / (1024**3) if avg_bytes > 0 else 0
        
        justification = (
            f"Queries are scanning {scan_ratio:.2f}x the table size on average ({avg_gb:.2f} GB per query). "
            f"Adding filters on {', '.join(filtering_columns[:3])} and selecting only needed columns "
            f"could significantly reduce data scanned and improve performance."
        )
            
        return {
            "table_id": table_id,
            "recommendation_type": "QUERY_OPTIMIZATION",
            "recommendation": "Optimize queries to reduce the amount of data scanned",
            "justification": justification,
            "potential_filter_columns": ", ".join(filtering_columns[:5]),
            "implementation": implementation.strip(),
            "estimated_savings_pct": estimated_savings,
            "scan_ratio": f"{scan_ratio:.2f}",
            "avg_bytes_per_query": f"{avg_gb:.2f} GB",
            "priority": priority
        }
    
    def _generate_materialized_view_recommendation(
        self, table_id: str, table: Dict[str, Any],
        schema_fields: List[Dict[str, Any]],
        query_count: int, size_gb: float
    ) -> Optional[Dict[str, Any]]:
        """
        Generate materialized view recommendations based on schema and query patterns
        
        Args:
            table_id: The table ID
            table: Table metadata
            schema_fields: Table schema fields
            query_count: Number of queries referencing this table
            size_gb: Table size in GB
            
        Returns:
            Recommendation dictionary or None
        """
        # Identify potential aggregation and dimension columns
        agg_candidates = []
        dim_candidates = []
        
        for field in schema_fields:
            field_type = field.get("type", "")
            field_name = field.get("name", "")
            
            if field_type in ["INTEGER", "FLOAT", "NUMERIC"]:
                agg_candidates.append(field_name)
            elif field_type in ["STRING", "DATE", "TIMESTAMP"]:
                dim_candidates.append(field_name)
        
        # Only recommend if we have both dimension and measure columns
        if not agg_candidates or not dim_candidates:
            return None
            
        # Determine if this table is likely to benefit from materialized views
        # Consider both query count and table size
        if query_count >= 10 or size_gb >= 10:
            priority = "HIGH"
            estimated_savings = 30
        elif query_count >= 5 or size_gb >= 1:
            priority = "MEDIUM"
            estimated_savings = 20
        else:
            priority = "LOW"
            estimated_savings = 10
            
        # Create sample materialized view SQL
        sample_dims = dim_candidates[:2]
        sample_aggs = agg_candidates[:2]
        
        implementation = f"""
-- Sample materialized view for common aggregation patterns:
CREATE MATERIALIZED VIEW `{table_id}_mv_daily_agg`
AS SELECT
  {', '.join([f'{dim}' for dim in sample_dims])},
  COUNT(*) as record_count,
  {', '.join([f'SUM({agg}) as total_{agg}' for agg in sample_aggs])},
  {', '.join([f'AVG({agg}) as avg_{agg}' for agg in sample_aggs])}
FROM `{table_id}`
GROUP BY {', '.join([str(i+1) for i in range(len(sample_dims))])};

-- Example query using the materialized view:
SELECT * FROM `{table_id}_mv_daily_agg`
WHERE {sample_dims[0]} = 'value';
"""
        
        # Detailed justification
        justification = (
            f"Table has {len(agg_candidates)} numeric columns that could benefit from pre-aggregated views. "
            f"Materialized views can dramatically improve performance for analytical queries while reducing "
            f"processing costs. They're automatically updated when the source table changes."
        )
            
        return {
            "table_id": table_id,
            "recommendation_type": "MATERIALIZED_VIEW",
            "recommendation": "Create materialized views for frequently queried aggregations",
            "justification": justification,
            "potential_agg_columns": ", ".join(agg_candidates),
            "potential_dim_columns": ", ".join(dim_candidates),
            "implementation": implementation.strip(),
            "estimated_savings_pct": estimated_savings,
            "query_improvement": "50-99% for aggregate queries",
            "priority": priority
        }
    
    def _generate_column_recommendations(
        self, table_id: str, table: Dict[str, Any],
        schema_fields: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Generate column and data type optimization recommendations
        
        Args:
            table_id: The table ID
            table: Table metadata
            schema_fields: Table schema fields
            
        Returns:
            List of recommendation dictionaries
        """
        recommendations = []
        
        # Check if there are many string columns that might be enums/categories
        string_columns = [f for f in schema_fields if f.get("type") == "STRING"]
        if len(string_columns) >= 3:
            # Recommend category data type for low-cardinality strings
            category_rec = {
                "table_id": table_id,
                "recommendation_type": "DATA_TYPE_OPTIMIZATION",
                "recommendation": "Consider CATEGORY data type for low-cardinality string columns",
                "justification": (
                    f"Table has {len(string_columns)} STRING columns. For low-cardinality string columns (like status, "
                    f"type, country), using the CATEGORY data type can improve compression and query performance."
                ),
                "implementation": f"""
-- Example of converting string columns to CATEGORY:
CREATE OR REPLACE TABLE `{table_id}_optimized`
AS SELECT
  *
  -- Convert low-cardinality string columns to CATEGORY
  -- Example: CAST(status AS CATEGORY) AS status,
  -- Example: CAST(country AS CATEGORY) AS country
FROM `{table_id}`;
                """.strip(),
                "estimated_savings_pct": 5,
                "potential_columns": ", ".join([c.get("name", "") for c in string_columns[:5]]),
                "priority": "MEDIUM"
            }
            recommendations.append(category_rec)
        
        # Check for tables with many columns that might benefit from column grouping
        if len(schema_fields) > 50:
            column_group_rec = {
                "table_id": table_id,
                "recommendation_type": "COLUMN_GROUPING",
                "recommendation": "Use column grouping to improve query performance",
                "justification": (
                    f"Table has {len(schema_fields)} columns. Using column grouping can improve performance for "
                    f"queries that only need to access a subset of columns."
                ),
                "implementation": f"""
-- Example of adding column grouping to a table:
CREATE OR REPLACE TABLE `{table_id}_grouped`
(
  -- Main group for frequently accessed columns
  id STRING,
  created_at TIMESTAMP,
  status STRING,
  -- Add other frequently accessed columns
  
  -- Group for descriptive columns
  descriptive STRUCT<
    description STRING,
    notes STRING,
    tags ARRAY<STRING>
    -- Add other descriptive columns
  >,
  
  -- Group for metrics
  metrics STRUCT<
    value FLOAT64,
    quantity INT64
    -- Add other metric columns
  >
)
AS SELECT
  id,
  created_at,
  status,
  -- Construct descriptive STRUCT
  STRUCT(
    description,
    notes,
    tags
  ) AS descriptive,
  -- Construct metrics STRUCT
  STRUCT(
    value,
    quantity
  ) AS metrics
FROM `{table_id}`;
                """.strip(),
                "estimated_savings_pct": 10,
                "priority": "MEDIUM"
            }
            recommendations.append(column_group_rec)
            
        # Check for integer timestamp fields that could be converted to TIMESTAMP
        int_columns = [f for f in schema_fields if f.get("type") == "INTEGER" and 
                      any(kw in f.get("name", "").lower() for kw in ["time", "timestamp", "date", "epoch"])]
        if int_columns:
            timestamp_rec = {
                "table_id": table_id,
                "recommendation_type": "DATA_TYPE_OPTIMIZATION",
                "recommendation": "Convert integer timestamp columns to TIMESTAMP type",
                "justification": (
                    f"Table has {len(int_columns)} integer columns that may contain timestamp data. "
                    f"Converting these to TIMESTAMP type allows using BigQuery's date/time functions and "
                    f"potentially enables partitioning."
                ),
                "implementation": f"""
-- Example of converting Unix timestamps to TIMESTAMP:
CREATE OR REPLACE TABLE `{table_id}_converted`
AS SELECT
  -- Convert Unix timestamps (seconds since epoch)
  TIMESTAMP_SECONDS({int_columns[0]}) AS {int_columns[0]},
  -- For milliseconds timestamps:
  -- TIMESTAMP_MILLIS({int_columns[0]}) AS {int_columns[0]},
  
  -- Keep other columns as is
  * EXCEPT ({', '.join([c.get("name", "") for c in int_columns])})
FROM `{table_id}`;
                """.strip(),
                "estimated_savings_pct": 5,
                "potential_columns": ", ".join([c.get("name", "") for c in int_columns]),
                "priority": "MEDIUM"
            }
            recommendations.append(timestamp_rec)
        
        return recommendations
    
    def _generate_lifecycle_recommendation(
        self, table_id: str, table: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Generate table lifecycle recommendations
        
        Args:
            table_id: The table ID
            table: Table metadata
            
        Returns:
            Recommendation dictionary or None
        """
        # Check if the table has expiration set
        has_expiration = table.get("has_expiration", False)
        days_since_modified = table.get("days_since_modified", 0) or 0
        
        # Recommend expiration for old tables that don't have it set
        if not has_expiration and days_since_modified > 180:  # Older than 6 months
            return {
                "table_id": table_id,
                "recommendation_type": "LIFECYCLE_MANAGEMENT",
                "recommendation": "Set table expiration for old data",
                "justification": (
                    f"Table hasn't been modified in {days_since_modified} days but doesn't have expiration set. "
                    f"Adding table expiration can reduce storage costs for old data."
                ),
                "implementation": f"""
-- Add expiration to existing table (e.g., expire after 1 year):
ALTER TABLE `{table_id}`
SET OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
);
                """.strip(),
                "estimated_savings_pct": 5,
                "priority": "MEDIUM"
            }
        
        return None
        
    def _priority_to_value(self, priority: str) -> int:
        """Convert priority string to numeric value for sorting"""
        priority_map = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        return priority_map.get(priority, 0)