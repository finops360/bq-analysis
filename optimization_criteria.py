"""
BigQuery Optimization Criteria

This module contains the criteria and thresholds used for determining optimization suggestions
for BigQuery tables. Each criterion includes a description, thresholds, and potential savings.
"""

# Size-based criteria
SIZE_CRITERIA = {
    "large_unpartitioned": {
        "threshold": 1,  # GB
        "description": "Large tables should be partitioned or clustered",
        "check": lambda table: table["table_size_gb"] > 1 and not table["is_partitioned"] and not table["is_clustered"],
        "savings": "20-50% cost reduction",
        "recommendation_id": "partition_large_table"
    },
    "very_large_table": {
        "threshold": 10,  # GB
        "description": "Very large tables should have unused columns pruned or filters applied",
        "check": lambda table: table["table_size_gb"] > 10,
        "savings": "10-30% cost saving",
        "recommendation_id": "prune_large_table"
    },
    "materialized_view_candidate": {
        "threshold": 5,  # GB
        "description": "Large tables with frequent queries should use materialized views",
        "check": lambda table: table["table_size_gb"] > 5,
        "savings": "30-70% cost/performance improvement",
        "recommendation_id": "use_materialized_views"
    },
    "large_table_no_expiration": {
        "threshold": 2,  # GB
        "description": "Large tables should have expiration policies set",
        "check": lambda table: table["table_size_gb"] > 2 and not table["has_expiration"],
        "savings": "10-30% long-term storage savings",
        "recommendation_id": "set_table_expiration"
    },
    "bi_engine_candidate": {
        "threshold": 1,  # GB
        "description": "Tables used for dashboards should consider BI Engine",
        "check": lambda table: table["table_size_gb"] > 1,
        "savings": "up to 50% performance gain",
        "recommendation_id": "use_bi_engine"
    },
    "large_partitioned_unclustered": {
        "threshold": 5,  # GB
        "description": "Large partitioned tables should also be clustered",
        "check": lambda table: table["table_size_gb"] > 5 and table["is_partitioned"] and not table["is_clustered"],
        "savings": "20-40% query performance improvement",
        "recommendation_id": "cluster_partitioned_table"
    },
    "large_nested_schema": {
        "threshold": 1,  # GB
        "description": "Large tables with nested schemas should consider flattening",
        "check": lambda table: table["table_size_gb"] > 1 and table["has_nested_schema"],
        "savings": "10-25% query cost reduction",
        "recommendation_id": "flatten_nested_schema"
    },
    "large_frequently_updated": {
        "threshold": 5,  # GB
        "description": "Large tables updated frequently should use incremental loads",
        "check": lambda table: table["table_size_gb"] > 5 and table["last_modified_days"] is not None and table["last_modified_days"] < 1,
        "savings": "20-40% cost saving",
        "recommendation_id": "use_incremental_loads"
    }
}

# Age-based criteria
AGE_CRITERIA = {
    "old_table_no_expiration": {
        "threshold": 90,  # days
        "description": "Old tables should have expiration or archiving",
        "check": lambda table: table["last_modified_days"] is not None and table["last_modified_days"] > 90,
        "savings": "20-40% storage saving",
        "recommendation_id": "expire_old_tables"
    },
    "very_old_table_no_expiration": {
        "threshold": 180,  # days
        "description": "Long-unused tables with no expiration should be archived or expired",
        "check": lambda table: table["last_modified_days"] is not None and table["last_modified_days"] > 180 and not table["has_expiration"],
        "savings": "20%+ storage cost saving",
        "recommendation_id": "expire_very_old_tables"
    }
}

# Schema and structure criteria
STRUCTURE_CRITERIA = {
    "sharded_table": {
        "description": "Date-sharded tables should be replaced with partitioned tables",
        "check": lambda table: bool(table.get("is_sharded")),
        "savings": "up to 80% scan cost reduction",
        "recommendation_id": "replace_sharded_tables"
    },
    "too_many_columns": {
        "threshold": 50,  # columns
        "description": "Tables with many columns should be split or restructured",
        "check": lambda table: table["column_count"] > 50,
        "savings": "10-20% efficiency gain",
        "recommendation_id": "reduce_column_count"
    },
    "partitioned_no_filter": {
        "description": "Partitioned tables should require partition filters",
        "check": lambda table: table["is_partitioned"] and not getattr(table, "require_partition_filter", False),
        "savings": "30-90% cost reduction",
        "recommendation_id": "require_partition_filters"
    },
    "view_not_materialized": {
        "description": "Views should be materialized for frequent queries",
        "check": lambda table: table["table_type"] == "VIEW",
        "savings": "30-60% repeated query savings",
        "recommendation_id": "materialize_views"
    },
    "external_table_optimization": {
        "description": "External tables should ensure proper data pruning",
        "check": lambda table: table["table_type"] == "EXTERNAL",
        "savings": "20-40% cost saving",
        "recommendation_id": "optimize_external_tables"
    },
    "streaming_buffer_optimization": {
        "description": "Tables using streaming buffer should consider batch loads",
        "check": lambda table: table["has_streaming_buffer"],
        "savings": "50%+ cheaper than streaming",
        "recommendation_id": "use_batch_loads"
    }
}

# Metadata criteria
METADATA_CRITERIA = {
    "missing_labels": {
        "description": "Tables should have labels for governance",
        "check": lambda table: not table["has_labels"],
        "savings": "indirect cost control benefits",
        "recommendation_id": "add_labels"
    },
    "missing_description": {
        "description": "Tables should have descriptions for discoverability",
        "check": lambda table: not table["has_description"],
        "savings": "reduces accidental queries ~10%",
        "recommendation_id": "add_descriptions"
    },
    "non_descriptive_name": {
        "description": "Tables should have descriptive names",
        "check": lambda table: not any(c.isalpha() for c in table["table_id"]),
        "savings": "indirect cost avoidance",
        "recommendation_id": "use_descriptive_names"
    }
}

# Partitioning strategy criteria
PARTITION_CRITERIA = {
    "misaligned_partitioning": {
        "description": "Partitioning strategy may be misaligned",
        "check": lambda table: table["is_partitioned"] and "time" in table["table_id"].lower() and table.get("is_sharded", False),
        "savings": "20-50% scan reduction",
        "recommendation_id": "realign_partition_strategy"
    }
}

# Combine all criteria into a single dictionary for easier iteration
ALL_CRITERIA = {}
ALL_CRITERIA.update(SIZE_CRITERIA)
ALL_CRITERIA.update(AGE_CRITERIA)
ALL_CRITERIA.update(STRUCTURE_CRITERIA)
ALL_CRITERIA.update(METADATA_CRITERIA)
ALL_CRITERIA.update(PARTITION_CRITERIA)

# Define an ordered list of recommendations to generate in a specific order
RECOMMENDATION_ORDER = [
    # Critical cost savings first
    "partition_large_table",
    "replace_sharded_tables",
    "require_partition_filters",
    
    # Performance improvements
    "cluster_partitioned_table", 
    "use_materialized_views",
    "materialize_views",
    
    # Storage optimization
    "expire_old_tables",
    "expire_very_old_tables",
    "set_table_expiration",
    
    # Structure optimization
    "prune_large_table",
    "reduce_column_count",
    "flatten_nested_schema",
    "realign_partition_strategy",
    
    # Data loading optimization
    "use_incremental_loads",
    "use_batch_loads",
    "optimize_external_tables",
    
    # Performance enhancements
    "use_bi_engine",
    
    # Governance and metadata
    "add_labels",
    "add_descriptions",
    "use_descriptive_names"
]

def get_recommendations_by_id():
    """
    Create a dictionary mapping recommendation IDs to criteria
    """
    recommendations = {}
    for criteria_id, criteria in ALL_CRITERIA.items():
        rec_id = criteria["recommendation_id"]
        recommendations[rec_id] = criteria
    return recommendations