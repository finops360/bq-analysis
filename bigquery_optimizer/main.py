"""
BigQuery Optimizer - Main Module

Entry point for the BigQuery Optimizer application.
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any

from bigquery_optimizer.utils.config import load_config
from bigquery_optimizer.analysis.metadata_collector import collect_table_metadata, collect_query_history, save_to_csv
from bigquery_optimizer.analysis.heuristic_analyzer import HeuristicAnalyzer
from bigquery_optimizer.vectordb.quadrant_manager import QuadrantManager
from bigquery_optimizer.llm_analyzer import LLMAnalyzer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Analyze BigQuery resources and provide optimization recommendations")

    parser.add_argument("--config", default="config.yaml",
                        help="Path to configuration file (default: config.yaml)")
    parser.add_argument("--project-id",
                        help="Override GCP Project ID from config")
    parser.add_argument("--lookback-days", type=int,
                        help="Override number of days of query history to analyze")
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM-based recommendations")
    parser.add_argument("--output-file",
                        help="Override output file for recommendations")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")

    # Optional stages control
    parser.add_argument("--skip-metadata", action="store_true",
                        help="Skip collecting table metadata (use existing data)")
    parser.add_argument("--skip-queries", action="store_true",
                        help="Skip collecting query history (use existing data)")
    parser.add_argument("--skip-vector-db", action="store_true",
                        help="Skip using vector database for schema storage")
    parser.add_argument("--query-limit", type=int, default=10,
                        help="Maximum number of queries to analyze with LLM (default: 10)")

    return parser.parse_args()

def summarize_recommendations(recommendations: List[Dict[str, Any]], output_file: str) -> None:
    """
    Print a summary of recommendations

    Args:
        recommendations: List of recommendation dictionaries
        output_file: Path where recommendations were saved
    """
    if not recommendations:
        logger.info("No recommendations to summarize")
        return

    # Group by table and type
    by_table = {}
    by_type = {}

    for rec in recommendations:
        table_id = rec["table_id"]
        rec_type = rec["recommendation_type"]

        if table_id not in by_table:
            by_table[table_id] = []
        by_table[table_id].append(rec)

        if rec_type not in by_type:
            by_type[rec_type] = []
        by_type[rec_type].append(rec)

    # Print summary
    print("\n===== BigQuery Optimization Recommendations =====\n")

    print(f"Total recommendations: {len(recommendations)}")
    print("\nRecommendations by type:")
    for rec_type, recs in by_type.items():
        print(f"  {rec_type}: {len(recs)}")

    print("\nRecommendations by table (top 10):")
    for i, (table_id, recs) in enumerate(sorted(by_table.items(), key=lambda x: len(x[1]), reverse=True)):
        if i >= 10:
            break
        print(f"\n{table_id}: {len(recs)} recommendations")
        for rec in recs[:3]:  # Show only top 3 recommendations per table
            print(f"  - {rec['recommendation_type']}: {rec['recommendation']} (Est. savings: {rec['estimated_savings_pct']}%, Priority: {rec['priority']})")
        if len(recs) > 3:
            print(f"  - ... and {len(recs) - 3} more recommendations")

    print("\nTop 5 highest-priority recommendations:")
    # Sort by priority and estimated savings
    top_recs = sorted(recommendations,
                     key=lambda x: (-{"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(x.get("priority", "LOW"), 0),
                                   -x.get("estimated_savings_pct", 0)))[:5]
    for i, rec in enumerate(top_recs):
        print(f"\n{i+1}. {rec['recommendation_type']}: {rec['recommendation']}")
        print(f"   Table: {rec['table_id']}")
        print(f"   Justification: {rec['justification']}")
        print(f"   Estimated savings: {rec['estimated_savings_pct']}%")
        print(f"   Priority: {rec['priority']}")

    print(f"\nFull recommendations saved to {output_file}")

def run(config: Dict[str, Any]) -> None:
    """
    Run the BigQuery Optimizer

    Args:
        config: Application configuration
    """
    logger.info("Starting BigQuery Optimizer")
    logger.info(f"Project: {config['project_id']}")
    logger.info(f"Lookback days: {config['lookback_days']}")
    logger.info(f"Use LLM: {config['use_llm']}")

    table_metadata = []
    query_history = []
    output_file = config.get('output_recommendations_file', 'query_recommendations.csv')

    # Parse command line arguments for optional steps
    collect_metadata = config.get('collect_metadata', True)
    collect_queries = config.get('collect_queries', True)
    use_vector_db = config.get('use_vector_db', True) and config['use_llm']

    # Step 1: Collect table metadata (if enabled)
    if collect_metadata:
        logger.info("Step 1: Collecting table metadata")
        table_metadata = collect_table_metadata(config)
        if not table_metadata:
            logger.warning("No table metadata collected, using existing metadata if available.")
            # Try to load existing metadata from file if it exists
            try:
                from csv import DictReader
                existing_metadata_file = config.get('output_metadata_file', 'table_metadata.csv')
                if os.path.exists(existing_metadata_file):
                    with open(existing_metadata_file, 'r') as f:
                        table_metadata = list(DictReader(f))
                    logger.info(f"Loaded {len(table_metadata)} table metadata records from {existing_metadata_file}")
            except Exception as e:
                logger.warning(f"Failed to load existing metadata: {e}")
                table_metadata = []
    else:
        logger.info("Skipping metadata collection (disabled in config)")
        # Try to load existing metadata from file if it exists
        try:
            from csv import DictReader
            existing_metadata_file = config.get('output_metadata_file', 'table_metadata.csv')
            if os.path.exists(existing_metadata_file):
                with open(existing_metadata_file, 'r') as f:
                    table_metadata = list(DictReader(f))
                logger.info(f"Loaded {len(table_metadata)} table metadata records from {existing_metadata_file}")
        except Exception as e:
            logger.warning(f"Failed to load existing metadata: {e}")

    # Step 2: Collect query history (if enabled)
    if collect_queries:
        logger.info("Step 2: Collecting query history")
        query_history = collect_query_history(config)
        if not query_history:
            logger.warning("No query history found. Continuing with metadata-only analysis.")
    else:
        logger.info("Skipping query history collection (disabled in config)")
        # Try to load existing query history from file if it exists
        try:
            from csv import DictReader
            existing_queries_file = config.get('output_queries_file', 'query_history.csv')
            if os.path.exists(existing_queries_file):
                with open(existing_queries_file, 'r') as f:
                    query_history = list(DictReader(f))
                logger.info(f"Loaded {len(query_history)} query history records from {existing_queries_file}")
        except Exception as e:
            logger.warning(f"Failed to load existing query history: {e}")

    # Initialize recommendations list
    all_recommendations = []

    # Step 3: Heuristic analysis - always run this with whatever data we have
    if table_metadata:
        logger.info("Step 3: Performing heuristic analysis")
        heuristic_analyzer = HeuristicAnalyzer(config)
        heuristic_recommendations = heuristic_analyzer.analyze_data(table_metadata, query_history)
        all_recommendations.extend(heuristic_recommendations)
        logger.info(f"Generated {len(heuristic_recommendations)} heuristic recommendations")
    else:
        logger.warning("Skipping heuristic analysis due to missing table metadata")

    # Step 4: LLM-based analysis (if enabled and we have both metadata and queries)
    if config['use_llm'] and table_metadata and query_history:
        # Only initialize vector DB if explicitly enabled
        if use_vector_db:
            logger.info("Step 4: Setting up vector database")
            quadrant_manager = QuadrantManager(config)
            vector_db_ready = quadrant_manager.initialize_collection()

            if vector_db_ready:
                # Store schemas in vector database
                store_success = quadrant_manager.store_schemas(table_metadata)
                if not store_success:
                    logger.warning("Failed to store schemas in vector database, but continuing with analysis")
            else:
                logger.warning("Vector database initialization failed, falling back to direct analysis")
                quadrant_manager = None
        else:
            logger.info("Skipping vector database setup (disabled in config)")
            quadrant_manager = None

        # Analyze with LLM (with or without vector DB)
        logger.info("Step 5: Performing LLM-based analysis")
        llm_analyzer = LLMAnalyzer(config)

        # Limit the number of queries to analyze to avoid excessive API calls
        query_limit = min(config.get('query_limit', 10), len(query_history))
        limited_queries = query_history[:query_limit]

        llm_recommendations = llm_analyzer.analyze_queries(limited_queries, quadrant_manager)
        all_recommendations.extend(llm_recommendations)
        logger.info(f"Generated {len(llm_recommendations)} LLM-based recommendations")
    elif config['use_llm']:
        logger.warning("Skipping LLM analysis due to missing data (requires both metadata and queries)")

    # Save all recommendations
    if all_recommendations:
        logger.info(f"Saving {len(all_recommendations)} total recommendations")
        save_to_csv(all_recommendations, output_file)

        # Summarize recommendations
        summarize_recommendations(all_recommendations, output_file)
    else:
        logger.warning("No recommendations generated")

    logger.info("BigQuery Optimizer completed")

def main():
    """Main entry point"""
    # Parse command line arguments
    args = parse_args()

    # Set verbose logging if requested
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    config = load_config(args.config)

    # Override config with command line arguments
    if args.project_id:
        config['project_id'] = args.project_id
    if args.lookback_days:
        config['lookback_days'] = args.lookback_days
    if args.no_llm:
        config['use_llm'] = False
    if args.output_file:
        config['output_recommendations_file'] = args.output_file

    # Set optional stage flags
    config['collect_metadata'] = not args.skip_metadata
    config['collect_queries'] = not args.skip_queries
    config['use_vector_db'] = not args.skip_vector_db
    config['query_limit'] = args.query_limit

    # Run the optimizer
    run(config)

if __name__ == "__main__":
    main()