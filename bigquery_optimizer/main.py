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
                        
    return parser.parse_args()

def summarize_recommendations(recommendations: List[Dict[str, Any]]) -> None:
    """Print a summary of recommendations"""
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
    
    print(f"\nFull recommendations saved to {config['output_recommendations_file']}")

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
    
    # Step 1: Collect table metadata
    logger.info("Step 1: Collecting table metadata")
    table_metadata = collect_table_metadata(config)
    if not table_metadata:
        logger.error("Failed to collect table metadata. Exiting.")
        sys.exit(1)
    
    # Step 2: Collect query history
    logger.info("Step 2: Collecting query history")
    query_history = collect_query_history(config)
    if not query_history:
        logger.warning("No query history found. Continuing with metadata-only analysis.")
    
    # Initialize recommendations list
    all_recommendations = []
    
    # Step 3: Heuristic analysis
    logger.info("Step 3: Performing heuristic analysis")
    heuristic_analyzer = HeuristicAnalyzer(config)
    heuristic_recommendations = heuristic_analyzer.analyze_data(table_metadata, query_history)
    all_recommendations.extend(heuristic_recommendations)
    logger.info(f"Generated {len(heuristic_recommendations)} heuristic recommendations")
    
    # Step 4: LLM-based analysis (if enabled)
    if config['use_llm']:
        logger.info("Step 4: Setting up vector database")
        quadrant_manager = QuadrantManager(config)
        if not quadrant_manager.initialize_collection():
            logger.error("Failed to initialize vector database. Skipping LLM analysis.")
        else:
            # Store schemas in vector database
            if not quadrant_manager.store_schemas(table_metadata):
                logger.error("Failed to store schemas in vector database. Skipping LLM analysis.")
            else:
                # Analyze with LLM
                logger.info("Step 5: Performing LLM-based analysis")
                llm_analyzer = LLMAnalyzer(config)
                llm_recommendations = llm_analyzer.analyze_queries(query_history[:10], quadrant_manager)  # Limit to 10 queries for demo
                all_recommendations.extend(llm_recommendations)
                logger.info(f"Generated {len(llm_recommendations)} LLM-based recommendations")
    
    # Save all recommendations
    if all_recommendations:
        logger.info(f"Saving {len(all_recommendations)} total recommendations")
        save_to_csv(all_recommendations, config['output_recommendations_file'])
        
        # Summarize recommendations
        summarize_recommendations(all_recommendations)
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
    
    # Run the optimizer
    run(config)

if __name__ == "__main__":
    main()