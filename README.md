# BigQuery Optimizer

A comprehensive tool for analyzing and optimizing BigQuery resources. This tool provides both heuristic and LLM-based recommendations for improving BigQuery performance and reducing costs.

## Features

- **Metadata Collection**: Collect detailed metadata about BigQuery tables and query history
- **Heuristic Analysis**: Generate optimization recommendations based on best practices and table/query patterns
- **LLM Integration**: Get intelligent recommendations using LLM analysis of queries
- **Quadrant Vector DB**: Store and search schema information for context-aware recommendations
- **Comprehensive Recommendations**:
  - Partitioning optimization
  - Clustering optimization
  - Query structure improvement
  - Materialized views
  - Table lifecycle management
  - Column and data type optimization

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/bigquery_optimization.git
   cd bigquery_optimization
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure you have set up authentication for Google Cloud:
   ```bash
   gcloud auth application-default login
   ```

4. Set up Ollama and Quadrant for LLM-based recommendations:
   - Install Ollama: https://ollama.ai/
   - Install Quadrant using Docker Compose (recommended):
     ```bash
     docker-compose up -d
     ```
   - Verify Quadrant setup with the included test script:
     ```bash
     ./test_quadrant.py
     ```
   - Alternatively, install Quadrant directly: https://qdrant.tech/documentation/quick-start/

   The tool now uses Quadrant for optimal vector database functionality, but includes a robust fallback if Quadrant is unavailable or if the vector database is disabled with the `--skip-vector-db` flag.

## Configuration

Edit `bigquery_optimizer/config/config.yaml` to customize settings:

```yaml
# Project Settings
project_id: your-gcp-project-id
lookback_days: 30

# LLM Settings
use_llm: true
ollama_endpoint: http://127.0.0.1:11434/api/generate
ollama_model: llama3
temperature: 0.2
max_tokens: 4096

# Quadrant Settings
quadrant_endpoint: http://localhost:6333
quadrant_collection: bigquery_schemas
vector_dimension: 768

# Output Settings
output_metadata_file: table_metadata.csv
output_queries_file: query_history.csv
output_recommendations_file: query_recommendations.csv

# Analysis Settings
table_size_threshold: 0.01  # GB, very low to include all tables
min_query_count: 0  # No minimum query count
recommendation_limit: 100  # Maximum recommendations to return
```

## Usage

Run the optimizer with default settings:

```bash
./run_analysis.sh
```

Override configuration settings:

```bash
./run_analysis.sh --project my-project-id --days 15 --no-llm
```

Available options:

```
  -p, --project PROJECT_ID   Override GCP Project ID
  -d, --days DAYS            Override number of days of query history to analyze
  -o, --output FILE          Override output file for recommendations
  -c, --config FILE          Use alternate config file (default: config.yaml)
  -n, --no-llm               Disable LLM-based recommendations
  -v, --verbose              Enable verbose logging

  --skip-metadata            Skip collecting table metadata (use existing data)
  --skip-queries             Skip collecting query history (use existing data)
  --skip-vector-db           Skip using vector database for schema storage
  --query-limit NUM          Maximum number of queries to analyze with LLM (default: 10)
  -h, --help                 Show this help message
```

The tool uses the Quadrant vector database by default for better semantic similarity between schemas. If Quadrant is not available or you prefer the fallback approach, use the `--skip-vector-db` flag.

## Architecture

The BigQuery Optimizer is structured into several components:

1. **Analysis Module**: Collects and analyzes metadata about BigQuery resources
   - Metadata collection
   - Heuristic analysis
   - Recommendation generation

2. **Vector Database Module**: Manages schema information storage
   - Quadrant integration (optional)
   - Schema embeddings via two methods:
     - LLM-based summary embeddings using Ollama
     - Fallback hash-based embeddings when Quadrant is not available
   - Similarity search with graceful degradation

3. **LLM Module**: Analyzes queries using LLMs
   - Ollama integration
   - Context-aware query analysis
   - SQL optimization

## Recommendations

The optimizer generates several types of recommendations:

- **Partitioning**: Identify tables that would benefit from partitioning
- **Clustering**: Suggest clustering columns for improved query performance
- **Query Optimization**: Provide suggestions for improving query efficiency
- **Materialized Views**: Identify opportunities for using materialized views
- **Data Type Optimization**: Suggest better data types for specific columns
- **Table Lifecycle**: Recommendations for table expiration and cleanup

For more details, see [RECOMMENDATIONS.md](bigquery_optimizer/docs/RECOMMENDATIONS.md).



  1. README.md - Main documentation
  2. requirements.txt - Python dependencies
  3. run_analysis.sh - Entry point script
  4. bigquery_optimizer/ - Main package with all code and documentation

  The project is now organized as follows:

  bigquery_optimization/
  ├── README.md                           # Main documentation
  ├── requirements.txt                    # Python dependencies
  ├── run_analysis.sh                     # Entry point script
  └── bigquery_optimizer/                 # Main package
      ├── __init__.py                     # Package initialization
      ├── analysis/                       # Analysis module
      │   ├── __init__.py
      │   ├── heuristic_analyzer.py       # Heuristic recommendations
      │   └── metadata_collector.py       # Table metadata collection
      ├── config/                         # Configuration
      │   └── config.yaml                 # Main configuration file
      ├── docs/                           # Documentation
      │   └── RECOMMENDATIONS.md          # Detailed recommendations
      ├── llm_analyzer.py                 # LLM analysis module
      ├── main.py                         # Main entry point
      ├── output/                         # Output directory
      │   ├── query_history.csv
      │   ├── query_recommendations.csv
      │   └── table_metadata.csv
      ├── utils/                          # Utility functions
      │   ├── __init__.py
      │   └── config.py                   # Configuration loader
      └── vectordb/                       # Vector database module
          ├── __init__.py
          └── quadrant_manager.py         # Quadrant integration
          

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.