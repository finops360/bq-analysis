#!/bin/bash
# Simple script to test the Ollama API

echo "Testing Ollama API connectivity..."
echo

# Default endpoint
ENDPOINT="http://127.0.0.1:11434/api/generate"

# Test if we can reach the API
echo "Sending test request to $ENDPOINT"

# Run a simple request
curl -s "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3",
    "prompt": "Summarize the text: BigQuery is a cloud-based data warehouse service",
    "stream": false
  }' | jq '.'

echo
echo "If you see a JSON response above with a 'response' field, your Ollama API is working correctly."
echo "The BigQuery Optimizer is now configured to use this API for generating embeddings."
echo
echo "Run the optimizer with: ./run_analysis.sh"