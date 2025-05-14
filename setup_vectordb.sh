#!/bin/bash
# Setup script for Quadrant vector database and validation

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}BigQuery Optimizer - Vector Database Setup${NC}"
echo "This script helps you set up and validate the Quadrant vector database."
echo

# Check if Docker and Docker Compose are installed
if command -v docker >/dev/null 2>&1 && command -v docker-compose >/dev/null 2>&1; then
  echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}"
else
  echo -e "${RED}✗ Docker and/or Docker Compose not found${NC}"
  echo "Please install Docker and Docker Compose to continue."
  echo "  Docker: https://docs.docker.com/get-docker/"
  echo "  Docker Compose: https://docs.docker.com/compose/install/"
  exit 1
fi

# Check if Quadrant is already running
if curl -s "http://localhost:6333/collections" >/dev/null; then
  echo -e "${GREEN}✓ Quadrant is already running${NC}"
else
  echo "Starting Quadrant using Docker Compose..."
  docker-compose up -d
  
  # Wait for Quadrant to start
  echo "Waiting for Quadrant to start..."
  for i in {1..10}; do
    if curl -s "http://localhost:6333/collections" >/dev/null; then
      echo -e "${GREEN}✓ Quadrant started successfully${NC}"
      break
    fi
    
    if [ $i -eq 10 ]; then
      echo -e "${RED}✗ Failed to start Quadrant${NC}"
      echo "Please check Docker logs for details:"
      echo "  docker-compose logs quadrant"
      exit 1
    fi
    
    sleep 2
  done
fi

# Test Quadrant connection and create collection
echo "Testing Quadrant connection..."
COLLECTION_NAME="bigquery_schemas"
VECTOR_DIM=768

# Check if collection exists
if curl -s "http://localhost:6333/collections/${COLLECTION_NAME}" | grep -q "${COLLECTION_NAME}"; then
  echo -e "${GREEN}✓ Collection '${COLLECTION_NAME}' exists${NC}"
else
  echo "Creating collection '${COLLECTION_NAME}'..."
  curl -X PUT "http://localhost:6333/collections/${COLLECTION_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{\"vectors\": {\"size\": ${VECTOR_DIM}, \"distance\": \"Cosine\"}}"
    
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Collection created successfully${NC}"
  else
    echo -e "${RED}✗ Failed to create collection${NC}"
    exit 1
  fi
fi

echo
echo -e "${GREEN}Vector database setup complete!${NC}"
echo
echo "You can now run the BigQuery Optimizer with:"
echo "  ./run_analysis.sh"
echo
echo "If you want to skip using the vector database:"
echo "  ./run_analysis.sh --skip-vector-db"
echo
echo "To stop the vector database:"
echo "  docker-compose down"