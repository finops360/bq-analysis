#!/usr/bin/env python3
"""
Test script for the LLM analyzer recommendation parser

This script tests the JSON parsing capabilities of the LLM analyzer.
"""

import json
import logging
import sys
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

def extract_recommendation_manually(json_str: str, referenced_tables=None):
    """
    Manually extract recommendation fields from improperly formatted JSON
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

def test_json_parsing():
    """Test JSON parsing with different input formats"""
    test_cases = [
        # Valid JSON
        {
            "name": "Valid JSON",
            "input": '{"recommendation_type": "PARTITION", "recommendation": "Add date partitioning", "justification": "Large table scanned frequently", "implementation": "ALTER TABLE...", "estimated_savings_pct": 30, "priority": "HIGH"}',
            "tables": ["test_table"],
            "expected_success": True
        },
        # JSON with control characters
        {
            "name": "JSON with control characters",
            "input": '{"recommendation_type": "PARTITION", "recommendation": "Add date partitioning", "justification": "Large table\nscanned\tfrequently", "implementation": "ALTER TABLE...", "estimated_savings_pct": 30, "priority": "HIGH"}',
            "tables": ["test_table"],
            "expected_success": True
        },
        # JSON with single quotes
        {
            "name": "JSON with single quotes",
            "input": "{'recommendation_type': 'PARTITION', 'recommendation': 'Add date partitioning', 'justification': 'Large table scanned frequently', 'implementation': 'ALTER TABLE...', 'estimated_savings_pct': 30, 'priority': 'HIGH'}",
            "tables": ["test_table"],
            "expected_success": True
        },
        # Malformed JSON - missing closing brace
        {
            "name": "Malformed JSON - missing closing brace",
            "input": '{"recommendation_type": "PARTITION", "recommendation": "Add date partitioning", "justification": "Large table scanned frequently", "implementation": "ALTER TABLE...", "estimated_savings_pct": 30, "priority": "HIGH"',
            "tables": ["test_table"],
            "expected_success": True
        },
        # Malformed JSON - no tables
        {
            "name": "Malformed JSON - no tables",
            "input": '{"recommendation_type": "PARTITION", "recommendation": "Add date partitioning", "justification": "Large table scanned frequently", "implementation": "ALTER TABLE...", "estimated_savings_pct": 30, "priority": "HIGH"}',
            "tables": [],
            "expected_success": True
        }
    ]
    
    for i, test in enumerate(test_cases):
        logger.info(f"Test case {i+1}: {test['name']}")
        
        try:
            # Try standard JSON parsing first
            try:
                recommendation = json.loads(test["input"])
                logger.info("Standard JSON parsing succeeded")
                
                # Add table_id
                if test["tables"] and len(test["tables"]) > 0:
                    recommendation["table_id"] = test["tables"][0]
                else:
                    recommendation["table_id"] = "unknown_table"
                    
                logger.info(f"Result: {recommendation}")
                
            except json.JSONDecodeError as e:
                logger.warning(f"Standard JSON parsing failed: {e}")
                
                # Try manual extraction
                logger.info("Attempting manual extraction")
                recommendation = extract_recommendation_manually(test["input"], test["tables"])
                
                if recommendation:
                    logger.info(f"Manual extraction succeeded")
                    logger.info(f"Result: {recommendation}")
                else:
                    logger.error("Manual extraction failed")
            
            if test["expected_success"]:
                if recommendation and "table_id" in recommendation:
                    logger.info("✅ Test passed")
                else:
                    logger.error("❌ Test failed - no recommendation or missing table_id")
            else:
                if recommendation and "table_id" in recommendation:
                    logger.error("❌ Test failed - should not have produced a valid recommendation")
                else:
                    logger.info("✅ Test passed - correctly failed to parse")
                    
        except Exception as e:
            logger.error(f"Error in test case: {e}")
            
        print()  # Line break between test cases

def main():
    """Main function"""
    logger.info("Running LLM parser tests")
    test_json_parsing()
    return 0

if __name__ == "__main__":
    sys.exit(main())