from __future__ import annotations
import argparse
import asyncio
import logging
import sys
import os

# Adjust sys.path to ensure project-wide imports work
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.utils.config_helper import ConfigHelper
from src.gateway import APIGateway

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AWS_CLI")

async def run_workflow(workflow_name: str, params: dict):
    logger.info(f"Starting workflow via Gateway: {workflow_name} with params: {params}")
    try:
        # Gateway handles UnifiedRequest normalization and execution tracking
        result = await APIGateway.dispatch_cli_workflow(workflow_name, params=params)
        logger.info(f"Workflow {workflow_name} completed successfully.")
        if result and hasattr(result, "items"):
            logger.info(f"Generated {len(result.items)} items.")
        return result
    except Exception as e:
        logger.error(f"Workflow {workflow_name} failed: {e}")
        sys.exit(1)

async def run_explore(query: str):
    logger.info(f"Starting exploration (Agent mode) via Gateway for query: {query}")
    try:
        result = await APIGateway.dispatch_cli_explore(intent=query)
        if result and "message" in result:
            print(f"\n[Agent Response]\n{result['message']}\n")
    except Exception as e:
        logger.error(f"Exploration failed: {e}")
        sys.exit(1)

def main():
    ConfigHelper.load_config()
    
    parser = argparse.ArgumentParser(description="AWS (Amazon Web Scraper) CLI Entry Point")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--workflow", type=str, help="Name of the workflow to run (e.g., product_screening)")
    group.add_argument("--explore", type=str, help="Natural language query for the MCP Agent to explore")
    
    parser.add_argument("--params", type=str, help="JSON string of parameters for the workflow", default="{}")
    
    args = parser.parse_args()

    import json
    try:
        params = json.loads(args.params)
    except json.JSONDecodeError:
        logger.error("Invalid JSON string provided for --params")
        sys.exit(1)

    if args.workflow:
        asyncio.run(run_workflow(args.workflow, params))
    elif args.explore:
        asyncio.run(run_explore(args.explore))

if __name__ == "__main__":
    main()
