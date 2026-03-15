#!/bin/bash
# ==============================================================================
# Script: start_mcp_server.sh
# Description: A simple runner to start the unified MCP server via stdio.
#              Useful for testing or pointing other IDEs (like Cursor) to it.
# ==============================================================================

PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "Starting AWS Market Intelligence MCP Server..."
echo "Root: $PROJECT_ROOT"

cd "$PROJECT_ROOT"

if [ ! -d "venv311" ]; then
    echo "❌ Error: venv311 not found. Please create the virtual environment first."
    exit 1
fi

source venv311/bin/activate

# Execute the MCP server via stdio
python src/mcp/server.py
