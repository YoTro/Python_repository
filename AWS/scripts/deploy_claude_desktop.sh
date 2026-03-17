#!/bin/bash
# ==============================================================================
# Script: deploy_claude_desktop.sh
# Description: Automates the deployment/integration of the AWS V2 MCP Server 
#              into Claude Desktop configuration across different operating systems.
# ==============================================================================

set -e

echo "🚀 Starting deployment of AWS Market Intelligence MCP Server to Claude Desktop..."

# 1. Resolve project root and python binary
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
PYTHON_BIN="$PROJECT_ROOT/venv311/bin/python"
SERVER_SCRIPT="src/mcp/server.py"

if [ ! -f "$PYTHON_BIN" ]; then
    echo "❌ Error: Python binary not found at $PYTHON_BIN."
    echo "Please ensure the virtual environment venv311 is created and dependencies are installed."
    exit 1
fi

# 2. Determine Claude Desktop config path based on OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    CONFIG_DIR="$HOME/Library/Application Support/Claude"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    CONFIG_DIR="$HOME/.config/Claude"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    CONFIG_DIR="$APPDATA/Claude"
else
    echo "❌ Error: Unsupported OS ($OSTYPE). Cannot determine Claude Desktop config path."
    exit 1
fi

CONFIG_FILE="$CONFIG_DIR/claude_desktop_config.json"

# 3. Create directory if it doesn't exist
if [ ! -d "$CONFIG_DIR" ]; then
    echo "📁 Creating Claude config directory at $CONFIG_DIR..."
    mkdir -p "$CONFIG_DIR"
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "📄 Creating initial claude_desktop_config.json..."
    echo "{}" > "$CONFIG_FILE"
fi

# 4. Use python to safely update the JSON file
echo "🔧 Injecting MCP server configuration..."

"$PYTHON_BIN" -c "
import json
import sys
import os

config_file = r'$CONFIG_FILE'
project_root = r'$PROJECT_ROOT'
python_bin = r'$PYTHON_BIN'
server_script = r'$SERVER_SCRIPT'

try:
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
except (json.JSONDecodeError, FileNotFoundError):
    config = {}

if 'mcpServers' not in config:
    config['mcpServers'] = {}

# Register the unified server which discovers all domain tools
config['mcpServers']['aws-market-intelligence'] = {
    'command': python_bin,
    'args': [server_script],
    'cwd': project_root
}

with open(config_file, 'w', encoding='utf-8') as f:
    json.dump(config, f, indent=2, ensure_ascii=False)

print(f'✅ Successfully updated: {config_file}')
"

echo "=============================================================================="
echo "🎉 Deployment complete!"
echo "All microservice domain tools (Amazon, Finance, Market, Compliance, etc.)"
echo "are now bound to Claude Desktop via the unified registry."
echo "Please completely RESTART Claude Desktop to apply the changes."
echo "=============================================================================="
