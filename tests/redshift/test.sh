#!/bin/bash
set -e

# Always run from this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Redshift MCP Server Test ==="
echo

echo "1. Building custom Redshift Docker image and testing MCP server (stdio)..."
python3 test_mcp.py

echo -e "\n2. Test completed!"

echo -e "\n3. No containers to clean up."

echo -e "\n✅ Redshift test passed!"


