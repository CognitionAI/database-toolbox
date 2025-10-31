#!/bin/bash
set -e

# Always run from this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Dataplex MCP Server Test ==="
echo

echo "1. Testing MCP server (stdio) with credentials from .env/.credentials.json..."
python3 test_mcp.py

echo -e "\n2. Test completed!"

echo -e "\n3. No containers to clean up."

echo -e "\n✅ Dataplex test passed!"


