#!/bin/bash
set -e

# Always run from this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== SQL Server MCP Server Test ==="
echo

echo "1. Starting SQL Server container..."
docker-compose up -d
echo "   Waiting for SQL Server to be ready..."
sleep 90
docker-compose ps

echo -e "\n2. Testing MCP server (stdio) with prebuilt target..."
python3 test_mcp.py

echo -e "\n3. Test completed!"

echo -e "\n4. Cleaning up..."
docker-compose down -v

echo -e "\n✅ SQL Server test passed!"


