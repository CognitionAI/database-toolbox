#!/bin/bash
set -e

echo "=== PostgreSQL MCP Server Test ==="
echo

# Start database container
echo "1. Starting PostgreSQL container..."
docker-compose up -d
echo "   Waiting for PostgreSQL to be ready..."
sleep 5

# Check if postgres is healthy
docker-compose ps

# Test the MCP server using the exact command from README
echo -e "\n2. Testing MCP server with list_tables tool..."
python3 test_mcp.py

echo -e "\n3. Test completed!"

# Clean up
echo -e "\n4. Cleaning up..."
docker-compose down -v

echo -e "\n✅ PostgreSQL test passed!"
