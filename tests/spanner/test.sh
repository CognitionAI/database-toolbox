#!/bin/bash
set -e

echo -e "\n=== Spanner MCP Server Test ==="

# 1. Start Spanner emulator
echo -e "\n1. Starting Spanner emulator..."
docker-compose up -d
sleep 10  # Give emulator time to start

# Check if emulator is running
docker-compose ps

# 2. Start MCP server - for emulator we set SPANNER_EMULATOR_HOST
echo -e "\n2. Testing MCP server..."
SPANNER_PROJECT=test-project \
SPANNER_INSTANCE=test-instance \
SPANNER_DATABASE=test-database \
SPANNER_EMULATOR_HOST=localhost:9010 \
docker run --rm -d \
  --name test-mcp-spanner-server \
  --network host \
  -e SPANNER_PROJECT \
  -e SPANNER_INSTANCE \
  -e SPANNER_DATABASE \
  -e SPANNER_EMULATOR_HOST \
  us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest \
  --prebuilt spanner

# Give MCP server a moment to start
sleep 5

# Run the Python test script
python3 test_mcp.py

echo -e "\n3. Test completed!"

# 4. Clean up
echo -e "\n4. Cleaning up..."
docker-compose down -v
docker rm -f test-mcp-spanner-server 2>/dev/null || true

echo -e "\n✅ Spanner test passed!"
