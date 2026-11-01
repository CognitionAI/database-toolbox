#!/bin/bash
set -e

# Always run from this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Snowflake MCP Server - Comprehensive Test Suite (pytest) ==="
echo

IMAGE_DIR="$SCRIPT_DIR/../../images/snowflake-mcp"

# Check if Docker image exists
if ! docker image inspect mcp-snowflake:local >/dev/null 2>&1; then
    echo "✗ Docker image 'mcp-snowflake:local' not found."
    echo "  Please build it first: cd ../../images/snowflake-mcp && docker build -t mcp-snowflake:local ."
    exit 1
fi

# Test environment: same pinned dependencies as the image, plus pytest
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r "$IMAGE_DIR/requirements.txt" pytest pytest-timeout
echo "✓ Test dependencies installed"
echo

echo "Running query policy tests (no credentials required)..."
python3 -m pytest test_query_policy.py -v --tb=short --color=yes --timeout=60 -x
echo

echo "Running smoke check inside the Docker image..."
docker run --rm --entrypoint python3 \
    -v "$SCRIPT_DIR/image_smoke_check.py:/tmp/image_smoke_check.py:ro" \
    -v "$IMAGE_DIR/requirements.txt:/tmp/requirements.txt:ro" \
    mcp-snowflake:local /tmp/image_smoke_check.py /tmp/requirements.txt
echo

# Connection tests need credentials
if [[ ! -f .env ]]; then
    echo "✗ .env file not found; skipping connection tests. Create one based on .env.example to run them."
    exit 0
fi

echo "Running connection tests with pytest..."
echo "All tests use the Docker image: mcp-snowflake:local"
echo

# Run tests with pytest - verbose, show progress, timeout per test
# Connection tests have 60s timeout, error tests are fast
python3 -m pytest test_mcp.py -v --tb=short --color=yes --timeout=60 -x

# Exit code from pytest will be preserved

