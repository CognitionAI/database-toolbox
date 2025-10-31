#!/bin/bash
set -e

# Always run from this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Snowflake MCP Server - Comprehensive Test Suite (pytest) ==="
echo

# Check if .env file exists
if [[ ! -f .env ]]; then
    echo "✗ .env file not found. Please create one based on .env.example"
    exit 1
fi

# Check if Docker image exists
if ! docker image inspect mcp-snowflake:local >/dev/null 2>&1; then
    echo "✗ Docker image 'mcp-snowflake:local' not found."
    echo "  Please build it first: cd ../../images/snowflake-mcp && docker build -t mcp-snowflake:local ."
    exit 1
fi

# Check if pytest is installed
if ! python3 -m pytest --version >/dev/null 2>&1; then
    echo "Installing pytest in virtual environment..."
    # Create venv if it doesn't exist
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
    fi
    source .venv/bin/activate
    pip install -q pytest
    echo "✓ pytest installed"
fi

echo "Running tests with pytest..."
echo "All tests use the Docker image: mcp-snowflake:local"
echo

# Activate venv if it exists and run pytest
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run tests with pytest - verbose, show progress, timeout per test
# Connection tests have 60s timeout, error tests are fast
python3 -m pytest test_mcp.py -v --tb=short --color=yes --timeout=60 -x

# Exit code from pytest will be preserved

