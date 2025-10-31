#!/bin/bash

# Snowflake MCP Server Entrypoint
# Auto-detects authentication mode (password or key-pair)

set -euo pipefail

# Set secure umask for any temp files
umask 077

# Cleanup function for temp files
cleanup() {
    if [[ -n "${TEMP_KEY_FILE:-}" ]] && [[ -f "$TEMP_KEY_FILE" ]]; then
        rm -f "$TEMP_KEY_FILE"
    fi
    if [[ -n "${CONFIG_FILE:-}" ]] && [[ -f "$CONFIG_FILE" ]]; then
        rm -f "$CONFIG_FILE"
    fi
}
trap cleanup EXIT

# Function to log errors (never log secrets)
log_error() {
    echo "ERROR: $*" >&2
}

# Function to display usage
usage() {
    cat <<EOF
Usage: docker run [docker-options] mcp-snowflake:local [options]

Required arguments:
  --account ACCOUNT       Snowflake account identifier
  --user USER            Snowflake username

Authentication (provide ONE of these):
  --password PASSWORD    Password (or use SNOWFLAKE_PASSWORD env var)
  SNOWFLAKE_PRIVATE_KEY  Private key PEM content (env var only)

Optional arguments:
  --role ROLE            Snowflake role
  --warehouse WAREHOUSE  Snowflake warehouse
  --database DATABASE    Snowflake database
  --schema SCHEMA        Snowflake schema
  --transport TYPE       Transport type (default: stdio)

Environment variables:
  SNOWFLAKE_PASSWORD              Password for authentication
  SNOWFLAKE_PRIVATE_KEY           Unencrypted private key PEM content for key-pair auth

EOF
    exit 1
}

# Initialize variables
ACCOUNT=""
USER=""
PASSWORD=""
ROLE=""
WAREHOUSE=""
DATABASE=""
SCHEMA=""
TRANSPORT="stdio"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --account)
            ACCOUNT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        --role)
            ROLE="$2"
            shift 2
            ;;
        --warehouse)
            WAREHOUSE="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --schema)
            SCHEMA="$2"
            shift 2
            ;;
        --transport)
            TRANSPORT="$2"
            shift 2
            ;;
        --help|-h)
            usage
            ;;
        *)
            log_error "Unknown parameter: $1"
            usage
            ;;
    esac
done

# Check for password from environment if not provided via CLI
if [[ -z "$PASSWORD" ]] && [[ -n "${SNOWFLAKE_PASSWORD:-}" ]]; then
    PASSWORD="$SNOWFLAKE_PASSWORD"
fi

# Validate required parameters
if [[ -z "$ACCOUNT" ]]; then
    log_error "Missing required parameter: --account"
    usage
fi

if [[ -z "$USER" ]]; then
    log_error "Missing required parameter: --user"
    usage
fi

# Auto-detect authentication mode and validate
HAS_PASSWORD=false
HAS_PRIVATE_KEY=false

if [[ -n "$PASSWORD" ]]; then
    HAS_PASSWORD=true
fi

if [[ -n "${SNOWFLAKE_PRIVATE_KEY:-}" ]]; then
    HAS_PRIVATE_KEY=true
fi

# Validate exactly one authentication method
if [[ "$HAS_PASSWORD" == true ]] && [[ "$HAS_PRIVATE_KEY" == true ]]; then
    log_error "Multiple authentication methods provided. Please provide EITHER password OR private key, not both."
    exit 1
fi

if [[ "$HAS_PASSWORD" == false ]] && [[ "$HAS_PRIVATE_KEY" == false ]]; then
    log_error "No authentication method provided. Please provide either --password/SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY."
    usage
fi

# Set environment variables for snowflake-labs-mcp
export SNOWFLAKE_ACCOUNT="$ACCOUNT"
export SNOWFLAKE_USER="$USER"

# Add authentication
if [[ "$HAS_PASSWORD" == true ]]; then
    export SNOWFLAKE_PASSWORD="$PASSWORD"
elif [[ "$HAS_PRIVATE_KEY" == true ]]; then
    # Key-pair mode: write private key to secure temp file
    # Handle both single-line and multi-line PEM formats
    TEMP_KEY_FILE="/tmp/snowflake_key_$$.p8"
    python3 -c "
import sys
key = sys.stdin.read().rstrip()
# Replace literal \\n escape sequences with actual newlines
key = key.replace('\\\\n', '\n')
# If key has no newlines but starts with PEM marker, try to format it
# (Some keys are stored as single-line, we'll write as-is and let Snowflake handle it)
sys.stdout.write(key)
if not key.endswith('\n'):
    sys.stdout.write('\n')
" <<< "$SNOWFLAKE_PRIVATE_KEY" > "$TEMP_KEY_FILE"
    chmod 0600 "$TEMP_KEY_FILE"
    
    export SNOWFLAKE_PRIVATE_KEY_FILE="$TEMP_KEY_FILE"
fi

# Add optional parameters
if [[ -n "$ROLE" ]]; then
    export SNOWFLAKE_ROLE="$ROLE"
fi

if [[ -n "$WAREHOUSE" ]]; then
    export SNOWFLAKE_WAREHOUSE="$WAREHOUSE"
fi

if [[ -n "$DATABASE" ]]; then
    export SNOWFLAKE_DATABASE="$DATABASE"
fi

if [[ -n "$SCHEMA" ]]; then
    export SNOWFLAKE_SCHEMA="$SCHEMA"
fi

# Create service config file with default tools enabled
CONFIG_FILE="/tmp/snowflake_service_config_$$.yaml"
cat > "$CONFIG_FILE" <<'EOF'
agent_services: []
search_services: []
analyst_services: []

other_services:
  object_manager: false
  query_manager: true
  semantic_manager: false

sql_statement_permissions:
  - Select: true
  - Show: true
  - Describe: true
  - Explain: true
  - Use: true
  - Create: false
  - Alter: false
  - Drop: false
  - Insert: false
  - Update: false
  - Delete: false
  - Merge: false
  - TruncateTable: false
  - Grant: false
  - Revoke: false
  - Commit: false
  - Rollback: false
  - Transaction: false
  - Command: false
  - Comment: false
  - Unknown: false
EOF

chmod 0600 "$CONFIG_FILE"

# Build snowflake-labs-mcp command
CMD_ARGS=()
CMD_ARGS+=("--service-config-file" "$CONFIG_FILE")
CMD_ARGS+=("--transport" "$TRANSPORT")

# Execute the MCP server (replace shell process)
exec snowflake-labs-mcp "${CMD_ARGS[@]}"

