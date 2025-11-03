#!/bin/bash

# Custom entrypoint for Redshift Toolbox
# Uses custom tools configuration file for Redshift-specific features

set -euo pipefail

# Constants
readonly ORIGINAL_ENTRYPOINT="/toolbox"
readonly REDSHIFT_TOOLS_FILE="/config/redshift.yaml"

# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [REDSHIFT-TOOLBOX] $*" >&2
}

# Function to check if original toolbox exists
check_original_toolbox() {
    if [[ ! -x "$ORIGINAL_ENTRYPOINT" ]]; then
        log "ERROR: Original toolbox entrypoint not found at $ORIGINAL_ENTRYPOINT"
        exit 1
    fi
}

# Function to check if Redshift tools file exists
check_redshift_tools_file() {
    if [[ ! -f "$REDSHIFT_TOOLS_FILE" ]]; then
        log "ERROR: Redshift tools configuration file not found at $REDSHIFT_TOOLS_FILE"
        exit 1
    fi
}

# Function to map REDSHIFT_* environment variables to POSTGRES_*
# The toolbox uses POSTGRES_* internally for the postgres kind source
map_redshift_to_postgres_vars() {
    # Map REDSHIFT_HOST to POSTGRES_HOST if POSTGRES_HOST is not set
    if [[ -n "${REDSHIFT_HOST:-}" && -z "${POSTGRES_HOST:-}" ]]; then
        export POSTGRES_HOST="${REDSHIFT_HOST}"
    fi
    
    # Map REDSHIFT_PORT to POSTGRES_PORT if POSTGRES_PORT is not set
    if [[ -n "${REDSHIFT_PORT:-}" && -z "${POSTGRES_PORT:-}" ]]; then
        export POSTGRES_PORT="${REDSHIFT_PORT}"
    fi
    
    # Map REDSHIFT_DATABASE to POSTGRES_DATABASE if POSTGRES_DATABASE is not set
    if [[ -n "${REDSHIFT_DATABASE:-}" && -z "${POSTGRES_DATABASE:-}" ]]; then
        export POSTGRES_DATABASE="${REDSHIFT_DATABASE}"
    fi
    
    # Map REDSHIFT_USER to POSTGRES_USER if POSTGRES_USER is not set
    if [[ -n "${REDSHIFT_USER:-}" && -z "${POSTGRES_USER:-}" ]]; then
        export POSTGRES_USER="${REDSHIFT_USER}"
    fi
    
    # Map REDSHIFT_PASSWORD to POSTGRES_PASSWORD if POSTGRES_PASSWORD is not set
    if [[ -n "${REDSHIFT_PASSWORD:-}" && -z "${POSTGRES_PASSWORD:-}" ]]; then
        export POSTGRES_PASSWORD="${REDSHIFT_PASSWORD}"
    fi
    
    # Map REDSHIFT_SSL_MODE to POSTGRES_SSL_MODE if POSTGRES_SSL_MODE is not set
    if [[ -n "${REDSHIFT_SSL_MODE:-}" && -z "${POSTGRES_SSL_MODE:-}" ]]; then
        export POSTGRES_SSL_MODE="${REDSHIFT_SSL_MODE}"
    fi
}

# Main execution
main() {
    log "Starting Redshift custom toolbox entrypoint"
    
    # Check if original toolbox exists
    check_original_toolbox
    
    # Check if Redshift tools file exists
    check_redshift_tools_file
    
    # Map REDSHIFT_* environment variables to POSTGRES_*
    map_redshift_to_postgres_vars
    
    # Execute the original toolbox with Redshift tools file, --stdio, and any additional arguments
    log "Executing Redshift toolbox with arguments: --tools-file $REDSHIFT_TOOLS_FILE --stdio $*"
    exec "$ORIGINAL_ENTRYPOINT" "--tools-file" "$REDSHIFT_TOOLS_FILE" "--stdio" "$@"
}

# Handle edge cases
handle_errors() {
    local exit_code=$?
    log "ERROR: Script failed with exit code $exit_code"
    exit $exit_code
}

# Set error trap
trap handle_errors ERR

# Execute main function
main "$@"