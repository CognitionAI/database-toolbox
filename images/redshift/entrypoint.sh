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

# Main execution
main() {
    log "Starting Redshift custom toolbox entrypoint"
    
    # Check if original toolbox exists
    check_original_toolbox
    
    # Check if Redshift tools file exists
    check_redshift_tools_file
    
    # Execute the original toolbox with Redshift tools file and passed arguments
    log "Executing Redshift toolbox with arguments: $*"
    exec "$ORIGINAL_ENTRYPOINT" "--tools-file" "$REDSHIFT_TOOLS_FILE" "$@"
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