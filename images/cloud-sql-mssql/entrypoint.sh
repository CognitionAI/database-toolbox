#!/bin/bash

# Custom entrypoint for Cloud SQL for SQL Server Google AI Toolbox
# Uses shared Google Cloud credentials setup

set -euo pipefail

# Constants
readonly ORIGINAL_ENTRYPOINT="/toolbox"
readonly SHARED_CREDS_SCRIPT="/usr/local/bin/setup-google-credentials.sh"

# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [BIGQUERY-TOOLBOX] $*" >&2
}

# Source the shared Google credentials setup script
if [[ -f "$SHARED_CREDS_SCRIPT" ]]; then
    source "$SHARED_CREDS_SCRIPT"
else
    log "ERROR: Shared credentials script not found at $SHARED_CREDS_SCRIPT"
    exit 1
fi

# Set up cleanup trap for Google credentials
trap cleanup_google_credentials EXIT INT TERM

# Function to check if original toolbox exists
check_original_toolbox() {
    if [[ ! -x "$ORIGINAL_ENTRYPOINT" ]]; then
        log "ERROR: Original toolbox entrypoint not found at $ORIGINAL_ENTRYPOINT"
        exit 1
    fi
}

# Main execution
main() {
    log "Starting Cloud SQL for SQL Server custom toolbox entrypoint"
    
    # Check if original toolbox exists
    check_original_toolbox
    
    # Setup Google Cloud credentials using shared script
    if ! setup_google_credentials; then
        log "ERROR: Failed to setup Google Cloud credentials"
        exit 1
    fi
    
    # Execute the original toolbox with Cloud SQL SQL Server prebuilt and passed arguments
    log "Executing Cloud SQL for SQL Server toolbox with arguments: $*"
    exec "$ORIGINAL_ENTRYPOINT" "--prebuilt" "cloud-sql-mssql" "$@"
}

# Handle edge cases
handle_errors() {
    local exit_code=$?
    log "ERROR: Script failed with exit code $exit_code"
    cleanup_google_credentials
    exit $exit_code
}

# Set error trap
trap handle_errors ERR

# Execute main function
main "$@"