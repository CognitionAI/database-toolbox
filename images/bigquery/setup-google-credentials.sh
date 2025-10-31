#!/bin/bash

# Google Cloud Credentials Setup Script
# Shared utility for custom Google AI Toolbox images
# Handles Google Cloud service account credentials passed via environment variables

set -euo pipefail

# Constants
readonly TEMP_SA_FILE="${GOOGLE_CREDS_TEMP_FILE:-/tmp/sa.json}"
readonly DEBUG_MODE="${DEBUG_GOOGLE_CREDS:-false}"

# Function to log messages with timestamp (only if debug enabled)
log_debug() {
    if [[ "$DEBUG_MODE" == "true" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [GOOGLE-CREDS] $*" >&2
    fi
}

# Function to log important messages (always shown)
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [GOOGLE-CREDS] $*" >&2
}

# Function to cleanup temporary files
cleanup_google_credentials() {
    if [[ -f "$TEMP_SA_FILE" ]]; then
        log_debug "Cleaning up temporary service account file: $TEMP_SA_FILE"
        rm -f "$TEMP_SA_FILE"
    fi
}

# Function to validate JSON format (basic check)
validate_service_account_json() {
    local json_content="$1"
    
    # Basic check for JSON structure - starts with { and ends with }
    if [[ ! "$json_content" =~ ^[[:space:]]*\{.*\}[[:space:]]*$ ]]; then
        log_info "ERROR: Invalid JSON format in GOOGLE_SERVICE_ACCOUNT_JSON - must be a JSON object"
        return 1
    fi
    
    # Check for required service account fields using basic string matching
    if [[ "$json_content" != *'"type"'*'"service_account"'* ]]; then
        log_info "ERROR: JSON does not appear to be a service account - missing 'type: service_account'"
        return 1
    fi
    
    if [[ "$json_content" != *'"project_id"'* ]]; then
        log_info "ERROR: Service account JSON missing required 'project_id' field"
        return 1
    fi
    
    if [[ "$json_content" != *'"private_key"'* ]]; then
        log_info "ERROR: Service account JSON missing required 'private_key' field"
        return 1
    fi
    
    return 0
}

# Main function to setup Google Cloud credentials
setup_google_credentials() {
    log_debug "Starting Google Cloud credentials setup"
    
    if [[ -n "${GOOGLE_SERVICE_ACCOUNT_JSON:-}" ]]; then
        log_info "Setting up Google Cloud credentials from GOOGLE_SERVICE_ACCOUNT_JSON environment variable"
        
        # Validate JSON format and content
        if ! validate_service_account_json "$GOOGLE_SERVICE_ACCOUNT_JSON"; then
            log_info "ERROR: Failed to validate service account JSON"
            return 1
        fi
        
        # Write JSON to temporary file with restrictive permissions
        echo "$GOOGLE_SERVICE_ACCOUNT_JSON" > "$TEMP_SA_FILE"
        
        # Set restrictive permissions (readable only by owner)
        chmod 600 "$TEMP_SA_FILE"
        
        # Set the Google Application Credentials environment variable
        export GOOGLE_APPLICATION_CREDENTIALS="$TEMP_SA_FILE"
        
        log_info "Google Cloud credentials configured successfully"
        log_debug "GOOGLE_APPLICATION_CREDENTIALS set to: $GOOGLE_APPLICATION_CREDENTIALS"
        
        # Extract project ID from the JSON for convenience using bash string manipulation
        if [[ "$GOOGLE_SERVICE_ACCOUNT_JSON" == *'"project_id"'* ]]; then
            # Extract project_id value using bash parameter expansion
            local temp="${GOOGLE_SERVICE_ACCOUNT_JSON#*\"project_id\"}"
            temp="${temp#*\"}"
            local project_id="${temp%%\"*}"
            if [[ -n "$project_id" && "$project_id" != "project_id" ]]; then
                export GOOGLE_CLOUD_PROJECT="$project_id"
                log_debug "GOOGLE_CLOUD_PROJECT set to: $project_id"
            fi
        fi
        
    else
        log_debug "No GOOGLE_SERVICE_ACCOUNT_JSON environment variable found"
        log_info "Proceeding with existing authentication configuration"
        
        # Check if GOOGLE_APPLICATION_CREDENTIALS is already set
        if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
            log_info "Using existing GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
        else
            log_debug "No Google Cloud credentials configured - relying on default authentication"
        fi
    fi
    
    return 0
}

# Function to check Google Cloud authentication status (optional utility)
check_google_auth_status() {
    log_debug "Checking Google Cloud authentication status"
    
    if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
        if [[ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
            log_debug "✓ GOOGLE_APPLICATION_CREDENTIALS points to existing file: $GOOGLE_APPLICATION_CREDENTIALS"
        else
            log_info "⚠ GOOGLE_APPLICATION_CREDENTIALS set but file not found: $GOOGLE_APPLICATION_CREDENTIALS"
        fi
    else
        log_debug "• No GOOGLE_APPLICATION_CREDENTIALS set - using default authentication"
    fi
    
    if [[ -n "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
        log_debug "✓ GOOGLE_CLOUD_PROJECT: $GOOGLE_CLOUD_PROJECT"
    fi
}

# Export functions for use by sourcing scripts
export -f setup_google_credentials
export -f cleanup_google_credentials  
export -f check_google_auth_status
export -f log_debug
export -f log_info

# If script is executed directly (not sourced), run setup
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    setup_google_credentials
    check_google_auth_status
fi