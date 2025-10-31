# Snowflake MCP Server Docker Image
# Base image with explicit version
FROM python:3.12-slim

# Metadata
LABEL maintainer="Cognition"
LABEL description="Snowflake MCP Server with password and key-pair authentication"
LABEL version="1.0.0"

# Set working directory
WORKDIR /app

# Install system dependencies and clean up in same layer to minimize image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python package with no cache to minimize image size
RUN pip install --no-cache-dir snowflake-labs-mcp

# Create non-root user (UID 1000, no home directory, no shell)
RUN useradd -u 1000 -M -s /usr/sbin/nologin mcp

# Copy entrypoint script with executable permissions
COPY --chmod=755 entrypoint.sh /entrypoint.sh

# Switch to non-root user
USER mcp

# Set entrypoint (exec form)
ENTRYPOINT ["/entrypoint.sh"]

