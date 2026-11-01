# Snowflake MCP Server Docker Image
# Base image with explicit version
FROM python:3.12-slim

# Metadata
LABEL maintainer="Cognition"
LABEL description="Snowflake MCP Server with password and key-pair authentication"
LABEL version="1.1.0"

# Set working directory
WORKDIR /app

# Install system dependencies and clean up in same layer to minimize image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Optional package index override for builds behind a mirror (pip reads PIP_INDEX_URL)
ARG PIP_INDEX_URL

# Install Python packages with pinned versions (see requirements.txt)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt && \
    pip check && \
    pip freeze

# Service configuration and the query policy wrapper
COPY service_config.yaml /app/service_config.yaml
COPY snowflake_mcp_guard /app/snowflake_mcp_guard
ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1

# Fail the build if the wrapper cannot hook the installed server package
RUN python -c "import snowflake_mcp_guard; snowflake_mcp_guard.install()"

# Create non-root user (UID 1000, no home directory, no shell)
RUN useradd -u 1000 -M -s /usr/sbin/nologin mcp

# Copy entrypoint script with executable permissions
COPY --chmod=755 entrypoint.sh /entrypoint.sh

# Switch to non-root user
USER mcp

# Set entrypoint (exec form)
ENTRYPOINT ["/entrypoint.sh"]
