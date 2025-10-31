# Custom Google AI Toolbox Docker Image with Environment Variable Authentication
# Extends the official Google AI Toolbox to support credentials via environment variables

# Stage 1: Get bash and required tools from a full Linux distribution
FROM --platform=linux/amd64 debian:bookworm-slim AS tools
RUN apt-get update && apt-get install -y bash coreutils && rm -rf /var/lib/apt/lists/*

# Stage 2: Main image - extend the Google AI Toolbox
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Set metadata
LABEL maintainer="Cognition"
LABEL description="Cloud SQL for PostgreSQL Toolbox with environment variable authentication support"
LABEL version="1.0.0"

# Copy essential binaries and libraries
COPY --from=tools /bin/bash /bin/bash
COPY --from=tools /bin/date /bin/date
COPY --from=tools /bin/chmod /bin/chmod
COPY --from=tools /bin/rm /bin/rm

# Copy essential library directories
COPY --from=tools /lib/x86_64-linux-gnu/ /lib/x86_64-linux-gnu/
COPY --from=tools /lib64/ /lib64/

# Copy the shared Google credentials setup script
COPY --chmod=755 setup-google-credentials.sh /usr/local/bin/setup-google-credentials.sh

# Copy our custom entrypoint script with executable permissions
COPY --chmod=755 entrypoint.sh /usr/local/bin/entrypoint.sh

# Set our custom entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command (will be passed to the original toolbox)
CMD []