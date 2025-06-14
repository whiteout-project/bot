# Use Python slim image for smaller footprint
FROM python:3.11-slim-bookworm

LABEL maintainer="Discord Bot Team"
LABEL description="Whiteout Survival Discord Bot"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    gosu \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Create data directories
RUN mkdir -p /app/data/db /app/data/log

# Copy requirements file first for better Docker layer caching
COPY requirements.txt* ./

# Install Python dependencies
# Note: The bot downloads requirements.txt if missing, so this is optional
RUN if [ -f requirements.txt ]; then \
        pip install --no-cache-dir -r requirements.txt; \
    else \
        echo "requirements.txt not found, will be downloaded by bot"; \
    fi

# Copy the bootstrap script first and make it executable
COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

# Copy the entire application
COPY . .

# Create a non-root user for security
RUN groupadd -r botuser && useradd -r -g botuser botuser
RUN chown -R botuser:botuser /app
RUN chown botuser:botuser /bootstrap.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import os; exit(0 if os.path.exists('/app/bot_token.txt') else 1)"

# Use bootstrap script as entrypoint
ENTRYPOINT ["/bootstrap.sh"]
