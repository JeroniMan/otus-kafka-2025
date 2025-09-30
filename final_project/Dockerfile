# Dockerfile - Production-ready with UV
FROM ghcr.io/astral-sh/uv:latest AS uv

FROM python:3.11-slim

# Copy uv from official image
COPY --from=uv /uv /usr/local/bin/uv

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create non-root user early
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# Switch to non-root user for all subsequent operations
USER appuser

# Copy dependency files first (for better caching)
COPY --chown=appuser:appuser pyproject.toml ./
COPY --chown=appuser:appuser uv.lock* ./
COPY --chown=appuser:appuser README.md ./

# Install dependencies
RUN uv venv && \
    uv pip install -e .

# Copy application code
COPY --chown=appuser:appuser slack_bot/ ./slack_bot/
COPY --chown=appuser:appuser workers/ ./workers/
COPY --chown=appuser:appuser shared/ ./shared/
COPY --chown=appuser:appuser indexer/ ./indexer/
COPY --chown=appuser:appuser data/ ./data/
COPY --chown=appuser:appuser scripts/ ./scripts/

# Environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command (will be overridden in docker-compose)
ENTRYPOINT ["uv", "run", "python", "-m"]
CMD ["workers.main"]