# Dockerfile
#
# Containerizes the FinSight Agent FastAPI application.
# Base: python:3.11-slim — small, no unnecessary packages.
#
# Build:   docker build -t finsight-agent .
# Run:     docker run -p 8000:8000 --env-file .env finsight-agent
# Deploy:  Azure Container Apps — consumption tier, free

FROM python:3.11-slim

# system dependencies
# curl — used by HEALTHCHECK
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# tell uv to install into /app/.venv
# this makes packages available without downloading at runtime
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# copy dependency files first
# Docker layer caching — if pyproject.toml and uv.lock dont change
# this layer is not rebuilt on code changes
COPY pyproject.toml .
COPY uv.lock .

# install all dependencies at BUILD time
# --frozen      use exact versions from uv.lock
# --no-dev      skip pytest, ruff etc — not needed in container
# --no-cache    do not use uv cache — install directly into .venv
RUN uv sync --frozen --no-dev --no-cache

# copy application code
COPY agents/         agents/
COPY api/            api/
COPY tools/          tools/
COPY schemas/        schemas/
COPY data_ingestion/ data_ingestion/

# expose FastAPI port
EXPOSE 8000

# health check
# Container Apps uses this for readiness probe
# 3 failures = container restarted automatically
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# start FastAPI using venv python directly
# NOT uv run — avoids uv downloading anything at startup
# single worker — Groq rate limits are the bottleneck not CPU
CMD ["/app/.venv/bin/uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1"]
