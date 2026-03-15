FROM python:3.11-slim

# system dependencies
# curl — health check during build
# no build tools needed — uv handles everything
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# install uv — fast Python package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# copy dependency files first
# Docker layer caching — if these dont change
# uv sync layer is not rebuilt on code changes
COPY pyproject.toml .
COPY uv.lock .

# install dependencies
RUN uv sync --frozen --no-dev

# copy application code
COPY agents/      agents/
COPY api/         api/
COPY tools/       tools/
COPY schemas/     schemas/
COPY data_ingestion/ data_ingestion/

# expose FastAPI port
EXPOSE 8000

# health check
# Docker checks this every 30s
# if it fails 3 times container is marked unhealthy
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# start FastAPI
# single worker — Groq rate limits mean
# multiple workers just queue 429 errors faster
CMD ["uv", "run", "uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1"]
