FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install deps
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project

# Copy application
COPY src ./src
COPY config ./config
COPY airflow_scripts ./airflow_scripts
COPY main.py ./

# Install the project
RUN uv sync --frozen

CMD ["uv", "run", "python", "main.py"]