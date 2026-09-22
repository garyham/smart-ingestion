FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src ./src
COPY config ./config

RUN uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"
