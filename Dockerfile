FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src ./src
COPY config ./config
COPY README.md ./
RUN uv sync --frozen

ENV PATH="/app/.venv/bin:$PATH"
