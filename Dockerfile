FROM python:3.14.4-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    PROCESSING_HOST=0.0.0.0 \
    PROCESSING_PORT=8000

WORKDIR /app

# Instala o uv usando a imagem oficial
COPY --from=ghcr.io/astral-sh/uv:0.12.20 /uv /uvx /bin/

# Instala dependências em uma camada separada
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

# Copia e instala o projeto
COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

RUN groupadd --gid 10001 processing \
    && useradd --uid 10001 --gid processing --create-home processing

USER processing
CMD ["uv", "run", "--no-sync", "python", "main.py"]
