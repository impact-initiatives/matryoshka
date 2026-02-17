FROM python:3.13-slim

WORKDIR /usr/app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock /usr/app/
RUN uv sync --frozen --no-dev

COPY . /usr/app

RUN uv run dbt deps

CMD ["uv", "run", "dbt", "build"]
