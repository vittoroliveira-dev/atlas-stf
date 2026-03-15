FROM python:3.14-slim AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --no-dev --frozen

COPY src/ src/
COPY schemas/ schemas/

FROM python:3.14-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/schemas /app/schemas

ENV PATH="/app/.venv/bin:$PATH"
ENV ATLAS_STF_DATABASE_URL="sqlite+pysqlite:///data/serving/atlas_stf.db"

EXPOSE 8000

USER app

CMD ["uvicorn", "atlas_stf.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
