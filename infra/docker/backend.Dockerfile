FROM python:3.11.11-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --gid 10001 risk \
    && useradd --uid 10001 --gid risk --create-home --shell /usr/sbin/nologin risk

COPY requirements.txt requirements-dev.txt ./
RUN pip install --upgrade pip==24.3.1 \
    && pip install -r requirements.txt

COPY . .
RUN python -m scripts.generate_fixture --output /opt/risk-fixture \
    && chown -R risk:risk /app /opt/risk-fixture

FROM base AS test
RUN pip install -r requirements-dev.txt
USER risk
CMD ["pytest"]

FROM base AS runtime
USER risk

CMD ["uvicorn", "services.api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
