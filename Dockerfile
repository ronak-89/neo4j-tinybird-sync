FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY neo4j_tinybird_backfill.py ./
COPY utils ./utils/

USER appuser

ENV PYTHONUNBUFFERED=1

CMD ["python", "neo4j_tinybird_backfill.py"]
