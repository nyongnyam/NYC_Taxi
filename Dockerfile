FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py pipeline.py spark_backfill.py ./
COPY monitoring/health_check.py ./monitoring/

ENTRYPOINT ["python", "pipeline.py"]
CMD ["--year", "2024", "--months", "1"]