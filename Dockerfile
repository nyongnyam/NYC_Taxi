# Debian bookworm 기반: Spark 실행에 필요한 Java 17을 패키지로 바로 설치할 수 있다
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# pip로 설치한 PySpark 안에 Spark 실행 파일(spark-class 등)이 들어 있다
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py metrics.py pipeline.py spark_pipeline.py benchmark.py ./
COPY sql/ ./sql/
COPY streaming/ ./streaming/
COPY monitoring/health_check.py ./monitoring/

ENTRYPOINT ["python"]
CMD ["pipeline.py"]
