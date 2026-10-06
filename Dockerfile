FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py pipeline.py ./
COPY streaming/ ./streaming/
COPY monitoring/health_check.py ./monitoring/

ENTRYPOINT ["python"]
CMD ["pipeline.py"]
