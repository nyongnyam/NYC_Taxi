FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py pipeline.py ./
COPY monitoring/health_check.py ./monitoring/

ENTRYPOINT ["python", "pipeline.py"]
CMD ["--year", "2024", "--months", "1"]
