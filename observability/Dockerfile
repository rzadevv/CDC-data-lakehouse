FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir prometheus-client requests
COPY platform_exporter.py /app/platform_exporter.py
CMD ["python", "/app/platform_exporter.py"]
