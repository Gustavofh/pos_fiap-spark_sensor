FROM python:3.11-slim
RUN pip install requests psycopg2-binary
COPY collector.py /app/
CMD ["python", "/app/collector.py"]