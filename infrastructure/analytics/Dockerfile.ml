FROM python:3.12-slim

RUN pip install --no-cache-dir \
    "numpy==2.3.3" \
    "psycopg[binary]==3.3.5" \
    "scikit-learn==1.9.0"

WORKDIR /app
COPY src /app/src
ENV PYTHONPATH=/app/src

CMD ["python", "-m", "fraud_streaming_pipeline.ml.isolation_forest_job"]
