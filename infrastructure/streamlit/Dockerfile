FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./

RUN pip install --no-cache-dir \
    "streamlit>=1.49,<2" "plotly>=6,<7" "pandas>=2.2,<3" \
    "sqlalchemy>=2.0,<3" "psycopg[binary]>=3.3,<4"

COPY src ./src

RUN pip install --no-cache-dir --no-deps .

EXPOSE 8501

HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "src/fraud_streaming_pipeline/dashboard/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501"]
