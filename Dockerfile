FROM python:3.12-slim

WORKDIR /app

# Install system dependencies needed by some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer caching — only rebuilds if requirements change)
COPY requirements.txt .

# Skip PySpark and pyodbc — not needed in the dashboard container (pipeline runs on Glue)
RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    streamlit>=1.36.0 \
    plotly==5.22.0 \
    boto3==1.34.107 \
    "s3fs>=2024.2.0" \
    pyarrow

# Copy Streamlit app code
# Note: local data/ folder is intentionally excluded — the app reads from S3 in production
COPY streamlit_app/ ./streamlit_app/

EXPOSE 8501

# Health check used by App Runner to verify the container is running
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD python -c "import pandas as pd; print('TESTING S3...'); df = pd.read_parquet('s3://global-partners-data-lake-561764228129/gold/dim_restaurant', storage_options={'client_kwargs': {'region_name': 'us-east-1'}}); print('SUCCESS! ROWS:', len(df))" && \
    streamlit run streamlit_app/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.enableCORS=false \
    --server.enableWebsocketCompression=false \
    --server.enableXsrfProtection=false
