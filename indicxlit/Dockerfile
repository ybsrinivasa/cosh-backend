FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/root/.cache/huggingface \
    XLIT_HOME=/root/.AI4Bharat

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Torch (CPU) on its own layer — ~200MB, kept stable across rebuilds.
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .

EXPOSE 8002

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8002"]
