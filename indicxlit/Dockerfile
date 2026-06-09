# ai4bharat-transliteration pulls fairseq, which uses mutable
# defaults in dataclasses (`CommonConfig(common=…)`). Python 3.11
# rejects that pattern at import time — observed live on prod as
# "mutable default … for field common is not allowed: use
# default_factory". Pinning to 3.10 dodges the issue cleanly.
FROM python:3.10-slim

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
