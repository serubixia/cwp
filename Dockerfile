FROM node:22-bookworm-slim

ENV NODE_ENV=production \
    PIP_NO_CACHE_DIR=1 \
    PORT=3000 \
    LOG_LEVEL=info \
    COQUI_MODEL=tts_models/es/css10/vits \
    COQUI_LANGUAGE=es \
    COQUI_DEVICE=cpu \
    COQUI_MAX_CONCURRENT_SYNTHESIS=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        espeak-ng \
        libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
COPY src ./src

RUN python3 -m venv /opt/coqui-venv \
    && /opt/coqui-venv/bin/python -m pip install --upgrade pip setuptools wheel \
    && /opt/coqui-venv/bin/python -m pip install TTS==0.22.0 \
    && PYTHON_EXECUTABLE=/opt/coqui-venv/bin/python COQUI_DEVICE=cpu /opt/coqui-venv/bin/python src/worker.py --preload-only

ENV PYTHON_EXECUTABLE=/opt/coqui-venv/bin/python

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["node", "src/healthcheck.mjs"]

CMD ["npm", "start"]
