FROM node:22-bookworm-slim

ENV NODE_ENV=production \
    PIP_NO_CACHE_DIR=1 \
    PORT=3000 \
    COQUI_MODEL=tts_models/es/css10/vits \
    COQUI_LANGUAGE=es \
    COQUI_DEVICE=cpu \
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
COPY services/coqui-tts-api ./services/coqui-tts-api

RUN python3 -m venv /opt/coqui-venv \
    && /opt/coqui-venv/bin/python -m pip install --upgrade pip setuptools wheel \
    && /opt/coqui-venv/bin/python -m pip install TTS==0.22.0 \
    && PYTHON_EXECUTABLE=/opt/coqui-venv/bin/python COQUI_DEVICE=cpu /opt/coqui-venv/bin/python services/coqui-tts-api/worker.py --preload-only

ENV PYTHON_EXECUTABLE=/opt/coqui-venv/bin/python

EXPOSE 3000

CMD ["npm", "start"]