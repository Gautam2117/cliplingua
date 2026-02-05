FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/var/lib/cliplingua \
    TMP_DIR=/tmp/cliplingua/tmp

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates git curl tini nodejs \
    espeak-ng \
    libsndfile1 libgomp1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["tini","--"]
CMD ["sh","-c","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
