FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATA_DIR=/tmp/cliplingua/data
ENV TMP_DIR=/tmp/cliplingua/tmp

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
