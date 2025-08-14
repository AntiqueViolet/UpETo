FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY etarch.py ./

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

VOLUME ["/app/data"]

ENV TZ=UTC CRON_TIME="10 2 * * *"

CMD ["python", "etarch.py"]
