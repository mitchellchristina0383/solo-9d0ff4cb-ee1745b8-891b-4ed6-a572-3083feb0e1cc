FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY gateway /app/gateway
COPY simulator /app/simulator
COPY web /app/web

EXPOSE 5020 8080 8081 8082 1502

CMD ["python", "-m", "gateway"]
