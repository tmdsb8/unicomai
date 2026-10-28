FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/unicomai
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY models ./models
COPY bin ./bin
RUN mkdir -p data logs

ENV UNICOMAI_HOME=/opt/unicomai
ENV PORT=8800
EXPOSE 8800

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", \
     "--port", "8800", "--app-dir", "/opt/unicomai/app", \
     "--proxy-headers", "--forwarded-allow-ips=*"]
