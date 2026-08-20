FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY aisstream_producer.py .

CMD ["python", "-u", "aisstream_producer.py"]