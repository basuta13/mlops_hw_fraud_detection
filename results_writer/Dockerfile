FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --default-timeout=120 --retries 10 --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "app.py"]
