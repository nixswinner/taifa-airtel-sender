FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py logger_config.py metrics.py sms_client.py main.py ./

RUN mkdir -p /app/logs

CMD ["python", "main.py", "consume"]
