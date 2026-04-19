import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # RabbitMQ
    RABBITMQ_HOST           = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT           = int(os.getenv("RABBITMQ_PORT", 5672))
    RABBITMQ_USERNAME       = os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD       = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_VIRTUAL_HOST   = os.getenv("RABBITMQ_VIRTUAL_HOST") or os.getenv("RABBITMQ_VHOST", "/")
    RABBITMQ_MAIN_QUEUE     = os.getenv("RABBITMQ_MAIN_QUEUE") or os.getenv("RABBITMQ_QUEUE", "mq_sms_airtel")
    RABBITMQ_RETRY_QUEUE    = os.getenv("RABBITMQ_RETRY_QUEUE", "mq_sms_airtel_retry")
    RABBITMQ_DLQ            = os.getenv("RABBITMQ_DLQ", "mq_sms_airtel_dlq")
    RABBITMQ_RETRY_EXCHANGE = os.getenv("RABBITMQ_RETRY_EXCHANGE", "mq_sms_airtel_retry_exchange")

    # Concurrency
    PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT") or os.getenv("CONCURRENT_LIMIT", 10))
    WORKER_THREADS = int(os.getenv("WORKER_THREADS", 1))

    # Retry / backoff
    MAX_RETRY_ATTEMPTS  = int(os.getenv("MAX_RETRY_ATTEMPTS", 10))
    BASE_RETRY_DELAY_MS = int(os.getenv("BASE_RETRY_DELAY_MS", 5000))
    MAX_RETRY_DELAY_MS  = int(os.getenv("MAX_RETRY_DELAY_MS", 300000))

    # Observability
    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8000))
    LOG_LEVEL       = os.getenv("LOG_LEVEL", "INFO")

    # Callback
    CALLBACK_TIMEOUT        = int(os.getenv("CALLBACK_TIMEOUT", 30))
    CALLBACK_RETRY_ATTEMPTS = int(os.getenv("CALLBACK_RETRY_ATTEMPTS", 3))
    CALLBACK_RETRY_DELAY    = int(os.getenv("CALLBACK_RETRY_DELAY", 2))
