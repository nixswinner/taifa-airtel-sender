from prometheus_client import Counter, Histogram, Gauge, start_http_server
from config import Config
from logger_config import setup_logger

logger = setup_logger(__name__)

messages_received_total = Counter("airtel_messages_received_total", "Total consumed")
messages_sent_total     = Counter("airtel_messages_sent_total",     "Total sent",    ["status"])
messages_failed_total   = Counter("airtel_messages_failed_total",   "Total failed",  ["error_code"])
messages_retry_total    = Counter("airtel_messages_retry_total",    "Total retries", ["attempt"])
messages_dlq_total      = Counter("airtel_messages_dlq_total",      "Total DLQ'd",   ["reason"])
callback_success_total  = Counter("airtel_callback_success_total",  "Callback OK")
callback_failed_total   = Counter("airtel_callback_failed_total",   "Callback fail", ["reason"])

api_duration        = Histogram("airtel_api_duration_seconds",                "API call duration",         ["status"])
callback_duration   = Histogram("airtel_callback_duration_seconds",           "Callback POST duration",    ["status"])
processing_duration = Histogram("airtel_message_processing_duration_seconds", "End-to-end processing time")

consumer_active = Gauge("airtel_consumer_active", "Consumer alive")
worker_active   = Gauge("airtel_worker_active",   "Worker alive", ["worker_id"])


def start_metrics_server() -> None:
    try:
        start_http_server(Config.PROMETHEUS_PORT)
        logger.info(f"Metrics server started on port {Config.PROMETHEUS_PORT}")
    except Exception as exc:
        logger.error(f"Failed to start metrics server: {exc}")
