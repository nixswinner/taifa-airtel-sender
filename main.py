"""
Airtel SMS POC — entrypoint / usage examples.

Usage:
    python main.py single  +254740XXXXXX "Hello from Airtel CPaaS!"
    python main.py flash   +254740XXXXXX "Flash message"
    python main.py bulk
    python main.py consume

Requires a .env file — copy .env.example and fill in your credentials.
"""

import json
import signal
import sys
import threading
import time
from typing import Dict, List

import pika
import requests

from config import Config
from logger_config import setup_logger
from metrics import (
    api_duration,
    callback_duration,
    callback_failed_total,
    callback_success_total,
    messages_dlq_total,
    messages_failed_total,
    messages_received_total,
    messages_retry_total,
    messages_sent_total,
    processing_duration,
    start_metrics_server,
    worker_active,
)
from sms_client import AirtelSMSClient, AirtelConfig

logger = setup_logger(__name__)

# ---------------------------------------------------------------------------
# Worker registry
# ---------------------------------------------------------------------------

_active_consumers: List["AirtelSMSConsumer"] = []
_consumers_lock  = threading.Lock()
_MAX_RECONNECT   = 5
_RECONNECT_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

class AirtelSMSConsumer:
    def __init__(self, worker_id: int, client: AirtelSMSClient) -> None:
        self.worker_id  = worker_id
        self.client     = client
        self.connection = None
        self.channel    = None
        self._lock      = threading.Lock()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        credentials = pika.PlainCredentials(
            Config.RABBITMQ_USERNAME,
            Config.RABBITMQ_PASSWORD,
        )
        parameters = pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            virtual_host=Config.RABBITMQ_VIRTUAL_HOST,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel    = self.connection.channel()
        self.channel.basic_qos(prefetch_count=Config.PREFETCH_COUNT)

        self.channel.exchange_declare(
            exchange=Config.RABBITMQ_RETRY_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        self.channel.queue_declare(
            queue=Config.RABBITMQ_RETRY_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange":    "",
                "x-dead-letter-routing-key": Config.RABBITMQ_MAIN_QUEUE,
            },
        )
        self.channel.queue_bind(
            queue=Config.RABBITMQ_RETRY_QUEUE,
            exchange=Config.RABBITMQ_RETRY_EXCHANGE,
            routing_key="retry",
        )
        self.channel.queue_declare(queue=Config.RABBITMQ_DLQ, durable=True)

        worker_active.labels(worker_id=str(self.worker_id)).set(1)
        logger.info(
            f"Worker {self.worker_id} connected",
            extra={"worker_id": self.worker_id, "queue": Config.RABBITMQ_MAIN_QUEUE},
        )

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def process_message(self, ch, method, properties, body) -> None:
        start_time = time.time()

        try:
            messages_received_total.inc()

            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                logger.warning(f"Worker {self.worker_id} — invalid JSON, discarding")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            retry_count = 0
            if properties.headers and "x-retry-count" in properties.headers:
                retry_count = properties.headers["x-retry-count"]

            dataset = payload.get("dataSet", [])
            if not dataset:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            all_ok  = True
            results = []

            for item in dataset:
                msisdn       = item.get("msisdn")
                message      = item.get("message")
                channel      = item.get("channel", "sms")
                unique_id    = item.get("uniqueId")
                callback_url = item.get("actionResponseURL")

                if not msisdn or not message:
                    continue

                t0 = time.time()
                if channel == "flash":
                    result = self.client.send_flash_sms(msisdn, message)
                else:
                    result = self.client.send_sms(msisdn, message)
                elapsed = time.time() - t0

                if result.success:
                    api_duration.labels(status="success").observe(elapsed)
                    messages_sent_total.labels(status="success").inc()
                    if callback_url:
                        self._send_callback(callback_url, payload)
                    results.append({"unique_id": unique_id, "success": True, "retryable": False})
                else:
                    api_duration.labels(status="error").observe(elapsed)
                    error_code = getattr(result, "error_code", None) or result.error or "SEND_FAILED"
                    retryable  = getattr(result, "retryable", True)
                    messages_failed_total.labels(error_code=error_code).inc()
                    all_ok = False
                    results.append({
                        "unique_id":  unique_id,
                        "success":    False,
                        "error_code": error_code,
                        "retryable":  retryable,
                    })

            if not all_ok:
                has_retryable = any(
                    r.get("retryable", False)
                    for r in results
                    if not r.get("success", False)
                )
                if has_retryable and retry_count < Config.MAX_RETRY_ATTEMPTS - 1:
                    self._republish_for_retry(body, properties, retry_count)
                else:
                    self._send_to_dlq(body, {"results": results}, retry_count)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            processing_duration.observe(time.time() - start_time)

        except Exception as exc:
            logger.error(f"Worker {self.worker_id} unexpected error: {exc}", exc_info=True)
            if ch.is_open:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _republish_for_retry(self, body: bytes, properties, retry_count: int) -> None:
        delay = min(
            Config.BASE_RETRY_DELAY_MS * (2 ** retry_count),
            Config.MAX_RETRY_DELAY_MS,
        )
        new_headers = dict(properties.headers or {})
        new_headers["x-retry-count"] = retry_count + 1

        with self._lock:
            self.channel.basic_publish(
                exchange=Config.RABBITMQ_RETRY_EXCHANGE,
                routing_key="retry",
                body=body,
                properties=pika.BasicProperties(
                    headers=new_headers,
                    delivery_mode=2,
                    content_type="application/json",
                    expiration=str(delay),
                ),
            )

        messages_retry_total.labels(attempt=str(retry_count + 1)).inc()
        logger.info(
            f"Worker {self.worker_id} — republished for retry",
            extra={"worker_id": self.worker_id, "retry_count": retry_count + 1, "delay_ms": delay},
        )

    def _send_to_dlq(self, original_body: bytes, error_info: Dict, retry_count: int) -> None:
        try:
            dlq_message = {
                "original_payload": json.loads(original_body),
                "error":            error_info,
                "retry_count":      retry_count,
                "worker_id":        self.worker_id,
                "timestamp":        int(time.time() * 1000),
            }
            with self._lock:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=Config.RABBITMQ_DLQ,
                    body=json.dumps(dlq_message),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                    ),
                )
            messages_dlq_total.labels(
                reason=error_info.get("error_code", "unknown")
            ).inc()
            logger.warning(
                f"Worker {self.worker_id} — sent to DLQ",
                extra={"worker_id": self.worker_id, "retry_count": retry_count},
            )
        except Exception as exc:
            logger.error(f"Worker {self.worker_id} — failed to publish to DLQ: {exc}")

    def _send_callback(self, callback_url: str, payload: Dict) -> bool:
        for attempt in range(1, Config.CALLBACK_RETRY_ATTEMPTS + 1):
            t0 = time.time()
            try:
                response = requests.post(
                    callback_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=Config.CALLBACK_TIMEOUT,
                )
                elapsed = time.time() - t0

                if response.status_code == 200 and "Success" in response.text:
                    callback_success_total.inc()
                    callback_duration.labels(status="success").observe(elapsed)
                    return True

                if attempt < Config.CALLBACK_RETRY_ATTEMPTS:
                    time.sleep(Config.CALLBACK_RETRY_DELAY)
                else:
                    callback_failed_total.labels(reason="invalid_response").inc()
                    callback_duration.labels(status="error").observe(elapsed)
                    return False

            except requests.exceptions.Timeout:
                if attempt < Config.CALLBACK_RETRY_ATTEMPTS:
                    time.sleep(Config.CALLBACK_RETRY_DELAY)
                else:
                    callback_failed_total.labels(reason="timeout").inc()
                    return False

            except Exception as exc:
                logger.error(f"Worker {self.worker_id} — callback error: {exc}")
                if attempt < Config.CALLBACK_RETRY_ATTEMPTS:
                    time.sleep(Config.CALLBACK_RETRY_DELAY)
                else:
                    callback_failed_total.labels(reason="exception").inc()
                    return False

        return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_consuming(self) -> None:
        try:
            self.channel.basic_consume(
                queue=Config.RABBITMQ_MAIN_QUEUE,
                on_message_callback=self.process_message,
                auto_ack=False,
            )
            logger.info(f"Worker {self.worker_id} — consuming from {Config.RABBITMQ_MAIN_QUEUE}")
            self.channel.start_consuming()
        finally:
            self.cleanup()

    def stop(self) -> None:
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()

    def cleanup(self) -> None:
        worker_active.labels(worker_id=str(self.worker_id)).set(0)
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()


# ---------------------------------------------------------------------------
# Worker thread entry point
# ---------------------------------------------------------------------------

def _run_worker(worker_id: int, client: AirtelSMSClient) -> None:
    for attempt in range(1, _MAX_RECONNECT + 1):
        consumer = AirtelSMSConsumer(worker_id, client)
        with _consumers_lock:
            _active_consumers.append(consumer)
        try:
            consumer.connect()
            consumer.start_consuming()
            break  # clean exit — stop reconnecting
        except Exception as exc:
            logger.error(
                f"Worker {worker_id} error (attempt {attempt}/{_MAX_RECONNECT}): {exc}",
                exc_info=True,
            )
            if attempt < _MAX_RECONNECT:
                time.sleep(_RECONNECT_DELAY)
        finally:
            with _consumers_lock:
                try:
                    _active_consumers.remove(consumer)
                except ValueError:
                    pass


# ---------------------------------------------------------------------------
# Demo helpers
# ---------------------------------------------------------------------------

def demo_single(client: AirtelSMSClient, destination: str, message: str) -> None:
    print(f"\n--- Sending default SMS to {destination} ---")
    result = client.send_sms(destination, message)
    print(result)
    if not result.success:
        print("Raw response:", result.raw_response)


def demo_flash(client: AirtelSMSClient, destination: str, message: str) -> None:
    print(f"\n--- Sending flash SMS to {destination} ---")
    result = client.send_flash_sms(destination, message)
    print(result)


def demo_bulk(client: AirtelSMSClient) -> None:
    print("\n--- Sending bulk SMS ---")
    messages = [
        {
            "destinationAddress": "+254740000001",
            "message": "Bulk SMS #1 from Airtel CPaaS",
            "messageType": "SMS",
        },
        {
            "destinationAddress": "+254740000002",
            "message": "Bulk Flash #2 from Airtel CPaaS",
            "messageType": "FLASH",
        },
    ]
    results = client.send_bulk_sms(messages)
    for i, r in enumerate(results, 1):
        print(f"  [{i}] {r}")


def demo_consume(client: AirtelSMSClient) -> None:
    capacity = Config.WORKER_THREADS * Config.PREFETCH_COUNT
    logger.info(
        "Starting Airtel SMS consumer",
        extra={
            "queue":       Config.RABBITMQ_MAIN_QUEUE,
            "workers":     Config.WORKER_THREADS,
            "capacity":    capacity,
            "metrics_url": f"http://localhost:{Config.PROMETHEUS_PORT}/metrics",
        },
    )

    start_metrics_server()

    def _shutdown(signum, _frame):
        logger.info(f"Signal {signum} received — stopping workers")
        with _consumers_lock:
            for c in list(_active_consumers):
                c.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    threads = []
    for i in range(Config.WORKER_THREADS):
        t = threading.Thread(
            target=_run_worker,
            args=(i, client),
            name=f"Worker-{i}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.1)

    for t in threads:
        t.join()

    logger.info("All workers stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        config = AirtelConfig.from_env()
    except EnvironmentError as exc:
        print(f"Configuration error: {exc}")
        print("Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    client = AirtelSMSClient(config)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "single"

    if cmd == "single":
        destination = sys.argv[2] if len(sys.argv) > 2 else "+254740000000"
        message     = sys.argv[3] if len(sys.argv) > 3 else "Hello from Airtel CPaaS POC!"
        demo_single(client, destination, message)

    elif cmd == "flash":
        destination = sys.argv[2] if len(sys.argv) > 2 else "+254740000000"
        message     = sys.argv[3] if len(sys.argv) > 3 else "Flash! Airtel CPaaS POC"
        demo_flash(client, destination, message)

    elif cmd == "bulk":
        demo_bulk(client)

    elif cmd == "consume":
        demo_consume(client)

    else:
        print(f"Unknown command: {cmd}")
        print("Available: single | flash | bulk | consume")
        sys.exit(1)


if __name__ == "__main__":
    main()
