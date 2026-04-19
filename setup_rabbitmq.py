"""
One-shot script to declare the retry exchange, retry queue, and DLQ
in the mq_vas vhost. Run once before starting the consumer.

    python setup_rabbitmq.py
"""

import pika
from dotenv import load_dotenv
from config import Config

load_dotenv()


def setup() -> None:
    credentials = pika.PlainCredentials(Config.RABBITMQ_USERNAME, Config.RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        virtual_host=Config.RABBITMQ_VIRTUAL_HOST,
        credentials=credentials,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 1. Retry exchange — messages published here with a TTL expiration
    #    will be routed to the retry queue and then dead-lettered back
    #    to the main queue once the TTL expires.
    channel.exchange_declare(
        exchange=Config.RABBITMQ_RETRY_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    print(f"[OK] exchange '{Config.RABBITMQ_RETRY_EXCHANGE}'")

    # 2. Retry queue — holds messages for the TTL duration, then
    #    dead-letters them back to the main queue via the default exchange.
    channel.queue_declare(
        queue=Config.RABBITMQ_RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": Config.RABBITMQ_MAIN_QUEUE,
        },
    )
    print(f"[OK] queue    '{Config.RABBITMQ_RETRY_QUEUE}'")

    # 3. Bind retry queue to retry exchange
    channel.queue_bind(
        queue=Config.RABBITMQ_RETRY_QUEUE,
        exchange=Config.RABBITMQ_RETRY_EXCHANGE,
        routing_key="retry",
    )
    print(f"[OK] binding  '{Config.RABBITMQ_RETRY_EXCHANGE}' -> '{Config.RABBITMQ_RETRY_QUEUE}' (key=retry)")

    # 4. Dead-letter queue — final resting place for unrecoverable messages
    channel.queue_declare(queue=Config.RABBITMQ_DLQ, durable=True)
    print(f"[OK] queue    '{Config.RABBITMQ_DLQ}'")

    connection.close()
    print("\nRabbitMQ topology ready.")


if __name__ == "__main__":
    setup()
