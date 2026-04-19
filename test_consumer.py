"""
Integration smoke test — mocked RabbitMQ, real Airtel API.

Builds a queue message for 0734000100, feeds it directly into
AirtelSMSConsumer.process_message(), and lets the real AirtelSMSClient
make the live API call.

Run:
    pytest test_consumer.py -v -s
"""

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sms_client import AirtelSMSClient, AirtelConfig
from main import AirtelSMSConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MSISDN = "254734000100"   # 0734000100 in international format (no +)

def _make_message(msisdn: str, text: str, channel: str = "sms") -> bytes:
    """Return a JSON queue body matching the expected dataSet schema."""
    payload = {
        "timeStamp": int(time.time()),
        "dataSet": [
            {
                "msisdn":            msisdn,
                "message":           text,
                "channel":           channel,
                "oa":                "Taifa",
                "uniqueId":          "test-001",
                "actionResponseURL": "",
            }
        ],
    }
    return json.dumps(payload).encode()


def _make_delivery(delivery_tag: int = 1):
    """Minimal pika method frame."""
    m = SimpleNamespace()
    m.delivery_tag = delivery_tag
    return m


def _make_properties(retry_count: int = 0):
    """Minimal pika properties."""
    p = SimpleNamespace()
    p.headers = {"x-retry-count": retry_count} if retry_count else {}
    return p


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sms_client():
    """Real AirtelSMSClient loaded from .env."""
    config = AirtelConfig.from_env()
    return AirtelSMSClient(config)


@pytest.fixture
def mock_channel():
    """Pika channel double — records ack/nack calls."""
    ch = MagicMock()
    ch.is_open = True
    return ch


@pytest.fixture
def consumer(sms_client, mock_channel):
    """AirtelSMSConsumer wired with a real client and mocked channel."""
    c = AirtelSMSConsumer(worker_id=0, client=sms_client)
    c.channel    = mock_channel
    c.connection = MagicMock(is_open=True)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcessMessage:
    def test_send_sms_live(self, consumer, mock_channel):
        """
        Feeds a real queue message through process_message().
        The SMS client hits the live Airtel API for 0734000100.
        """
        body       = _make_message(MSISDN, "Hello from Taifa POC test")
        method     = _make_delivery()
        properties = _make_properties()

        consumer.process_message(mock_channel, method, properties, body)

        # Message must always be acked (success or routed to retry/DLQ)
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_channel.basic_nack.assert_not_called()

    def test_send_flash_sms_live(self, consumer, mock_channel):
        """Same as above but channel='flash'."""
        body       = _make_message(MSISDN, "Flash test from Taifa POC", channel="flash")
        method     = _make_delivery(delivery_tag=2)
        properties = _make_properties()

        consumer.process_message(mock_channel, method, properties, body)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=2)
        mock_channel.basic_nack.assert_not_called()

    def test_invalid_json_nacked(self, consumer, mock_channel):
        """Malformed JSON must be nacked without requeue."""
        method     = _make_delivery()
        properties = _make_properties()

        consumer.process_message(mock_channel, method, properties, b"not json{{")

        mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
        mock_channel.basic_ack.assert_not_called()

    def test_empty_dataset_acked(self, consumer, mock_channel):
        """Empty dataSet is a no-op — message is acked immediately."""
        body       = json.dumps({"timeStamp": 0, "dataSet": []}).encode()
        method     = _make_delivery()
        properties = _make_properties()

        consumer.process_message(mock_channel, method, properties, body)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_missing_msisdn_skipped(self, consumer, mock_channel):
        """Items without msisdn are skipped; message is still acked."""
        body = json.dumps({
            "timeStamp": 0,
            "dataSet": [{"message": "no msisdn here", "channel": "sms"}],
        }).encode()
        method     = _make_delivery()
        properties = _make_properties()

        consumer.process_message(mock_channel, method, properties, body)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_retry_routing_on_failure(self, consumer, mock_channel):
        """
        When send_sms returns a failure the consumer republishes to the
        retry exchange (not nack).  Uses a mocked client so no real call.
        """
        failing_result = MagicMock(
            success=False,
            error="simulated failure",
            error_code=None,
            retryable=True,
        )
        consumer.client = MagicMock()
        consumer.client.send_sms.return_value        = failing_result
        consumer.client.send_flash_sms.return_value  = failing_result

        body       = _make_message(MSISDN, "will fail")
        method     = _make_delivery()
        properties = _make_properties(retry_count=0)

        consumer.process_message(mock_channel, method, properties, body)

        # Should ack (not nack) and publish to retry exchange
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_channel.basic_publish.assert_called_once()
        call_kwargs = mock_channel.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == "mq_sms_airtel_retry_exchange"

    def test_dlq_after_max_retries(self, consumer, mock_channel):
        """After MAX_RETRY_ATTEMPTS exhausted, message goes to DLQ."""
        from config import Config

        failing_result = MagicMock(
            success=False,
            error="persistent failure",
            error_code=None,
            retryable=True,
        )
        consumer.client = MagicMock()
        consumer.client.send_sms.return_value = failing_result

        body   = _make_message(MSISDN, "max retries exceeded")
        method = _make_delivery()
        # Simulate message already at the retry ceiling
        properties = _make_properties(retry_count=Config.MAX_RETRY_ATTEMPTS - 1)

        consumer.process_message(mock_channel, method, properties, body)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        # publish called once for DLQ (exchange="")
        mock_channel.basic_publish.assert_called_once()
        call_kwargs = mock_channel.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == ""
        assert call_kwargs["routing_key"] == "mq_sms_airtel_dlq"
