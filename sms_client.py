"""
Airtel CPaaS SMS Client
-----------------------
Handles Basic Auth, request execution, and exponential-backoff retry.

Auth flow:
  Credentials (username + password) come from Kong credential generation.
  They are base64-encoded as  username:password  and sent as
  Authorization: Basic <token> on every request.

Retry policy:
  Retries on transient HTTP errors (429, 5xx) and connection/timeout
  exceptions.  Uses exponential backoff with full jitter to avoid
  thundering-herd problems.
"""

from __future__ import annotations

import base64
import logging
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# HTTP status codes that warrant a retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class AirtelConfig:
    username: str
    password: str
    customer_id: str
    sender_id: str
    base_url: str = "https://www.airtelkenya.com/gateway/v1"
    max_retries: int = 3
    retry_delay: float = 1.0          # base delay in seconds
    retry_backoff_factor: float = 2.0
    request_timeout: float = 30.0

    @classmethod
    def from_env(cls) -> "AirtelConfig":
        required = {
            "username": "AIRTEL_USERNAME",
            "password": "AIRTEL_PASSWORD",
            "customer_id": "AIRTEL_CUSTOMER_ID",
            "sender_id": "AIRTEL_SENDER_ID",
        }
        values: dict[str, Any] = {}
        missing = []
        for attr, env_key in required.items():
            val = os.getenv(env_key)
            if not val:
                missing.append(env_key)
            else:
                values[attr] = val

        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        return cls(
            **values,
            base_url=os.getenv("AIRTEL_BASE_URL", "https://www.airtelkenya.com/gateway/v1"),
            max_retries=int(os.getenv("AIRTEL_MAX_RETRIES", "3")),
            retry_delay=float(os.getenv("AIRTEL_RETRY_DELAY_SECONDS", "1.0")),
            retry_backoff_factor=float(os.getenv("AIRTEL_RETRY_BACKOFF_FACTOR", "2.0")),
            request_timeout=float(os.getenv("AIRTEL_REQUEST_TIMEOUT_SECONDS", "30")),
        )


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

@dataclass
class SMSResult:
    success: bool
    message_request_id: str | None = None
    raw_response: dict = field(default_factory=dict)
    error: str | None = None

    def __str__(self) -> str:
        if self.success:
            return f"SMS sent — messageRequestId={self.message_request_id}"
        return f"SMS failed — {self.error}"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class AirtelSMSClient:
    """Thin HTTP client for Airtel CPaaS SMS endpoints."""

    def __init__(self, config: AirtelConfig | None = None) -> None:
        self.config = config or AirtelConfig.from_env()
        self._auth_header = self._build_auth_header()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": self._auth_header,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_sms(
        self,
        destination: str | list[str],
        message: str,
        *,
        sender_id: str | None = None,
        otp: bool = False,
        metadata: dict | None = None,
    ) -> SMSResult:
        """Send a single (or multi-destination) default SMS."""
        destinations = [destination] if isinstance(destination, str) else destination
        payload: dict[str, Any] = {
            "customerId": self.config.customer_id,
            "senderId": sender_id or self.config.sender_id,
            "destinationAddress": destinations,
            "message": message,
            "otp": otp,
            "metaData": {
                "subAccountId": self.config.username.replace("_", "-"),
                **(metadata or {}),
            },
        }

        response = self._request_with_retry("POST", "sendDefaultSms", payload)
        return self._parse_single_result(response)

    def send_flash_sms(
        self,
        destination: str | list[str],
        message: str,
        *,
        sender_id: str | None = None,
    ) -> SMSResult:
        """Send a flash SMS (displayed immediately, not stored on device)."""
        destinations = [destination] if isinstance(destination, str) else destination
        payload = {
            "customerId": self.config.customer_id,
            "senderId": sender_id or self.config.sender_id,
            "destinationAddress": destinations,
            "message": message,
        }
        response = self._request_with_retry("POST", "sendFlashSms", payload)
        return self._parse_single_result(response)

    def send_bulk_sms(self, messages: list[dict]) -> list[SMSResult]:
        """
        Send bulk SMS.

        Each item in `messages` should follow the bulk schema:
            {
                "customerId": "...",
                "destinationAddress": "+254...",
                "message": "...",
                "senderId": "...",
                "messageType": "SMS" | "FLASH"
            }
        customerId and senderId default to the configured values if omitted.
        """
        enriched = []
        for msg in messages:
            enriched.append(
                {
                    "customerId": self.config.customer_id,
                    "senderId": self.config.sender_id,
                    "messageType": "SMS",
                    **msg,  # caller values override defaults
                }
            )

        response = self._request_with_retry("POST", "sendBulkSms", enriched)
        return self._parse_bulk_result(response)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_auth_header(self) -> str:
        token = base64.b64encode(
            f"{self.config.username}:{self.config.password}".encode()
        ).decode()
        return f"Basic {token}"

    def _request_with_retry(
        self, method: str, endpoint: str, payload: Any
    ) -> requests.Response:
        """Execute an HTTP request with exponential-backoff retry."""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint}"
        attempt = 0
        last_exc: Exception | None = None

        while attempt <= self.config.max_retries:
            try:
                logger.debug(
                    "Attempt %d/%d — %s %s",
                    attempt + 1,
                    self.config.max_retries + 1,
                    method,
                    url,
                )
                response = self._session.request(
                    method=method,
                    url=url,
                    json=payload,
                    timeout=self.config.request_timeout,
                )

                if response.status_code not in _RETRYABLE_STATUSES:
                    return response  # success or non-retryable error

                logger.warning(
                    "Retryable status %d on attempt %d",
                    response.status_code,
                    attempt + 1,
                )

            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                logger.warning("Network error on attempt %d: %s", attempt + 1, exc)

            if attempt < self.config.max_retries:
                delay = self._jittered_delay(attempt)
                logger.info("Retrying in %.2fs…", delay)
                time.sleep(delay)

            attempt += 1

        if last_exc:
            raise last_exc
        # If we exhausted retries but have a response, return it anyway
        return response  # type: ignore[return-value]

    def _jittered_delay(self, attempt: int) -> float:
        """Full-jitter exponential backoff — avoids thundering herd."""
        cap = self.config.retry_delay * (self.config.retry_backoff_factor ** attempt)
        return random.uniform(0, cap)

    @staticmethod
    def _parse_single_result(response: requests.Response) -> SMSResult:
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text}

        if response.ok and "messageRequestId" in body:
            return SMSResult(
                success=True,
                message_request_id=body["messageRequestId"],
                raw_response=body,
            )

        error_msg = (
            body.get("errorMessage")
            or body.get("displayMessage")
            or f"HTTP {response.status_code}"
        )
        return SMSResult(success=False, raw_response=body, error=error_msg)

    @staticmethod
    def _parse_bulk_result(response: requests.Response) -> list[SMSResult]:
        try:
            body = response.json()
        except ValueError:
            body = []

        if not response.ok:
            error_msg = (
                body.get("errorMessage") if isinstance(body, dict) else None
            ) or f"HTTP {response.status_code}"
            return [SMSResult(success=False, raw_response=body if isinstance(body, dict) else {}, error=error_msg)]

        results = []
        items = body if isinstance(body, list) else [body]
        for item in items:
            results.append(
                SMSResult(
                    success=True,
                    message_request_id=item.get("messageRequestId"),
                    raw_response=item,
                )
            )
        return results
