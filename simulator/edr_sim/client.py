"""HTTP client for the Spring Boot consumer service."""
from __future__ import annotations

import json
import time

import requests

from .events import Message


class ApiClient:

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    # ------------------------------------------------------------ sending

    def send(self, message: Message) -> dict:
        """POST one message. A dict is sent as JSON; a str is sent as-is (may be broken JSON)."""
        body = message if isinstance(message, str) else json.dumps(message)
        r = self.session.post(f"{self.base_url}/events", data=body.encode("utf-8"),
                              headers={"Content-Type": "application/json"}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------ reading

    def _get(self, path: str, **params):
        r = self.session.get(f"{self.base_url}{path}", params=params or None, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def health(self) -> str:
        return self._get("/actuator/health")["status"]

    def metrics(self) -> dict:
        return self._get("/metrics")

    def orders(self) -> list[dict]:
        return self._get("/orders")

    def order(self, order_id: str) -> dict | None:
        r = self.session.get(f"{self.base_url}/orders/{order_id}", timeout=self.timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def ledger(self, order_id: str | None = None) -> list[dict]:
        return self._get("/ledger", **({"order_id": order_id} if order_id else {}))

    def dead_letters(self, limit: int = 1000) -> list[dict]:
        return self._get("/dead-letters", limit=limit)

    def inbox(self, event_id: str | None = None, limit: int = 1000) -> list[dict]:
        params = {"limit": limit}
        if event_id:
            params["event_id"] = event_id
        return self._get("/inbox", **params)

    # ------------------------------------------------------------ admin (dev only)

    def reset(self) -> None:
        self.session.post(f"{self.base_url}/admin/reset", timeout=self.timeout).raise_for_status()

    def arm_crash(self, event_ids: list[str]) -> None:
        self.session.post(f"{self.base_url}/admin/faults/fail-once", json={"event_ids": event_ids},
                          timeout=self.timeout).raise_for_status()

    # ------------------------------------------------------------ waiting

    def wait_until_idle(self, timeout: float = 60.0, settle: float = 1.2) -> dict:
        """Waits until no message is PENDING and the counters stop changing for `settle` seconds
        (the consumer polls every 500 ms and releases parked events right after)."""
        deadline = time.monotonic() + timeout
        last, stable_since = None, None
        while time.monotonic() < deadline:
            m = self.metrics()
            snapshot = (m["pending"], m["applied"], m["currently_parked"], m["dead_lettered"])
            if m["pending"] == 0 and snapshot == last:
                if time.monotonic() - stable_since >= settle:
                    return m
            else:
                last, stable_since = snapshot, time.monotonic()
            time.sleep(0.25)
        raise TimeoutError(f"Service still busy after {timeout}s: {self.metrics()}")
