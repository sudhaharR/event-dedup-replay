"""Builds valid events that follow the event contract (docs/event-contract.md).

Every order gets a lifecycle - a list of events with versions 1, 2, 3 ... -
in the order they really happened. Faults (duplicates, reordering, poison)
are applied afterwards by faults.py, so the "truth" is always known.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

# How an order's life ends. Each path is a list of event types after OrderCreated.
PATHS = {
    "shipped":         ["PaymentCaptured", "OrderShipped"],
    "paid":            ["PaymentCaptured"],
    "created":         [],
    "cancel_unpaid":   ["OrderCancelled"],
    "cancel_paid":     ["PaymentCaptured", "OrderCancelled"],
}

Message = dict | str   # a dict is a JSON event; a str is sent as raw text (e.g. broken JSON)


def iso(ts: datetime) -> str:
    """UTC timestamp in the contract format, e.g. 2026-11-10T10:00:05.000Z"""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


class EventFactory:
    """Creates events with ids that are unique per run (run_id), so runs never collide."""

    def __init__(self, run_id: str, start: datetime | None = None):
        self.run_id = run_id
        # events "happened" ~10 s before we send them: a realistic producer-to-consumer delay
        self.start = start or datetime.now(timezone.utc) - timedelta(seconds=10)

    def order_id(self, n: int) -> str:
        return f"ORD-{self.run_id}-{n}"

    @staticmethod
    def event_id(order_id: str, version: int) -> str:
        return f"E-{order_id}-v{version}"

    def event(self, order_id: str, version: int, event_type: str, payload: dict,
              event_id: str | None = None, offset_s: float = 0.0) -> dict:
        return {
            "event_id": event_id or self.event_id(order_id, version),
            "entity_id": order_id,
            "version": version,
            "event_type": event_type,
            "event_time": iso(self.start + timedelta(seconds=offset_s)),
            "payload": payload,
        }

    def lifecycle(self, n: int, amount: float, path: str = "shipped", currency: str = "INR") -> list[dict]:
        """All events of one order, in true order (versions 1..k), a few milliseconds apart."""
        order_id = self.order_id(n)
        base = (n % 1000) * 0.005                       # keeps every event_time inside the 10 s window
        events = [self.event(order_id, 1, "OrderCreated",
                             {"customer_id": f"CUST-{n}", "amount": amount, "currency": currency},
                             offset_s=base)]
        for version, event_type in enumerate(PATHS[path], start=2):
            events.append(self.event(order_id, version, event_type,
                                     self.payload_for(event_type, order_id, amount, currency),
                                     offset_s=base + version * 0.001))
        return events

    def now(self) -> str:
        return iso(datetime.now(timezone.utc))

    @staticmethod
    def payload_for(event_type: str, order_id: str, amount: float, currency: str) -> dict:
        if event_type == "PaymentCaptured":
            return {"payment_id": "PAY-" + order_id, "amount": amount, "currency": currency}
        if event_type == "OrderShipped":
            return {"tracking_id": "TRK-" + order_id}
        if event_type == "OrderCancelled":
            return {"reason": "customer request"}
        raise ValueError(event_type)

    def random_orders(self, count: int, rng: random.Random) -> list[dict]:
        """Lifecycles for `count` orders with a mix of paths and amounts, in true order."""
        weights = {"shipped": 5, "paid": 2, "created": 1, "cancel_unpaid": 1, "cancel_paid": 1}
        events: list[dict] = []
        for n in range(1, count + 1):
            path = rng.choices(list(weights), weights=list(weights.values()))[0]
            amount = round(rng.uniform(100, 5000), 2)
            events.extend(self.lifecycle(n, amount, path))
        return events
