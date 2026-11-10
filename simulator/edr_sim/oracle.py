"""Reference model ("oracle") - computes the CORRECT final state for a stream of messages,
independently of the Java service. The simulator compares the service's result with this.

It follows the same contract and rules as the consumer pipeline:
  * invalid messages are ignored (they are dead-lettered by the service)
  * the first arrival of an event_id wins; the first arrival of an (order, version) wins
  * events are applied per order in version order, only while there is no gap
  * business rules (state machine, amount match) stop an order at the first violation
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from .events import Message

REQUIRED = ("event_id", "entity_id", "version", "event_type", "event_time", "payload")
TYPES = {"OrderCreated", "PaymentCaptured", "OrderShipped", "OrderCancelled"}
CURRENCY = re.compile(r"^[A-Z]{3}$")


def money(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _text(v, max_len=64) -> bool:
    return isinstance(v, str) and v.strip() != "" and len(v.strip()) <= max_len


def _amount(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0


def parse_valid(message: Message) -> dict | None:
    """Returns the event if it passes structural validation, else None."""
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except ValueError:
            return None
    if not isinstance(message, dict) or any(message.get(f) is None for f in REQUIRED):
        return None
    e = message
    if not (_text(e["event_id"]) and _text(e["entity_id"]) and _text(e["event_type"], 40)):
        return None
    if not isinstance(e["version"], int) or isinstance(e["version"], bool):
        return None
    if not isinstance(e["event_time"], str):
        return None
    try:
        if datetime.fromisoformat(e["event_time"]).tzinfo is None:   # the service requires an offset / Z
            return None
    except ValueError:
        return None
    p = e["payload"]
    if not isinstance(p, dict) or e["event_type"] not in TYPES:
        return None
    t, v = e["event_type"], e["version"]
    if v < 1 or (t == "OrderCreated" and v != 1) or (t != "OrderCreated" and v < 2):
        return None
    if t in ("OrderCreated", "PaymentCaptured"):
        key = "customer_id" if t == "OrderCreated" else "payment_id"
        if not (_text(p.get(key)) and _amount(p.get("amount"))
                and isinstance(p.get("currency"), str) and CURRENCY.match(p["currency"])):
            return None
    if t == "OrderShipped" and not _text(p.get("tracking_id")):
        return None
    if t == "OrderCancelled" and not _text(p.get("reason"), 255):
        return None
    return e


@dataclass
class Expected:
    orders: dict[str, dict] = field(default_factory=dict)   # order_id -> status, amount_paid, last_version
    charges: dict[str, tuple[str, Decimal]] = field(default_factory=dict)  # event_id -> (order_id, amount)


def expected_state(arrivals: list[Message]) -> Expected:
    """Final state the service must reach after receiving `arrivals` in this order."""
    seen_ids: dict[str, str] = {}                  # event_id -> canonical content
    by_order: dict[str, dict[int, dict]] = {}      # order -> version -> event (first arrival wins)
    for message in arrivals:
        e = parse_valid(message)
        if e is None:
            continue
        content = json.dumps(e, sort_keys=True)
        if e["event_id"] in seen_ids:
            continue                               # duplicate (or conflicting re-use) - ignored
        seen_ids[e["event_id"]] = content
        by_order.setdefault(e["entity_id"], {}).setdefault(e["version"], e)

    result = Expected()
    used_payments: set[str] = set()
    for order_id, versions in by_order.items():
        state = None
        v = 1
        while v in versions:
            e, p = versions[v], versions[v]["payload"]
            t = e["event_type"]
            if t == "OrderCreated":
                state = {"status": "CREATED", "order_amount": money(p["amount"]), "currency": p["currency"],
                         "amount_paid": money(0), "last_version": 1}
            elif t == "PaymentCaptured":
                if (state is None or state["status"] != "CREATED" or money(p["amount"]) != state["order_amount"]
                        or p["currency"] != state["currency"] or p["payment_id"] in used_payments):
                    break
                used_payments.add(p["payment_id"])
                state["status"], state["amount_paid"] = "PAID", money(p["amount"])
                result.charges[e["event_id"]] = (order_id, money(p["amount"]))
            elif t == "OrderShipped":
                if state is None or state["status"] != "PAID":
                    break
                state["status"] = "SHIPPED"
            elif t == "OrderCancelled":
                if state is None or state["status"] not in ("CREATED", "PAID"):
                    break
                state["status"] = "CANCELLED"
            state["last_version"] = v
            v += 1
        if state is not None:
            result.orders[order_id] = {k: state[k] for k in ("status", "amount_paid", "last_version")}
    return result
