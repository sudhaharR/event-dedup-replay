"""Fault injectors - turn a clean, ordered event stream into a realistic messy one.

All functions are pure: they take a list and a random.Random and return a new list,
so the same --seed always produces exactly the same stream.
"""
from __future__ import annotations

import copy
import random

from .events import Message


def duplicate(messages: list[Message], count: int, rng: random.Random) -> list[Message]:
    """Re-sends `count` randomly chosen events (same event_id, same content) at a later position."""
    result = list(messages)
    candidates = [m for m in messages if isinstance(m, dict)]
    for original in rng.sample(candidates, min(count, len(candidates))):
        first = result.index(original)
        result.insert(rng.randint(first + 1, len(result)), copy.deepcopy(original))
    return result


def shuffle(messages: list[Message], rng: random.Random) -> list[Message]:
    """Random arrival order - the evaluator's 'change the arrival order' test."""
    result = list(messages)
    rng.shuffle(result)
    return result


def hold_back(messages: list[Message], count: int, rng: random.Random) -> tuple[list[Message], list[Message]]:
    """Picks `count` events to deliver late. Returns (on_time, late).
    The runner sends the late ones after a pause, so their event_time is far behind arrival."""
    candidates = [i for i, m in enumerate(messages) if isinstance(m, dict)]
    late_idx = set(rng.sample(candidates, min(count, len(candidates))))
    on_time = [m for i, m in enumerate(messages) if i not in late_idx]
    late = [m for i, m in enumerate(messages) if i in late_idx]
    return on_time, late


def reverse_versions(messages: list[dict]) -> list[dict]:
    """Highest version first - the strongest out-of-order case."""
    return sorted(messages, key=lambda m: m["version"], reverse=True)


# ---------------------------------------------------------------- poison messages
# Each returns (message, expected_error_code, event_id or None).
# `ts` is the event_time to use (the scenario passes "now").

def broken_json(tag: str) -> tuple[Message, str, None]:
    return f'{{"event_id": "E-BROKEN-{tag}", "entity_id": ', "MALFORMED_JSON", None


def missing_entity_id(tag: str, ts: str) -> tuple[Message, str, str]:
    eid = f"E-NOENTITY-{tag}"
    return ({"event_id": eid, "version": 2, "event_type": "PaymentCaptured",
             "event_time": ts,
             "payload": {"payment_id": "PAY-X", "amount": 100, "currency": "INR"}},
            "MISSING_FIELD", eid)


def text_amount(order_id: str, tag: str, ts: str) -> tuple[Message, str, str]:
    eid = f"E-BADAMOUNT-{tag}"
    return ({"event_id": eid, "entity_id": order_id, "version": 2, "event_type": "PaymentCaptured",
             "event_time": ts,
             "payload": {"payment_id": "PAY-Y", "amount": "abc", "currency": "INR"}},
            "INVALID_PAYLOAD", eid)


def unknown_type(order_id: str, tag: str, ts: str) -> tuple[Message, str, str]:
    eid = f"E-UNKNOWN-{tag}"
    return ({"event_id": eid, "entity_id": order_id, "version": 2, "event_type": "PaymentRefunded",
             "event_time": ts, "payload": {}},
            "UNKNOWN_EVENT_TYPE", eid)


def poison_set(order_id: str, tag: str, ts: str) -> list[tuple[Message, str, str | None]]:
    """One of each poison kind."""
    return [broken_json(tag), missing_entity_id(tag, ts), text_amount(order_id, tag, ts), unknown_type(order_id, tag, ts)]
