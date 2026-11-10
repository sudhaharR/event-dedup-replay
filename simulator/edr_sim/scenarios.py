"""Test scenarios. Each one builds a messy stream, sends it, waits for the consumer,
then checks the service's final state - explicitly and against the oracle."""
from __future__ import annotations

import json
import random
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import faults
from .client import ApiClient
from .display import StreamPrinter
from .events import EventFactory, Message
from .oracle import Expected, expected_state, money

SAMPLE_FILE = Path(__file__).resolve().parent.parent / "samples" / "sample_events.json"


@dataclass
class Options:
    orders: int = 10
    rate_ms: int = 50
    late_ms: int = 1500
    reset: bool = True
    timeout: float = 60.0
    show_messages: bool = True    # print every message as it is sent
    show_json: bool = False       # ... including its full JSON


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Result:
    name: str
    description: str
    checks: list[Check] = field(default_factory=list)
    sent: int = 0
    seconds: float = 0.0
    metrics: dict | None = None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and all(c.passed for c in self.checks)


class Run:
    """Context for one scenario: sends messages with pacing and records checks."""

    def __init__(self, client: ApiClient, seed: int, opts: Options, result: Result):
        self.client = client
        self.rng = random.Random(seed)
        self.opts = opts
        self.result = result
        # random tag keeps ids unique across runs, even without a reset (the seed only drives the faults)
        self.factory = EventFactory(run_id=secrets.token_hex(3))
        self.arrivals: list[Message] = []
        self.printer = StreamPrinter(opts.show_messages, opts.show_json)

    # -------------------------------------------------------------- sending

    def send(self, messages: list[Message], late: list[Message] = ()) -> None:
        for m in messages:
            self._send_one(m)
        if late:
            time.sleep(self.opts.late_ms / 1000)          # these arrive long after their event_time
            for m in late:
                self._send_one(m, late=True)

    def _send_one(self, m: Message, late: bool = False) -> None:
        self.client.send(m)
        self.printer.sent(m, late)
        self.arrivals.append(m)
        self.result.sent += 1
        if self.opts.rate_ms:
            time.sleep(self.opts.rate_ms / 1000)

    def wait(self) -> dict:
        self.result.metrics = self.client.wait_until_idle(timeout=self.opts.timeout)
        return self.result.metrics

    # -------------------------------------------------------------- checks

    def check(self, name: str, actual, expected) -> None:
        ok = actual == expected
        self.result.checks.append(Check(name, ok, "" if ok else f"expected {expected!r}, got {actual!r}"))

    def check_true(self, name: str, condition: bool, detail: str = "") -> None:
        self.result.checks.append(Check(name, bool(condition), "" if condition else detail))

    def check_order(self, order_id: str, status: str, paid, version: int) -> None:
        o = self.client.order(order_id)
        if o is None:
            self.check_true(f"{order_id} exists", False, "order not found")
            return
        self.check(f"{order_id} status", o["status"], status)
        self.check(f"{order_id} amount_paid", money(o["amount_paid"]), money(paid))
        self.check(f"{order_id} last_version", o["last_version"], version)

    def inbox_statuses(self, event_id: str) -> list[str]:
        return sorted(row["status"] for row in self.client.inbox(event_id=event_id))

    def check_against_oracle(self, order_ids: set[str] | None = None) -> Expected:
        """Compares every order of this run, and the ledger, with the oracle's expected state."""
        expected = expected_state(self.arrivals)
        prefix = f"ORD-{self.factory.run_id}-"
        in_scope = (lambda oid: oid in order_ids) if order_ids else (lambda oid: oid.startswith(prefix))

        actual = {o["order_id"]: o for o in self.client.orders() if in_scope(o["order_id"])}
        wanted = {oid: s for oid, s in expected.orders.items() if in_scope(oid)}
        mismatches = []
        for oid in sorted(set(actual) | set(wanted)):
            a, w = actual.get(oid), wanted.get(oid)
            got = None if a is None else (a["status"], money(a["amount_paid"]), a["last_version"])
            want = None if w is None else (w["status"], w["amount_paid"], w["last_version"])
            if got != want:
                mismatches.append(f"{oid}: expected {want}, got {got}")
        self.check_true(f"all {len(wanted)} orders match the oracle", not mismatches, "; ".join(mismatches[:5]))

        ledger = {row["event_id"]: money(row["amount"]) for row in self.client.ledger() if in_scope(row["order_id"])}
        charges = {eid: amt for eid, (oid, amt) in expected.charges.items() if in_scope(oid)}
        self.check(f"ledger = {len(charges)} charges, one per real payment", ledger, charges)
        return expected

    def dead_letter_codes(self) -> dict[str, str]:
        """event_id (or raw text for broken JSON) -> error_code"""
        return {(d["event_id"] or d["raw_payload"]): d["error_code"] for d in self.client.dead_letters()}


# ====================================================================== scenarios

def sample(r: Run) -> None:
    """The 8 events from samples/sample_events.json, in the file's (messy) order."""
    messages = json.loads(SAMPLE_FILE.read_text(encoding="utf-8"))
    r.send(messages)
    r.wait()
    r.check_order("ORD-1", "SHIPPED", 500, 3)
    r.check_order("ORD-2", "SHIPPED", 300, 3)
    r.check("E102 delivered twice -> APPLIED + DUPLICATE", r.inbox_statuses("E102"), ["APPLIED", "DUPLICATE"])
    r.check("charges for ORD-1 + ORD-2", len(r.client.ledger("ORD-1")) + len(r.client.ledger("ORD-2")), 2)
    r.check("E300 dead-lettered", r.dead_letter_codes().get("E300"), "MISSING_FIELD")
    r.check_against_oracle({"ORD-1", "ORD-2"})


def duplicate_payment(r: Run) -> None:
    """A payment event delivered 3 times must charge the customer exactly once."""
    created, payment = r.factory.lifecycle(1, 2500.00, "paid")
    r.send([created, payment, payment, payment])
    r.wait()
    order_id = created["entity_id"]
    r.check_order(order_id, "PAID", 2500, 2)
    r.check("ledger rows for the order", len(r.client.ledger(order_id)), 1)
    r.check("3 copies -> 1 APPLIED + 2 DUPLICATE", r.inbox_statuses(payment["event_id"]),
            ["APPLIED", "DUPLICATE", "DUPLICATE"])


def out_of_order_status(r: Run) -> None:
    """Shipped (v3) and Paid (v2) arrive before Created (v1) - final status must still be SHIPPED."""
    events = r.factory.lifecycle(1, 1200.00, "shipped")
    r.send(faults.reverse_versions(events))
    r.wait()
    order_id = events[0]["entity_id"]
    r.check_order(order_id, "SHIPPED", 1200, 3)
    for e in events[1:]:
        rows = r.client.inbox(event_id=e["event_id"])
        r.check(f"v{e['version']} was parked then applied",
                [(x["status"], bool(x["was_out_of_order"])) for x in rows], [("APPLIED", True)])
    r.check("ledger rows for the order", len(r.client.ledger(order_id)), 1)


def poison_event(r: Run) -> None:
    """Malformed events are isolated in the dead-letter table and do not block valid events."""
    events = r.factory.lifecycle(1, 750.00, "shipped")
    order_id = events[0]["entity_id"]
    poison = faults.poison_set(order_id, r.factory.run_id, r.factory.now())
    stream = [poison[0][0], events[0], poison[1][0], events[1], poison[2][0], poison[3][0], events[2]]
    r.send(stream)
    r.wait()
    codes = r.dead_letter_codes()
    for message, code, event_id in poison:
        key = event_id if event_id else message
        r.check(f"{event_id or 'broken JSON'} -> {code}", codes.get(key), code)
    r.check_order(order_id, "SHIPPED", 750, 3)


def partial_side_effect(r: Run) -> None:
    """The consumer crashes right after charging; the whole transaction must roll back and retry once."""
    created, payment = r.factory.lifecycle(1, 999.99, "paid")
    try:
        r.client.arm_crash([payment["event_id"]])
    except Exception as e:                                  # admin endpoints switched off
        r.check_true("crash armed via /admin/faults/fail-once", False, f"{e} (is admin.enabled=true?)")
        return
    r.send([created, payment])
    r.wait()
    order_id = created["entity_id"]
    row = r.client.inbox(event_id=payment["event_id"])[0]
    r.check("payment failed once, then applied (attempts)", (row["status"], row["attempts"]), ("APPLIED", 1))
    r.check("ledger rows (no double charge after retry)", len(r.client.ledger(order_id)), 1)
    r.check_order(order_id, "PAID", 999.99, 2)


def shuffled_full(r: Run) -> None:
    """Evaluator test: many orders, duplicates, poison, random order and late delivery."""
    n = r.opts.orders
    events = r.factory.random_orders(n, r.rng)
    stream = faults.duplicate(events, max(2, n // 3), r.rng)
    stream += [m for m, _, _ in faults.poison_set(r.factory.order_id(1), r.factory.run_id, r.factory.now())]
    stream = faults.shuffle(stream, r.rng)
    on_time, late = faults.hold_back(stream, max(1, n // 5), r.rng)
    r.send(on_time, late)
    metrics = r.wait()
    r.check_against_oracle()
    r.check("nothing left parked", metrics["currently_parked"], 0)
    r.check_true("4 poison messages dead-lettered",
                 sum(1 for c in r.dead_letter_codes().values()
                     if c in ("MALFORMED_JSON", "MISSING_FIELD", "INVALID_PAYLOAD", "UNKNOWN_EVENT_TYPE")) >= 4,
                 "fewer than 4 structural dead letters")


SCENARIOS: dict[str, tuple[str, Callable[[Run], None]]] = {
    "duplicate_payment":   ("Payment delivered 3x is charged once", duplicate_payment),
    "out_of_order_status": ("v3, v2, v1 arrive reversed; final state still correct", out_of_order_status),
    "poison_event":        ("4 kinds of poison are dead-lettered; valid order unaffected", poison_event),
    "partial_side_effect": ("Crash after charge rolls back and retries; one charge", partial_side_effect),
    "shuffled_full":       ("Evaluator test: shuffled, duplicated, delayed, poisoned stream", shuffled_full),
    "sample":              ("samples/sample_events.json in file order", sample),
}

# 'sample' uses fixed ids (ORD-1, E102 ...) so it always starts from an empty database
NEEDS_RESET = {"sample"}


def run_scenario(name: str, client: ApiClient, seed: int, opts: Options) -> Result:
    description, fn = SCENARIOS[name]
    result = Result(name, description)
    start = time.monotonic()
    try:
        if opts.reset or name in NEEDS_RESET:
            client.reset()
        fn(Run(client, seed, opts, result))
    except Exception as e:                                   # report, do not crash the whole run
        result.error = f"{type(e).__name__}: {e}"
    result.seconds = time.monotonic() - start
    return result
