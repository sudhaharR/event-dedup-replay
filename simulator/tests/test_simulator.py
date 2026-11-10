"""Unit tests for the simulator - no running service needed."""
import json
import random
from decimal import Decimal
from pathlib import Path

from edr_sim import faults
from edr_sim.events import EventFactory
from edr_sim.oracle import expected_state, parse_valid

SAMPLE = json.loads((Path(__file__).resolve().parent.parent / "samples" / "sample_events.json").read_text())


def factory() -> EventFactory:
    return EventFactory(run_id="t1")


def test_lifecycle_has_contiguous_versions_and_valid_events():
    events = factory().lifecycle(1, 500.0, "shipped")
    assert [e["version"] for e in events] == [1, 2, 3]
    assert [e["event_type"] for e in events] == ["OrderCreated", "PaymentCaptured", "OrderShipped"]
    assert all(parse_valid(e) is not None for e in events)
    assert len({e["event_id"] for e in events}) == 3


def test_duplicate_inserts_identical_copies_later():
    events = factory().lifecycle(1, 500.0, "shipped")
    result = faults.duplicate(events, 2, random.Random(1))
    assert len(result) == 5
    for e in events:
        copies = [i for i, m in enumerate(result) if m == e]
        assert copies[0] == min(copies)          # the original comes first


def test_shuffle_is_reproducible_with_the_same_seed():
    events = factory().random_orders(5, random.Random(0))
    assert faults.shuffle(events, random.Random(42)) == faults.shuffle(events, random.Random(42))


def test_hold_back_splits_without_losing_messages():
    events = factory().random_orders(5, random.Random(0))
    on_time, late = faults.hold_back(events, 3, random.Random(1))
    assert len(late) == 3 and len(on_time) + len(late) == len(events)


def test_every_poison_message_is_invalid():
    for message, _code, _eid in faults.poison_set("ORD-X", "t", "2026-11-10T10:00:00.000Z"):
        assert parse_valid(message) is None


def test_oracle_on_sample_file():
    expected = expected_state(SAMPLE)
    assert expected.orders["ORD-1"] == {"status": "SHIPPED", "amount_paid": Decimal("500.00"), "last_version": 3}
    assert expected.orders["ORD-2"] == {"status": "SHIPPED", "amount_paid": Decimal("300.00"), "last_version": 3}
    assert set(expected.charges) == {"E102", "E202"}


def test_oracle_result_does_not_depend_on_arrival_order():
    rng = random.Random(7)
    events = factory().random_orders(30, rng)
    ordered = expected_state(events)
    for seed in range(20):
        messy = faults.shuffle(faults.duplicate(events, 10, random.Random(seed)), random.Random(seed))
        assert expected_state(messy) == ordered


def test_oracle_stops_an_order_at_a_business_rule_violation():
    f = factory()
    created, _payment, shipped = f.lifecycle(1, 500.0, "shipped")
    ship_unpaid = dict(shipped, version=2, event_id="E-ship-early")
    expected = expected_state([created, ship_unpaid])
    assert expected.orders[created["entity_id"]]["status"] == "CREATED"
    assert expected.charges == {}


def test_oracle_gap_means_order_does_not_exist_yet():
    _created, payment, shipped = factory().lifecycle(1, 500.0, "shipped")
    assert expected_state([payment, shipped]).orders == {}


def test_stream_printer_tags_each_kind_of_fault(capsys):
    from edr_sim.display import StreamPrinter
    f = factory()
    created, payment, shipped = f.lifecycle(1, 500.0, "shipped")
    printer = StreamPrinter()
    printer.sent(shipped)                      # v3 before v1, v2
    printer.sent(created)
    printer.sent(payment)
    printer.sent(payment)                      # same event_id again
    printer.sent('{"event_id": "X", ')        # broken JSON
    printer.sent(dict(created, event_id="E-late", entity_id="ORD-L"), late=True)
    lines = [l for l in capsys.readouterr().out.splitlines() if l.strip()[:1].isdigit()]
    assert "OUT OF ORDER (v1 not sent yet)" in lines[0]
    assert lines[1].rstrip().endswith("OrderCreated")      # no tag: next in line
    assert "REPEAT" in lines[3]
    assert "POISON" in lines[4]
    assert "LATE" in lines[5]
