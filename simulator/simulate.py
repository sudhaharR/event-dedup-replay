"""CAP-33 event simulator - command line.

  python simulate.py list
  python simulate.py run                                   # all scenarios
  python simulate.py run --scenario duplicate_payment
  python simulate.py run --scenario shuffled_full --orders 50 --seed 42
  python simulate.py send samples/sample_events.json --shuffle --seed 7 --repeat E102 --check

Exit code 0 = every check passed, 1 = something failed (used by CI in Stage 12).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

import requests

from edr_sim.client import ApiClient
from edr_sim.scenarios import SCENARIOS, Options, Result, Run, run_scenario

DEFAULT_URL = os.environ.get("EDR_API_URL", "http://localhost:8080")


def print_result(result: Result, index: int, total: int) -> None:
    print(f"\n[{index}/{total}] {result.name} - {result.description}")
    print(f"      sent {result.sent} message(s) in {result.seconds:.1f}s")
    for c in result.checks:
        print(f"      [{'PASS' if c.passed else 'FAIL'}] {c.name}" + (f"  -> {c.detail}" if c.detail else ""))
    if result.error:
        print(f"      [ERROR] {result.error}")
    if result.metrics:
        m = result.metrics
        print(f"      metrics: received={m['total_received']} applied={m['applied']} duplicates={m['duplicates']} "
              f"stale={m['stale']} out_of_order={m['out_of_order']} parked={m['currently_parked']} "
              f"dead_lettered={m['dead_lettered']} charges={m['total_charges']} "
              f"max_end_to_end_lag_ms={m['max_end_to_end_lag_ms']}")
    print(f"      RESULT: {'PASS' if result.passed else 'FAIL'}")


def connect(url: str) -> ApiClient:
    client = ApiClient(url)
    try:
        status = client.health()
    except requests.RequestException:
        sys.exit(f"Cannot reach the consumer service at {url}.\n"
                 f"Start it first:  cd consumer-service  then  .\\mvnw.cmd spring-boot:run")
    if status != "UP":
        sys.exit(f"Service at {url} is not healthy (status {status}).")
    return client


def cmd_list(_args) -> int:
    for name, (description, _) in SCENARIOS.items():
        print(f"  {name:22} {description}")
    return 0


def cmd_run(args) -> int:
    client = connect(args.base_url)
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    opts = Options(orders=args.orders, rate_ms=args.rate_ms, late_ms=args.late_ms,
                   reset=not args.no_reset, timeout=args.timeout)

    print(f"=== CAP-33 event simulator ===  api={args.base_url}  seed={seed}  "
          f"(re-run with --seed {seed} for the same stream)")
    results = []
    for i, name in enumerate(names, start=1):
        result = run_scenario(name, client, seed, opts)
        print_result(result, i, len(names))
        results.append(result)

    passed = sum(r.passed for r in results)
    print(f"\n=== SUMMARY: {passed}/{len(results)} scenario(s) passed ===")
    return 0 if passed == len(results) else 1


def cmd_send(args) -> int:
    """Sends a file of events one by one - the evaluator can reorder or repeat messages."""
    client = connect(args.base_url)
    messages = json.loads(Path(args.file).read_text(encoding="utf-8"))
    if not isinstance(messages, list):
        sys.exit("The file must contain a JSON array of events.")

    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    rng = random.Random(seed)
    if args.shuffle:
        rng.shuffle(messages)
    for event_id in args.repeat or []:
        copies = [m for m in messages if isinstance(m, dict) and m.get("event_id") == event_id]
        if not copies:
            sys.exit(f"--repeat {event_id}: no such event_id in the file")
        first = messages.index(copies[0])
        messages.insert(rng.randint(first + 1, len(messages)), copies[0])

    if not args.no_reset:
        client.reset()

    result = Result("send", f"{args.file} ({'shuffled, ' if args.shuffle else ''}seed {seed})")
    run = Run(client, seed, Options(rate_ms=args.rate_ms, timeout=args.timeout), result)
    print(f"Arrival order ({len(messages)} messages, seed {seed}):")
    for i, m in enumerate(messages, start=1):
        label = (f"{m.get('event_id')} {m.get('entity_id')} v{m.get('version')} {m.get('event_type')}"
                 if isinstance(m, dict) else f"(raw) {m[:40]}")
        print(f"  {i:3}. {label}")

    start = time.monotonic()
    run.send(messages)
    run.wait()
    if args.check:
        order_ids = {m["entity_id"] for m in messages if isinstance(m, dict) and isinstance(m.get("entity_id"), str)}
        run.check_against_oracle(order_ids)
    result.seconds = time.monotonic() - start

    print("\nFinal state:")
    for o in client.orders():
        print(f"  {o['order_id']:16} {o['status']:10} paid={o['amount_paid']:>10}  v{o['last_version']}")
    print_result(result, 1, 1)
    return 0 if result.passed else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="CAP-33 streaming-style event simulator")
    parser.add_argument("--base-url", default=DEFAULT_URL, help=f"consumer service URL (default {DEFAULT_URL})")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list the scenarios").set_defaults(func=cmd_list)

    run = sub.add_parser("run", help="run scenarios and check the final state")
    run.add_argument("--scenario", default="all", choices=["all", *SCENARIOS])
    run.add_argument("--seed", type=int, help="random seed (same seed = same stream)")
    run.add_argument("--orders", type=int, default=10, help="orders in shuffled_full (default 10)")
    run.add_argument("--rate-ms", type=int, default=50, help="pause between messages (default 50)")
    run.add_argument("--late-ms", type=int, default=1500, help="delay for held-back events (default 1500)")
    run.add_argument("--timeout", type=float, default=60, help="seconds to wait for the consumer")
    run.add_argument("--no-reset", action="store_true", help="keep existing data (ids never collide)")
    run.set_defaults(func=cmd_run)

    send = sub.add_parser("send", help="send a JSON file of events one by one")
    send.add_argument("file")
    send.add_argument("--shuffle", action="store_true", help="random arrival order")
    send.add_argument("--seed", type=int)
    send.add_argument("--repeat", action="append", metavar="EVENT_ID", help="send this event again (repeatable)")
    send.add_argument("--check", action="store_true", help="compare the final state with the oracle")
    send.add_argument("--rate-ms", type=int, default=50)
    send.add_argument("--timeout", type=float, default=60)
    send.add_argument("--no-reset", action="store_true")
    send.set_defaults(func=cmd_send)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
