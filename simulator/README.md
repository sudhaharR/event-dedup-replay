# Event Producer Simulator (Python)

Generates messy, streaming-style event traffic (duplicates, out-of-order, delayed, poison)
for the consumer service, then checks the final state through the service's API -
both with explicit checks and against an independent reference model (`edr_sim/oracle.py`).

## Setup (once)

```powershell
cd simulator
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

The consumer service must be running (`cd consumer-service` then `.\mvnw.cmd spring-boot:run`).

## Commands

| Command | What it does |
|---|---|
| `python simulate.py list` | Lists the scenarios |
| `python simulate.py run` | Runs every scenario (resets the database first) |
| `python simulate.py run --scenario duplicate_payment` | One scenario |
| `python simulate.py run --scenario shuffled_full --orders 50 --seed 42` | Bigger evaluator stream, reproducible |
| `python simulate.py send samples/sample_events.json --shuffle --seed 7 --repeat E102 --check` | Evaluator test on a file: new arrival order + repeated message, then verify |
| `pytest` | Unit tests of the generator, faults and oracle (no service needed) |

Options: `--base-url` (default `http://localhost:8080`, or env `EDR_API_URL`), `--no-reset`,
`--rate-ms` (pause between messages), `--late-ms` (delay for held-back events), `--timeout`.

Exit code `0` = all checks passed, `1` = a check failed.

## Scenarios

| Scenario | Demonstrates |
|---|---|
| `duplicate_payment` | Payment delivered 3x -> 1 charge, 2 DUPLICATE |
| `out_of_order_status` | v3, v2, v1 arrive reversed -> parked, then SHIPPED |
| `poison_event` | Broken JSON, missing field, text amount, unknown type -> dead letters; valid order unaffected |
| `partial_side_effect` | Consumer crashes after charging -> rollback, retry, exactly 1 charge |
| `shuffled_full` | Evaluator test: random orders, duplicates, poison, shuffle, late delivery -> matches the oracle |
| `sample` | `samples/sample_events.json` in file order |

`/admin/reset` and `/admin/faults/fail-once` are development-only endpoints (`admin.enabled=true`).
