Step 2.1: The event format
Every event follows this JSON structure:

{
  "event_id":   "E102",
  "entity_id":  "ORD-1",
  "version":    2,
  "event_type": "PaymentCaptured",
  "event_time": "2026-11-10T10:00:05Z",
  "payload": {
    "payment_id": "PAY-1",
    "amount": 500.00,
    "currency": "INR"
  }
}

Step 2.2: Event types and the state machine
event_type	Required payload	Allowed version	Resulting status
OrderCreated	customer_id (string), amount (number > 0), currency (3 letters)	must be 1	CREATED
PaymentCaptured	payment_id (string), amount (number > 0), currency	≥ 2	PAID
OrderShipped	tracking_id (string)	≥ 2	SHIPPED
OrderCancelled	reason (string)	≥ 2	CANCELLED
Allowed status transitions:

(none) ──OrderCreated──▶ CREATED ──PaymentCaptured──▶ PAID ──OrderShipped──▶ SHIPPED
                            │                          │
                            └──OrderCancelled──▶ CANCELLED ◀──OrderCancelled──┘


Step 2.3: Validation rules and error codes

Validation happens in two phases.

Phase A: structural validation. This runs as soon as the consumer picks up the event.

Error code	When
MALFORMED_JSON	The message isn’t valid JSON
MISSING_FIELD	Any required top-level field is missing
INVALID_FIELD_TYPE	A wrong type, e.g. version: "two" or amount: "abc"
UNKNOWN_EVENT_TYPE	event_type isn’t one of the four types
INVALID_VERSION	version < 1, or OrderCreated with a version other than 1
INVALID_PAYLOAD	A required payload field is missing or invalid for that type

Phase B: business validation. This runs at apply time, when the event is the next version for its entity.

Error code	When
INVALID_TRANSITION	The status change isn’t allowed by the state machine
AMOUNT_MISMATCH	The PaymentCaptured amount or currency differs from the order
EVENT_ID_CONFLICT	The same event_id arrives again with different content

Every failure goes to dead_letter with its code and message. Processing of other events continues.

A limitation to document: if a Phase B failure rejects version N of an order, later versions of that order stay parked, because version N is missing. Other orders aren’t affected. An operator has to fix and reprocess the rejected event.

Step 2.4: Processing decision rules

This is the exact logic the Consumer Pipeline will implement in Stage 4:

1. Parse JSON                          → fail: DLQ (MALFORMED_JSON)
2. Structural validation               → fail: DLQ (code)
3. Idempotency check on event_id:
     already in processed_events:
        same payload hash              → DUPLICATE (skip, count)
        different payload hash         → DLQ (EVENT_ID_CONFLICT)
     already in park_buffer            → DUPLICATE (skip, count)
4. Ordering check  (current = order's last_version, or 0 if new):
     version <= current                → STALE (ignore, count)
     version >  current + 1            → PARK (wait for missing versions)
     version == current + 1            → continue
5. Business validation                 → fail: DLQ (code)
6. ATOMIC APPLY (one transaction):
     - update order_projection
     - insert payment_ledger row (PaymentCaptured only; skip if event_id exists)
     - insert processed_events row
     - mark inbox row APPLIED
7. Drain the park buffer: if version current+1 is parked, apply it (repeat step 5–7)

Inbox status values: PENDING, APPLIED, DUPLICATE, STALE, PARKED, DEAD_LETTERED.