"""Prints every message as it is sent, with a tag saying what is special about it.

The tags are the simulator's own view of the stream (what it sent and why):
  OUT OF ORDER  - a later version arrived before the version it depends on
  REPEAT        - this event_id was already sent (a duplicate delivery)
  POISON        - breaks the event contract; the consumer should dead-letter it
  LATE          - held back and sent after a pause (event_time far behind arrival)
"""
from __future__ import annotations

import json
import time

from .events import Message
from .oracle import parse_valid


class StreamPrinter:

    def __init__(self, enabled: bool = True, show_json: bool = False):
        self.enabled = enabled
        self.show_json = show_json
        self.count = 0
        self.started: float | None = None
        self.seen_ids: set[str] = set()
        self.versions: dict[str, set[int]] = {}   # entity -> versions sent so far
        self.contiguous: dict[str, int] = {}      # entity -> highest version with no gap below it

    def header(self) -> None:
        if self.enabled:
            print("      Sending:")
            print(f"      {'#':>4}  {'time':>7}  {'event_id':<24} {'entity':<14} {'ver':<4} {'event_type':<16} note")

    def sent(self, message: Message, late: bool = False) -> None:
        tags = self._tags(message, late)
        if not self.enabled:
            return
        if self.count == 0:
            self.header()
            self.started = time.monotonic()
        self.count += 1
        elapsed = time.monotonic() - self.started

        if isinstance(message, dict):
            eid = str(message.get("event_id"))
            entity = str(message.get("entity_id", "-"))
            version = f"v{message.get('version')}" if "version" in message else "-"
            etype = str(message.get("event_type", "-"))
            line = f"{_cut(eid, 24):<24} {_cut(entity, 14):<14} {version:<4} {_cut(etype, 16):<16}"
        else:
            line = f"(raw text) {_cut(message, 47):<47}"
        stamp = f"+{elapsed:.2f}s"
        print(f"      {self.count:>4}  {stamp:>7}  {line} {', '.join(tags)}", flush=True)

        if self.show_json:
            body = message if isinstance(message, str) else json.dumps(message)
            print(f"            {body}", flush=True)

    def _tags(self, message: Message, late: bool) -> list[str]:
        tags: list[str] = []
        event = parse_valid(message)
        if event is None:
            tags.append("POISON")
        elif event["event_id"] in self.seen_ids:
            tags.append("REPEAT")
        else:
            self.seen_ids.add(event["event_id"])
            entity, version = event["entity_id"], event["version"]
            done = self.contiguous.get(entity, 0)
            if version > done + 1:
                tags.append(f"OUT OF ORDER (v{done + 1} not sent yet)")
            sent = self.versions.setdefault(entity, set())
            sent.add(version)
            while done + 1 in sent:
                done += 1
            self.contiguous[entity] = done
        if late:
            tags.append("LATE")
        return tags


def _cut(text: str, width: int) -> str:
    return text if len(text) <= width else text[: width - 3] + "..."
