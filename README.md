# Streaming-Style Event Deduplication & Replay Simulator (CAP-33)

Testable event-processing prototype with idempotency, ordering, dead-letter handling, replay and reconciliation.

## Tech stack
Java/Spring Boot · Python · MySQL · AWS S3 · Snowflake · React · Docker · GitHub Actions

## Project structure
- `consumer-service/` – Spring Boot ingest, consumer pipeline, replay, query API
- `simulator/` – Python event producer and scenario tests
- `dashboard/` – React monitoring dashboard
- `snowflake/` – Snowflake setup, analytics and reconciliation SQL
- `db/init/` – MySQL schema
- `docs/` – Documentation and diagrams

## Quick start
1. Copy `.env.example` to `.env` and set your values
2. `docker compose up -d`