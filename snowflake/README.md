# Snowflake - independent reconciliation and analytics

Snowflake loads the S3 raw event log, the dead letters and the replay snapshots, recomputes the
expected final state of every order **in SQL, independently of the Java service**, and compares it
with the projection rebuilt by the latest replay. It also provides lag, duplicate and dead-letter analytics.

| Script | Run as | Purpose |
|---|---|---|
| `01_setup.sql` | ACCOUNTADMIN | Warehouse `EDR_WH`, database `EDR_DB`, schema `ANALYTICS`, role `EDR_APP_ROLE`, service user `EDR_APP` (key-pair login) |
| `02_s3_integration.sql` | ACCOUNTADMIN | Storage integration `EDR_S3_INT` (Snowflake reads S3 through an IAM role) |
| `03_stage_and_tables.sql` | EDR_APP_ROLE | File formats, stage `EDR_STAGE`, tables `RAW_EVENTS`, `DEAD_LETTERS`, `SNAPSHOTS` |
| `04_load.sql` | EDR_APP_ROLE | `COPY INTO` - loads only new files (also run by `POST /reconciliation/refresh`) |
| `05_views.sql` | EDR_APP_ROLE | Validation, expected state (recursive SQL), reconciliation, analytics views |
| `06_checks.sql` | EDR_APP_ROLE | Queries to inspect the results |

Main views: `RECONCILIATION_SUMMARY`, `RECONCILIATION`, `EXPECTED_STATE`, `EXPECTED_CHARGES`,
`DAILY_METRICS`, `EARLY_ARRIVALS`, `DEAD_LETTER_TRENDS`.

From the project: `python simulate.py reconcile` (or `POST /reconciliation/refresh`).
