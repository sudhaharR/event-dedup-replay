-- =====================================================================
-- CAP-33 Event Dedup & Replay Simulator - MySQL schema
-- All times are stored in UTC.
-- =====================================================================
SET NAMES utf8mb4;

-- ---------------------------------------------------------------------
-- 1. INBOX: queue of every received message (acts as the "stream")
--    Fields other than raw_payload are extracted best-effort at ingest
--    and may be NULL for malformed messages.
-- ---------------------------------------------------------------------
CREATE TABLE inbox (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,   -- arrival order
  event_id         VARCHAR(64)  NULL,
  entity_id        VARCHAR(64)  NULL,
  version          INT          NULL,
  event_type       VARCHAR(40)  NULL,
  event_time       DATETIME(3)  NULL,
  raw_payload      MEDIUMTEXT   NOT NULL,                -- exactly as received
  source           ENUM('LIVE','REPLAY') NOT NULL DEFAULT 'LIVE',
  status           ENUM('PENDING','APPLIED','DUPLICATE','STALE','PARKED','DEAD_LETTERED')
                   NOT NULL DEFAULT 'PENDING',
  was_out_of_order BOOLEAN      NOT NULL DEFAULT FALSE,  -- was parked at least once
  attempts         INT          NOT NULL DEFAULT 0,
  last_error       VARCHAR(500) NULL,
  s3_key           VARCHAR(255) NULL,                    -- raw copy location (Stage 6)
  received_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  processed_at     DATETIME(3)  NULL,
  INDEX idx_inbox_status (status, id),
  INDEX idx_inbox_event  (event_id),
  INDEX idx_inbox_entity (entity_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 2. PROCESSED_EVENTS: idempotency store
--    PRIMARY KEY on event_id = database-level duplicate guarantee.
-- ---------------------------------------------------------------------
CREATE TABLE processed_events (
  event_id      VARCHAR(64) PRIMARY KEY,
  entity_id     VARCHAR(64) NOT NULL,
  version       INT         NOT NULL,
  event_type    VARCHAR(40) NOT NULL,
  payload_hash  CHAR(64)    NOT NULL,                    -- SHA-256 of payload
  event_time    DATETIME(3) NOT NULL,
  processed_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_processed_entity_version (entity_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 3. ORDER_PROJECTION: current state per order (built from events)
-- ---------------------------------------------------------------------
CREATE TABLE order_projection (
  order_id         VARCHAR(64)   PRIMARY KEY,
  status           ENUM('CREATED','PAID','SHIPPED','CANCELLED') NOT NULL,
  customer_id      VARCHAR(64)   NOT NULL,
  order_amount     DECIMAL(12,2) NOT NULL,
  amount_paid      DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  currency         CHAR(3)       NOT NULL,
  tracking_id      VARCHAR(64)   NULL,
  cancel_reason    VARCHAR(255)  NULL,
  last_version     INT           NOT NULL,
  last_event_id    VARCHAR(64)   NOT NULL,
  last_event_time  DATETIME(3)   NOT NULL,
  created_at       DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at       DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                 ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 4. PARK_BUFFER: events that arrived before their predecessor version
-- ---------------------------------------------------------------------
CREATE TABLE park_buffer (
  id                BIGINT AUTO_INCREMENT PRIMARY KEY,
  inbox_id          BIGINT      NOT NULL,
  event_id          VARCHAR(64) NOT NULL,
  entity_id         VARCHAR(64) NOT NULL,
  version           INT         NOT NULL,
  event_type        VARCHAR(40) NOT NULL,
  raw_payload       MEDIUMTEXT  NOT NULL,
  expected_version  INT         NOT NULL,                -- version we were waiting for
  parked_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_park_event (event_id),
  UNIQUE KEY uk_park_entity_version (entity_id, version),
  UNIQUE KEY uk_park_inbox (inbox_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 5. DEAD_LETTER: rejected / poison events (isolated, never block others)
-- ---------------------------------------------------------------------
CREATE TABLE dead_letter (
  id                BIGINT AUTO_INCREMENT PRIMARY KEY,
  inbox_id          BIGINT       NOT NULL,
  event_id          VARCHAR(64)  NULL,
  entity_id         VARCHAR(64)  NULL,
  raw_payload       MEDIUMTEXT   NOT NULL,
  error_code        VARCHAR(40)  NOT NULL,
  error_message     VARCHAR(500) NOT NULL,
  status            ENUM('NEW','REPROCESSED','IGNORED') NOT NULL DEFAULT 'NEW',
  s3_key            VARCHAR(255) NULL,                   -- archive location (Stage 6)
  received_at       DATETIME(3)  NOT NULL,
  dead_lettered_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_dlq_inbox (inbox_id),
  INDEX idx_dlq_code (error_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 6. PAYMENT_LEDGER: side effects ("charges").
--    UNIQUE event_id   = a charge can never happen twice for one event.
--    UNIQUE payment_id = extra business-level guard.
--    This table is NOT cleared during replay.
-- ---------------------------------------------------------------------
CREATE TABLE payment_ledger (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id    VARCHAR(64)   NOT NULL,
  order_id    VARCHAR(64)   NOT NULL,
  payment_id  VARCHAR(64)   NOT NULL,
  amount      DECIMAL(12,2) NOT NULL,
  currency    CHAR(3)       NOT NULL,
  created_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_ledger_event   (event_id),
  UNIQUE KEY uk_ledger_payment (payment_id),
  INDEX idx_ledger_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------
-- 7. REPLAY_RUNS: audit of each replay (used in Stage 7)
-- ---------------------------------------------------------------------
CREATE TABLE replay_runs (
  id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
  status                ENUM('RUNNING','COMPLETED','FAILED') NOT NULL DEFAULT 'RUNNING',
  events_read           INT          NOT NULL DEFAULT 0,
  events_applied        INT          NOT NULL DEFAULT 0,
  side_effects_skipped  INT          NOT NULL DEFAULT 0,
  snapshot_s3_key       VARCHAR(255) NULL,
  error_message         VARCHAR(500) NULL,
  started_at            DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  finished_at           DATETIME(3)  NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;