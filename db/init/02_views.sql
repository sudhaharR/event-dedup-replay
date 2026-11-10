-- =====================================================================
-- Metrics view: lag, duplicates, out-of-order, DLQ (used by Query API)
-- =====================================================================
CREATE OR REPLACE VIEW v_metrics AS
SELECT
  (SELECT COUNT(*) FROM inbox WHERE source = 'LIVE')            AS total_received,
  (SELECT COUNT(*) FROM inbox WHERE status = 'PENDING')         AS pending,
  (SELECT COUNT(*) FROM inbox WHERE status = 'APPLIED')         AS applied,
  (SELECT COUNT(*) FROM inbox WHERE status = 'DUPLICATE')       AS duplicates,
  (SELECT COUNT(*) FROM inbox WHERE status = 'STALE')           AS stale,
  (SELECT COUNT(*) FROM inbox WHERE was_out_of_order = TRUE)    AS out_of_order,
  (SELECT COUNT(*) FROM park_buffer)                            AS currently_parked,
  (SELECT COUNT(*) FROM dead_letter)                            AS dead_lettered,
  (SELECT COUNT(*) FROM payment_ledger)                         AS total_charges,
  -- event_time -> received_at : how late the event was delivered
  (SELECT ROUND(AVG(TIMESTAMPDIFF(MICROSECOND, event_time, received_at)) / 1000)
     FROM inbox WHERE event_time IS NOT NULL)                   AS avg_delivery_delay_ms,
  -- received_at -> processed_at : how long our consumer took
  (SELECT ROUND(AVG(TIMESTAMPDIFF(MICROSECOND, received_at, processed_at)) / 1000)
     FROM inbox WHERE processed_at IS NOT NULL)                 AS avg_processing_lag_ms,
  -- event_time -> processed_at : total end-to-end lag
  (SELECT ROUND(MAX(TIMESTAMPDIFF(MICROSECOND, event_time, processed_at)) / 1000)
     FROM inbox WHERE processed_at IS NOT NULL
                  AND event_time IS NOT NULL)                   AS max_end_to_end_lag_ms;