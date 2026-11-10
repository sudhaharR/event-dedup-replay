-- Stage 1 placeholder: verifies init scripts run.
CREATE TABLE IF NOT EXISTS setup_check (
  id INT PRIMARY KEY,
  note VARCHAR(100)
);
INSERT INTO setup_check VALUES (1, 'Stage 1 setup OK');