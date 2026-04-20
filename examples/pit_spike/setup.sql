-- Toy point-in-time spike data.
--
-- Two tables represent the same feature stream, modeled two ways:
--   1. `customer_features`        — mutable table, one row per customer, current values.
--                                    Relies on Dolt's commit history for point-in-time.
--   2. `customer_features_log`    — append-only log, one row per (customer, created_ts).
--                                    Point-in-time requires a ROW_NUMBER dedupe at query time
--                                    (this is what warehouse-shaped offline stores do today).
--
-- Three daily snapshots are committed; the first is tagged `train_2026_04_01`
-- to represent a pinned training run.

CREATE TABLE customer_features (
    customer_id INT PRIMARY KEY,
    event_ts    DATETIME       NOT NULL,
    spend_30d   DECIMAL(10, 2) NOT NULL,
    spend_90d   DECIMAL(10, 2) NOT NULL
);

CREATE TABLE customer_features_log (
    customer_id INT            NOT NULL,
    event_ts    DATETIME       NOT NULL,
    created_ts  DATETIME       NOT NULL,
    spend_30d   DECIMAL(10, 2) NOT NULL,
    spend_90d   DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (customer_id, created_ts)
);

-- ───────── Day 1: 2026-04-01 ─────────
INSERT INTO customer_features VALUES
    (1, '2026-04-01', 100.00, 300.00),
    (2, '2026-04-01',  50.00, 150.00);

INSERT INTO customer_features_log VALUES
    (1, '2026-04-01', '2026-04-01', 100.00, 300.00),
    (2, '2026-04-01', '2026-04-01',  50.00, 150.00);

CALL DOLT_COMMIT('-A', '-m', 'day 1 features: 2026-04-01');
CALL DOLT_TAG('train_2026_04_01', '-m', 'training snapshot for churn model v1');

-- ───────── Day 2: 2026-04-08 — customer 1 spent more ─────────
UPDATE customer_features
   SET event_ts = '2026-04-08', spend_30d = 120.00, spend_90d = 320.00
 WHERE customer_id = 1;

INSERT INTO customer_features_log VALUES
    (1, '2026-04-08', '2026-04-08', 120.00, 320.00);

CALL DOLT_COMMIT('-A', '-m', 'day 2 features: 2026-04-08');

-- ───────── Day 3: 2026-04-15 — customer 2 spent more ─────────
UPDATE customer_features
   SET event_ts = '2026-04-15', spend_30d = 60.00, spend_90d = 180.00
 WHERE customer_id = 2;

INSERT INTO customer_features_log VALUES
    (2, '2026-04-15', '2026-04-15', 60.00, 180.00);

CALL DOLT_COMMIT('-A', '-m', 'day 3 features: 2026-04-15');
