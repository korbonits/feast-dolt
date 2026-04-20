-- Toy point-in-time spike: three feature views on a single entity (customer_id).
--
-- For each feature view we create two parallel tables:
--   * mutable `<name>`       — one row per customer, current value.
--                              Point-in-time handled by Dolt commit history.
--   * append-only `<name>_log` — one row per (customer, created_ts).
--                              Point-in-time handled by a ROW_NUMBER() dedupe
--                              at query time (what warehouse offline stores do).
--
-- The three feature views mirror a realistic churn-model setup:
--     customer_profile       → demographics (tier, signup_days_ago)
--     customer_transactions  → rolling spend (spend_30d, spend_90d)
--     customer_support       → ticket signals (tickets_opened_30d, sentiment)
--
-- Three daily snapshots are committed. Day 1 is tagged `train_2026_04_01`
-- to represent a pinned training run.

-- ───────── Schema: mutable tables ─────────
CREATE TABLE customer_profile (
    customer_id      INT PRIMARY KEY,
    event_ts         DATETIME      NOT NULL,
    tier             VARCHAR(16)   NOT NULL,
    signup_days_ago  INT           NOT NULL
);

CREATE TABLE customer_transactions (
    customer_id INT PRIMARY KEY,
    event_ts    DATETIME       NOT NULL,
    spend_30d   DECIMAL(10, 2) NOT NULL,
    spend_90d   DECIMAL(10, 2) NOT NULL
);

CREATE TABLE customer_support (
    customer_id            INT PRIMARY KEY,
    event_ts               DATETIME    NOT NULL,
    tickets_opened_30d     INT         NOT NULL,
    last_ticket_sentiment  VARCHAR(16) NOT NULL
);

-- ───────── Schema: append-only log tables ─────────
CREATE TABLE customer_profile_log (
    customer_id      INT          NOT NULL,
    event_ts         DATETIME     NOT NULL,
    created_ts       DATETIME     NOT NULL,
    tier             VARCHAR(16)  NOT NULL,
    signup_days_ago  INT          NOT NULL,
    PRIMARY KEY (customer_id, created_ts)
);

CREATE TABLE customer_transactions_log (
    customer_id INT            NOT NULL,
    event_ts    DATETIME       NOT NULL,
    created_ts  DATETIME       NOT NULL,
    spend_30d   DECIMAL(10, 2) NOT NULL,
    spend_90d   DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (customer_id, created_ts)
);

CREATE TABLE customer_support_log (
    customer_id            INT         NOT NULL,
    event_ts               DATETIME    NOT NULL,
    created_ts             DATETIME    NOT NULL,
    tickets_opened_30d     INT         NOT NULL,
    last_ticket_sentiment  VARCHAR(16) NOT NULL,
    PRIMARY KEY (customer_id, created_ts)
);

-- ───────── Day 1: 2026-04-01 ─────────
INSERT INTO customer_profile VALUES
    (1, '2026-04-01', 'gold',   180),
    (2, '2026-04-01', 'silver',  45);

INSERT INTO customer_transactions VALUES
    (1, '2026-04-01', 100.00, 300.00),
    (2, '2026-04-01',  50.00, 150.00);

INSERT INTO customer_support VALUES
    (1, '2026-04-01', 0, 'none'),
    (2, '2026-04-01', 2, 'frustrated');

INSERT INTO customer_profile_log VALUES
    (1, '2026-04-01', '2026-04-01', 'gold',   180),
    (2, '2026-04-01', '2026-04-01', 'silver',  45);
INSERT INTO customer_transactions_log VALUES
    (1, '2026-04-01', '2026-04-01', 100.00, 300.00),
    (2, '2026-04-01', '2026-04-01',  50.00, 150.00);
INSERT INTO customer_support_log VALUES
    (1, '2026-04-01', '2026-04-01', 0, 'none'),
    (2, '2026-04-01', '2026-04-01', 2, 'frustrated');

CALL DOLT_COMMIT('-A', '-m', 'day 1 features: 2026-04-01');
CALL DOLT_TAG('train_2026_04_01', '-m', 'training snapshot for churn model v1');

-- ───────── Day 2: 2026-04-08 ─────────
-- customer 2 upgraded tier; customer 1 spent more; customer 1 opened a ticket
UPDATE customer_profile
   SET event_ts = '2026-04-08', tier = 'gold', signup_days_ago = 52
 WHERE customer_id = 2;

UPDATE customer_transactions
   SET event_ts = '2026-04-08', spend_30d = 120.00, spend_90d = 320.00
 WHERE customer_id = 1;

UPDATE customer_support
   SET event_ts = '2026-04-08', tickets_opened_30d = 1, last_ticket_sentiment = 'neutral'
 WHERE customer_id = 1;

INSERT INTO customer_profile_log VALUES
    (2, '2026-04-08', '2026-04-08', 'gold', 52);
INSERT INTO customer_transactions_log VALUES
    (1, '2026-04-08', '2026-04-08', 120.00, 320.00);
INSERT INTO customer_support_log VALUES
    (1, '2026-04-08', '2026-04-08', 1, 'neutral');

CALL DOLT_COMMIT('-A', '-m', 'day 2 features: 2026-04-08');

-- ───────── Day 3: 2026-04-15 ─────────
-- customer 2 spent more; customer 2 sentiment improved
UPDATE customer_transactions
   SET event_ts = '2026-04-15', spend_30d = 60.00, spend_90d = 180.00
 WHERE customer_id = 2;

UPDATE customer_support
   SET event_ts = '2026-04-15', tickets_opened_30d = 0, last_ticket_sentiment = 'none'
 WHERE customer_id = 2;

INSERT INTO customer_transactions_log VALUES
    (2, '2026-04-15', '2026-04-15', 60.00, 180.00);
INSERT INTO customer_support_log VALUES
    (2, '2026-04-15', '2026-04-15', 0, 'none');

CALL DOLT_COMMIT('-A', '-m', 'day 3 features: 2026-04-15');
