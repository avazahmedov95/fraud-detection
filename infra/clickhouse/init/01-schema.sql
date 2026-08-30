-- ClickHouse schema for the fraud-detection pipeline.
-- Runs automatically on first container start.

CREATE DATABASE IF NOT EXISTS fraud;

-- Analytical store of every scored transaction (powers Grafana dashboards).
CREATE TABLE IF NOT EXISTS fraud.transactions_scored
(
    transaction_id      String,
    event_time          DateTime64(3),
    sender_card         String,
    receiver_card       String,
    amount_uzs          UInt64,
    channel             LowCardinality(String),
    sender_region       LowCardinality(String),
    receiver_region     LowCardinality(String),
    is_new_payee        UInt8,
    cep_score           Float32,                 -- rule/CEP contribution
    ml_score            Float32,                 -- gradient-boosting probability
    final_score         Float32,                 -- combined risk score
    decision            LowCardinality(String),  -- ALLOW / REVIEW / BLOCK
    predicted_type      LowCardinality(String),  -- model's fraud-type guess
    model_version       String,
    scored_at           DateTime64(3) DEFAULT now64(3),
    active_call         UInt8 DEFAULT 0,
    secs_login_to_confirm Float32 DEFAULT 0,
    secs_login_z        Float32 DEFAULT 0,
    -- Latency instrumentation. `event_time` is the SIMULATED moment the
    -- transaction happened and says nothing about the pipeline; these three are
    -- wall clock at the producer (t0), after scoring in Flink (t1) and at the
    -- ClickHouse write (t2, the `scored_at` column above). End-to-end latency is
    -- t2 - t0; `scoring_ms` isolates the scoring work from transport and
    -- queueing. All stages run on one host, so the clock is common and no skew
    -- correction is needed — a property to state, not to assume, in production.
    ingested_at         DateTime64(3) DEFAULT toDateTime64(0, 3),
    scored_at_job       DateTime64(3) DEFAULT toDateTime64(0, 3),
    scoring_ms          Float32 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, transaction_id);

-- Append-only audit trail.
-- WORM intent: the application role is granted INSERT/SELECT only — never
-- UPDATE/DELETE/ALTER — so records are immutable once written. Enforce that at
-- the grant level (see note below) and/or via a storage policy / object-lock
-- backend in production.
CREATE TABLE IF NOT EXISTS fraud.audit_log
(
    audit_id        UUID DEFAULT generateUUIDv4(),
    transaction_id  String,
    event_time      DateTime64(3),
    decision        LowCardinality(String),
    final_score     Float32,
    model_version   String,
    rule_hits       Array(String),               -- which CEP patterns fired
    payload         String,                      -- full JSON snapshot of the event
    -- Integrity chain (see sink-writer/integrity.py).
    ingress_hash    String,                      -- SHA-256 of the raw event at ingress
    seq             UInt64,                      -- monotonic per writer; gaps = dropped records
    prev_hash       String,                      -- record_hash of seq-1
    record_hash     String,                      -- SHA-256(prev_hash || seq || content)
    recorded_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (recorded_at, transaction_id);

-- Example WORM grant (apply once the application user exists):
--   GRANT INSERT, SELECT ON fraud.audit_log TO fraud;
--   -- deliberately NOT granting ALTER / DELETE / TRUNCATE / DROP
