-- The warehouse: every decision and the audit trail. ClickHouse runs these files
-- once, on an empty data directory.

CREATE DATABASE IF NOT EXISTS fraud;

-- Every decision: Grafana, the demo and the retrainer read it.
CREATE TABLE IF NOT EXISTS fraud.transactions_scored
(
    transaction_id      String,
    event_time          DateTime64(3),           -- the transfer's simulated moment
    sender_card         String,
    receiver_card       String,
    amount_uzs          UInt64,
    sender_region       LowCardinality(String),
    is_new_payee        UInt8,
    cep_score           Float32,                 -- the rules' score
    ml_score            Float32,                 -- the model's probability
    final_score         Float32,
    decision            LowCardinality(String),  -- ALLOW / REVIEW
    predicted_type      LowCardinality(String),
    model_version       String,
    scored_at           DateTime64(3) DEFAULT now64(3),
    active_call         UInt8 DEFAULT 0,
    secs_login_to_confirm Float32 DEFAULT 0,
    secs_login_z        Float32 DEFAULT 0,
    -- Wall clock at the producer and at the job's decision (one host, one clock).
    ingested_at         DateTime64(3) DEFAULT toDateTime64(0, 3),
    scored_at_job       DateTime64(3) DEFAULT toDateTime64(0, 3),
    scoring_ms          Float32 DEFAULT 0,
    -- Each stage of the job's decision in ms (fraud_job.py STAGES); NULL if untimed.
    stage_kafka_ms      Nullable(Float32),
    stage_handoff_ms    Nullable(Float32),
    stage_decode_ms     Nullable(Float32),
    stage_state_ms      Nullable(Float32),
    stage_redis_ms      Nullable(Float32),
    stage_rules_ms      Nullable(Float32),
    stage_model_ms      Nullable(Float32),
    stage_decide_ms     Nullable(Float32),
    -- The feature values the model was served, NaN where not computed: what the
    -- retrainer learns from.
    features            Array(Float32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, transaction_id);

-- The audit trail, append-only: the application should hold INSERT and SELECT only
-- (GRANT INSERT, SELECT ON fraud.audit_log TO fraud), never ALTER or DELETE.
CREATE TABLE IF NOT EXISTS fraud.audit_log
(
    audit_id        UUID DEFAULT generateUUIDv4(),
    transaction_id  String,
    event_time      DateTime64(3),
    decision        LowCardinality(String),
    final_score     Float32,
    model_version   String,
    rule_hits       Array(String),
    payload         String,                      -- the decision's full JSON
    -- The integrity chain (data-generator/integrity.py).
    ingress_hash    String,                      -- SHA-256 of the raw event at ingress
    seq             UInt64,                      -- gaps are dropped records
    prev_hash       String,                      -- record_hash of seq - 1
    record_hash     String,                      -- SHA-256(prev_hash, seq, content)
    recorded_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (recorded_at, transaction_id);
