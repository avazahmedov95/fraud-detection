-- The analyst's queue: a case for every hold, opened by the sink writer. The one
-- table that changes - a verdict replaces the open row - so it stands apart from
-- the append-only decisions and audit trail. One case per hold: a mule paid by
-- twelve senders opens up to twelve.

CREATE TABLE IF NOT EXISTS fraud.cases
(
    case_id         String,                  -- the transaction id
    transaction_id  String,
    event_time      DateTime64(3),
    opened_at       DateTime64(3),
    sender_card     String,
    receiver_card   String,
    amount_uzs      UInt64,
    final_score     Float32,
    decision        LowCardinality(String),  -- REVIEW; a client's report keeps ALLOW
    predicted_type  LowCardinality(String),
    rule_hits       Array(String),
    disposition     LowCardinality(String) DEFAULT 'NEW',
    resolved_by     String DEFAULT '',
    resolved_at     DateTime64(3) DEFAULT toDateTime64(0, 3),
    -- The highest version per case stays: an opened case is 0 or 1, a verdict its
    -- epoch ms, so a redelivered hold never reverts a verdict.
    version         UInt64,
    explanation     Array(String),           -- the hold's reasons in words
    -- Why there is none: NO_MODEL, NO_FEATURES, MODEL_MISMATCH or FAILED.
    explanation_status LowCardinality(String) DEFAULT '',
    model_version   LowCardinality(String) DEFAULT '',  -- the second look's, if it held
    ml_score        Nullable(Float32)        -- the served model's; NULL with no model
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY case_id;
