-- The analyst work queue.
--
-- Everything else this system writes is append-only: transactions_scored is a
-- record of what was decided, audit_log is a hash-chained record of the same
-- and is deliberately immutable. A case is the one thing here that CHANGES -
-- it opens unresolved and later carries a human's verdict - so it is a separate
-- table with a different engine rather than a column on an audit row. Making
-- the audit trail mutable to hold a workflow field would defeat the point of
-- the chain.
--
-- The disposition is also the only source of REAL labels this system can ever
-- have. Everything measured so far is measured against generated ground truth;
-- what an analyst confirms is the thing a production model would actually be
-- retrained on. That is why the column exists in a prototype that has no
-- analysts: it is the shape of the feedback loop, and it is where a real
-- deployment would start collecting.

CREATE TABLE IF NOT EXISTS fraud.cases
(
    case_id         String,                  -- one per alert; see note below
    transaction_id  String,
    event_time      DateTime64(3),
    opened_at       DateTime64(3),
    sender_card     String,
    receiver_card   String,
    amount_uzs      UInt64,
    final_score     Float32,
    decision        LowCardinality(String),  -- REVIEW / BLOCK (never ALLOW)
    predicted_type  LowCardinality(String),
    rule_hits       Array(String),           -- the reason codes, for the analyst
    priority        UInt8,                   -- 0 = highest; derived, see case.py

    -- Workflow state.
    disposition     LowCardinality(String) DEFAULT 'NEW',
    resolved_by     String DEFAULT '',
    resolved_at     DateTime64(3) DEFAULT toDateTime64(0, 3),

    -- ReplacingMergeTree keeps the row with the HIGHEST version per case_id.
    -- Opening a case always writes version 0; a resolution writes epoch millis.
    -- That ordering is not cosmetic: the alert topic is AT_LEAST_ONCE, so an
    -- alert can be redelivered after an analyst has already resolved its case.
    -- Were the opening row versioned by wall clock, the replay would carry a
    -- larger version than the resolution and silently revert a human's verdict.
    version         UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY case_id;

-- The model's own reasons, for the cases where no rule fired.
--
-- ADD COLUMN IF NOT EXISTS, not a wider CREATE TABLE: this file is applied by
-- the service on every connect (see case-manager/store.py for why), so on any
-- cluster where the table already exists a changed CREATE would do nothing at
-- all and the new columns would silently never appear. A schema file that runs
-- repeatedly has to be written as a migration.
ALTER TABLE fraud.cases
    ADD COLUMN IF NOT EXISTS explanation Array(String);

-- Why the explanation is absent, when it is. Stored so that "no explanation"
-- is never read as "nothing was notable": NO_MODEL, NO_FEATURES (the alert
-- predates feature publication), MODEL_MISMATCH (the explaining model disagreed
-- with the one that scored - see case-manager/explain.py) or FAILED.
ALTER TABLE fraud.cases
    ADD COLUMN IF NOT EXISTS explanation_status LowCardinality(String) DEFAULT '';

-- NOTE ON GRANULARITY. One case per alert. A mule receiving from twelve
-- senders therefore produces up to twelve cases, where an investigator wants
-- one. Grouping alerts into an investigation (by payee, within a window) is the
-- obvious next step and is deliberately NOT done here: it changes what a
-- "false positive" counts, and the counting is what the disposition exists for.
