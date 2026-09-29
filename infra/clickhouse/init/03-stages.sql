-- Where each decision's time went, stage by stage, in milliseconds
-- (stream-processor/fraud_job.py STAGES). NULL where a stage was not timed: a
-- producer that stamps no ingested_at leaves no kafka stage, and a decision
-- written before 29.09.2026 has none at all.
--
-- ADD COLUMN IF NOT EXISTS, not a wider CREATE TABLE in 01-schema.sql:
-- ClickHouse runs these scripts only on an empty data directory, so sink-writer
-- applies this file on every connect as well (ch_writer.py), as case-manager
-- does with 02-cases.sql. One statement, so it can be sent as one query.
ALTER TABLE fraud.transactions_scored
    ADD COLUMN IF NOT EXISTS stage_kafka_ms   Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_handoff_ms Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_decode_ms  Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_state_ms   Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_redis_ms   Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_rules_ms   Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_model_ms   Nullable(Float32),
    ADD COLUMN IF NOT EXISTS stage_decide_ms  Nullable(Float32);
