-- The retrainer's runs (ml/retrain.py), one row each: the current model against the
-- retrained one on decisions a week old or more. Grafana shows them; a person
-- switches a better model on by hand.

CREATE TABLE IF NOT EXISTS fraud.retrain_runs
(
    run_at          DateTime64(3),
    outcome         LowCardinality(String),  -- better, not_better, too_few_fraud, too_few_rows, failed
    logged          UInt32,                  -- decisions with a whole feature vector
    known_fraud     UInt32,                  -- of them, confirmed by an analyst or a client
    settling        UInt32,                  -- the last week's, left out of the comparison
    deciding_fraud  UInt32,                  -- known frauds among the decisions compared
    served_alerts   UInt32,
    served_caught   UInt32,
    served_f1       Float32,
    new_alerts      UInt32,
    new_caught      UInt32,
    new_f1          Float32,
    moved           Array(String),           -- features past the drift line
    error           String
)
ENGINE = MergeTree
ORDER BY run_at;
