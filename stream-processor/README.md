# stream-processor (PyFlink) — phase 4 ✅ (CEP) + phase 6 ✅ (ML serving + fusion)

The streaming detection job:

```
transactions.raw --(key by sender)--> enrich (Neo4j account lookup + Redis cache)
                                    --> CEP rules (pure, stateful)
                                    --> ONNX model on the feature vector
                                    --> final_score + decision
                 --> transactions.scored   (every event)
                 --> fraud.alerts          (decision != ALLOW)
```

## Files (flat layout)

```
config.py        connections, rule windows/thresholds, weights, fusion cutoffs
features.py      shared train/serve feature contract (Welford baseline) — the core
rules.py         pure CEP engine + SenderState; returns the feature vector too
capabilities.py  what the deploying bank can observe -> features + active rules
enrichment.py    Neo4j receiver-age lookup, Redis-cached, fails open
geo.py           region coordinates + haversine, for the travel-speed rule
fusion.py        final_score + decision + reason-code fraud-type tag (pure)
fraud_job.py     PyFlink job: Kafka -> enrich+CEP -> ONNX -> fusion -> Kafka sinks
replay_eval.py   offline CEP replay (no cluster needed)
test_rules.py    unit tests for the rule engine
test_geo.py      unit tests for the geo reference table + distance maths
test_capabilities.py       registry integrity + derived contract
test_receiver_age_modes.py the three receiver-age availability regimes
test_fusion.py   unit tests for the fusion/decision logic
requirements.txt host-side deps (the Flink image already bundles them)
```

## Deployment capabilities

Not every bank can observe everything. `capabilities.py` is the single source of
truth for which integration enables which features and rules; `FEATURE_NAMES` and
the active rule set are both derived from it, so a capability can be switched off
in one place and the train/serve contract follows automatically.

```
CAP_RECEIVER_AGE=always|on_us|off   account-age lookup / inter-bank exchange
CAP_MYID_KINSHIP=off|on             MyID verified family relationships
CAP_DEVICE_TELEMETRY=on|off         stable device identifier
CAP_GEO_TELEMETRY=on|off            operation region
CAP_SESSION_TELEMETRY=on|off        mobile-app session signals
CAP_CHANNEL=on|off                  channel identity
```

`python capabilities.py` prints the active profile. Changing any of these
changes the feature contract — retrain and re-export; `ml/ablation.py` sweeps
configurations and measures what each integration is worth.

## How CEP and ML combine (decision-layer fusion)

We evaluated naive score blends (noisy-OR, weighted average, ML-augmented) and
**every one degraded ranking** versus the model alone (PR-AUC 0.953 -> ~0.91-0.94):
the rule score is lower-resolution and dilutes a strong model. So fusion happens
at the **decision layer**, not by averaging:

- `final_score` = the **model probability** (graded risk), with the CEP score as a
  fallback only when the model is unavailable;
- the CEP layer adds **deterministic regulatory must-flags** (`STRUCTURING`,
  `DAILY_LIMIT_BREACH`) that force at least REVIEW regardless of the model score —
  high-precision on synthetic data (38 fraud vs 2 legit) — plus per-alert
  **reason codes** (`rule_hits`) and a `predicted_type` tag explaining each alert.

The model is served inside the Flink operator via **ONNX Runtime** on the exact
same feature vector used in training (`features.py`), so there is no train/serve
skew. If `model.onnx` is missing the job degrades gracefully to CEP-only scoring.

## Indicative results on the held-out slice (design targets, NOT validated)

| layer            | precision | recall | f1    |
|------------------|-----------|--------|-------|
| CEP rules only   | 0.472     | 0.589  | 0.524 |
| ML only @0.50    | 0.851     | 0.829  | 0.840 |
| **Fused**        | **0.847** | **0.873** | **0.860** |

`final_score` ranking ROC-AUC 0.999 / PR-AUC 0.952. Fused recall by type:
STRUCTURING 98%, ATO 95%, APP 84%, MULE 68%. The hybrid beats both CEP-only and
ML-only at the chosen operating point (REVIEW 0.40 / BLOCK 0.80).

## Verify without the cluster

```bash
pip install -r requirements.txt        # or just: pip install pandas pytest
python test_rules.py                    # CEP engine unit tests
python test_fusion.py                   # fusion/decision unit tests
python replay_eval.py --file ../data-generator/out/transactions.csv
# end-to-end fusion vs cep-only vs ml-only (uses the real ONNX model):
python ../ml/fusion_eval.py
```

## Run on the cluster

```bash
make serve-prep      # copies ml/models/model.onnx + feature_names.json here
make submit-job      # submits the job (serve-prep runs automatically)
# or manually:
docker compose exec jobmanager flink run -d -py /opt/flink/usrjobs/fraud_job.py \
  --pyFiles /opt/flink/usrjobs/config.py,/opt/flink/usrjobs/features.py,\
/opt/flink/usrjobs/rules.py,/opt/flink/usrjobs/enrichment.py,/opt/flink/usrjobs/fusion.py
```

The model file is mounted into both jobmanager and taskmanager via
`./stream-processor:/opt/flink/usrjobs`, so the operator (running in the
taskmanager) can load it.

## Known limitations / next enhancements

- **MULE fan-in** is invisible keyed by sender; a parallel receiver-keyed stream
  would catch it (MULE recall is the lowest of the four types for this reason).
- **Device / home-region profiles** are learned within-stream; in production these
  long-lived profiles belong in the Redis feature store, seeded from history.
- Phase 7 writes `transactions.scored` / `fraud.alerts` to **ClickHouse** (WORM
  audit) and **Neo4j**; phase 8 adds the **Grafana** dashboards.
