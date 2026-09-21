# stream-processor (PyFlink)

The streaming detection job:

```
transactions.raw --(key by sender)--> CEP rules (pure, stateful; payee windows in Redis)
                                    --> ONNX model on the feature vector
                                    --> final_score + decision
                 --> transactions.scored   (every event)
                 --> fraud.alerts          (decision != ALLOW)
```

## Files

```
deployed to the cluster - every one of these is in the Makefile's PYFILES
config.py        connections, rule windows/thresholds, weights, fusion cutoffs
capabilities.py  what the deploying bank can observe -> features + active rules
features.py      shared train/serve feature contract (Welford baseline) — the core
rules.py         pure CEP engine + SenderState; returns the feature vector too
fusion.py        final_score + decision + reason-code fraud-type tag (pure)
receiver_store.py  the payee's inbound window and the population baseline, in Redis
geo.py           region coordinates + haversine, for the travel-speed rule
payload_crypto.py  AES-256-GCM envelope; duplicated in data-generator/ by design
fraud_job.py     PyFlink job: Kafka -> CEP -> ONNX -> fusion -> Kafka sinks

experiments/     harnesses. Nothing here is deployed; each one produces a
                 NUMBER, and none is imported by the modules above.
  replay.py      one replay loop, three questions - no cluster needed:
                 (default)      what the CEP layer alone would have done
                 fan-in-mode    absolute vs population-relative MULE_FAN_IN
                 payee-seeding  seeded vs unseeded APP episodes
  latency.py     order statistics on the decision path (stack up);
                 `throughput` sweeps it against offered load
  outage.py      break one thing and measure what stops (stack up):
                 --service scorer (default)  what is lost, what is duplicated
                 --service redis|neo4j|clickhouse|kafka  what silently stops
                 --service control           the healthy reference pass

tests/           run with `python -m pytest stream-processor -q`
requirements.txt host-side deps (the Flink image already bundles them)
```

## Deployment capabilities

Not every bank can observe everything. `capabilities.py` is the single source of
truth for which integration enables which features and rules; `FEATURE_NAMES` and
the active rule set are both derived from it, so a capability can be switched off
in one place and the train/serve contract follows automatically.

```
CAP_MYID_KINSHIP=off|on             MyID verified family relationships
CAP_RECEIVER_VELOCITY=on|off        receiver-keyed fan-in counter (Redis)
CAP_COUNTERPARTY_HISTORY=on|off     counterparties per day / week, and transit
CAP_GEO_TELEMETRY=on|off            operation region
CAP_SESSION_TELEMETRY=on|off        mobile-app session signals
CAP_PAYEE_IDENTITY=card|pinfl       what the payee can be resolved to

`CAP_CORE_HISTORY` is not listed because it cannot be switched off: it is the
input stream itself.
```

`python capabilities.py` prints the active profile. Changing any of these
changes the feature contract — retrain and re-export; `ml/experiments/ablate_seeds.py` sweeps
configurations and measures what each integration is worth.

## How CEP and ML combine (decision-layer fusion)

We evaluated naive score blends (noisy-OR, weighted average, ML-augmented) and
**every one degraded ranking** versus the model alone — from a baseline of 0.953
PR-AUC at the time of that experiment down to ~0.91-0.94: the rule score is
lower-resolution and dilutes a strong model. So fusion happens at the **decision
layer**, not by averaging:

- `final_score` = the **model probability** (graded risk), with the CEP score as a
  fallback only when the model is unavailable;
- the CEP layer adds **deterministic regulatory must-flags** (`STRUCTURING`,
  `DAILY_LIMIT_BREACH`) that force at least REVIEW regardless of the model score
  — there for the regulation, not the score: on the current held-out month they
  add one alert, a false one, to the model's 186 (`ml/experiments/layers.py`) —
  plus per-alert **reason codes** (`rule_hits`) and a `predicted_type` tag
  explaining each alert.

A blend is not the only way the rules could reach the decision, and the other way
is measured too. Letting the CEP verdict act as a **floor** — it can only raise a
decision, never lower one, so unlike a blend it cannot degrade ranking at all —
cost precision 0.847 -> 0.457 for two additional true positives on the baseline
profile's held-out slice. The layers stay decoupled because of that number
(`docs/irp-framing.md` §8, fifteenth).

The model is served inside the Flink operator via **ONNX Runtime** on the exact
same feature vector used in training (`features.py`), so there is no train/serve
skew. If `model.onnx` is missing the job degrades to CEP-only scoring, stamps
`model_version = cep-only-fallback` so the warehouse can tell the two apart, and
takes its cutoff from `fusion.review_cutoff(cep_only=True)` — which scales with what the
deployment can observe, because an additive score's cutoff is a claim about how
many rules must agree.

## Results

Not reproduced here. `ml/models/metrics.json` is regenerated by `ml/train.py` and
is the only copy; `ml/README.md` quotes it with the caveats it needs.

The table that used to sit here is why. It was measured before the receiver-side
aggregation capability existed and was never refreshed, so it reported MULE
recall at 68% against the 85.7% the same pipeline was actually achieving - the
largest single gain in the project, contradicted by the file a reader of this
package reaches first. `ml/README.md` records the identical failure in its own
copy. Two independent instances of one habit is enough: the number lives in one
place now, and this file points at it.

To see the layers compared on the current model:

```bash
python ../ml/experiments/layers.py     # CEP-only vs ML-only vs fused, held-out slice
```

## Verify without the cluster

```bash
pip install -r requirements.txt        # the tests alone: pip install pytest cryptography
python -m pytest ../stream-processor -q   # from the repo root: pytest stream-processor
python experiments/replay.py --file ../data-generator/out/transactions.csv
# end-to-end fusion vs cep-only vs ml-only (uses the real ONNX model):
python ../ml/experiments/layers.py
```

Run the packages one at a time — `config.py`, `integrity.py` and
`payload_crypto.py` each exist twice across packages that deploy separately, and
pytest cannot import two modules of one name.

## Run on the cluster

```bash
make serve-prep      # copies ml/models/model.onnx + thresholds.json here
make submit-job      # submits the job (serve-prep runs automatically)
make resume-job      # same, restoring keyed state from the newest checkpoint
```

The module list the job is submitted with lives in the `PYFILES` variable of the
`Makefile` (and `$JobModules` in `run.ps1`) and is not repeated here: it drifted
once already, losing `bins.py` from the Makefile, which made `make submit-job`
submit a job that reported `RUNNING` and then died on its first record.
`tools/boundary_audit.py` now derives the closure from `fraud_job.py`'s imports
and fails if either submitter falls behind it.

`./stream-processor` is mounted at `/opt/flink/usrjobs` in both jobmanager and
taskmanager, which is how the operator loads `model.onnx`. That mount is **not**
on `sys.path`: `--pyFiles` unpacks modules into a Beam temp directory and that is
what `__file__` resolves to, which is why `config._resolve_artefact` names the
mount as a literal rather than deriving it — and why a module absent from
`--pyFiles` is simply absent.

## Known limitations / next enhancements

- **The Python worker processes records serially**, so one stall delays
  everything queued behind it — two stalls became eighteen target breaches in one
  run (§7.3). p99 here is a property of the worst individual record.
- **Home-region profiles** are learned within-stream; in production
  these long-lived profiles belong in the Redis feature store, seeded from
  history.
- **Receiver-side signals are keyed by card**, because a sending bank can resolve
  the destination PAN to a person for only ~6.9% of transfers. `CAP_PAYEE_IDENTITY=pinfl`
  models the platform-level deployment and is reachable offline only.

**Resolved, and recorded because this file claimed otherwise for months:** MULE
fan-in is no longer invisible. `receiver_store.py` keeps the payee's inbound
window in Redis — outside Flink keyed state, because the stream is keyed by
sender — and pooled mule recall rose from 55.7% to 83.4% [78.1%, 87.6%] over
8 seeds. That store sits outside the checkpoint, which is its own trade-off:
replayed transfers read a window that already contains them
(`docs/irp-framing.md` §5, point 6).
