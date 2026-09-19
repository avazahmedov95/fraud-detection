# IRP framing: from "which engine" to "which data"

Working note for the research plan revision and the point-by-point reviewer
response. Not thesis text. Condensed on 2026-09-15; the full version, with every
run and every reading later withdrawn, is in git history
(`git show 1ebde33:docs/irp-framing.md`).

---

## 1. The mismatch

The reviewer read the research plan as a benchmark of Apache Flink against Spark
Structured Streaming; the prototype has no Spark component and never did. A plan
that names candidate engines and states no falsifiable question invites that
reading, so the gap is in the plan. The resolution: state the question the work
actually answers, and decline the engine benchmark explicitly.

---

## 2. Research question

> **To what extent is real-time P2P fraud detection determined by which data a
> deploying institution can observe, rather than by the detection method?**

**RQ1.** What is the marginal contribution of each institutional data source -
receiver account age, mobile-app session telemetry, receiver-side aggregation,
MyID kinship, device and geo telemetry - to detection quality, with confidence
intervals?

**RQ2.** Which of those sources are unobtainable to a single bank under current
Uzbek inter-bank arrangements, and what does their absence cost? A field worth
mandating is one whose absence measurably degrades detection.

**RQ3.** Does the streaming topology itself constrain which fraud patterns are
detectable, independently of the model?

Each is falsifiable: RQ1 and RQ2 fail if the deltas are indistinguishable from
zero across seeds, RQ3 if patterns are detected equally well whatever the
partitioning key.

---

## 3. Why Flink-only is the honest scope

1. **It answers a settled question**: micro-batching's latency floor is
   established in the streaming literature.
2. **It measures the wrong axis**: detection quality moves by 0.01-0.03 PR-AUC
   with *data availability* (§4), and an engine comparison holds the data constant.
3. **It cannot be done credibly here**: a fair benchmark needs both engines tuned
   by someone with no stake in the outcome.

The reviewer's concern that micro-batching may miss rapid velocity attacks is
kept, as a design constraint on this system (§5, point 7).

---

## 4. Evidence already in hand

All figures: PR-AUC on held-out time slices, paired within generator seed, 95%
CI for the mean delta, 5 seeds unless stated. Baseline 0.960 ± 0.018. Synthetic
data - design targets, not validated findings.

| Data source removed | Δ PR-AUC (95% CI) | sign | verdict |
|---|---|---|---|
| receiver-side aggregation | −0.036 [−0.068, −0.004] | 5/5 | real |
| mobile-app session telemetry | −0.034 [−0.051, −0.016] | 5/5 | real |
| receiver account age | −0.022 [−0.040, −0.004] | 4/5 | real - removed 2026-09-19: unobtainable on 93% of transfers |
| MyID kinship (added) | +0.002 [−0.000, +0.004] | 4/5 | negligible |
| device identity | +0.002 [−0.001, +0.005] | 4/5 | negligible - removed 2026-09-19 |
| payee keyed by person, not card | +0.001 [−0.004, +0.007] | 2/5 | negligible |
| geo telemetry | +0.000 [−0.002, +0.002] | 2/5 | negligible (see caveat) |

Channel identity was removed on 07.09.2026: −0.002 across five seeds, no rule
fed by it, and no public dataset carries it. **Caveat:** this measures the
model's ranking alone. Geo telemetry adds one weak feature but enables the
impossible-travel control - 22 hijacked sessions flagged with **zero false
positives across 775 legitimate inter-regional journeys**.

### RQ3: the topology constrains what is detectable

The stream is partitioned by sender, so behavioural features describe the
sender's own history. Held-out recall on mule events, by leg:

| leg | share | recall |
|---|---|---|
| fan-in (many senders → mule) | 80% | 57.8% |
| fan-out (mule → destinations) | 20% | 93.8% |

Fan-in is invisible in sender-keyed state - to each sender it is one ordinary
transfer. State keyed by receiver raised pooled mule recall from 55.7% to 83.4%
[78.1%, 87.6%] over 8 seeds. **A topology partitioned on one party to a transfer
cannot aggregate over the other**, whatever the engine.

### Methodological result: a signal that was an artefact

MyID kinship (`is_family`) once ranked first by SHAP (1.29) only because the
generator routed no fraud to relatives. With both directions modelled it is worth
+0.002 PR-AUC [-0.000, +0.004]. A feature that dominates SHAP on synthetic data
is a suspect until the generator models both classes of its behaviour - the same
lesson as PaySim's balance-column leakage.

---

## 5. Point-by-point response to the review

| # | Reviewer's point | Response |
|---|---|---|
| 1 | Formal adversarial threat model | **Done - `docs/threat-model.md`.** Per control: what the attacker must do, whether it is attacker-controllable, what evasion costs. Result: detection value and evasion cost are different axes - session telemetry, the second most valuable capability, is the cheapest to evade. |
| 2 | Exact mathematical specification of the generator | **Done - `docs/generator-spec.md`**, every distribution stated and the parametric approach defended: no Uzbek P2P data exists to fit a GAN or copula to. `verify_spec.py` re-checks it against the output (16/16). |
| 3 | Security-overhead benchmarking (mTLS, payload encryption) | **Done (§7.4, 7.5, 7.5a).** AES-256-GCM: no detectable cost on the decision path (p99 183 ms both arms), ~6.8 µs to decrypt, +50% message size. Mutual TLS: below a ~4 ms per-arm drift; one handshake costs +11.2 ms, paid by the client before the measured clock starts. |
| 4 | Integrity audit - cryptographic hashing at ingress and sink | **Done.** Ingress SHA-256 at the producer, bound into the audit record; a hash chain makes alteration, deletion or reordering evident (`verify_audit.py`); head hashes are published in `docs/audit-anchors.md`. |
| 5 | Distinguish organic concept drift from adversarial evasion | **Done - `docs/threat-model.md`.** Drift moves both classes; evasion moves the fraud class only, and only on attacker-controllable features. Falsifiable prediction: once `COACHED_SESSION` is announced, `P(active_call = 1 \| APP fraud)` should decay toward the ~3% base rate. |
| 6 | Non-parametric statistics for tail latency; validate exactly-once by fault injection | **Done, with exactly-once reframed (§6.6, §7).** Order statistics and a distribution-free median CI in `experiments/latency.py`. The system is `AT_LEAST_ONCE` by design: across eighteen kills nothing was lost, duplication ran at a median of about 0.9%, and duplicate copies can carry different scores. |
| 7 | Spark micro-batching may miss rapid velocity attacks | **Scope changed (§3).** The engine comparison is declined; the concern stays as a constraint - rule windows are stated and end-to-end latency is measured against them (§7). |

Point 7 must read as narrowing the claim, not evading it: whether a burst is
detected *before settlement* is the right concern, and it survives the reframing.

---

## 6. What is still owed

Everything owed to the review is now done; the items keep their numbers.

1. **Latency** - done, then redone once the first figures turned out to come from
   a job running without the model (§7).
2. **Threat model** (point 1) - `docs/threat-model.md`; it also settles point 5.
3. **Generator specification** (point 2) - `docs/generator-spec.md`, checked by
   `verify_spec.py`.
4. **Integrity hashing** (point 4) - `integrity.py`, `verify_audit.py`.
5. **Security overhead** (point 3) - §7.4, 7.5, 7.5a.
6. **Fault injection for exactly-once** (point 6) -
   `stream-processor/experiments/outage.py`. The system is `AT_LEAST_ONCE` by
   design (Kafka sink, MergeTree without deduplication, a Redis fan-in store
   outside the checkpoint), so the questions were whether anything is lost and
   what the permitted duplicates cost. Method: `docker compose kill taskmanager`
   mid-stream, 500 transactions per round, jittered kill times, a warehouse
   emptied beforehand.

   - **Nothing lost** - 500 of 500 in every round, across eighteen kills in three
     series.
   - **Duplication**: median 0.89%, 96.9% interval [0.40%, 1.38%] over six kills;
     a third series on the regenerated dataset gave a median of 0.90% [0.00%,
     3.20%]. The count is the traffic between the last checkpoint and the kill.
   - **Duplicates can disagree.** 3 of 26 duplicate rows carried a different
     score (at most 0.0003), and 6 of 34 in the third series (at most 0.0076); no
     decision changed. The cause is in the code: `ReceiverStore` writes are
     idempotent but its reads are not - Redis does not roll back with a Flink
     checkpoint, so a replayed transfer sees a payee window that already holds
     itself. The disagreement grows with receiver-side concentration, the very
     structure the system detects. **Only state inside the checkpoint replays
     exactly.**

   Exactly-once was not attempted, deliberately: transactional Kafka writes would
   add the whole checkpoint interval (2 s) to every decision. `ReplacingMergeTree`
   (eventual) or an idempotent sink keyed on `transaction_id` are the defensible
   routes, and the audit chain already identifies duplicates after the fact.
7. **External validation** - `validation/README.md`. The result worth putting in
   the thesis: **relational fraud detection cannot be validated end-to-end on
   public real data, because the account identifiers that make it relational are
   exactly what cannot be published.** 14 of the 20 features measured here are
   relational (13 of the 16 deployed since 2026-09-19);
   removing them costs 0.937 → 0.761 PR-AUC on the baseline profile. So the
   question is split:
   - **PaySim** (`paysim_adapter.py`), the one public dataset with identifiers on
     both sides: `NEW_PAYEE_HIGH_AMOUNT`, computed purely from per-sender history,
     separates the classes **4.0x** on data this project did not produce, while
     `MULE_FAN_IN` finds nothing - PaySim has no collection stage.
   - **Base rate**, from the ULB card dataset: ~0.17% against the generator's
     1.5%, so precision measured on synthetic data is optimistic.
   - **Rejected**: IEEE-CIS (e-commerce, no receiver), CCF/Kaggle for detection
     (PCA-anonymised), and Zenodo 20030065 - published as production data, it is
     a 1/5 sample of the ULB dataset with 432 rows of someone's demo session
     appended. The checks are cheap and the failure mode is severe.

### RQ3, second result: additive thresholds do not survive capability loss

An additive CEP score with a fixed cutoff states how many rules must agree; as
the available rules shrink, the layer goes silent. On PaySim the highest score
any fraud reached was 0.35 against a 0.40 cutoff - **0 of 2,520 fraud flagged**,
while the same rules separated the classes 4:1. The fix expresses the threshold
relative to what the weakest pattern can still reach
(`capabilities.scaled_threshold`):

| capability profile | weakest reachable | REVIEW |
|---|---|---|
| full | 0.70 | 0.40 (unchanged) |
| no session telemetry | 0.50 | 0.29 |
| own stream + fan-in only | 0.30 | 0.17 |

On PaySim: 0 → 150 of 2,520 flagged at 1.55% false positives, a 3.9x lift that
reproduces the single rule's 4.0x. **Scaling restores sensitivity; it does not
create signal.** Any additive rule layer deployed across institutions with
different data access needs thresholds relative to what is observable.

### RQ3, third result: a rule threshold can encode the population it was tuned on

`MULE_FAN_IN` fires at six distinct senders converging on one payee in an hour.
On IBM's AMLSim simulator (run 2026-08-31, removed 2026-09-19; `validation/README.md`)
it fired on **3.12% of legitimate
traffic and caught 0.0% of the fan-in typology**: in a scale-free graph 2.69% of
receiver-days exceed six as ordinary hub behaviour. The constant encoded the
density of the population it was tuned on. `MULE_FAN_IN_MODE = relative`
replaces it with a quantile of the live population (`rules.PopulationBaseline`
offline, `receiver_store.PopulationStore` in the Flink job, batched and cached),
falling back to the constant - and saying so - when Redis is unreachable.

At home the quantile lands on 5 at q=0.9995 and 7 at q=0.9999, bracketing the
hand-set six, and it is measurably better at no cost in alerts (five seeds,
paired within seed, q=0.999, `experiments/replay.py fan-in-mode`):

| | delta | 95% CI | sign |
|---|---|---|---|
| MULE recall | **+6.9 pp** | [+4.2, +9.7] | 5/5 |
| overall fraud recall | +1.8 pp | [+1.1, +2.5] | 5/5 |
| false-positive rate | +0.01 pp | [−0.01, +0.02] | 2/5 |

Two limits: q=0.999 was chosen on the frozen dataset before these seeds existed,
and all seeds come from one generator - the threshold is shown to adapt, not to
adapt correctly to a foreign institution (on AMLSim the sign itself was inverted).

---

### RQ3, fourth result: the payee is not an identity the deploying bank holds

A card-to-card transfer reaches the sending bank as a destination PAN; resolving
it to a person is possible only for the bank's own clients. Weighted by the real
card market (69.0 million cards, 34 banks, largest share 16.3%), two random
parties share a bank with probability **6.85%** (the generated stream: 6.73%).
So receiver account age - among the most-cited APP-fraud features - is
unobtainable on 93% of transfers here: an argument for resolving it at the
national-platform level. It was removed from this system on 2026-09-19 for that
reason, at a measured cost (`ml/README.md`).

The obvious repair fails. Resolving to PINFL where possible and to the PAN
otherwise, on 50,000 transactions:

| receiver key | `MULE_FAN_IN` fired | of which fraud |
|---|---|---|
| PINFL throughout | 24 | 23 |
| PAN throughout | 24 | 23 |
| PINFL where on-us, else PAN | **19** | **19** |

The mixed key loses **17.4% of the rule's true positives**: it depends on the
sender's bank, splitting one payee's window across two keys. The default is the
card; `payee_identity = pinfl` measures what platform-level resolution would add
(`test_payee_identity.py`). A closed bank's BIN (Yangi Bank, 986040) still
appears on 1.10% of card sides and resolves to no issuer (`bins.RETIRED_BINS`).

## 7. Latency: measured

### 7.0 The July figures are withdrawn

They were taken on a job that was not running the model. `model.onnx` was
resolved relative to `__file__`, and `flink run --pyFiles` unpacks the modules
into a temporary directory without that binary, so the job took its CEP-only
fallback while still stamping `cep+ml-fusion-v1`. Fixed by resolving artefacts
against the mounted job directory, stamping `MODEL_VERSION_CEP_ONLY` when no
session exists, and announcing the fallback in a banner. Every figure below was
checked fused on `model_version`, the one field written from what actually ran.
The second check once used beside it, `countIf(ml_score IS NULL) = 0`, could not
fail - `ml_score` is a non-nullable `Float32` and a missing score is written as
0.0: **a verification criterion that cannot fail is the absence of one.**

### 7.1 Re-measured, with the model verifiably loaded

Live stack, producer inside the Docker network, paced replay; order statistics,
with a distribution-free CI for the median. The <300 ms target applies to the
decision, which leaves on `fraud.alerts`; the warehouse write has no real-time
requirement. Two short runs - 1,602 records warm, 430 cold - first read as "met
warm, missed cold" (p99 186 ms against 1533 ms). Three later cold runs breached
0, 0 and 1 times: a cold cache raises the cost of scoring (7.2), and the target
is missed only when a stall occurs (7.3).

### 7.1a The reference run on the July dataset

5,956 records over thirty minutes, warm cache:

| stage | median (95% CI) | p95 | p99 | max |
|---|---|---|---|---|
| ingest -> decision | 69 ms [68, 70] | 131 ms | **176 ms** | 325 ms |
| of which scoring work | 4.2 ms [4, 4] | 13.8 ms | 22.0 ms | 168.3 ms |
| decision -> ClickHouse | 30.3 s | 69.8 s | 79.7 s | 87.8 s |

**2 of 5,956 over 300 ms (0.03%)**, both marginal; the multi-second stall of the
short runs did not recur.

### 7.1b The regenerated dataset: the figure to quote

The same protocol on the system as it now stands - 7,000 records, a fresh job,
every row stamped fused, nothing clipped:

| stage | n | median (95% CI) | p95 | p99 | max |
|---|---|---|---|---|---|
| ingest -> decision | 7,000 | 86 ms [85, 87] | 164 ms | 204 ms | 1,494 ms |
| of which scoring work | 7,000 | 5.8 ms [6, 6] | 24.5 ms | 39.3 ms | 696.7 ms |
| decision -> ClickHouse | 7,000 | 25.1 s | 62.9 s | 70.2 s | 978 s |

**3 of 7,000 over 300 ms (0.04%).** On 7.1a's clipped window the tail improved
(no breach, maximum 259 ms) and the median rose 15 ms; dataset, model and
feature contract all changed in between, so the difference is recorded
unattributed. The 1,494 ms maximum is record 1 - the Python worker initialises
on its first element, once. The 978 s warehouse maximum is the laptop entering
Modern Standby for sixteen minutes mid-run (System log, Kernel-Power 506/507):
everything froze together, no container logged it, and no decision spans the
pause. The audit-chain anchor for this run is in `audit-anchors.md`.

### 7.2 The enrichment lookup, removed 2026-09-19

Until 2026-09-19 the payee's age was looked up in Neo4j, cached in Redis for an
hour, synchronously inside `process_element`. A miss cost 7.62 ms at the median
against 3.01 for a hit, and ~31 ms more at p99. The lookup went with the age; the
latency figures in this section were measured with it in place and have not been
re-measured.

### 7.3 Two stalls became eighteen breaches

The Python worker processes records serially, so one multi-second stall delays
everything queued behind it: in the 430-record run, two records with
`scoring_ms > 300` produced eighteen breaches. A p99 that depends on the worst
single record needs parallelism or an interruption-free critical path, and this
pipeline has neither.

### 7.4 Security overhead I: payload encryption

AES-256-GCM on the payload, decrypted inside the scoring bracket; 400 records
per arm, enrichment cache flushed before each:

| | plaintext | AES-256-GCM |
|---|---|---|
| ingest -> decision, median | 88 ms | 87 ms |
| **p99** | **183 ms** | **183 ms** |
| scoring, median (95% CI) | 5.7 ms [5,6] | 5.0 ms [5,5] |

No detectable cost on the decision path. A microbenchmark supplies the figure
the pipeline cannot resolve: ~8.9 µs to encrypt and ~6.8 µs to decrypt, the
order of the JSON parse already done. The measurable cost is size - about +50%
per message, 28 bytes of envelope and then base64. Two invariants: the plaintext
is hashed before encryption, so an auditor can recompute `ingress_hash`; and the
routing key is the GCM associated data, so altering it makes a record
undecryptable rather than misrouted. The key comes from `PAYLOAD_KEY_HEX`, with
no default.

### 7.5 Security overhead II: transport

Mutual TLS (`ssl.client.auth = required`) against plaintext, four arms in
A-B-B-A order. Chosen because transport security is the largest high-severity
defect class in the 2025 findings of Uzbekistan's Cybersecurity Centre on mobile
applications: 54 of 157.

| # | arm | n | median (95% CI) | p99 | over 300 ms |
|---|---|---|---|---|---|
| 1 | plaintext | 400 | 77 [73, 81] | 201 | 0 |
| 2 | mutual TLS | 400 | 82 [79, 88] | 243 | 1 |
| 3 | mutual TLS | 1456 | 83 [81, 85] | 225 | 1 |
| 4 | plaintext | 1456 | 87 [85, 89] | 230 | 0 |

The target holds in every arm. The apparent effect follows the order, not the
transport - in both pairs the arm run second was slower, the buffering component
rising ~4 ms per arm - so **the cost of mutual TLS is below the resolution of
this measurement, and the resolution is set by that drift.**

### 7.5a Connection churn, and what one connection costs

Measured directly by a microbenchmark over forty alternating pairs,
a mutual-TLS handshake costs **+11.2 ms** at the median over plaintext (14.5
against 3.3 ms). Four churn arms, reconnecting every 20 messages, met the target
at p99 in every arm (worst 210 ms) and put TLS 4 ms *faster* - read as a
confound, since the handshake falls outside the clock and a longer gap drains
the buffers. A switch opening a connection per transaction pays 11.2 ms of its
own budget each time; the answer is connection pooling on the switch side.

### The work was never the constraint

Scoring - rules, ONNX inference, Redis and Neo4j lookups - takes a few
milliseconds of a 300 ms budget. The rest is framework buffering, and each
default that mattered was tuned for throughput:

| setting | default | set to | why the default hurts |
|---|---|---|---|
| `python.fn-execution.bundle.time` | 1000 ms | 50 ms | Below ~100k events/s the bundle never fills, so every record waited the full second: 1923 ms of the original 1931. |
| `fetch.max.wait.ms` | 500 ms | 20 ms | A fetch against an empty topic parks for the full interval. |
| checkpoint interval | 30 s | 2 s | With AT_LEAST_ONCE the Kafka sink flushes only at checkpoint barriers. |
| `KafkaOffsetsInitializer` | `earliest()` | `committed_offsets(EARLIEST)` | Not latency: every restart replayed the topic and raised duplicate alerts. |

**A streaming fraud pipeline assembled from throughput-tuned defaults misses a
sub-second target by two orders of magnitude, and none of it shows in model
inference time** - latency estimated from the model alone would have been off by
a factor of 400.

### Two artefacts of desktop Docker

The containers' VM clock drifts from the host's - +205 then -279 ms, minutes
apart - which held p95 at ~640 ms through three rounds of tuning, until the
producer moved inside the Docker network (p95 159 ms); `experiments/latency.py`
now probes the offset. And the VM pauses: Neo4j's monitor logged a 254 ms
stop-the-world pause with no garbage collection. Neither is visible from inside
the application, so **a sub-second latency claim produced on desktop Docker
needs to say so.**

### What is not yet resolved

- **The warehouse path takes 20-33 s**, undiagnosed. Nothing real-time rides on
  it, but Grafana inherits the lag.
- **A rare multi-second scoring stall** - 1452 ms and 323 ms in the short runs -
  did not recur in the 12,955 records of 7.1a and 7.1b, so it is rarer than about
  one in 13,000. Not ONNX warm-up, not cache misses, not a Neo4j pause; a
  VM-level pause and Python garbage collection remain open.
- **One machine.** Every arm shares whatever the host was doing, which is what
  the counterbalancing in 7.5 works around.

### 7.6 Throughput: where the 300 ms target stops holding

3,000 messages per arm, deadline-based pacing, the decision path only (ms;
`work` is in-operator processing):

| offered/s | achieved | p50 | p95 | p99 | over 300 ms | work | state |
|---:|---:|---:|---:|---:|---:|---:|---|
| 5 | 5 | 87 | 101 | 116 | 1 | 5.5 | ok |
| 10 | 10 | 87 | 139 | 156 | 0 | 1.9 | ok |
| 25 | 25 | 124 | 218 | 340 | 49 | 1.6 | p99 over target |
| 50 | 50 | 156 | 282 | 382 | 110 | 1.2 | p99 over target |
| 100 | 100 | 240 | 805 | 1,162 | 1,039 | 0.9 | p99 over target |
| 250 | 250 | 5,671 | 9,263 | 9,515 | 2,961 | 1.0 | SATURATED |

**The target stops holding at or below 25 events/s**, two orders of magnitude
below a national switch. `work` stays near a millisecond while decision time
climbs sixty-fold: records queue behind the synchronous Redis and Neo4j lookups,
one at a time per slot (the Neo4j one was removed on 2026-09-19). Flink async I/O is the documented
fix and is not implemented. This is a per-slot figure on one machine, and the
sweep ends where the producer saturates.

### 7.7 Dependency matrix: what each outage silently removes

`experiments/outage.py` stops one service per arm while 1,000 transactions are
produced through the outage, each prediction written in `EXPECTED` before the
run; loss is offered minus stored. All four loss predictions held. Redis, Neo4j
and a 20 s mid-stream Kafka outage lost nothing; a ClickHouse outage lost all
1,000 rows by design - the sink commits offsets on a timer - so the pipeline
keeps deciding and the audit trail is the part that yields first.

### 7.7a Fail-open is not fail-fast

Killed under a running job, Redis or Neo4j left 1,000 events undrained after
five minutes, against six seconds healthy. Neither client has a timeout, and a
handle is set to `None` only in `open()`, so every later event pays a failed
round trip: the pipeline keeps deciding and stops keeping up. **A decision that
arrives after the payment has settled is no decision.** The fix is declared, not
made: timeouts on both clients, and a client marked dead after N consecutive
failures.

### 7.7b Five ways this harness computed a confident wrong number

Distinct transaction ids as the loss denominator, though the producer replays
one CSV; the requested count instead of the delivered one; the whole table as
the alert-mix reference, with no control pass; a service restart skipped on a
failed run; and a state reset that flushed Redis but kept the job's keyed state,
so each pass stacked on the last. None failed - each returned a plausible
number. The method that survived: *loss as delivered minus stored, the alert mix
against a healthy control over the same transactions, each pass on a fresh job
with empty state.*

### 7.7c Re-measured on 2026-09-13: one fresh job per pass

| arm | stored | lost | drain | alert mix against the control |
|---|---:|---:|---|---|
| control | 1,000 | 0 | 14 s | reference: unlabelled 17, APP 6, MULE 1 |
| Redis down | 1,000 | 0 | 13 s | **MULE 1 -> 0**, APP 6 -> 2, unlabelled 17 -> 7 |
| Neo4j down | 1,000 | 0 | 13 s | **APP 6 -> 20, unlabelled 17 -> 30**, MULE 1 |
| ClickHouse down | 0 | **1,000** | 14 s | none observable - nothing stored |
| Kafka down 20 s, mid-stream | 1,000 | 0 | drained | **identical**: unlabelled 17, APP 6, MULE 1 |

The Kafka arm reproducing the control count for count makes the other rows
effects, not noise. Redis down silenced `MULE_FAN_IN`, as predicted, and cut the
model-driven alerts by about sixty per cent. Neo4j down *raised* alerts: an
unknown payee age was encoded as -1, which every age split reads as younger than
any real account. Fixed on the training side on 2026-09-13 - the age is NaN in
every mode and `train.py` withholds it on a tenth of its rows - and on the
baseline profile an offline replay with every age withheld then raised 14 false
alarms against 10, where the -1 had raised 95 (`ml/README.md`). Since 2026-09-19
the job reads nothing from Neo4j - the age is gone - so a graph outage costs only
the alert graph. Drain was fast
in every arm here because the dependency was already down when the worker opened
its clients; 7.7a is the other half of the same code path.

## 8. Silent failure modes

Twenty-one failures, numbered in the order found; most left every health
indicator green. The pattern is the result, not the individual bugs.

**Twenty-first: the session cluster leaks on every redeploy, and the eighth one
kills it.** Each job loads its own classloader into JVM Metaspace and a cancelled
job does not return it; on 2026-09-13 the eighth submission to one TaskManager
died with `OutOfMemoryError: Metaspace`. Silent several times over - restart
policy `no`, fifty seconds to a missed heartbeat, every other container Up - and
first misread as the Kafka outage it coincided with. Mitigated: every `run.ps1`
submission restarts the TaskManager first, and the TaskManager has
`restart: unless-stopped`. The production answer is a per-job cluster.

**Twentieth: the feature extractor was quadratic in a population this project
never streams.** Replaying IBM AML (4.49M rows; since removed as not P2P) took 5.5 hours: five linear passes
over the sender's 24-hour history per event, invisible at two events per retail
sender-day, dominant at IBM's 26,365. The latency chapter could not see it - a
benchmark from the same generator shares its assumptions. The same property
explains a detection result: retail limits (`DAILY_LIMIT_BREACH` 0.9x, `VELOCITY`
and `DISTINCT_PAYEE_BURST` 1.2x) count events against a person on a stream of
institutions. Hub accounts were not dropped to speed it up: that would select on
a quantity correlated with the label.

**Nineteenth: a fraud category the system could never assign.** MULE was named by
`DISTINCT_PAYEE_BURST` and `VELOCITY` alone, neither of which fired on the
generator's traffic, so the label was unreachable - 0 of 50,000 rows - while
`MULE_FAN_IN` fired 30 times at 100% precision outside the table. Every guard
passed; only counting the outcomes showed it. `test_fusion.py` now asserts that
every label has a trigger the engine can emit.

**Eighteenth: the external baseline was quoted against the wrong prevalence.**
`related-work.md` §6 placed PaySim's published AUPRC 0.380 at 0.129% fraud; it
belongs to a holdout at 1.142%. Checkable all along, and now reproduced
(`paysim_adapter.py --baseline`: 0.397). **A truthful admission of not having
checked is not a check.** The audit pins the reported figures to
`paysim_adapter.BASELINE`.

**Seventeenth: the clause that certified the latency figures could not fail.**
`countIf(ml_score IS NULL) = 0` holds on every run, since a missing score is
written as 0.0; only `model_version` worked. **A criterion that cannot fail is
the absence of a check wearing its shape.**

**Sixteenth: `make submit-job` was broken, and submitting it said otherwise.**
`bins.py` was in `run.ps1`'s module list and missing from the `Makefile`'s. The
job reported RUNNING - PyFlink starts Python lazily - and died on the first
record. **A broken submitter and a working one are indistinguishable until
traffic arrives.** The audit now derives `fraud_job`'s import closure and holds
both lists to it.

**Fifteenth: a fix that did not reach the deployed path.** The capability-scaled
threshold (§6, second RQ3 result) was wired only into the offline harnesses -
invisible at full capability, where scaling is the identity. It now lives in
`fusion.score_and_decide`, the only form the job uses, pinned by tests that parse
`fraud_job.py`. Measured on the way: letting the CEP verdict act as a floor on
the fused decision costs precision **0.847 -> 0.457** for 2 more true positives,
so the layers stay decoupled.

**Fourteenth: fail-open without fail-fast.** With Redis or Neo4j gone the pipeline
keeps deciding and stops keeping up - neither client has a timeout (§7.7a).

| # | failure | what an operator saw | what it actually did |
|---|---|---|---|
| 1 | ClickHouse not accepting queries when sink-writer connected at boot | one WARNING, then silence | sink disabled for the process: **331 events consumed, 0 rows stored** |
| 2 | Neo4j losing the same startup race | same | every graph write discarded |
| 3 | `model.onnx` unreachable under `--pyFiles` | one INFO line | CEP-only scoring stamped `cep+ml-fusion-v1` (§7.0) |
| 4 | jobmanager restart (session cluster, no HA) | **entire stack healthy** | no job running at all |
| 5 | resubmission without `-s` | job RUNNING | empty keyed state: history-dependent rules blind until refilled |
| 6 | producer stopped with Ctrl+C | "produced N" never printed | buffered messages never sent - a fake "loss" in a fault-injection run |
| 7 | unpinned dependencies | build succeeded | three libraries crossed major versions with no repository change |
| 9 | ClickHouse init scripts run only on an empty data directory | service up, offsets committed | every insert failing against a missing table |
| 10 | a DDL splitter splitting on `;` inside a comment | - | three invalid SQL fragments; caught by a test |
| 11 | a data file resolved against `__file__` under `--pyFiles` | an empty queue, two components away | job FAILED in Flink; the alert topic stayed empty |
| 12 | LightGBM's OpenMP runtime missing from the slim image | service healthy | `libgomp.so.1` at import; every case filed `NO_MODEL` |
| 13 | booleans travelling as text (`"active_call": "False"`) | the JSON looked right | `active_call = 1` on **100%** of live events: false positives **20 → 459** |

Only the eighth - a root-owned checkpoint directory - failed loudly. Row 13
inverts the usual assumption: every offline path converted the flag, so the
defect existed only in production; the coercion now lives in `features.py`. Row 11
had already been written down as failure 3: **a hazard met twice should be made
unavailable, not annotated** - there is now one artefact resolver.

**The claim.** The dangerous failures in a streaming fraud pipeline are the ones
that leave every health indicator intact while the system silently stops doing
part of its job. Three consequences, all implemented: "fails open" must mean
"keeps running and says so"; provenance is recorded from what ran, not from what
was configured; reproducibility is a property of the pinned environment, not of
the seed. And an instrument that fails in the direction of its own hypothesis
(failure 6) is the one worth checking first.

## 9. What the work queue found that the metrics did not

Building the missing alert consumer (`case-manager/`) turned into an instrument:
running a real queue exposed two properties no reported metric looks at.

### 9.1 The probabilities rank well and mean nothing

On the baseline profile every case carried `final_score = 1.000`:

| quantity | value |
|---|---|
| Brier score | 0.00244 |
| alerts (p >= 0.40) | 131 |
| alerts rounding to 1.000 | **70.2%** (89.1% over the full set) |
| distinct rounded alert scores | **35** |
| alerts in the REVIEW band [0.40, 0.80) | **14** |
| median alert probability | 0.999975 |

ROC-AUC 0.9992 and PR-AUC 0.9591 were correct - rank statistics, and the ranking
was near-perfect - while the scores could not order a queue. The queue was
re-ordered by exposure, and `train.py` now reports calibration beside the AUCs.
On the realistic profile 11.9% of alerts round to 1.000 and the queue can be
ordered again (`ml/README.md`, Calibration).

### 9.2 One alert in seven was an automated adverse decision with no reason

| | count | share |
|---|---|---|
| alerts | 758 | |
| with at least one rule hit | 645 | 85.1% |
| **with no reason code at all** | **113** | **14.9%** |
| BLOCKs with no reason code | 111 of 744 | 14.9% |
| of those, actually fraud | 109 of 111 | **98.2%** |

Accurate and mute - a compliance problem whatever the accuracy. The model was
given a voice: exact tree contributions, computed downstream in case-manager
rather than on the scoring path (1.89 ms per event, and nobody reads an
explanation at decision time), and refused whenever the recomputed probability
differs from the recorded one by more than 1e-4.

### 9.3 The mechanism behind the ML layer's advantage, on one transaction

| | value | rule | threshold | fired? |
|---|---|---|---|---|
| payee account age | 31 days | `FRESH_RECEIVER` | < 30 days | no, by one day |
| distinct senders in 1h | 2 | `MULE_FAN_IN` | >= 6 | no |
| inbound to payee in 1h | 14 615 204 UZS | - | - | - |
| this transfer | 7 307 603 UZS | `NEW_PAYEE_HIGH_AMOUNT` | new payee and > 3x mean | no |

*As measured then; `FRESH_RECEIVER` and the payee's age were removed on 2026-09-19.*

**Rules test thresholds one at a time; the model tests combinations that cross
none.** Across the 113 model-only alerts, 69% had receiver-side concentration as
their strongest contribution - fan-in detected below the rule's constant.

### 9.4 What is not defensible in this, stated first

- Hour of day was the strongest contribution on 27 of the 113 - a diurnal
  artefact of the generator, not a finding.
- `predicted_type` is empty for every model-only alert; deriving it from
  contributions was rejected as a claim of a different strength.
- The queue's precision figure is biased upward: analysts work the top of it.
- One case per alert: a mule paid by twelve senders opens up to twelve cases.

### 9.5 Why the disposition field exists in a prototype with no analysts

`CONFIRMED_FRAUD` / `FALSE_POSITIVE` is the only real label this system can
produce, and what a production model would be retrained on: the system cannot
conjure labels, but operating it produces them.

## 10. Notes for the written revision

- Every quoted figure needs its interval: seed-to-seed spread is wider than most
  effects measured here, and single-run numbers were wrong twice.
- Report calibration next to discrimination wherever a probability is quoted
  (§9.1), and disclose ROC-AUC near 0.99 as a property of synthetic data.
- For impossible travel the reportable result is 0 false positives on 775
  journeys; the detection rate is true by construction.
- **Declare the scope in the introduction**: a *detection* system, not an
  *enforcement* one - `fraud.alerts` opens an analyst case, and nothing declines a
  transfer. The latency is the time to *reach* a decision.
- The cost of a false BLOCK is never paid by this system; a deployment would
  argue the operating point over declined payments, not F1.
