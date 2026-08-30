# IRP framing: from "which engine" to "which data"

Working note for the research plan revision and the point-by-point reviewer
response. Not thesis text.

---

## 1. The mismatch

The reviewer read the research plan as a benchmark of Apache Flink against Spark
Structured Streaming. The prototype contains no Spark component and never did.

This is not a misreading to argue with. A plan that describes a streaming
architecture, names candidate engines, and states no falsifiable question leaves
the reader to supply one — and "which engine is faster" is the obvious candidate.
The gap is in the plan, not in the review.

The resolution taken here is to state the question the work actually answers,
and to decline the engine benchmark explicitly rather than silently.

---

## 2. Research question

> **To what extent is real-time P2P fraud detection determined by which data a
> deploying institution can observe, rather than by the detection method?**

Sub-questions, each measurable on the existing system:

**RQ1.** What is the marginal contribution of each institutional data source —
receiver account age, mobile-app session telemetry, receiver-side aggregation,
MyID kinship, device and geo telemetry — to detection quality, with confidence
intervals?

**RQ2.** Which of those sources are unobtainable to a single bank under current
Uzbek inter-bank arrangements, and what does their absence cost? This converts a
detection result into a regulatory one: a field worth mandating is a field whose
absence measurably degrades detection.

**RQ3.** Does the streaming topology itself constrain which fraud patterns are
detectable at all, independently of the model?

Each is falsifiable. RQ1 and RQ2 fail if the deltas are indistinguishable from
zero across seeds. RQ3 fails if patterns are detected equally well regardless of
partitioning key.

---

## 3. Why Flink-only is the honest scope

The engine benchmark is declined for three stated reasons:

1. **It answers a settled question.** That micro-batch execution imposes a
   latency floor which continuous processing does not is established in the
   streaming literature. Reproducing it on one synthetic dataset adds little.

2. **It measures the wrong axis.** The deltas below show detection quality moving
   by 0.01–0.03 PR-AUC depending on *data availability*. An engine comparison
   holds data constant and varies the runtime — the axis this work finds to be
   the less consequential one.

3. **It cannot be done credibly at this scale.** A fair engine benchmark needs
   both implementations tuned by someone with no stake in the outcome. A
   single-author comparison where one engine is the author's working system and
   the other is a port written to be compared against is not evidence.

The reviewer's specific concern — that Spark's micro-batching might miss rapid
velocity attacks — is addressed in §5 rather than dismissed.

---

## 4. Evidence already in hand

All figures: PR-AUC on held-out time slices, paired within generator seed, 95%
CI for the mean delta, 5 seeds unless stated. Baseline 0.966 ± 0.008. Synthetic
data — design targets, not validated findings.

| Data source removed | Δ PR-AUC (95% CI) | sign | verdict |
|---|---|---|---|
| receiver-side aggregation | −0.032 [−0.055, −0.009] | 5/5 | real |
| mobile-app session telemetry | −0.019 [−0.025, −0.014] | 5/5 | real |
| receiver account age | −0.011 [−0.020, −0.001] | 5/5 | real |
| MyID kinship (added) | +0.004 [+0.001, +0.007] | 5/5 | negligible |
| channel identity | −0.002 [−0.004, +0.001] | 5/5 | negligible |
| geo telemetry | −0.001 [−0.004, +0.003] | 3/5 | negligible (see caveat) |
| device identity | ±0.000 | 5/5 | none |

**Caveat that must travel with this table.** The measurement is of the ML
model's ranking quality alone. A source whose value lies in a deterministic CEP
rule is invisible to it: geo telemetry contributes one weak feature but enables
the impossible-travel control, which flags 22 hijacked sessions with **zero false
positives across 775 legitimate inter-regional journeys**. Rule-side value is
reported separately.

### RQ3: the topology constrains what is detectable

The stream is partitioned by sender, so every behavioural feature described the
sender's own history. Held-out recall on mule events, split by leg:

| leg | share | recall |
|---|---|---|
| fan-in (many senders → mule) | 80% | 57.8% |
| fan-out (mule → destinations) | 20% | 93.8% |

Fan-out is visible in sender-keyed state; fan-in is not, and cannot be — to each
contributing sender it is one ordinary transfer to a new payee. Adding state
keyed by receiver raised pooled mule recall from 55.7% to 83.4% [78.1%, 87.6%]
over 8 seeds.

The general claim: **a topology partitioned on one party to a transfer cannot
express aggregation over the other party**, and any fraud pattern defined on the
counterparty inherits that blind spot. This is a property of the architecture,
not of Flink, and applies equally to a Spark implementation — which is itself an
argument that the engine is not the interesting variable.

### Methodological result: a signal that was an artefact

An earlier revision identified MyID kinship (`is_family`) as the strongest
feature (SHAP 1.29, rank 1) and framed it as the project's core contribution. It
was an artefact: the generator routed no fraud to relatives, so the feature
separated the classes by construction.

The generator now models both directions — 25% of legitimate transfers go to
relatives, and a realistic minority of fraud does too (mule recruitment through
families, complicit relatives). Under those conditions the feature is worth
+0.004 PR-AUC.

This is worth reporting as a finding about synthetic-data methodology, alongside
the documented balance-column leakage in PaySim. A feature that dominates SHAP on
synthetic data should be treated as a suspect until the generator is shown to
model both classes of its behaviour.

---

## 5. Point-by-point response to the review

| # | Reviewer's point | Response |
|---|---|---|
| 1 | Formal adversarial threat model | **Accepted, and done — `docs/threat-model.md`.** The four fraud patterns were raw material, not a threat model. The document states, per control, what the attacker must be able to do, whether the required capability is attacker-controllable, and what evasion costs. It produced a result belonging in the main argument: **detection value and evasion cost are different axes**, and the second most valuable capability — session telemetry — is the cheapest to evade. |
| 2 | Exact mathematical specification of the generator | **Done — `docs/generator-spec.md`.** Every distribution and parameter stated formally; the PaySim-style parametric approach defended against copulas/GANs on the grounds that both estimate a joint distribution *from data* and no Uzbek P2P data exists to fit — a GAN trained on IEEE-CIS would reproduce e-commerce covariance under Uzbek field names. Includes an explicit list of what the generator does **not** model. `verify_spec.py` re-checks the document against the output (16/16). |
| 3 | Security-overhead benchmarking (mTLS, payload encryption) | **Half done — payload encryption measured, see §7.4.** AES-256-GCM on the event payload, decrypted inside the scored path: on matched 400-record arms the decision path is unchanged (p99 183 ms both, median CIs overlapping). A microbenchmark supplies the figure the pipeline cannot resolve — ~6.8 µs to decrypt, ~0.15% of the scoring budget. The measurable cost is **size**, about +50% per message, roughly half of it an artefact of the string deserialiser rather than of the cryptography. **Still owed:** mTLS between switch, broker and consumers, whose cost profile is per-connection rather than per-record. |
| 4 | Integrity audit — cryptographic hashing at ingress and sink | **Done.** Ingress SHA-256 over the raw event at the producer, carried through Flink untouched and bound into the audit record; a hash chain over audit records makes any alteration, deletion or reorder evident; `verify_audit.py` recomputes it. Residual (a full-table rewrite) closed by publishing the head hash externally. |
| 5 | Distinguish organic concept drift from adversarial evasion | **Accepted, sharpened, and settled in `docs/threat-model.md`.** The two are separated by *where* the shift appears: organic drift moves both classes, evasion moves the fraud class only, and only on features the adversary controls. Session timing is attacker-controllable; receiver account age is not. The document commits to a falsifiable prediction — if `COACHED_SESSION` is deployed and announced, `P(active_call = 1 | APP fraud)` should decay toward the ~3% population base rate while the legitimate rate holds. A control whose evasion is predictable in advance is a stronger claim than one whose robustness is merely asserted. |
| 6 | Non-parametric statistics for tail latency; validate exactly-once by fault injection | **Done, with the exactly-once half reframed — see §6.6 and §7.** Non-parametric tail statistics are built into `latency_report.py` (nearest-rank order statistics, distribution-free CI for the median). On exactly-once the honest answer is that **the system does not provide it and should not**: the sink is `AT_LEAST_ONCE`, ClickHouse is plain MergeTree with no deduplication, and the Redis fan-in store sits outside the checkpoint. Fault injection measured what that costs: **nothing lost (500/500), duplication 0.20%, and — the finding — the duplicate copies carry different scores**, because the fan-in store's reads do not roll back with the checkpoint. Duplicate alerts are cheap; the checkpoint-interval latency that transactional writes would add is not, against a 300 ms budget. Owed: repeated kills for a distribution rather than a single observation. |
| 7 | Spark micro-batching may miss rapid velocity attacks | **Scope changed — see §3.** The engine comparison is declined. The underlying concern is retained as a *design constraint on this system*: the detection window for the velocity and structuring rules is stated explicitly, and end-to-end latency is measured against it. If Flink's own latency exceeds the window the objection applies to this system too, and that is the version worth testing. |

Point 7 is the one requiring care in the written response. It should read as
narrowing the claim, not evading the question — the reviewer's concern about
whether the pipeline detects a burst *before settlement* is the right concern,
and it survives the reframing intact.

---

## 6. What is still owed

Ordered by what blocks what.

1. ~~**Latency measurement.**~~ **Done, then redone — see §7.** The first
   measurement was invalid: the job was running without the ONNX model and
   stamping its output as though it were not. Re-measured with the model
   verifiably loaded, the result is conditional — target met on a warm
   enrichment cache (p99 186 ms), missed on a cold one (4.19% over 300 ms).
2. ~~**Threat model** (point 1).~~ **Done — `docs/threat-model.md`.** Also
   settles point 5: drift and evasion are distinguished by whether the shift
   appears in both classes or only the fraud class, and on features the
   adversary controls. Produced one result that belongs in the main argument —
   detection value and evasion cost are different axes, and the second most
   valuable capability (session telemetry, −0.019) is the cheapest to evade.
3. ~~**Generator mathematical specification** (point 2).~~ **Done** —
   `docs/generator-spec.md`, verified against the output by `verify_spec.py`.
4. ~~**Integrity hashing at ingress and sink** (point 4).~~ **Done** —
   `integrity.py`, `verify_audit.py`. Ingress hash binds each decision to its
   event; the audit hash chain makes tampering evident and survives restarts.
5. **Security-overhead measurement** (point 3) — **payload encryption done,
   §7.4; mTLS still owed.** The payload half found no cost on the decision path
   and a ~50% cost in message size. The transport half is the one that could
   plausibly move the budget, since a handshake is per-connection and TLS record
   framing is per-message; it needs a CA, broker and client certificates, and an
   SSL listener alongside the existing plaintext one so both arms can be run
   against one deployment as the payload arms were.
6. ~~**Fault injection for exactly-once**~~ (point 6). **Done — `stream-processor/
   fault_injection.py`.** The system does not provide exactly-once and does not
   claim to: `AT_LEAST_ONCE` on the Kafka sink, a `MergeTree` with no
   deduplication, and a Redis fan-in store outside the checkpoint. So the
   questions asked were the two that matter for money — is anything lost, and
   what do the permitted duplicates cost.

   Method: baseline row count, paced stream, `docker compose kill taskmanager`
   mid-stream, recount after recovery. 500 transactions sent.

   **Nothing lost — 500 of 500 present.** Checkpointing plus committed offsets
   replayed the window between the last checkpoint and the kill. Duplication:
   1 row, 0.20%.

   **The result worth reporting is not the rate but that the duplicates
   disagree.** The two copies of the duplicated transaction carried different
   scores — 0 and 0.0012. Replay is not a pure function of the event, and the
   mechanism is in the code rather than in the observation: `ReceiverStore`
   writes are idempotent (the sorted-set member encodes `transaction_id`) but
   *reads* are not. Redis does not roll back with a Flink checkpoint, so a
   replayed transaction sees a payee window that already contains itself and
   every later transaction processed before the kill; `rcv_inflow_1h` then counts
   the transfer's own amount twice and the payee looks busier than it was.

   Consequences: "at-least-once means duplicate rows" understates it — it means
   duplicate rows that may disagree. Deduplicating by `transaction_id`
   (`ReplacingMergeTree`, or an idempotent sink) silently picks one of two
   different answers unless the version column is chosen deliberately, which
   makes that choice a policy decision rather than a schema one. And generally:
   **only state inside the checkpoint replays exactly; any feature computed from
   an external store does not.** The drift is one-directional here — replay can
   only add members to the window — so a replayed transfer looks riskier rather
   than safer, which is the benign direction but is a property of this store, not
   a guarantee.

   **Caveat on the rate.** 0.20% is a single observation, not an estimate. The
   replay window is one checkpoint interval (2 s) of traffic, so the kill landed
   almost immediately after a checkpoint. Non-parametric statistics over repeated
   kills — the other half of point 6 — are still owed.
7. **External validation.** Harnesses built and tested; both need a manual
   download (`validation/README.md`). The investigation produced a result worth
   putting in the thesis rather than in an appendix:

   **Relational fraud detection cannot be validated end-to-end on public real
   data, because the account identifiers that make it relational are exactly
   what cannot be published.** 14 of 24 features here are relational; removing
   them costs 0.959 → 0.812 PR-AUC and 0.860 → 0.624 precision. Every public
   real dataset examined is identifier-free.

   The response is to split the question:
   - **PaySim** (`paysim_adapter.py`) — the only public dataset with identifiers
     on both sides. Runs this project's own extractor and CEP rules unchanged, so
     it tests whether the relational features are an artefact of our generator.
     Synthetic, but written by others for another market.

     **Result on 500,000 TRANSFER rows (2,520 fraud): the relational feature
     transfers.** `NEW_PAYEE_HIGH_AMOUNT` — computed purely from per-sender
     history — separates the classes **4.0x** on data this project did not
     produce. `MULE_FAN_IN` finds nothing, correctly: PaySim models account
     draining straight to cash-out, with no collection stage, so fraud
     *phenomena* differ between markets even where the detection machinery
     carries over. That difference supports building a market-specific system
     rather than importing a generic one.

### RQ3, second result: additive thresholds do not survive capability loss

Found through the PaySim run rather than by design. The CEP score is additive, so
a fixed cutoff is implicitly a statement about **how many rules must agree**. Hold
it fixed while the available rules shrink and the layer does not degrade — it
goes silent.

Measured: with most capabilities unavailable, the highest score any fraud reached
was 0.35 against a 0.40 review cutoff. **0 of 2,520 fraud flagged**, while the
same rules separated the classes 4:1.

The fix carries the calibrated threshold across as a proportion of what the
weakest fraud pattern can still reach (`capabilities.scaled_threshold`):

| capability profile | weakest reachable | REVIEW |
|---|---|---|
| full | 0.70 | 0.40 (unchanged) |
| no session telemetry | 0.50 | 0.29 |
| own stream + fan-in only | 0.30 | 0.17 |

On PaySim this moved detection from 0/2,520 to 150/2,520 at 1.55% false
positives — a 3.9x decision lift, which reproduces the single available rule's
4.0x rather than exceeding it. **Scaling restores sensitivity; it does not
create signal.** A deployment that cannot observe a pattern still cannot detect
it.

The generalisable claim: *any* additively-scored rule layer deployed across
institutions with differing data access needs thresholds expressed relative to
what is observable, not as absolute constants. This is the operational
counterpart to the capability-ablation result in §4.
   - **Base rate**, cited to the ULB/Kaggle card dataset: ~0.17% against the
     generator's 1.5%. Recall is unaffected by class balance, **precision is
     not**, so precision measured on synthetic data is optimistic and must be
     reported as such.

   IEEE-CIS rejected: e-commerce, no receiver as a party, so the fan-in finding
   cannot be tested. CCF/Kaggle not usable for detection: PCA-anonymised, SHAP
   meaningless — but its base rate is usable, since that needs no feature
   semantics.

   **Zenodo 20030065 examined and rejected**, and the examination is a result in
   its own right. Published as production-collected from a live system, it does
   not match its own description: 57,394 rows against 56,962 claimed, 111 fraud
   against 98, and the per-record response latency the description promises is
   absent. Structurally, `v7..v28` are PCA components (pairwise |r| ≤ 0.10,
   medians ~0.0003, σ ≈ 1.17) while `v1..v6` are not (σ ≈ 110,000, one pair
   correlated 0.9996 — the signature of before/after balance columns, which is
   PaySim's documented leakage mode). Row and fraud counts sit within rounding of
   a 1/5 sample of ULB (284,807/5 = 56,961.4; 492/5 = 98.4; fraud rate 0.1727%
   vs 0.172% claimed).

   Reported not to accuse anyone but because **the checks are cheap and the
   failure mode is severe**: a thesis citing this as real-world validation would
   be asked, correctly, how it differs from the PCA-anonymised dataset rejected
   two pages earlier. `validation/zenodo_provenance.py` runs the check.

---

## 7. Latency: measured

### 7.0 The July figures are withdrawn

The measurements previously reported here were taken on a job that **was not
running the model**. `config.py` resolved `model.onnx` relative to `__file__`,
but `flink run --pyFiles` ships Python modules to the TaskManager and unpacks
them into a per-job temporary directory, where the ONNX file — a binary
artefact, not a pyFile — is not present. The job took its documented CEP-only
fallback path and announced it with a single INFO line among thousands of Kafka
configuration dumps. Every record it produced was still stamped
`model_version = cep+ml-fusion-v1`, so nothing downstream could tell a rules-only
run from a fused one.

Three things follow, and all three are worth stating rather than quietly fixing:

1. The stated scoring cost — "rules, ONNX inference, Redis and Neo4j lookups" —
   never included inference.
2. A degraded run was indistinguishable from a healthy one **in the warehouse**,
   which is where such a claim would normally be audited.
3. The failure was legible only by reading a container log at the moment of
   submission.

Fixed by resolving deploy-time artefacts against the mounted job directory
first, by stamping `MODEL_VERSION_CEP_ONLY` when the session is absent, and by
making the fallback announce itself in a banner. Every figure below was taken
after confirming in ClickHouse that the run was fused:
`model_version = cep+ml-fusion-v1`, `countIf(ml_score IS NULL) = 0`.

### 7.1 Re-measured, with the model verifiably loaded

Live stack, producer inside the Docker network, paced replay. Order statistics,
distribution-free CI for the median.

**The <300 ms target applies to the decision, not to the warehouse write.** The
decision leaves for the switch on `fraud.alerts`; ClickHouse is where it is
queried afterwards and has no real-time requirement. Holding the reporting stack
to a real-time budget would be measuring the wrong thing.

Two runs, differing in one variable — the state of the receiver-age enrichment
cache in Redis.

**Run A — warm cache** (1,602 transactions, ~6 events/s):

| stage | median | p95 | p99 | max |
|---|---|---|---|---|
| ingest -> decision | 80 ms | 138 ms | **186 ms** | 570 ms |
| of which scoring work | 2.3 ms | 5.1 ms | 11.8 ms | 323.7 ms |
| decision -> ClickHouse | 33.2 s | 69.5 s | 75.3 s | 77.8 s |

**Target met: 6 of 1,602 (0.37%) exceeded 300 ms.**

**Run B — cold cache** (430 transactions, ~4 events/s, `age:*` keys deleted
immediately beforehand):

| stage | median | p95 | p99 | max |
|---|---|---|---|---|
| ingest -> decision | 89 ms | 223 ms | **1533 ms** | 1567 ms |
| of which scoring work | 7.4 ms | 19.0 ms | 39.5 ms | 1452.5 ms |
| decision -> ClickHouse | 20.2 s | 63.8 s | 71.7 s | 72.4 s |

**Target NOT met: 18 of 430 (4.19%) exceeded 300 ms.**

**A conclusion withdrawn on further runs.** Run B was first read as "the target
is met warm and missed cold". Three later cold-cache runs — 209, 400 and 400
records — breached 0, 0 and 1 times respectively. Cold cache is therefore *not*
sufficient to miss the target. What run B had that the others did not was a
1452 ms stall, and §7.3 explains how one stall becomes eighteen breaches.

The corrected statement is narrower and better supported: **a cold enrichment
cache raises the cost of scoring measurably (§7.2) but stays inside the budget;
the target is missed only when a stall occurs, and the stall's cause is
unidentified (§"What is not yet resolved")**. The single observation that
suggested otherwise was over-read, which is the same error the reviewer's point
6 warns about in a different context — and worth recording rather than
overwriting, since the first reading survived long enough to be written down.

### 7.2 The enrichment cache is the variable, and the prototype flatters it

`enrichment.py` looks up receiver account age in Neo4j, cached in Redis with a
1-hour TTL, **synchronously inside `process_element`**. Its own docstring already
notes that production would use Flink async I/O. That recommendation now has a
measurement behind it rather than an intuition.

Cost of a lookup, within run B:

| | n | median | p95 | p99 | max |
|---|---|---|---|---|---|
| cache hit | 27 | 3.01 ms | 6.10 ms | 8.44 ms | 9.20 ms |
| cache miss | 403 | 7.62 ms | 19.16 ms | 39.43 ms | 1452.54 ms |

A miss costs ~4.6 ms at the median and ~31 ms at p99. The graph lookup is
index-backed (`person_pinfl`, confirmed in the Neo4j log), so this is a network
round trip, not a scan.

The warming is visible directly. Median `scoring_ms` per 10-second bucket across
run B, from a cold start:

```
14.48  12.32  11.49  8.65  10.96  8.32  7.41  7.87  6.25  7.34  6.70  5.68
```

**The prototype's hit rate is an artefact of its scale.** Run A executed against
2,879 cached `age:*` keys over a population of 5,200 persons — better than half
of every receiver that could possibly appear. A few thousand synthetic
transactions saturate a five-thousand-person graph; a bank with millions of
accounts and a one-hour TTL will not see that hit rate, so the realistic
per-event scoring cost sits nearer run B's miss column than run A's headline.
Quoting 2.3 ms without that qualification would export a property of the test
population as a property of the system.

### 7.3 Two stalls became eighteen breaches

Run B contained exactly **two** records with `scoring_ms > 300`, yet **eighteen**
records breached the 300 ms decision-path target. The Python worker processes
records serially, so a single multi-second stall delays everything queued behind
it. Head-of-line blocking is what converts a rare stall into a target-breach
rate, and it is a property of the execution model rather than of the stall's
cause — a system whose p99 depends on its worst individual record needs either
parallelism or an interruption-free critical path, and this one has neither.

### 7.4 Security overhead I: payload encryption (reviewer point 3)

AES-256-GCM applied to the event payload at the producer and reversed inside
`process_element`, so the cost falls inside the measured `scoring_ms` bracket
rather than beside it. Both arms are 400 records at 3 events/s with the
enrichment cache flushed immediately beforehand, and both are consumed by one
job binary — the envelope carries a magic prefix, so encrypted and plaintext
records are discriminated per record without a redeploy.

| | plaintext | AES-256-GCM |
|---|---|---|
| ingest -> decision, median | 88 ms | 87 ms |
| p95 | 147 ms | 150 ms |
| **p99** | **183 ms** | **183 ms** |
| over 300 ms | 0 / 400 | 1 / 400 |
| scoring, median (95% CI) | 5.7 ms [5,6] | 5.0 ms [5,5] |
| scoring p95 | 12.6 ms | 13.8 ms |

**No detectable cost on the decision path.** The p99 figures are identical and
the median confidence intervals overlap. The one breach on the encrypted arm is
a 1030 ms stall of the kind discussed above, present on plaintext runs too.

"No detectable" is not "none", and the difference matters for how this is
reported. A controlled microbenchmark on the same envelope gives the actual
figure: **~8.9 µs to encrypt and ~6.8 µs to decrypt**, against a JSON
round-trip of ~7.7 µs on the same event. Decryption is therefore of the same
order as the parsing the pipeline already does, and roughly 0.15% of a 5 ms
scoring budget — three orders of magnitude below the resolution of an
end-to-end measurement. The pipeline result confirms the microbenchmark by
failing to see it.

Two disclosures the number needs:

- **The scoring median came out lower on the encrypted arm** (5.0 vs 5.7 ms),
  which is the wrong direction for an added cost. The cause is known and was
  designed in: the job keys the stream by sender before scoring, and on the
  encrypted arm that extracts an authenticated clear routing field by string
  split, where the plaintext arm parses JSON. Partitioning is cheaper under
  encryption. Both effects are sub-millisecond and neither is resolvable here,
  but the asymmetry is real and favours the encrypted arm.
- **The measurable cost is size, not time: about +50% per message.** The
  envelope adds 28 fixed bytes and base64 adds a third on top. Roughly half of
  that inflation is an artefact of `SimpleStringSchema`, which decodes records
  as UTF-8 and so cannot carry raw ciphertext; a binary deserialiser would pay
  the same CPU and none of the expansion. The figure is an upper bound on
  transport cost and an accurate one for compute. It does not appear in the
  latency figures because the decision path is dominated by buffer intervals,
  not by message volume, at this rate.

The regulatory reading is the useful one. TLS 1.3 and AES-256-GCM are already
scoped in the compliance analysis; what was unknown was whether the real-time
budget could absorb them. For payload encryption the answer is that it can,
with the caveat that the cost shows up in bandwidth and storage rather than in
the 300 ms budget — and that a keyed topic still discloses metadata, since the
partitioning key must stay readable to the broker.

Transport security (mTLS between the switch, the broker and the consumers) is a
separate measurement with a different cost profile — per-connection handshakes
rather than per-record work — and is not yet done.

### The work was never the constraint

Scoring — rules, ONNX inference, Redis and Neo4j lookups — is 2.3 ms at the
median and 12 ms at p99 warm, 7.4 ms and 40 ms cold, against a 300 ms budget.
Everything else in the figure is framework buffering, and each default that
mattered was chosen for throughput:

| setting | default | set to | why the default hurts |
|---|---|---|---|
| `python.fn-execution.bundle.time` | 1000 ms | 50 ms | PyFlink batches records before crossing into Python. Below ~100k events/s the bundle never fills, so every record waits the full second. This alone was 1923 ms of the original 1931. |
| `fetch.max.wait.ms` | 500 ms | 20 ms | A fetch against an empty topic parks for the full interval. |
| checkpoint interval | 30 s | 2 s | With AT_LEAST_ONCE the Kafka sink flushes at checkpoint barriers, so nothing leaves the job between them. |
| `KafkaOffsetsInitializer` | `earliest()` | `committed_offsets(EARLIEST)` | Not latency: every restart replayed the whole topic and re-scored settled transactions, raising duplicate alerts. |

This is the engineering finding worth reporting: **a streaming fraud pipeline
assembled from components whose defaults are tuned for throughput and
reprocessing will miss a sub-second target by two orders of magnitude, and none
of that is visible in model inference time.** Latency estimated from the model
alone would have been off by a factor of 400.

The re-measurement sharpens it. The correction for the missing model went the
*opposite* way to expectation: adding ONNX inference to the critical path
**lowered** median scoring from 4.2 ms to 2.3 ms, because the July figure was
never dominated by inference in the first place — it was dominated by the
enrichment lookups that 7.2 has now isolated. Inference on 22 features is not
measurable against this budget. What is measurable is every I/O hop the model
sits behind, which is the reverse of where optimisation effort is usually
directed in an ML system.

### A measurement artefact that nearly became a finding

Three rounds of tuning failed to move a stable p95 of ~640 ms. It was not
latency. `ingested_at` was stamped by the producer on the Windows host while
`scored_at_job` came from inside Docker, and on Windows and macOS containers run
in a VM whose clock drifts from the host and is periodically resynced. Measured
offsets minutes apart: **+205 ms, then −279 ms** — a swing comparable to the
quantity being measured.

Running the producer inside the Docker network removed it: the tail collapsed
from 644 ms to 159 ms at p95 with no configuration change at all.

Worth stating in the methodology section: measuring sub-second latency across
two unsynchronised clocks produces a systematic error of the same order as the
measurement, and the desktop-Docker setup where most prototypes are evaluated is
exactly the configuration that has this problem. `latency_report.py` now probes
the offset and reports it. Offsets observed since: **+219 ms** and **−270 ms** on
consecutive runs.

### The same platform, a second way

The clock drift is not the only thing the virtualisation layer does to a
measurement. Neo4j ships a monitor for it, and it fired on this stack:

```
WARN [o.n.k.i.c.VmPauseMonitorComponent] Detected VM stop-the-world pause:
     {pauseTime=254, gcTime=0, gcCount=0}
```

`gcCount = 0`: no garbage collection ran. The virtual machine hosting the
containers stopped executing for 254 ms and every process inside it stopped
together. On a 300 ms budget that is a target breach caused by nothing in the
system under test.

Generalised, and this belongs in the methodology chapter rather than the
results: **desktop virtualisation perturbs sub-second measurements by amounts
comparable to the measurements themselves, and it does so in at least two
independent ways — by moving the clocks and by stopping the processes.** The
first was found by chasing a tail that would not move; the second by a
monitor that happened to be running in a component nobody was looking at.
Neither is visible from inside the application. A sub-second latency claim
produced on desktop Docker needs to say so.

### What is not yet resolved

- **Warehouse path is 20–33 s**, and the figure moved in the wrong direction
  after the graph sink was repaired (§8): it had been 15.6 s while the Neo4j
  writer was silently disabled and doing no work. The two numbers are therefore
  not comparable, and the current one has not been diagnosed. No real-time
  requirement rides on it, but Grafana dashboards inherit the lag.
- **A rare multi-second stall in scoring**, 1452 ms in run B, 323 ms in run A.
  Three hypotheses were tested and rejected or left open: it is **not** ONNX
  warm-up (the stalls occur late in a run, not on the first records); it is
  **not** enrichment cache misses (the worst stall fell in the cache-hit
  bucket); and it is **not** a Neo4j pause (that component's own pause monitor
  logged nothing at the time). VM-level pause and Python worker GC remain
  plausible and were not separated — with n=2 any attribution would be fitted
  rather than measured. A thirty-minute run would be needed to characterise it.
- Single machine. Security overhead (reviewer point 3) still has to be measured
  against this baseline, which is now the only baseline taken with the model
  actually running.

## 8. Silent failure modes

Seven distinct failures were found in one working session. Six of them left the
system looking healthy: containers `Up`, no exception anywhere, Kafka offsets
advancing, the producer reporting success. They are collected here because the
pattern is the result, not the individual bugs.

| # | failure | what an operator saw | what it actually did |
|---|---|---|---|
| 1 | ClickHouse not accepting queries when sink-writer connected at boot (`depends_on: service_started` waits for the container, not the service) | nothing — one WARNING at startup, then silence | sink disabled for the life of the process; `flush()` cleared its buffer with no log. **331 events consumed, offsets committed, 0 rows stored** |
| 2 | Neo4j losing the same startup race | same | every alert's graph write discarded; `MULE_FAN_IN` querying a graph that was never written |
| 3 | `model.onnx` unreachable under `--pyFiles` | one INFO line among thousands | job scored CEP-only while stamping `model_version = cep+ml-fusion-v1` — §7.0 |
| 4 | jobmanager restart (session cluster, no HA) | **entire stack healthy**, producer succeeding | no job running at all; only symptom was consumer lag climbing |
| 5 | resubmission without `-s` | job RUNNING, offsets committed, nothing re-read | empty keyed state — velocity and structuring windows blank, history-dependent rules unable to fire until refilled |
| 6 | producer stopped with Ctrl+C | "produced N messages" never printed | `KeyboardInterrupt` bypassed `producer.flush()`; buffered messages never sent, and in the fault-injection run they would have been counted as **transactions lost** — manufacturing the exact correctness failure the experiment exists to rule out |
| 7 | unpinned dependencies | build succeeded | one rebuild moved three libraries across major versions with no change to the repository, so the code no longer matched the environment its results were measured on |

Only the eighth — a checkpoint directory created root-owned by the volume mount
while Flink runs as `flink` — failed loudly, with a stack trace at submission.
It was the least costly of the eight and the fastest to fix.

**The claim.** In a streaming fraud pipeline the dangerous failures are not the
ones that stop it. They are the ones that leave every health indicator intact
while the system silently stops doing part of its job — and the indicators an
operator naturally reaches for (containers running, no errors, offsets moving,
rows arriving with a plausible model version) are exactly the ones that stay
green. Two of these had been present for weeks and were invisible to a working
dashboard.

Three design consequences, each of which was implemented:

- **"Fails open" must mean "keeps running and says so."** A component that
  disables itself to protect availability has to retry, and has to log the loss
  with a running total. Discarding buffered records at WARNING once and then
  silently thereafter is the difference between degraded and dishonest.
- **Provenance must be recorded from what ran, not from what was configured.**
  `model_version` now reflects whether an ONNX session actually existed. A field
  that reports intent rather than outcome is worse than no field, because it
  survives audit.
- **Reproducibility is a property of the environment, not of the seed.** Pinning
  every dependency is what makes "seed = 42" mean the same thing next year.

There is also a measurement-integrity point that belongs with the methodology:
failure 6 would not have broken the pipeline. It would have broken the
*experiment*, by producing evidence of data loss that never happened. An
instrument that fails in the direction of its own hypothesis is the one worth
checking first.

## 9. Notes for the written revision

- Every quoted figure needs its interval. Baseline PR-AUC varies by ±0.008–0.035
  across generator seeds depending on configuration, which is wider than most of
  the effects being measured. Single-run numbers were wrong twice during
  development — once by a factor of two, once in sign.
- ROC-AUC ≈ 0.999 is a synthetic-data artefact and should be disclosed as such
  wherever it appears, next to PR-AUC, which is the meaningful figure at a 1.5%
  positive rate.
- The claim "0 false positives on 775 legitimate journeys" is the reportable
  result for impossible travel. The detection rate on injected hijacks is **not**
  — the generator and the detector share a reachability threshold, so that half
  is true by construction and should be stated as such.
