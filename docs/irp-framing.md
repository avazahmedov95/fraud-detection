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
CI for the mean delta, 5 seeds unless stated. Baseline 0.960 ± 0.018. Synthetic
data — design targets, not validated findings.

| Data source removed | Δ PR-AUC (95% CI) | sign | verdict |
|---|---|---|---|
| receiver-side aggregation | −0.036 [−0.068, −0.004] | 5/5 | real |
| mobile-app session telemetry | −0.034 [−0.051, −0.016] | 5/5 | real |
| receiver account age | −0.022 [−0.040, −0.004] | 4/5 | real |
| MyID kinship (added) | +0.002 [−0.000, +0.004] | 4/5 | negligible |
| device identity | +0.002 [−0.001, +0.005] | 4/5 | negligible |
| payee keyed by person, not card | +0.001 [−0.004, +0.007] | 2/5 | negligible |
| geo telemetry | +0.000 [−0.002, +0.002] | 2/5 | negligible (see caveat) |

**Channel identity is no longer a row here.** It measured −0.002 across five
seeds, fed no rule, and is a concept no public dataset carries, so no external
evidence could ever have arrived. It was removed on 07.09.2026 — the four
features, the wire field, the ingress hash input, the warehouse column and the
dashboard panel that read it. Recorded rather than deleted quietly, because a
capability that leaves the registry takes its measurement history with it, and
the reason it left is the measurement.

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
+0.001 PR-AUC [-0.001, +0.003] over 5 seeds - an interval straddling zero, so
no measurable effect at all. It read +0.004 [+0.001, +0.007] on the 2026-07-19
dataset, which was already negligible; the 2026-09-07 regeneration removed even
that.

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
| 3 | Security-overhead benchmarking (mTLS, payload encryption) | **Done — both halves plus the churn arm.** Payload encryption, see §7.4: AES-256-GCM on the event payload, decrypted inside the scored path: on matched 400-record arms the decision path is unchanged (p99 183 ms both, median CIs overlapping). A microbenchmark supplies the figure the pipeline cannot resolve — ~6.8 µs to decrypt, ~0.15% of the scoring budget. The measurable cost is **size**, about +50% per message, roughly half of it an artefact of the string deserialiser rather than of the cryptography. **Transport half now measured too, see 7.5:** four counterbalanced arms show no transport effect that survives the ordering - the cost sits below a per-arm drift of about 4 ms - while the 300 ms target is met at p99 in every arm. **Churn arm done, see 7.5a:** a microbenchmark puts one mutual-TLS handshake at +11.2 ms over plaintext, and four further arms with 60 reconnects each still show no cost on the decision path - the handshake is paid by the client before the measured clock starts, so it lands on the switch's latency budget and not on the bank's. |
| 4 | Integrity audit — cryptographic hashing at ingress and sink | **Done.** Ingress SHA-256 over the raw event at the producer, carried through Flink untouched and bound into the audit record; a hash chain over audit records makes any alteration, deletion or reorder evident; `verify_audit.py` recomputes it. Residual (a full-table rewrite) now closed in practice: the head hash of the reference run is published in `docs/audit-anchors.md` and pushed to the remote, which separates custody of the value from custody of the database. Not a cryptographic timestamp, and recorded as the weaker option it is. |
| 5 | Distinguish organic concept drift from adversarial evasion | **Accepted, sharpened, and settled in `docs/threat-model.md`.** The two are separated by *where* the shift appears: organic drift moves both classes, evasion moves the fraud class only, and only on features the adversary controls. Session timing is attacker-controllable; receiver account age is not. The document commits to a falsifiable prediction — if `COACHED_SESSION` is deployed and announced, `P(active_call = 1 | APP fraud)` should decay toward the ~3% population base rate while the legitimate rate holds. A control whose evasion is predictable in advance is a stronger claim than one whose robustness is merely asserted. |
| 6 | Non-parametric statistics for tail latency; validate exactly-once by fault injection | **Done, with the exactly-once half reframed — see §6.6 and §7.** Non-parametric tail statistics are built into `experiments/latency.py` (nearest-rank order statistics, distribution-free CI for the median). On exactly-once the honest answer is that **the system does not provide it and should not**: the sink is `AT_LEAST_ONCE`, ClickHouse is plain MergeTree with no deduplication, and the Redis fan-in store sits outside the checkpoint. Fault injection measured what that costs: **nothing lost (500/500), duplication 0.20%, and — the finding — the duplicate copies carry different scores**, because the fan-in store's reads do not roll back with the checkpoint. Duplicate alerts are cheap; the checkpoint-interval latency that transactional writes would add is not, against a 300 ms budget. Repeated kills now done: six jittered kills on a clean warehouse give a median duplication of 0.89% [0.40%, 1.38%], nothing lost in any of twelve kills across two series, and a divergence magnitude - 3 of 26 duplicates, at most 0.0003, no decision changed - that the original single observation could not supply. |
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
5. ~~**Security-overhead measurement** (point 3).~~ **Both halves done —
   payload encryption in §7.4, transport in §7.5.** The payload half found no
   cost on the decision path and a ~50% cost in message size. The transport half
   was expected to be the one that could move the budget, since a handshake is
   per-connection and TLS record framing is per-message. Four counterbalanced
   arms say otherwise: the sign of the transport difference reverses with the
   order of the arms, so the cost sits below a per-arm drift of about 4 ms,
   while the 300 ms target is met at p99 in every arm. The churn arm that was
   owed is now run (§7.5a): a handshake costs +11.2 ms measured directly, and
   four arms reconnecting every 20 messages still show no cost on the decision
   path — because the handshake is paid before the measured clock starts, it is
   the switch's latency budget it comes out of, not the bank's.
6. ~~**Fault injection for exactly-once**~~ (point 6). **Done — `stream-processor/
   experiments/outage.py`.** The system does not provide exactly-once and does not
   claim to: `AT_LEAST_ONCE` on the Kafka sink, a `MergeTree` with no
   deduplication, and a Redis fan-in store outside the checkpoint. So the
   questions asked were the two that matter for money — is anything lost, and
   what do the permitted duplicates cost.

   Method: baseline row count, paced stream, `docker compose kill taskmanager`
   mid-stream, recount after recovery. 500 transactions per round, six rounds
   against a warehouse emptied beforehand.

   **Nothing lost — 500 of 500 in every round.** Across eighteen kills in three
   series, not one transaction was lost. Checkpointing plus committed offsets
   replayed the window between the last checkpoint and the kill.

   **Duplication, six kills: median 0.89%, distribution-free 96.9% interval
   [0.40%, 1.38%]** (for six order statistics that interval is the range).
   Per round: 0.40, 1.38, 0.79, 0.40, 0.99, 1.19 per cent, i.e. 2 to 7 rows in
   500. The count is the traffic between the last completed checkpoint and the
   kill, and which value comes up is decided by where the kill lands in the
   cycle - a phase nobody controls in production either.

   **A third series, 2026-09-13, on the regenerated dataset** (fresh job, empty
   warehouse): 1.20, 0.60, 0.00, 0.20, 1.60 and 3.20 per cent - 6, 3, 0, 1, 8 and
   16 rows in 500. **Median 0.90%**, against 0.89% before; the 96.9% interval
   widens to [0.00%, 3.20%].

   Round six's 16 rows broke what this section used to call "a ceiling near
   ten", and the ceiling was the error. It was 2 s at about 5 events/s, which
   assumes an even stream, and the paced replay is not one: at 200x the
   dataset's own bursts compress into seconds, and the busiest 2 s of each
   500-row slice held 19 to 30 events. Sixteen sits inside that. Nor does the
   count simply follow the traffic just before the kill - round three had 21
   events in the 2 s before its kill and produced no duplicates at all. Phase
   decides, as the paragraph above says; only the bound was wrong.

   **The result worth reporting is not the rate but that the duplicates can
   disagree — and how much, which is a separate question from whether.**

   On the clean six-round series, 26 fault-induced duplicate rows produced
   **3 with a different final score, a largest divergence of 0.0003, and no
   decision change at all**. The mechanism below is confirmed; its operational
   magnitude on this traffic is small, and both halves have to be said.

   The third series moved the magnitude, not the conclusion: 34 duplicate rows,
   **6 with a different final score, a largest divergence of 0.0076** -
   twenty-five times the earlier maximum - and again **no decision change**.
   That is the direction the mechanism below predicts wherever receivers are
   more concentrated. Which payees the diverging copies went to was not checked,
   so the attribution stays a prediction rather than a finding.

   The reason it is small is the reason it matters. Divergence needs the payee's
   window to be non-empty: the replayed transfer counts its own amount twice in
   `rcv_inflow`, and on a 500-record slice across 5,200 people most payees
   receive exactly once, so the double count moves almost nothing. **The size of
   the disagreement scales with receiver-side concentration** - with the fan-in
   structure the system exists to detect. On a mule payee with six inbound
   transfers in the hour it would be large. An earlier measurement taken against
   a warehouse still holding the security-overhead arms' deliberate re-sends
   showed divergences up to 0.9275 and 18 changed decisions; those figures
   belong to replays hours apart with full windows, not to the injected fault,
   and are quoted here only as the ceiling of the same mechanism.

   The original single-kill observation of two copies scoring 0 and 0.0012: The two copies of the duplicated transaction carried different
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

   **What the repetition corrected, and how.** The previously reported 0.20%
   was a single observation, and it turned out to be the MINIMUM of the range -
   the kill had landed almost immediately after a checkpoint. Two method errors
   surfaced on the way to replacing it, both worth recording:

   - A first six-round series killed at a FIXED offset and returned 1.96, 1.38,
     1.19, 0.60, 0.20, 0.20 per cent - strictly non-increasing, which six
     independent draws do once in 720 times. A fixed offset does not sample the
     checkpoint phase, it tracks it. The offset is now drawn per round.
   - Score divergence and decision changes are computed over the whole table,
     not the round's delta, so a warehouse left populated by earlier runs
     swamps them. The series now refuses to start against a non-empty table.

   **What exactly-once would have cost, since it was not attempted.** Three
   routes exist and they are not equivalent:

   - `ReplacingMergeTree` on ClickHouse keyed on `transaction_id`. The cheapest,
     but deduplication is *eventual*: a query issued before the merge still sees
     both rows, so the guarantee is about the table's eventual state and not
     about what an analyst reads.
   - `DeliveryGuarantee.EXACTLY_ONCE` on the Kafka sink. Transactional writes,
     paid for by consumers reading only committed data - which adds the whole
     checkpoint interval to end-to-end latency, 2 s here against a 300 ms
     budget.
   - An idempotent sink keyed on `transaction_id`. Correct and costs no latency,
     but needs a deduplication store sized to the replay window.

   For a fraud pipeline the second is the wrong trade, and that is the reason
   `AT_LEAST_ONCE` is not an oversight: a duplicate alert is cheap, a two-second
   delay before a decision is not. The first and third are the defensible routes,
   and the audit chain in `sink-writer/integrity.py` already gives a way to
   identify duplicates after the fact.

   Non-parametric statistics over repeated kills, the other half of point 6, are
   no longer owed.
7. **External validation.** Harnesses built and tested; both need a manual
   download (`validation/README.md`). The investigation produced a result worth
   putting in the thesis rather than in an appendix:

   **Relational fraud detection cannot be validated end-to-end on public real
   data, because the account identifiers that make it relational are exactly
   what cannot be published.** 14 of 20 features here are relational; removing
   them costs 0.937 → 0.761 PR-AUC and 0.928 → 0.563 precision on the baseline profile. Every public
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

### RQ3, third result: a rule threshold can encode the population it was tuned on

The same claim has a second half, found the same way - by running the rules on
someone else's data - and it concerns not *which capabilities exist* but *what
the traffic looks like*.

`MULE_FAN_IN` fires at six distinct senders converging on one payee within an
hour. On IBM AMLSim (`validation/README.md` §3) that rule fired on **3.12% of
legitimate traffic and caught 0.0% of the fan-in typology**: in a scale-free
transaction graph 2.69% of receiver-days exceed six senders as ordinary hub
behaviour. The constant was not arbitrary and it was not wrong - it was an
**unstated assumption about the density of the population it was tuned on**,
and it travelled silently.

Replacing it with a quantile of the population's own live distribution
(`MULE_FAN_IN_MODE = relative`, `rules.PopulationBaseline`) makes that
assumption explicit.

**Where this runs, stated before the numbers.** The measurement below is taken
through `experiments/replay.py`, which drives the deployed `rules.evaluate` unchanged -
so it is the real rule layer, not a reimplementation, and the baseline it passes
is the in-process `rules.PopulationBaseline`.

It is **also** reachable in the Flink job, which was not true when this section
was first written. The baseline is population-wide state and the stream is keyed
by sender, so it went to Redis beside `ReceiverStore` for the same reason the
receiver window did: `receiver_store.PopulationStore`, opened and passed
unconditionally by `fraud_job.py` (inert in `absolute` mode). A per-worker
histogram was rejected rather than merely avoided - it would hold a *partition*
baseline, so two workers would judge identical transactions differently. Writes
are batched and reads cached, because a `HINCRBY` or `HGETALL` per event is a
Redis round trip on the 300 ms path.

Two fallbacks to the absolute constant remain, and both announce themselves once
per process rather than happening silently: Redis unreachable
(`PopulationStore.threshold`), and `relative` set with no baseline object passed
at all (`rules._warn_relative_without_baseline`). A third is deliberate and
quiet - fewer than `MULE_FAN_IN_MIN_OBS` observations - because a quantile of
5,000 receivers is not yet a population property.

Two things follow from the measurement, and the first matters more.

**The learned threshold reproduces the hand-set one at home.** On the project's
own data the quantile lands on 5 at q=0.9995 and 7 at q=0.9999, bracketing the
six that was chosen by hand: the constant had been encoding roughly the 99.97th
percentile of this population all along. Nothing about home behaviour changes;
what changes is that the quantity is now named.

**And it is measurably better at home, at no cost in alerts.** Five generator
seeds, each replayed under both modes, deltas paired within seed
(`stream-processor/experiments/replay.py fan-in-mode`, q=0.999):

| | delta | 95% CI | sign |
|---|---|---|---|
| MULE recall | **+6.9 pp** | [+4.2, +9.7] | 5/5 |
| overall fraud recall | +1.8 pp | [+1.1, +2.5] | 5/5 |
| false-positive rate | +0.01 pp | [−0.01, +0.02] | 2/5 |

The recall intervals exclude zero; the false-positive interval contains it.
That is the only shape in which such a claim is worth making - a recall gain
bought with alerts is a threshold move, and the decision layer already has a
knob for that. The mechanism is the additivity above: `W_MULE_FAN_IN` is 0.35
against a REVIEW cutoff of 0.40, so the rule never decides alone. Lowering its
threshold adds hits where another rule has already fired and adds nothing on
isolated legitimate traffic.

**Two limits, stated because they bound the claim.** The quantile 0.999 was
chosen by a three-value sweep on the frozen dataset *before* these seeds
existed; the seeds establish that the effect survives across generator seeds at
a fixed quantile, not that 0.999 is optimal. And all five seeds come from one
generator, so what is demonstrated is that the threshold **adapts**, not that
it adapts correctly to a foreign institution. The AMLSim run cannot supply that
second test: there the sign was inverted - SAR receivers had *fewer* senders,
because AML layering deliberately spreads collection - and a high-tail quantile
cannot and should not repair a sign flip between two different phenomena.
   - **Base rate**, cited to the ULB/Kaggle card dataset: ~0.17% against the
     generator's 1.5%. Recall is unaffected by class balance, **precision is
     not**, so precision measured on synthetic data is optimistic and must be
     reported as such.

   IEEE-CIS rejected: e-commerce, no receiver as a party, so the fan-in finding
   cannot be tested. CCF/Kaggle not usable for detection: PCA-anonymised, SHAP
   meaningless — but its base rate is usable, since that needs no feature
   semantics.

   **Zenodo 20030065 examined and rejected**, and the examination is a result in
   its own right. Published as production-collected from a live system, the file
   holds 57,394 rows against 56,962 claimed and 111 fraud against 98, and the
   per-record response latency the description promises is absent. The count
   mismatch has an exact explanation and it is not miscounting: partitioning on
   the shape of `transaction_id` isolates a block of **56,962 rows with 98
   fraud — precisely as described** — plus 432 rows appended after publication,
   carrying no `test_date`, timestamps months past the dataset's own window, a
   PaySim-shaped feature schema in slots named for PCA components, two
   probability scales in one column, and what appear to be the testers' real IP
   addresses. The dataset is what it says it is; the release shipped somebody's
   live demo session on top of it. Structurally, `v7..v28` are PCA components (pairwise |r| ≤ 0.10,
   medians ~0.0003, σ ≈ 1.17) while `v1..v6` are not (σ ≈ 110,000, one pair
   correlated 0.9996 — the signature of before/after balance columns, which is
   PaySim's documented leakage mode). Row and fraud counts sit within rounding of
   a 1/5 sample of ULB (284,807/5 = 56,961.4; 492/5 = 98.4; fraud rate 0.1727%
   vs 0.172% claimed).

   Reported not to accuse anyone but because **the checks are cheap and the
   failure mode is severe**: a thesis citing this as real-world validation would
   be asked, correctly, how it differs from the PCA-anonymised dataset rejected
   two pages earlier.

---

### RQ3, fourth result: the payee is not an identity the deploying bank holds

Every receiver-side signal in this system - `is_new_payee`, the fan-in window,
the account-age lookup - was keyed on the payee's PINFL. A card-to-card P2P
transfer does not carry one. It reaches the sending bank as a destination PAN;
resolving that PAN to a person is a core-banking lookup the bank can perform
only for **its own** clients.

How large "its own clients" is, is measurable rather than arguable. Weighting by
the real card market - 69.0 million cards across 34 banks, largest share 16.3% -
the probability that two randomly paired parties bank at the same institution is
`sum(p_i^2)` = **6.85%**, and the generated stream realises **6.73%**. So the
identity the whole receiver-side contour was built on is available on roughly
one transfer in fifteen.

This has a consequence for the literature, not just for the implementation. The
receiver's account age is among the most-cited single features in APP-fraud
work; here it is **unobtainable on 93% of transfers**, not because the technique
is weak but because the UzCard/HUMO switch carries no account-age field and the
card market is fragmented. That is a quantitative argument for resolution at the
national-platform level, and it arrives from the same direction as the
threshold result above: what is detectable is set by the deployment's position
in the payment topology, not by the model.

**The obvious repair does not work, and the failure is instructive.** Resolving
to PINFL where the bank *can* and falling back to the PAN otherwise seems
strictly better - more information wherever it is available. Measured on 50,000
transactions with the window and threshold unchanged:

| receiver key | `MULE_FAN_IN` fired | of which fraud |
|---|---|---|
| PINFL throughout | 24 | 23 |
| PAN throughout | 24 | 23 |
| PINFL where on-us, else PAN | **19** | **19** |

The mixed key loses **17.4% of the rule's true positives** and gains nothing.
The mechanism: the key then depends on the **sender's** bank, so one payee's
inbound window is split across two Redis keys by a property that is not about
the payee at all. A mule collecting from senders at several institutions
accumulates in neither bucket. A key derived from the pair cannot aggregate over
one side of it.

PINFL and PAN keying are identical here because the generated population holds
exactly one card per person. That equality is a property of the data, not a
finding: a real customer holds several cards, and PAN keying then splits a mule
who spreads inbound transfers across their own cards into as many fan-in buckets.
So the honest default *understates* what a bank-level deployment can see, and
`capabilities.payee_identity = pinfl` exists to quantify what platform-level
resolution would add. `test_payee_identity.py` asserts the limitation rather
than describing it, so it cannot quietly stop being true.

**A smaller instance of the same class.** The BIN table is maintained by hand
and the population was generated against an earlier version of it; a bank that
has since closed (Yangi Bank, BIN 986040) therefore appears in the traffic and
resolves to no issuer - 1.10% of card sides. This is the ordinary operating
condition of any BIN table, which always lags the cards in circulation, and it
is why `is_on_us` requires a non-empty issuer on **both** sides: two unresolvable
BINs are not evidence of a shared institution. The unresolvable set is kept as a
named list (`bins.RETIRED_BINS`) so that a row deleted by accident still fails a
test while a genuinely closed bank does not.

## 7. Latency: measured

Condensed on 2026-09-15 to the figures and what each supports. The full log -
every run, including the readings later withdrawn - is in git history:
`git show 1ebde33:docs/irp-framing.md`.

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

### 7.2 The enrichment cache is the variable, and the prototype flatters it

Receiver age is looked up in Neo4j, cached in Redis for an hour, synchronously
inside `process_element`. A miss costs 7.62 ms at the median against 3.01 for a
hit, and ~31 ms more at p99. A few thousand transactions saturate a
five-thousand-person graph, so the prototype's hit rate is a property of its
scale; a bank with millions of accounts sits nearer the miss column.

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
climbs sixty-fold: records queue behind the synchronous Redis and Neo4j lookups
in `enrichment.py`, one at a time per slot. Flink async I/O is the documented
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
alarms against 10, where the -1 had raised 95 (`ml/README.md`). Drain was fast
in every arm here because the dependency was already down when the worker opened
its clients; 7.7a is the other half of the same code path.

## 8. Silent failure modes

**Twenty-first: the session cluster leaks a little on every redeploy, and the
eighth one kills it.** Every job submitted to the Flink session cluster loads its
own user-code classloader into JVM Metaspace, and cancelling the job does not give
it back. On 2026-09-13 one TaskManager process ran eight jobs in succession - the
dependency matrix had just been changed to restart the job before every pass - and
two seconds after the eighth was submitted it died: `java.lang.OutOfMemoryError:
Metaspace` in `FlinkUserCodeClassLoader`, against the default 256 MB. Flink's own
message offers two readings, a job that needs more metaspace or a class-loading
leak. One job runs for hours in that budget and the eighth submission exhausts it,
which is the leak's signature rather than a large job's. It is not root-caused
here.

Three things make it worth the catalogue.

*It is silent several times over.* The container's restart policy is `no`, so the
TaskManager stays down. The JobManager takes fifty seconds to notice the missing
heartbeat. The job then spends its failure-rate budget - ten restarts in under a
minute, each failing for want of a slot - and goes FAILED. Every other container
reads Up, and the events keep arriving in Kafka and wait there, unscored.

*The harness that hit it read it as something else, and so did its operator.* The
pass it landed on was the Kafka arm, so the report said "LOST 1,000" and the
obvious culprit was the twenty-second broker outage - which had not started yet.
The first diagnosis written here repeated it: a job twenty seconds old does not
survive Kafka going away. The TaskManager's own log said Metaspace, two seconds
after submission. The lesson is the section's usual one, sharpened: a failure
that coincides with the injected fault is attributed to the fault, and the
coincidence has to be checked before the attribution is written down.

*It is a deployment property, not a test artefact.* Anything that redeploys onto a
long-lived session cluster - a model update, a configuration change, a capability
switched on - is a submission, and on this configuration the eighth one takes the
scorer down without an error anywhere a dashboard would look. Two mitigations are now
in place, tested live on 2026-09-13. Every submission through `run.ps1`
restarts the TaskManager first, so the leak never accumulates. And the TaskManager
has `restart: unless-stopped`: its JVM was made to exit on its own, Docker brought
it back, a new TaskManager registered, and the running job restored from
checkpoint 35 and completed checkpoint 37 without anyone touching it. A deliberate
`docker compose kill` is still left down - also checked - so a fault injected
that way and restarted by hand measures exactly what it did
before. Neither mitigation fixes the leak itself: the production answer remains a
per-job cluster (application mode), and the `Makefile` submit path does neither.

**Twentieth: the feature extractor is quadratic in a population this project
never streams, and only foreign data could show it.** Replaying IBM's AML set
(4.49M rows) took **5.5 hours**, and the throughput collapsed as it ran:
22,102 rows/s over the first 100,000, 226 rows/s cumulative at the end. Profiled
rather than guessed - one generator expression in `features.extract` was entered
**157 million times for 400,000 events**, roughly 400 passes per event.

`extract` makes five separate linear passes over the sender's 24-hour history on
every event - `vel_10m`, `vel_1h`, the structuring band, the distinct-payee set,
`daily_sum` - each recomputed from scratch. Per-event cost is O(the sender's
recent history). On this project's own rail that history is 2 events on a median
sender-day, so the term is invisible. IBM's file contains banks and corporates:
its maximum sender-day is **26,365**.

Three things make this worth the catalogue.

*The latency chapter cannot see it.* §7.1a measures 5,956 records of generated
retail traffic, where short histories are true by construction. Every figure
there is correct and none of them exercises the term that grows. A benchmark
drawn from the same generator as the system shares the system's assumptions, and
this is what that costs - not a wrong number, an unasked question.

*The same property explains a detection result.* `DAILY_LIMIT_BREACH` scores
**0.9x** on this dataset - below one, anti-correlated with the label - because
breaching a retail daily limit is ordinary corporate behaviour and laundering,
which stays deliberately small, does it less often than the background.
`VELOCITY` and `DISTINCT_PAYEE_BURST` land at 1.2x for the same reason, and
together they flag 22.18% of legitimate traffic. The runtime and the false
positives are one finding seen from two sides: three rules count events against
a limit calibrated for a person, on a stream that contains institutions.

*The fix was available and was not taken.* Excluding hub accounts would have made
the run finish in minutes. They are almost entirely legitimate, so dropping them
raises the base rate and inflates every lift in the table - selecting on a
quantity correlated with the label. The run was left slow and the boundary
recorded, which is the whole point of validating on data this project did not
produce. What was added instead is progress output, so the next person can tell
a five-hour run from a hung one.

**Nineteenth: a fraud category the system could never assign.** `classify_type`
names an alert from whichever rule fired, and named MULE from
`DISTINCT_PAYEE_BURST` and `VELOCITY` alone. Both fire when one sender pays out
more than five times in ten minutes, and **neither has ever fired**: the
generator's fastest burst reaches exactly five. `STRUCTURING` episodes spread
5-11 transfers over up to 150 minutes, ATO episodes cap at four events. So the
MULE label was unreachable - **0 assignments across 50,000 rows** - while real
mule transfers were labelled "(none)" 65% of the time and APP 33%.

`MULE_FAN_IN` detects the pattern, fires 30 times at 100% precision, and was not
in the table. Adding it gives 30 MULE alerts, all 30 on true mule fraud, none
misattributed.

Three things make this worth the catalogue rather than a one-line fix.

*Half of it had already been found, and filed as something else.* §9 records an
outage experiment whose prediction was about `MULE_FAN_IN` while the column it
read maps MULE to two other rules, and concluded: "a category error in the
metric, not a confound in the experiment". True, and the same sentence contains
the defect - the metric is wrong because the *mapping* is wrong, and the analysis
stopped at the measurement. A finding that explains why a number is meaningless
is not finished until it asks whether the thing producing the number is correct.

*Every guard passed.* The rules fire, the fusion returns a decision, the label is
a valid enum value, tests are green. Nothing is broken. A category is simply
never chosen, and only counting the outcomes shows it.

*The two dead rules are still dead*, and that is a separate question this does
not close. `VELOCITY` misses its threshold by exactly one event, on a generator
whose structuring pattern spaces transfers 3-15 minutes apart while real
smurfing is fast because the attacker is racing detection. Either the threshold
is wrong for this rail or the generator is too slow; both are testable and
neither is tested. Recorded rather than adjusted, because tuning a threshold
until a rule fires on synthetic data is how a detection claim becomes a
tautology.

`test_fusion.py` now pins the label and, more usefully, asserts that **every**
label has at least one trigger the engine can actually emit.

**Eighteenth: the external number that anchored this project's headline claim was
quoted against the wrong prevalence, and nothing in the repository could tell.**
§6 of `related-work.md` calibrated this project's PR-AUC against a published
PaySim baseline at AUPRC 0.380, and described that figure as measured "at a
0.129% fraud rate". 0.129% is PaySim's rate over all 6.36M rows; the AUPRC
belongs to a 161,426-row holdout at **1.142%**. AUPRC's random-classifier floor
*is* the prevalence, so the number had been placed on a floor nine times too low
for a year.

Three things make it worth cataloguing rather than just fixing.

*It was checkable the whole time.* The full PaySim log has sat in `validation/`
since the beginning, and the section carried the line "not independently
reproduced here". Reproducing it took one function: `paysim_adapter.py
--baseline` returns 0.397 against their 0.380, 0.878 against 0.908, 46.8%
against 49.4% and 7.25× against 7.0×. The disclaimer was accurate and therefore
never itched — **a truthful admission of not having checked is not a check**, and
it reads enough like one to stop the question being asked.

*The error pointed away from the project's interest, which is why it survived.*
Correcting the prevalence **strengthens** the comparison: 1.142% there against
1.23% on this project's own held-out slice is matched, so the ratio of the two
AUPRCs is a like-for-like reading, which under 0.129% it would not have been. An
error that made the claim look better would have drawn scrutiny. This one made a
defensible claim rest on an indefensible justification, and nobody audits a
number that is already conceding ground.

*The same slip was being made about this project's own data.* `generator-spec.md`
§7 quoted PR-AUC 0.966 "at a 1.5% positive rate" — the dataset rate, where the
held-out slice it is measured on runs at 1.23%. Smaller, same shape, and found
only by looking for it after the external one turned up.

Fixed by a twenty-third boundary check: `paysim_adapter.BASELINE` holds the
reported figures as data, and the audit fails if `related-work.md` §6 disagrees
with it or stops stating them. That does not make an external citation true — it
makes the two copies of it agree, which is the failure that actually happened.
The reproduction is what makes it true, and it is now a command.

**Seventeenth: the clause that certified the latency figures could not fail.**
§7.0 states that every figure in §7 was taken after confirming the run was fused,
by `model_version = cep+ml-fusion-v1` **and** `countIf(ml_score IS NULL) = 0`.
The second clause is vacuous: `ml_score` is a plain `Float32`, and `record.py`
coerces a missing score to `0.0` before the insert - with a comment saying it
does. No row in the table's history can satisfy `ml_score IS NULL`, so the count
is zero whether the model ran or not.

It sat inside the sentence that repaired failure 3, which is what makes it worth
recording: the paragraph explaining how a rules-only run had been mistaken for a
fused one certified the repair with a test that a rules-only run also passes. The
surviving clause, `model_version`, is the one that works, and it works because it
is written from what actually ran rather than from what was configured - the
lesson failure 3 already produced. **A criterion that cannot fail is not a weak
check; it is the absence of one wearing the shape of a check.**

**Sixteenth: `make submit-job` was broken, and submitting it said otherwise.**
`fraud_job.py` is submitted with `--pyFiles <list>`, and the list is written out
twice - `$JobModules` in `run.ps1`, `PYFILES` in the `Makefile`. `bins.py` was in
the first and missing from the second, and `features.py` imports it at module
level.

What that cost is worse than a missing module, because of *when* it surfaces.
`flink run` returned `Job has been submitted with JobID ...`, the REST API
reported `state: RUNNING`, and both vertices reported `RUNNING` - because PyFlink
starts its Python process lazily, on the first bundle. Against an idle topic the
job sits in that state indefinitely, green in the UI and green in the API, having
never executed a line of Python. It dies on the first record that arrives, with
`ModuleNotFoundError: No module named 'bins'`, then crash-loops until the restart
strategy gives up. **A submitter that is completely broken and a submitter that
works are indistinguishable until traffic arrives.**

This was measured, not reasoned: submitted on the live stack with the old list,
observed RUNNING, fed five records, and read the failure out of the taskmanager
log. An earlier draft of this entry asserted the opposite - that the job survived
because `./stream-processor` is also bind-mounted at `/opt/flink/usrjobs` and
`fraud_job.py` prepends its own directory to `sys.path`. That is wrong, and the
same run says why: with the model removed, `config._resolve_artefact` reported
its last-resort path as
`/tmp/python-dist-.../python-files/blob_.../model.onnx`. **`__file__` is the Beam
temp directory, so the mount is not on `sys.path` at all** - it is reachable only
because `config.MOUNTED_JOB_DIR` names it as a literal, and that constant covers
artefacts, not imports. The mount could never have rescued a module.

Two things follow. It is failure 11 from the other side - there a path resolved
against `__file__` and broke loudly; here the same property broke an import while
submission reported success. And the check that should have caught it existed:
`boundary_audit.py` already derived `fraud_job`'s import closure and compared it
to `run.ps1`. It simply never knew about the second submitter. **A check that
covers one of two call sites reports green for the half it can see** - it now
derives the closure to a fixed point and holds both lists to it, and removing
`bins.py` from either fails the audit.

**Fifteenth, and the first one where nothing was broken except the reach of a
fix.** The capability-scaled threshold of §6's second RQ3 result was reached only
through `rules._thresholds()` - and `fraud_job` discards the rule layer's
verdict, taking `fusion.decide` against the unscaled `FINAL_*` constants instead.
So the fix ran in `validation/paysim_adapter.py` and `amlsim_adapter.py`, the
harnesses where it was measured, and nowhere in the deployed job. The measurement
was sound; the thing measured was not on the path.

What makes it belong here rather than in an errata list is *where* it hides. At
full capability `scaled_threshold` is the identity function, so the reference
configuration - the one that gets run, demonstrated and tested - behaves
identically whether the fix is wired or not. The defect is unobservable in
precisely the profile anyone would check it in, and live only in the combination
nobody demonstrates: a reduced capability profile with no model loaded, where
`final_score` falls back to the additive CEP score and the layer goes mute
against a cutoff calibrated for a richer deployment. That is the PaySim failure
the fix exists to prevent, reproduced inside our own system.

It was found by reading the call graph, not by any instrument here.
`boundary_audit.py` checks that what one component produces is what the next
expects; this is a value a component computes on every event and then discards
*within itself*, which no boundary check can see. The repair puts the choice in
one place - `fusion.cutoffs(cep_only=...)` - and states the asymmetry that makes
it a choice: a model probability is recalibrated by retraining when the feature
contract changes, an additive rule sum is not.

**The by-product is a stronger result than the fix.** Asking whether the fused
path should honour the rule layer's verdict at all is now a measurement rather
than an assertion. Letting the CEP verdict act as a *floor* on the fused decision
- it can only raise a decision, never lower one, so unlike the blends already
rejected it cannot degrade ranking - costs, on the held-out slice: precision
**0.847 -> 0.457**, F1 0.874 -> 0.611, buying 2 additional true positives for 114
additional false ones, an increment precision of 1.7%. The layers stay decoupled
because that number says so, which is a better reason than the one `fusion.py`
gave before. The same measurement prices the narrower version: adding
`IMPOSSIBLE_TRAVEL` to `MANDATORY_REVIEW_RULES` changes **nothing** on this slice
- the rule fired 3 times, all 3 fraud, all 3 already caught by the model, none
muted - so the weight comment that claimed the rule "alone must reach REVIEW" was
describing the CEP layer only, and now says so.

**The repair had a hole in it, found by review rather than by running it.**
The first version left `fraud_job` computing `final_score(...)` and then
`decide(..., cep_only=ml_score is None)` - re-deriving at the call site a fact
`final_score` had just decided. `decide`'s `cep_only` defaults to `False`, which
is the right default for every other caller, and that is exactly what makes the
re-derivation dangerous: drop the keyword and the pre-fix behaviour returns with
nothing failing, in a profile where old and new behave identically. The fix would
have been true only of the library, not of the deployment.

Now the derivation lives in `fusion.score_and_decide`, which does both steps and
is the only form the job uses, so a caller cannot get it wrong by omission. The
call site is pinned by two tests that parse `fraud_job.py`'s AST - it cannot be
imported, PyFlink is not installed on the host - one asserting it goes through
the combined form, one asserting it has not gone back to the split one. Both were
confirmed to fail against an injected regression before being kept, which this
document's seventeenth entry is the argument for.

**What of the repair has actually run, and what has not.** The fallback branch
was exercised on the live stack: the job was submitted with `model.onnx` removed,
five records went through it, the CEP-only banner fired and the rows landed
stamped `cep-only-fallback`. So `cep_only=True` reaches `fusion.cutoffs` in the
runtime, not only in tests. But that run was at **full capability**, where
`scaled_threshold` is the identity - so the rescaled cutoff itself has still
never been exercised inside Flink, only in unit tests. The combination this
entry is about, reduced capability with no model, remains verified offline.

**Fourteenth, found by the dependency matrix and belonging here rather than in
§7:** with Redis or Neo4j gone the pipeline keeps deciding and stops keeping up
— a 1,000-event slice that drains in six seconds healthy had not drained in five
minutes. Neither client carries a timeout, and the handle is only nulled at
`open()`, so a dependency that dies later leaves a live client that every
subsequent event pays for. Containers `Up`, no exception, alerts still
published, offsets still advancing. See §7.7a.

Seventeen distinct failures are recorded here: eight found while building the
pipeline, five more while building the alert consumer that closes it, and the
four above — one from the dependency matrix, three from an audit of the code
against its own documentation. **Fourteen left the system looking healthy**: containers `Up`, no exception anywhere, Kafka
offsets advancing, the producer reporting success. The thirteen in the table are
numbered; the two most recent are stated above it because they were found after
it was written. They are collected here because the pattern is the result, not
the individual bugs.

| # | failure | what an operator saw | what it actually did |
|---|---|---|---|
| 1 | ClickHouse not accepting queries when sink-writer connected at boot (`depends_on: service_started` waits for the container, not the service) | nothing — one WARNING at startup, then silence | sink disabled for the life of the process; `flush()` cleared its buffer with no log. **331 events consumed, offsets committed, 0 rows stored** |
| 2 | Neo4j losing the same startup race | same | every alert's graph write discarded; `MULE_FAN_IN` querying a graph that was never written |
| 3 | `model.onnx` unreachable under `--pyFiles` | one INFO line among thousands | job scored CEP-only while stamping `model_version = cep+ml-fusion-v1` — §7.0 |
| 4 | jobmanager restart (session cluster, no HA) | **entire stack healthy**, producer succeeding | no job running at all; only symptom was consumer lag climbing |
| 5 | resubmission without `-s` | job RUNNING, offsets committed, nothing re-read | empty keyed state — velocity and structuring windows blank, history-dependent rules unable to fire until refilled |
| 6 | producer stopped with Ctrl+C | "produced N messages" never printed | `KeyboardInterrupt` bypassed `producer.flush()`; buffered messages never sent, and in the fault-injection run they would have been counted as **transactions lost** — manufacturing the exact correctness failure the experiment exists to rule out |
| 7 | unpinned dependencies | build succeeded | one rebuild moved three libraries across major versions with no change to the repository, so the code no longer matched the environment its results were measured on |
| 9 | ClickHouse init scripts run **only on an empty data directory**, so a schema file added later never executes on a cluster that has been up before | service starts, consumes the alert topic, commits offsets | every insert fails against a table that does not exist — the same shape as #1, rediscovered in a new component |
| 10 | a DDL splitter that stripped whole comment lines and then split on `;` | — | tore `CREATE TABLE` in half at a semicolon **inside an inline comment**, yielding three fragments of which none was valid SQL. Caught by a test before deployment, which is why it is the cheapest row here |
| 11 | `--pyFiles` copies modules into a Beam temp directory, so a data file resolved against `__file__` is never found | job `FAILED` in Flink; **downstream, an empty queue** | the alert topic stayed empty and the work queue looked simply quiet. The remote symptom is the point: the loud failure was two components away from where it was noticed |
| 12 | the LightGBM wheel installs cleanly and its native library needs an OpenMP runtime the slim image does not ship | service healthy, alerts consumed, cases opening | `OSError: libgomp.so.1` at import — not a Python-level error and invisible to any dependency check; every case filed as `NO_MODEL` |
| 13 | `csv.DictReader` returns every column as text; numbers were cast on the producer, booleans were not | the JSON on the wire looks right — `"active_call": "False"` | a non-empty string is true, so the live job scored `active_call = 1` on **100%** of events against a model trained on 3.5%. Measured through the deployed model: false positives **20 → 459**, PR-AUC 0.9956 → 0.9842, for five additional true positives |

Only the eighth — a checkpoint directory created root-owned by the volume mount
while Flink runs as `flink` — failed loudly, with a stack trace at submission.
It was the least costly of the first eight and the fastest to fix.

Rows 9–12 were found later, building the alert consumer, and they repeat the
pattern rather than extending it: two left every health indicator intact, one
failed loudly two components away from where anyone was looking, and the one
that failed immediately was the one a test caught before it ever ran.

**Row 13 is the one to read twice, because it inverts the usual assumption
about where results are safest.** Every number reported in this document was
measured offline, and every offline path converted the flag correctly - the
replay harness through its own `_as_bool`, the training matrix through pandas'
bool dtype. Three private conversions, each right. The one path with no
conversion of its own was the live pipeline, so the defect existed *only* in
production: nothing in the reported results is wrong, and none of them described
the running system.

That is the opposite of the failure mode usually guarded against. The concern is
normally that offline evaluation flatters a model the deployment cannot match;
here the deployment was quietly worse than every measurement of it, and no
measurement could have found it, because measuring is exactly what the offline
paths do. Only serving the wire format found it.

The repair is structural rather than local: the coercion now lives in
`features.py`, the module every caller reaches the model through, so it cannot
be missing from one of them. `test_wire_types.py` states the property directly -
an event whose every field is a string must produce the same feature vector as
its typed equivalent - which is the invariant that was never written down.

**Row 11 deserves to be uncomfortable.** The lesson it teaches was already
written down — in this very document as failure 3, and in a fifteen-line
comment above `config._resolve_artefact` explaining that `--pyFiles` ships
Python modules to a temp directory and leaves data artefacts behind. A new
module resolved `banks.csv` against its own `__file__` anyway, and the job died
at import. Writing the lesson down did not prevent its recurrence; only routing
the new artefact through the same resolver did. The design consequence is
narrower than "document more": **a hazard that has been met twice should be made
unavailable, not annotated.** There is now one artefact resolver and every
deploy-time file goes through it.

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

## 9. What the work queue found that the metrics did not

The alert topic had no consumer. The pipeline computed a decision, published it,
and nothing read it - `BLOCK` and `REVIEW` were strings in a warehouse rather
than work anyone did. Building the missing consumer (`case-manager/`) was
intended as scope-completion. It turned into an instrument: within one session
of running a real queue against real alerts it exposed two properties of the
detector that no reported metric could show, because no reported metric looks
at them.

### 9.1 The probabilities rank well and mean nothing

Every case in the queue carried `final_score = 1.000`. Measured over the
held-out slice:

| quantity | value |
|---|---|
| Brier score | 0.00244 |
| alerts (p >= 0.40) | 131 |
| alerts rounding to 1.000 | **70.2%** (89.1% over the full set) |
| distinct rounded alert scores | **35** |
| alerts in the REVIEW band [0.40, 0.80) | **14** |
| median alert probability | 0.999975 |

ROC-AUC is 0.9992 and PR-AUC 0.9591, and both are *correct*: they are rank
statistics, and the ranking genuinely is near-perfect. A model that scores every
alert at 0.99999 posts the same AUCs as one whose scores are spread across the
interval, because ranking is all they see. What is broken is **calibration** -
the probabilities are not usable as magnitudes - and nothing in the metrics
suite was sensitive to it.

Two consequences follow, and neither is visible from an AUC:

- **The two-tier decision is nominal.** 98.2% of alerts exceed the 0.80 block
  cutoff; the REVIEW band holds 14 of 758. The thresholds were calibrated as if
  the score were distributed, and it is not.
- **The queue cannot be prioritised by score.** Every case arrives at the same
  priority and ties at 1.000, leaving arrival order as the only tiebreak. The
  queue was re-ordered by **exposure** instead: amount spans four orders of
  magnitude, and between two cases the model is equally sure about, the larger
  one costs more to be wrong about.

This is a property of the data, not of the method. Synthetic fraud is close to
separable, so the trees drive the log-odds to the extremes; real traffic with
label noise and overlapping classes does not behave this way. It is reported
because it would otherwise be discovered by whoever first tried to operate the
system. `train.py` now emits Brier score, saturated share, distinct alert
scores and review-band occupancy beside the AUCs, and warns when the saturated
share exceeds one half - so this class of problem is caught by a number at
training time rather than by a screenshot of a queue.

### 9.2 One alert in seven was an automated adverse decision with no reason

`predicted_type` and `rule_hits` are derived from the CEP layer. When the model
alerts and no rule fires, both are empty:

| | count | share |
|---|---|---|
| alerts | 758 | |
| with at least one rule hit | 645 | 85.1% |
| **with no reason code at all** | **113** | **14.9%** |
| BLOCKs with no reason code | 111 of 744 | 14.9% |
| of those, actually fraud | 109 of 111 | **98.2%** |

The uncomfortable part is not that the model is wrong. It is right, at 98.2%
precision on exactly these cases - and mute. An automated block with nothing to
tell the customer or the supervisor is a compliance problem independent of its
accuracy.

Two responses were considered and one measured. Capping an unexplained decision
at REVIEW costs no detection at all (the alert is raised either way; 111 cases
move from automatic block to human review, alert recall unchanged at 0.984) but
trades enforcement speed for explainability. The alternative - **give the model
a voice** - was chosen instead, and all 113 now carry exact tree contributions
in words:

```
money into this payee in an hour: 14 615 204 UZS (+9.84)
payee's account age: 31 days (+5.38)
amount: 7 307 603 UZS (+4.59)
```

Where this runs, and why it is not on the scoring path: exact contributions cost
**1.89 ms per event** (400 trees, 24 features at the time, LightGBM `pred_contrib`), which
at ~1.5% alert traffic the 300 ms budget could absorb. It was still the wrong
place. It would put a second copy of the model in the serving worker beside
`model.onnx`, and nobody consumes an explanation at decision time - the analyst
reads it from the case, the auditor from the record. So the job publishes the
feature vector it scored on (alerts only) and the explanation is computed
downstream, where a millisecond costs nothing. The measured 1.89 ms is recorded
so that the choice is a comparison rather than an assertion.

The guard matters more than the feature. The explaining artefact (`model.txt`)
and the serving artefact (`model.onnx`) agree to 3.3e-07 across 50,000 events;
every explanation recomputes the probability and refuses to speak if it differs
from the recorded `ml_score` by more than 1e-4. A confident, specific, wrong
reason is worse than an admitted absence, so most of `test_explain.py` is about
refusing rather than explaining.

### 9.3 The mechanism behind the ML layer's advantage, on one transaction

The head-to-head figure - F1 0.881 fused against 0.417 rules-only - says the ML
layer helps. It does not say *how*, and a reviewer is entitled to ask. One of
the model-only alerts answers it:

| | value | rule | threshold | fired? |
|---|---|---|---|---|
| payee account age | 31 days | `FRESH_RECEIVER` | < 30 days | no, by one day |
| distinct senders in 1h | 2 | `MULE_FAN_IN` | >= 6 | no |
| inbound to payee in 1h | 14 615 204 UZS | - | - | - |
| this transfer | 7 307 603 UZS | `NEW_PAYEE_HIGH_AMOUNT` | new payee and > 3x mean | no |

A month-old account taking 14.6 million sum in an hour from more than one
sender. Every individual threshold is missed, two of them narrowly; the
conjunction is decisive. **Rules test thresholds one at a time; the model tests
combinations that cross no threshold at all.** That is the mechanism, shown
rather than asserted, and it generalises: across the 113 model-only alerts, 69%
had receiver-side concentration as their strongest contribution - the model
systematically detects fan-in *below* the rule's constant, which is the same
finding §6's third result reached from the opposite direction.

### 9.4 What is not defensible in this, stated first

- **Hour of day is the strongest contribution on 27 of the 113** model-only
  alerts (24%). "The transfer was at 23:00" is weak grounds for an automated
  block, and it should be read as the model exploiting a diurnal artefact of the
  generator rather than as a finding about fraud.
- **`predicted_type` is empty for every model-only alert**, because the type is
  derived from which rules fired. The cases that most need triage are the ones
  the queue cannot categorise. Deriving a type from feature contributions was
  rejected: "rule MULE_FAN_IN fired" and "the model weighted receiver-side
  features" are claims of different strength, and substituting one for the other
  would be dishonest.
- **The queue's precision figure is biased upward** and says so in its own
  output: analysts work the top of the queue, so the resolved set over-samples
  high scores.
- **One case per alert.** A mule receiving from twelve senders produces up to
  twelve cases where an investigator wants one. Grouping is deferred because it
  changes what a "false positive" counts, and the counting is what the
  disposition exists for.

### 9.5 Why the disposition field exists in a prototype with no analysts

`CONFIRMED_FRAUD` / `FALSE_POSITIVE` is the only real label this system can
produce. Every other figure in this document is measured against generated
ground truth. What an analyst confirms is what a production model would actually
be retrained on, so the column is the shape of the feedback loop and the place a
real deployment would begin collecting - which is also the honest answer to the
absence of labelled Uzbek P2P fraud data: the system cannot conjure labels, but
it can be built so that operating it produces them.

## 10. Notes for the written revision

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
- **Scope must be declared in the introduction, not inferred from the
  architecture.** This is a *detection* system, not an *enforcement* one.
  Nothing in it answers an authorisation request, declines a transfer, holds an
  account, or challenges a customer; `fraud.alerts` is consumed by the
  case-manager, which opens an analyst case. Two code comments previously
  asserted that the decision "reached the switch" — naming an integration that
  has never existed — and have been corrected. The latency figure is therefore
  the time to *reach a decision*, which is the necessary condition for acting
  before settlement, not evidence that anything acted.
- **The cost of a false BLOCK is never paid by this system**, so the operating
  point was chosen against a cost it does not incur. In a deployment that
  threshold would be argued over declined payments and call-centre volume, not
  over F1. Say so before it is asked.
- Report calibration next to discrimination wherever a probability is quoted.
  §9.1 is the case for it: two near-perfect rank statistics coexisted with a
  score that could not order a work queue, and only an instrument that consumed
  the scores as magnitudes revealed it.
