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
| 3 | Security-overhead benchmarking (mTLS, payload encryption) | **Done — both halves plus the churn arm.** Payload encryption, see §7.4: AES-256-GCM on the event payload, decrypted inside the scored path: on matched 400-record arms the decision path is unchanged (p99 183 ms both, median CIs overlapping). A microbenchmark supplies the figure the pipeline cannot resolve — ~6.8 µs to decrypt, ~0.15% of the scoring budget. The measurable cost is **size**, about +50% per message, roughly half of it an artefact of the string deserialiser rather than of the cryptography. **Transport half now measured too, see 7.5:** four counterbalanced arms show no transport effect that survives the ordering - the cost sits below a per-arm drift of about 4 ms - while the 300 ms target is met at p99 in every arm. **Churn arm done, see 7.5a:** a microbenchmark puts one mutual-TLS handshake at +11.2 ms over plaintext, and four further arms with 60 reconnects each still show no cost on the decision path - the handshake is paid by the client before the measured clock starts, so it lands on the switch's latency budget and not on the bank's. |
| 4 | Integrity audit — cryptographic hashing at ingress and sink | **Done.** Ingress SHA-256 over the raw event at the producer, carried through Flink untouched and bound into the audit record; a hash chain over audit records makes any alteration, deletion or reorder evident; `verify_audit.py` recomputes it. Residual (a full-table rewrite) now closed in practice: the head hash of the reference run is published in `docs/audit-anchors.md` and pushed to the remote, which separates custody of the value from custody of the database. Not a cryptographic timestamp, and recorded as the weaker option it is. |
| 5 | Distinguish organic concept drift from adversarial evasion | **Accepted, sharpened, and settled in `docs/threat-model.md`.** The two are separated by *where* the shift appears: organic drift moves both classes, evasion moves the fraud class only, and only on features the adversary controls. Session timing is attacker-controllable; receiver account age is not. The document commits to a falsifiable prediction — if `COACHED_SESSION` is deployed and announced, `P(active_call = 1 | APP fraud)` should decay toward the ~3% population base rate while the legitimate rate holds. A control whose evasion is predictable in advance is a stronger claim than one whose robustness is merely asserted. |
| 6 | Non-parametric statistics for tail latency; validate exactly-once by fault injection | **Done, with the exactly-once half reframed — see §6.6 and §7.** Non-parametric tail statistics are built into `latency_report.py` (nearest-rank order statistics, distribution-free CI for the median). On exactly-once the honest answer is that **the system does not provide it and should not**: the sink is `AT_LEAST_ONCE`, ClickHouse is plain MergeTree with no deduplication, and the Redis fan-in store sits outside the checkpoint. Fault injection measured what that costs: **nothing lost (500/500), duplication 0.20%, and — the finding — the duplicate copies carry different scores**, because the fan-in store's reads do not roll back with the checkpoint. Duplicate alerts are cheap; the checkpoint-interval latency that transactional writes would add is not, against a 300 ms budget. Repeated kills now done: six jittered kills on a clean warehouse give a median duplication of 0.89% [0.40%, 1.38%], nothing lost in any of twelve kills across two series, and a divergence magnitude - 3 of 26 duplicates, at most 0.0003, no decision changed - that the original single observation could not supply. |
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
   fault_injection.py`.** The system does not provide exactly-once and does not
   claim to: `AT_LEAST_ONCE` on the Kafka sink, a `MergeTree` with no
   deduplication, and a Redis fan-in store outside the checkpoint. So the
   questions asked were the two that matter for money — is anything lost, and
   what do the permitted duplicates cost.

   Method: baseline row count, paced stream, `docker compose kill taskmanager`
   mid-stream, recount after recovery. 500 transactions per round, six rounds
   (`run-kill-series.ps1`), against a warehouse emptied beforehand.

   **Nothing lost — 500 of 500 in every round.** Across twelve kills in two
   series, not one transaction was lost. Checkpointing plus committed offsets
   replayed the window between the last checkpoint and the kill.

   **Duplication, six kills: median 0.89%, distribution-free 96.9% interval
   [0.40%, 1.38%]** (for six order statistics that interval is the range).
   Per round: 0.40, 1.38, 0.79, 0.40, 0.99, 1.19 per cent, i.e. 2 to 7 rows in
   500. The count is the traffic in one checkpoint interval - 2 s at about 5
   events/s, so a ceiling near ten - and which value comes up is decided by
   where the kill lands in the cycle, a phase nobody controls in production
   either. Twelve observations across both series span 1 to 10 rows, which is
   the predicted range.

   **The result worth reporting is not the rate but that the duplicates can
   disagree — and how much, which is a separate question from whether.**

   On the clean six-round series, 26 fault-induced duplicate rows produced
   **3 with a different final score, a largest divergence of 0.0003, and no
   decision change at all**. The mechanism below is confirmed; its operational
   magnitude on this traffic is small, and both halves have to be said.

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

   Non-parametric statistics over repeated kills, the other half of point 6, are
   no longer owed.
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
through `replay_eval.py`, which drives the deployed `rules.evaluate` unchanged -
so it is the real rule layer, not a reimplementation. It is **not** yet reachable
in the Flink job: the baseline is population-wide state, and this stream is keyed
by sender, so it belongs in Redis beside `ReceiverStore` for the same reason the
receiver window does. Until it is wired there, setting the mode on the deployed
job falls back to the constant. That fallback now logs a warning once per process
rather than happening silently, which is the minimum this project's own catalogue
of silent failures demands of it.

Two things follow from the measurement, and the first matters more.

**The learned threshold reproduces the hand-set one at home.** On the project's
own data the quantile lands on 5 at q=0.9995 and 7 at q=0.9999, bracketing the
six that was chosen by hand: the constant had been encoding roughly the 99.97th
percentile of this population all along. Nothing about home behaviour changes;
what changes is that the quantity is now named.

**And it is measurably better at home, at no cost in alerts.** Five generator
seeds, each replayed under both modes, deltas paired within seed
(`stream-processor/fan_in_mode_eval.py`, q=0.999):

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
   two pages earlier. `validation/zenodo_provenance.py` runs the check.

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

### 7.1a The reference run: 5,956 records, thirty minutes, no stall

Runs A and B above are 1,602 and 430 records. This one is 5,956 over about
thirty minutes of continuous paced traffic, producer inside the Docker network,
warm enrichment cache, and it supersedes them as the figure to quote.

| stage | median (95% CI) | p95 | p99 | max |
|---|---|---|---|---|
| ingest -> decision | 69 ms [68, 70] | 131 ms | **176 ms** | 325 ms |
| of which scoring work | 4.2 ms [4, 4] | 13.8 ms | 22.0 ms | 168.3 ms |
| decision -> ClickHouse | 30.3 s | 69.8 s | 79.7 s | 87.8 s |

**Target met: 2 of 5,956 over 300 ms (0.03%).** Both breaches are marginal -
the maximum on the decision path is 325 ms - rather than the tail of a stall.

Every decision-path statistic is better than run A's on a sample 3.7 times
larger: median 69 against 80, p99 176 against 186, maximum 325 against 570, and
a breach rate of 0.03% against 0.37%. The host-to-container clock offset read
+0 ms with a 6 ms round trip, the cleanest of any run taken, so the confound
that historically dominated these figures is absent here.

**The multi-second stall did not recur.** Characterising it is what this run was
for: two observations, 1,452 ms in run B and 323 ms in run A, are too few to
attribute to anything. Across 5,956 records the worst scoring excursion was
168 ms - an order of magnitude below the stall that prompted the investigation.

That does not identify the cause; it bounds the frequency. **Under warm
steady-state conditions the stall occurs less often than once in ~6,000
records**, where run B put it at 2 in 430. Both observed stalls fell in short
runs taken shortly after a cold start or a job submission, which suggests they
belong to startup rather than to steady state - a hypothesis on n=2, not a
finding, and one this run can only make plausible by failing to reproduce it.

The head-of-line mechanism in 7.3 is confirmed from the other side: with no
stall there is nothing to queue behind, and the breach count falls to two
marginal records rather than run B's eighteen.

Three limits on what this measures. The reporting window clipped the first
~1,044 records of the run, which were the coldest, so this describes warm steady
state and says nothing about a cold start. Throughput is 5 events/s on one
machine - a pacing choice, not a load test, and p99 at this rate implies nothing
about behaviour at a switch's volumes. And the warehouse path at a 30.3 s median
sits at the top of the 20-33 s band reported earlier and is still undiagnosed;
nothing real-time rides on it, but it did not improve.

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

Two invariants of the envelope, recorded here because they are easy to break
later and neither is visible from the measurement:

- **Hash the plaintext, then encrypt.** `ingress_hash` is computed over the
  plaintext fields and travels inside the encrypted payload. Hashing the
  ciphertext instead would break the audit guarantee outright: GCM nonces make
  every encryption of the same event distinct, so an auditor holding the
  original event could never recompute it.
- **The routing key is authenticated, not merely appended.** It is the GCM
  associated data, so altering it makes the record undecryptable rather than
  silently misrouted. A record steered into the wrong key group would
  accumulate into another sender's velocity and structuring windows — an
  integrity failure in the detection logic, not only a privacy one.

The key comes from `PAYLOAD_KEY_HEX` with no default. A hard-coded fallback
would be worse than a startup failure, because it encrypts everything under a
value that is in the source tree; the version digit in the magic prefix is what
a rotation scheme would extend.

Transport security (mTLS between the switch, the broker and the consumers) is a
separate measurement with a different cost profile — per-connection handshakes
rather than per-record work — and is taken in §7.5 and §7.5a below.

### 7.5 Security overhead II: transport (reviewer point 3, second half)

**Why this control and not another.** The reviewer asked for transport-security
overhead, but there is a stronger reason to measure it here. In the 2025 annual
report of Uzbekistan's State Institution "Cybersecurity Centre", transport
security is the **largest single class of high-severity defect found in mobile
applications**: of 157 high-severity findings, 33 are "interception of
transmitted data", 13 are "transport security disabled in the application", and
8 are "data transmitted unencrypted" - 54 of 157, over a third, in one class.
The measurement below therefore prices the control that national data identifies
as the one most often missing. That reframes the result: 7.5 and 7.5a do not
report that a control the reviewer named happens to be affordable, they report
that **the most commonly omitted high-severity control in this market costs less
than this pipeline can measure** - which removes performance as a defence for
omitting it.

Mutual TLS between producer, broker and consumers, measured 2026-08-31. The
broker runs a plaintext listener on 9092 and an SSL listener on 9094 side by
side over the same partitions, so an arm changes only the port the clients dial
and `KAFKA_SECURITY_PROTOCOL` for the job. `ssl.client.auth = required`: the
broker rejects any client without a certificate signed by the CA, so this is
mutual TLS rather than server-side TLS, and the handshake - the part with a real
cost - is inside the measurement.

Four arms, counterbalanced A-B-B-A. Each discards a warm-up of a quarter its
length, waits for the backlog to drain, settles 90 s so earlier rows leave the
reporting window, and flushes the `age:*` enrichment cache so both arms start
cold.

| # | arm | n | median (95% CI) | p95 | p99 | max | over 300 ms |
|---|---|---|---|---|---|---|---|
| 1 | plaintext | 400 | 77 [73, 81] | 167 | 201 | 212 | 0 / 400 |
| 2 | mutual TLS | 400 | 82 [79, 88] | 196 | 243 | 306 | 1 / 400 |
| 3 | mutual TLS | 1456 | 83 [81, 85] | 174 | 225 | 304 | 1 / 1456 |
| 4 | plaintext | 1456 | 87 [85, 89] | 168 | 230 | 268 | 0 / 1456 |

**The target is met in every arm at the 99th percentile.** Operationally that is
the result: mutual TLS does not put the 300 ms decision budget at risk.

**The transport effect does not survive counterbalancing, and that is the
finding.** Read the first pair alone and it says mutual TLS costs +5 ms at the
median, on intervals that barely overlap. Read the second pair alone and it says
plaintext costs +4 ms - the same magnitude, the opposite sign. What is
consistent is not the transport but the position: in both pairs the arm that ran
second was the slower one.

Subtracting the scoring bracket from the decision path leaves the buffering
component, and in run order across all four arms it is monotone:

| arm (run order) | decision | scoring | buffering |
|---|---|---|---|
| 1, plaintext | 77 | 8.1 | ~69 |
| 2, mutual TLS | 82 | 8.9 | ~73 |
| 3, mutual TLS | 83 | 6.8 | ~76 |
| 4, plaintext | 87 | 6.7 | ~80 |

The subtraction is of medians rather than a median of differences, so it
indicates the shape and not the exact value. Scoring falls across the session as
the JVM and the Python workers warm; the buffering component rises by roughly
4 ms per arm regardless of transport. The cause is not diagnosed - a growing
ClickHouse table changing the sink's back-pressure, keyed state accumulating
across `resume-job` restores, and host-level drift over a forty-minute session
are all candidates, and four arms cannot separate them.

**Stated at the resolution the setup supports: the cost of mutual TLS on the
decision path is below the resolution of this measurement, and that resolution
is set by a per-arm drift of about 4 ms rather than by the transport.** This is
the same shape as the payload result in 7.4 - the pipeline confirms a small cost
by failing to see it - but it is reached differently. There a microbenchmark
supplied the figure the pipeline could not resolve; here the counterbalancing
supplied the reason that no figure should be quoted at all.

**What the arms above do not measure.** Each holds one long-lived connection, so
the handshake is amortised across it and what remains is mostly TLS record
framing. A payment switch with many short-lived connections pays the handshake
repeatedly, and that is the one scenario in which this answer could change. It
is measured in 7.5a.

### 7.5a Connection churn, and what one connection costs

**The handshake, measured directly.** `data-generator/handshake_bench.py`
constructs and closes one producer per iteration and times the constructor,
which blocks until bootstrap completes. Forty pairs, arms alternating *within*
each pair rather than in two blocks, one discarded warm-up pair:

| transport | median | p95 | min | max |
|---|---|---|---|---|
| plaintext | 3.3 | 5.0 | 2.6 | 6.8 |
| mutual TLS | 14.5 | 20.4 | 12.4 | 26.0 |

Paired difference, TLS minus plaintext: **median +11.2 ms**, over the range
[+9.2, +20.6]. Both arms pay the TCP connect, the API-version probe and the
metadata fetch, so only the *difference* is the handshake; the absolutes are
not. This is the same instrument 7.4 used for AES-GCM: a microbenchmark for a
cost the pipeline cannot resolve.

**The churn arms.** Four more arms, same A-B-B-A protocol as above, same 1200
messages, with the producer closed and reopened every 20 messages - 60
reconnects per arm, against one connection for the whole of 7.5.

| # | arm | n | median (95% CI) | p95 | p99 | max | over 300 ms |
|---|---|---|---|---|---|---|---|
| 1 | plaintext, churn | 1456 | 66 [65, 68] | 128 | 170 | 475 | 3 / 1456 |
| 2 | mutual TLS, churn | 1456 | 64 [62, 65] | 131 | 179 | 410 | 4 / 1456 |
| 3 | mutual TLS, churn | 1389 | 62 [60, 64] | 127 | 171 | 226 | 0 / 1389 |
| 4 | plaintext, churn | 1456 | 68 [65, 71] | 166 | 210 | 328 | 2 / 1456 |

**The target is met at p99 in every churn arm**, with the worst arm at 210 ms.
Operationally that is again the result.

**The counterbalanced contrast has the wrong sign, and that is the finding.**
Averaging the two arms of each transport - which cancels a drift linear in
position - gives plaintext 67 ms against mutual TLS 63 ms: TLS **faster** by
4 ms. Unlike 7.5 the sign does not reverse with the order. TLS was faster when
it ran second (pair 1) and faster when it ran first (pair 2), and the per-arm
confidence intervals do not overlap in the second pair. Subtracting the scoring
bracket puts the whole difference in buffering (plaintext ~61.6 ms both arms;
mutual TLS ~58.5 and ~57.7), not in work.

Mutual TLS cannot make the decision path faster. So a sign-consistent,
tight-interval result is being read here as evidence of a confound rather than
of an effect, and two candidate confounds were named before the run:

- **The handshake is outside the measurement by construction.** The reconnect
  happens after the send, and the producer constructor blocks until bootstrap
  finishes, so the next row's `ingested_at` is stamped on the far side of the
  handshake. Every millisecond of the 11.2 ms above is paid by the client
  *before* the clock this section reads starts. The churn arm can therefore only
  show broker-side spillover - repeated handshakes competing with the partitions
  Flink reads - never the handshake itself.
- **The pause drains the pipeline.** Each reconnect is a gap in the stream, and
  the mutual-TLS gap is ~11 ms longer. A longer gap lets buffers empty, which
  biases the TLS arm toward *lower* latency. The direction of the observed
  effect is the direction this confound predicts.

Two pairs favouring one arm is `p = 0.25` under a null of no effect, by sign
alone; the tight per-arm intervals do not improve on that, because 7.5 already
established that they measure sampling within an arm rather than variation
between arms. Nothing here is established. What the four arms do establish is
the negative: **60 reconnects per arm did not make mutual TLS visible on the
decision path, and the resolution floor is not the transport but the ~60 ms of
pipeline buffering that swamps an 11 ms per-connection cost.**

**Deployment consequence, stated plainly.** A switch that opens a connection per
transaction pays 11.2 ms of its own latency each time - real, measured, and
nearly 4% of a 300 ms end-to-end budget it does not get back. It does not,
however, degrade the bank's detection path. The engineering answer is connection
pooling on the switch side, and it is a switch-side answer: nothing in this
pipeline is where that cost lands.

**A comparison deliberately not drawn.** The churn arms sit ~20 ms below the
arms in 7.5 across the board. That is a between-session difference - a different
job submission, a larger ClickHouse table, restored checkpoints, two hours of
uptime - and 7.5 established that per-arm drift of ~4 ms is already enough to
reverse a conclusion. Quoting churn as a 20 ms improvement would repeat exactly
the error this section exists to document. It would need its own counterbalanced
design, alternating churn and no-churn within one session.

**Why the arms were reversed.** Had only the first pair been run - which is the
normal thing to do - this section would have reported that mutual TLS costs 5 ms
at the median, on intervals clean enough to publish. Reversing the arm order is
what prevented it. That is the third time in this project that a single-order or
single-run figure was wrong: once by a factor of two, once in sign, and now once
in sign again through an ordering confound.

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
  **Frequency now bounded (7.1a):** a 5,956-record run over thirty minutes did
  not reproduce it at all, worst scoring excursion 168 ms, so under warm
  steady-state conditions it is rarer than one in ~6,000 records. The cause
  remains unidentified and the thirty-minute run that was owed is done.
  Three hypotheses were tested and rejected or left open: it is **not** ONNX
  warm-up (the stalls occur late in a run, not on the first records); it is
  **not** enrichment cache misses (the worst stall fell in the cache-hit
  bucket); and it is **not** a Neo4j pause (that component's own pause monitor
  logged nothing at the time). VM-level pause and Python worker GC remain
  plausible and were not separated — with n=2 any attribution would be fitted
  rather than measured. Characterising the cause needs *occurrences*, and the
  thirty-minute run produced none: bounding the frequency and diagnosing the
  cause turn out to need opposite conditions, and the run that settled the
  first made the second no easier.
- Single machine. Security overhead has since been measured against this
  baseline (§7.4 payload, §7.5 transport, §7.5a churn), and the limitation that
  remains is the baseline itself: one host, so every arm shares whatever that
  host was doing at the time. That is what the counterbalancing in §7.5 exists
  to work around, and §7.5a shows it working around it imperfectly - a
  sign-consistent result with the wrong sign.

### 7.6 Throughput: where the 300 ms target stops holding

Every latency figure above was taken at one arrival rate. That answers "how fast
is a decision" and not "how fast can decisions arrive", which is the question a
capacity plan asks. The sweep drives the producer at a fixed offered rate and
reports the DECISION path — `ingested_at` to `scored_at_job` — separately from
the warehouse path, which carries up to five seconds of sink batching and would
otherwise dominate the headline by two orders of magnitude.

Pacing is deadline-based (`due = t0 + n / rate`) rather than `sleep(1 / rate)`:
the naive form adds the send cost to every interval, so the achieved rate drifts
below the requested one and the arm silently measures a slower stream than it
claims. The producer prints the achieved rate on every run and says SATURATED
when it falls below 95% of the request, because a client that cannot keep up
produces latency figures that describe the CLIENT.

3,000 messages per arm, milliseconds, `work` = in-operator processing time:

| offered/s | achieved | p50 | p95 | p99 | over 300ms | work | e2e | state |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 5 | 5 | 87 | 101 | 116 | 1 | 5.5 | 49,913 | ok |
| 10 | 10 | 87 | 139 | 156 | 0 | 1.9 | 25,045 | ok |
| 25 | 25 | 124 | 218 | 340 | 49 | 1.6 | 10,185 | p99 over target |
| 50 | 50 | 156 | 282 | 382 | 110 | 1.2 | 5,211 | p99 over target |
| 100 | 100 | 240 | 805 | 1,162 | 1,039 | 0.9 | 2,931 | p99 over target |
| 250 | 250 | 5,671 | 9,263 | 9,515 | 2,961 | 1.0 | 7,823 | SATURATED |

**The pipeline stops meeting the 300 ms target somewhere at or below 25
events/s** — a figure that has to be stated plainly, because it is two orders of
magnitude below what a national switch carries.

The shape of the failure says where it comes from. `work` stays at roughly one
millisecond across the whole sweep while decision time climbs by a factor of
sixty: the operator is not getting slower, records are waiting. That is pure
queueing, and `enrichment.py` names the cause in its own comment — the Redis and
Neo4j lookups are SYNCHRONOUS, so the operator blocks per record and one slot
processes one transaction at a time. Flink's async I/O is the documented fix and
is not implemented here; the sweep is what turns that known shortcut into a
number. `work` FALLING as load rises (5.5 ms to 0.9 ms) is the cache warming:
at 250/s the same accounts recur inside the TTL.

Two things this does not measure: a single machine with one TaskManager slot, so
it is a per-slot figure and not a ceiling for the design; and the sweep tops out
where the PRODUCER saturates, not where the pipeline does.

### 7.7 Dependency matrix: what each outage silently removes

`fault_injection.py` kills the scorer. This kills what the scorer leans on, and
asks the question that matters for a fail-open design: not "did it crash" but
"what did it stop doing without saying so". Each expectation was written down
BEFORE the run, in `EXPECTED` in `dependency_failure.py`, so the result is a test
of a prediction rather than a description of whatever happened.

Loss is counted as **offered minus stored** — what the producer reported
delivering, against the row delta in the warehouse. Neither half of that was
obvious: §7.7b lists three ways this harness computed a confident wrong number
before it computed a right one.

1,000 transactions per arm, each arm stopping one service, producing through the
outage, restarting it and letting the topic drain.

| stopped | offered | stored | lost | prediction |
|---|---:|---:|---:|---|
| redis | 1,000 | 1,000 | 0 | held |
| neo4j | 1,000 | 1,000 | 0 | held |
| clickhouse | 1,000 | **0** | **1,000 (100%)** | held |
| kafka (mid-stream, 20 s) | 1,000 | 1,000 | 0 | held |

**All four predictions held. None of these arms is a discovery, and the
ClickHouse row least of all** — stopping a database and observing that nothing
was written to it is a tautology, and the mechanism behind the 100% was already
stated in the sink's own code: `consumer.py` sets `enable_auto_commit=True`, so
offsets advance on a timer regardless of whether the insert succeeded, and
`ch_writer._discard` logs in as many words that "Kafka offsets have already
advanced, so these events will not be re-delivered." The measurement confirms a
documented design property; it did not find one.

What the run adds beyond the code is narrower and worth stating at its real
size: the reconnect path (`_reconnect_due`) rescues **nothing** — not a partial
batch, not the tail — so the loss is the whole slice rather than some fraction
of it; and the failure is silent across component boundaries, which no single
file shows. Scoring was unaffected, alerts were published, and the analyst queue
filled normally while the warehouse took a 1,000-row hole.

That makes the ClickHouse arm useful for one thing, and it is not a defect
report. It puts a number on a **trade the design makes deliberately**: the
pipeline keeps deciding on live payments while the warehouse is gone, and pays
for that with the completeness of the audit trail. Blocking instead would stop
detection on real transactions, which is the worse outage. For a system that
offers its audit trail as a defensibility argument (§8, §9.4), the qualification
this earns is that **the record is the part that yields first**, by choice, and
that a routine warehouse restart is enough to exercise it. Corroborated by
`docker compose logs sink-writer | Select-String DISCARDED`.

The Kafka arm is the one that had to be redesigned to say anything. Stopping the
transport before producing means the producer cannot bootstrap: nothing is
offered, and an arm that offered nothing cannot lose anything — the experiment
tested nothing while appearing to run. Taken out MID-STREAM for twenty seconds,
the result is that the producer still reported `produced 1,000 messages` and all
1,000 reached the warehouse. Client-side buffering and retry absorbed the outage
completely, and the job resumed from its committed offsets. AT_LEAST_ONCE held
in both directions.

**On the loss column the matrix validated four predictions and discovered
nothing** — which is what engineering validation looks like when the design is
understood, and is worth reporting as such rather than dressed up. The discovery
is in the other column, and it took two attempts to reach: the alert-mix
measurement was first reporting the contents of the test slice rather than the
effect of the outage, exposed by the arm that predicted no degradation. Rebuilt
as a paired design, it produced the one result here that was not predicted in
advance — **the pipeline fails open without failing fast**, and a 1,000-event
slice that drains in six seconds healthy had not drained in five minutes with
Redis or Neo4j gone (§7.7a).

### 7.7a Fail-open is not fail-fast: what the paired run actually found

The matrix reports a second quantity beside loss: which alert types stop
appearing while a dependency is down. The first attempt at it was an artefact —
every arm replayed the same slice and was compared against the whole table, so
all three arms reported the same "degradation", **including the arm that
predicted none**. An effect visible in the control is not an effect.

The rerun fixed the design: a healthy CONTROL pass over the same 1,000
transactions supplies the reference mix, Redis state is flushed before every
pass (all three namespaces — `age:*`, `rcv:*` and `mule:fanin:hist`), and the
dependency stays down **through the drain** so no record is scored healthy.
Results, alerts by `predicted_type`:

| arm | MULE | STRUCTURING | ATO | drain time |
|---|---:|---:|---:|---|
| control (healthy) | 23 | 4 | 1 | ~6 s |
| redis down | 22 | 4 | 1 | **did not drain in 300 s** |
| neo4j down | 31 | 4 | 1 | **did not drain in 300 s** |
| clickhouse down | — | — | — | ~6 s |
| kafka mid-stream | 25 | 4 | 1 | ~6 s |

Three things fall out, and the third is the finding.

**The alert-type column cannot test its own prediction, and no experimental
design fixes that.** The prediction for the Redis arm is that `MULE_FAN_IN`
stops firing. The column reports `predicted_type`, and `fusion._TYPE_PRIORITY`
maps the MULE label to `DISTINCT_PAYEE_BURST` and `VELOCITY` — `MULE_FAN_IN` is
**not in the table at all**. Both mapped rules are sender-side, served from
Flink keyed state, and touch neither Redis nor Neo4j. So a Redis outage was
never going to move this number, and 22-against-23 says nothing about the
prediction. This is a category error in the metric, not a confound in the
experiment: the prediction is about a *rule*, the column reports a *pattern
label* derived from different rules. Testing it needs `rule_hits`, which the
warehouse stores and this query does not read.

**The Kafka arm supplies the noise floor.** It predicts no degradation and is
the closest thing to a second control: +2 MULE against the reference with no
treatment applied. Redis at −1 is inside that; Neo4j at +8 is outside it.

**Failing open is not the same as failing fast, and only the clock shows it.**
The healthy pass drained 1,000 events in about six seconds. With Redis down, and
again with Neo4j down, the same 1,000 events **had not drained after five
minutes** — a slowdown of at least fifty times. The ClickHouse arm drained
normally, which localises it: ClickHouse is downstream of the decision, while
Redis and Neo4j sit on the synchronous per-event lookup path (§7.6).

The mechanism is in `enrichment.py` and is not subtle once looked for. Neither
client is constructed with a timeout — no `socket_timeout` or
`socket_connect_timeout` on the Redis handle, no `connection_timeout` on the
Neo4j driver. And the handle is only set to `None` inside `open()`, which runs
once per worker at job start: a dependency that dies *later* leaves a live
client object behind, so **every subsequent event pays a failed round trip** —
for Redis a failed read and a failed cache write, for Neo4j a session open that
cannot connect. The pipeline keeps deciding, exactly as designed, and stops
keeping up.

For a system whose entire claim is a 300 ms budget, that distinction is the
whole point: **a decision that arrives after the payment has settled is not a
degraded decision, it is no decision.** The row count cannot see this, the alert
mix cannot see this, and the containers stay `Up` throughout — it belongs with
the silent failures of §8 rather than with the loss column above.

It also compounds with §7.6 rather than duplicating it. There, the synchronous
lookup caps healthy throughput at 25 events/s. Here the same synchronous lookup,
against an *absent* dependency and with no timeout to bound it, drives that cap
toward zero. One design decision, two measured costs. The fix is small and
declared rather than made: bound both clients with timeouts, and mark a client
dead after N consecutive failures so the fail-open path stops paying for a
connection that is not coming back.

**A second result, weaker and stated as such.** Losing Neo4j did not remove
alerts, it added them: +8 MULE, above the ±2 noise floor the Kafka arm
establishes. A mechanism exists in the code and matches the direction. In the
default `receiver_age` mode an unknown age is encoded as `-1.0`, which sorts
*below every real account age* — so to a model trained on "younger is riskier",
an unreachable Neo4j does not read as "unknown", it reads as "newer than the
newest account that exists". The argument against exactly this is already
written in `features.py`, in the comment explaining why the `on_us` branch uses
NaN instead ("a sentinel such as -1 would be ordered against real ages"); it was
simply never applied to the default branch. One arm is not enough to establish
this, and it does not need the cluster to settle: replaying the slice offline
with the age forced to unknown would separate the mechanism from the noise.


### 7.7b Four ways this harness computed a confident wrong number

Recorded because all four produced output that looked like a result, and because
three of them were caught only by a value that could not have been true:

1. **`uniqExact(transaction_id)` as the loss denominator.** The producer replays
   the same CSV, so distinct ids do not grow. The tool reported "1000 LOST" for
   four services including two it had never touched. Distinct count is a
   duplication indicator, not a loss measurement.
2. **`--expect` as the loss denominator.** What the producer was asked to send is
   not what reached the topic, and the two differ for exactly one arm. The first
   version printed the same caution — "check whether the producer could send at
   all" — for both the Kafka case, where it is correct, and the ClickHouse case,
   where it is wrong. That single conflation reported the matrix's one
   data-loss result as an inconclusive arm.
3. **The whole table as the alert-mix denominator**, with no control pass and no
   state reset — §7.7a.
4. **The service restart in the success path.** A producer that died left the
   dependency stopped, so the next run took no baseline — and because PowerShell
   ignores a native command's exit code, the arm then produced traffic for five
   minutes before reporting that it had nothing to compare against. The restart
   is now in a `finally` and the baseline's exit code is checked.

The pattern across all four is worth naming, because it is the same one §8
catalogues for the pipeline: **none of these failed. Each returned a plausible
number.** A measurement harness is a piece of production software with no user
to notice when it is wrong, and the only defences that worked here were a
predicted value written down before the run, and an arm that was supposed to
show nothing.

For chapter 7 this section reduces to one sentence of method: *loss is measured
as delivered minus stored rather than requested minus stored, and the alert mix
against a healthy control pass over the same transactions, because the producer
replays a fixed slice and stopping the transport also stops the offer.*

## 8. Silent failure modes

**Fourteenth, found by the dependency matrix and belonging here rather than in
§7:** with Redis or Neo4j gone the pipeline keeps deciding and stops keeping up
— a 1,000-event slice that drains in six seconds healthy had not drained in five
minutes. Neither client carries a timeout, and the handle is only nulled at
`open()`, so a dependency that dies later leaves a live client that every
subsequent event pays for. Containers `Up`, no exception, alerts still
published, offsets still advancing. See §7.7a.

Thirteen distinct failures were found across two working sessions — eight while
building the pipeline, five more while building the alert consumer that closes
it. Ten of them left the system looking healthy: containers `Up`, no exception
anywhere, Kafka offsets advancing, the producer reporting success. They are
collected here because the pattern is the result, not the individual bugs.

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
**1.89 ms per event** (400 trees, 24 features, LightGBM `pred_contrib`), which
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
