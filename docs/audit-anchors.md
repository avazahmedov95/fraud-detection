# Audit chain anchors

`sink-writer/integrity.py` chains every decision record: each one carries a hash
over its own content and over its predecessor's hash, so any edit, deletion or
reorder inside the warehouse breaks the chain, and `verify_audit.py` finds the
break.

The chain alone leaves exactly one gap, and the tool says so even when it
passes: an attacker able to rewrite *every* record from some point onward can
recompute the chain from there and leave it internally consistent. Closing that
gap requires the head hash to exist somewhere the attacker does not control.

This file is that somewhere. Each entry pins the head of a chain to a commit,
and the commit's timestamp on the remote is what gives the pin its meaning.

## What this is, and what it is not

**It is** evidence that a given head hash existed no later than the commit that
introduced it, timestamped by a third party, and orderable against every other
commit in the repository.

**It is not** a timestamping authority. The repository belongs to the same
person as the warehouse, and history can be rewritten and force-pushed. A
signed tag raises the cost of doing so quietly; a public timestamping service or
a transparency log would close the gap properly. In a bank the anchor would sit
somewhere neither the operator of the warehouse nor the operator of the pipeline
controls - which is the same separation-of-duties argument the WORM grants on
the audit table make inside ClickHouse.

Saying this plainly is the point. An anchor whose limits are unstated is worth
less than no anchor, because it invites more confidence than it earns.

## Anchors

### 2026-08-31 - reference latency run

- **Run.** `latency-setup` on a clean stack, then `produce-stream-docker 7000`
  with the producer inside the Docker network and no faults injected. This is
  the run reported in `irp-framing.md` 7.1a: 5,956 records inside the reporting
  window, p99 176 ms on the decision path, 2 breaches of the 300 ms target.
- **Dataset.** The frozen dataset of record, rows 0..7000 of
  `data-generator/out/transactions.csv`, SHA-256
  `b767f38489ab65628028b91638ca6cbfa7e0377128c0f86e844dffb35e0db596`
  (see `generator-spec.md`, "The dataset of record").
- **Chain.** 7,000 audit records, seq 0..6999, INTACT, no gaps, projections
  consistent.
- **Head `record_hash`:**

```
3b20c07cffeb3a6a19c8078205b77ba24ca4cec48d35d14d967747874a896184
```

Recompute with `.\run.ps1 verify-audit` against the same warehouse. The head is
derived from the stored records, so a match means that nothing between this
commit and that query altered, removed or reordered a decision.

## Change of hashed field list — 01.09.2026

`receiver_pinfl` was removed from `integrity.INGRESS_FIELDS` when it was removed
from the wire: a hash can only bind fields the event actually carries, and
`receiver_card` is the identifier a sending bank holds for the payee.

This is a **breaking change to the ingress hash**. Records anchored before this
date were hashed over the ten-field list including `receiver_pinfl`; records
after are hashed over nine. Both are verifiable, but only against the definition
in force when they were written, so the anchor registry above should be read
with this line beside it. The head
`3b20c07cffeb3a6a19c8078205b77ba24ca4cec48d35d14d967747874a896184`
(7,000 records) predates the change.

The chained `record_hash` is unaffected — it folds the previous hash, a sequence
number and the record's own content, none of which changed.
