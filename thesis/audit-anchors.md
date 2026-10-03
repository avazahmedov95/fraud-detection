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

> **The warehouse this anchor was recomputable against was truncated on
> 2026-09-07**, to clear the way for runs on the regenerated dataset
> (`generator-spec.md`, "The dataset of record"). The rows were exported first,
> to `_warehouse_backup_2026-09-07/` — gitignored, `audit_log.native` plus the
> scored and case tables in ClickHouse `Native` format. The export was verified
> by restoring it into a scratch table and confirming both the row count (87,470)
> and the presence of the head hash above **before** the truncation ran.
>
> Recorded because the sentence above promises something an empty table cannot
> deliver. To check the anchor now: restore the export
> (`cat _warehouse_backup_2026-09-07/audit_log.native | clickhouse-client -q
> "INSERT INTO fraud.audit_log FORMAT Native"`) into an empty warehouse, then run
> `verify-audit`. If that file is ever lost, this anchor becomes a claim about a
> database nobody can produce — which is the honest status to record rather than
> to leave implied.

### 2026-09-12 - reference latency run on the regenerated dataset

- **Run.** A fresh job (empty keyed state) on a stack whose warehouse and Redis
  were empty and whose graph matched the dataset, then `produce-stream-docker
  7000` with the producer inside the Docker network and no faults injected.
  Not through `latency-setup`: the stack had been started with `resume-job`
  from a 2026-09-07 checkpoint built on the previous dataset, so that job was
  cancelled and replaced rather than the volumes wiped. Figures:
  `irp-framing.md` 7.1b.
- **Dataset.** The dataset of record, rows 0..7000 of
  `data-generator/out/transactions.csv`, SHA-256
  `ca8a3dcd48bf586be144e6c7b96c78f27d0a7edfe57bfe1282994d6ab53f7899`
  (`generator-spec.md`, "The dataset of record").
- **Chain.** 7,000 audit records, seq 0..6999, INTACT, no gaps, projections
  consistent.
- **Head `record_hash`:**

```
c50b725bddee106d33b2512c582a17b31969fb5fff8b86f2d12a04d584f83d20
```

The host was in Modern Standby from 23:11:09 to 23:27:25 local time during this
run (Windows System log, Kernel-Power 506 and 507). The chain runs straight
across the pause - it is a property of the stored records, not of wall time -
but the 24 decisions taken just before it reached the warehouse only after it.

> **Exported and then cleared on 2026-09-12**, ahead of the fault-injection
> series, which refuses to start against a non-empty warehouse. Exported first
> to `_warehouse_backup_2026-09-12/` (gitignored, ClickHouse `Native`), and the
> export verified by restoring every table into a scratch copy - 7,000, 7,000
> and 278 rows matched, and the head hash above was present - before
> `latency-setup` removed the volumes. To check the anchor, restore and run
> `verify-audit` exactly as for the 2026-08-31 entry.

## Change of hashed field list — 03.09.2026

`receiver_region` was removed from `integrity.INGRESS_FIELDS`, for the same
reason and by the same test: a sending bank holds the destination PAN and
nothing else, so it cannot know where the receiver is, and the field was never
read by any feature or rule - it travelled the wire, entered the hash and was
stored, unused. Removed from the wire format, the ingress hash, the job's output
record, `transactions_scored` and its schema (an `ALTER TABLE ... DROP COLUMN`
ships with the DDL, since init scripts run only on an empty data dir).

Second **breaking change to the ingress hash**: records after this date are
hashed over eight fields. The known-answer vector in both `test_integrity.py`
copies moved from `80f4245868803adc…` to `1e3525aea867ee63…`; those tests failed
on the change, which is what they are for, and were re-anchored deliberately.

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
