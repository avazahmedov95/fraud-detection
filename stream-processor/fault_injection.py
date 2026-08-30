"""
Fault injection: what actually happens to a transaction when the job dies.

Reviewer point 6 asks to "validate exactly-once via deliberate fault injection".
The honest starting position is that **this system does not provide exactly-once
and does not claim to**:

    fraud_job.py      DeliveryGuarantee.AT_LEAST_ONCE
    ClickHouse        MergeTree, no deduplication on transaction_id
    Redis fan-in      external to the checkpoint, so it does not roll back

So the question this answers is not "is it exactly-once" — it is not — but the
two that matter for money:

    1. Is any transaction LOST when a worker dies mid-stream?
    2. What is the cost of the duplicates that at-least-once permits?

Loss would be a correctness failure. Duplication is a design choice with
consequences worth quantifying rather than hiding.

Usage — three terminals, or three steps:

    # 1. baseline: how many rows before the fault
    python fault_injection.py --phase before

    # 2. start a paced producer, then kill the worker mid-stream
    #    (from the repo root, in another terminal)
    #    .\run.ps1 produce-stream-docker
    docker compose kill taskmanager && docker compose start taskmanager

    # 3. after the job recovers and the topic drains
    python fault_injection.py --phase after --expect 50000

Reports transactions lost, duplicated, and where duplication lands.
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "fault_injection_state.json")


def query(sql):
    url = (f"http://{CH_HOST}:{CH_PORT}/?"
           + urllib.parse.urlencode({"query": sql + " FORMAT TabSeparated",
                                     "user": CH_USER, "password": CH_PASSWORD}))
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            body = resp.read().decode()
    except Exception as exc:                           # noqa: BLE001
        raise SystemExit(f"ClickHouse unreachable at {CH_HOST}:{CH_PORT} — {exc}")
    return [line.split("\t") for line in body.strip().splitlines() if line]


def snapshot():
    rows = query(f"SELECT count(), uniqExact(transaction_id) "
                 f"FROM {CH_DB}.transactions_scored")
    total, distinct = (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)
    return {"total": total, "distinct": distinct}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["before", "after"], required=True)
    ap.add_argument("--expect", type=int, default=None,
                    help="transactions the producer sent, for the loss check")
    args = ap.parse_args()

    snap = snapshot()

    if args.phase == "before":
        with open(STATE, "w") as fh:
            json.dump(snap, fh)
        print(f"baseline recorded: {snap['total']:,} rows, "
              f"{snap['distinct']:,} distinct transaction_id\n")
        print("Now, with a paced producer running, kill the worker:")
        print("    docker compose kill taskmanager && docker compose start taskmanager")
        print("\nThen re-run with --phase after --expect <transactions sent>.")
        return

    if not os.path.exists(STATE):
        raise SystemExit("no baseline — run --phase before first")
    with open(STATE) as fh:
        base = json.load(fh)

    written = snap["total"] - base["total"]
    unique = snap["distinct"] - base["distinct"]
    dupes = written - unique

    # Nothing arrived between the two phases. Reporting that as "100% lost"
    # would be a false alarm about the most serious property the script checks,
    # so it is caught before anything else is claimed.
    if written <= 0:
        print("=" * 66)
        print("NO DATA — the experiment did not run")
        print("=" * 66)
        print(f"  rows at baseline : {base['total']:,}")
        print(f"  rows now         : {snap['total']:,}")
        print("\nNothing was written between the two phases, so there is nothing"
              "\nto measure — this says nothing about loss or duplication.\n")
        print("The sequence needs traffic flowing WHILE the worker is killed:")
        print("\n  1. python fault_injection.py --phase before")
        print("  2. in another terminal:  .\\run.ps1 produce-stream-docker")
        print("     leave it running")
        print("  3. after ~30 s, in a third terminal:  .\\run.ps1 kill-worker")
        print("  4. let the producer run another minute, then stop it (Ctrl+C)")
        print("  5. wait ~30 s for the job to recover and drain the topic")
        print("  6. python fault_injection.py --phase after --expect <sent>")
        print("\n`--expect` is the number the PRODUCER sent during this window,"
              "\nnot the size of the CSV. produce-stream is paced, so a few"
              "\nminutes sends a few thousand — check with:")
        print("     .\\run.ps1 query-scored")
        print("\nIf rows are still not arriving, the job itself may be down:")
        print("     docker compose ps")
        print("     Flink UI: http://localhost:8081")
        return

    print("=" * 66)
    print("FAULT INJECTION RESULT")
    print("=" * 66)
    print(f"  rows written        : {written:,}")
    print(f"  distinct transactions: {unique:,}")
    print(f"  duplicate rows      : {dupes:,}"
          f"  ({dupes/max(written,1):.2%} of what was written)")

    # Rows arriving that add no new transaction ids means the job is re-reading
    # the topic, not processing new traffic. Duplicates from reprocessing are
    # indistinguishable from duplicates caused by the fault, so the measurement
    # is void — say so rather than reporting a number that means nothing.
    if unique == 0 and written > 0:
        ratio = snap["total"] / max(snap["distinct"], 1)
        print("\n" + "!" * 66)
        print("MEASUREMENT VOID — the job is reprocessing, not progressing")
        print("!" * 66)
        print(f"   {written:,} rows arrived and not one carried a transaction id"
              f"\n   that was not already stored. Across the whole table each"
              f"\n   transaction now appears {ratio:.1f} times on average"
              f"\n   ({snap['total']:,} rows over {snap['distinct']:,} transactions).")
        print("\n   The job restarts from the beginning of the topic instead of"
              "\n   from where it stopped, so duplicates here come from"
              "\n   resubmission rather than from the injected fault, and the"
              "\n   two cannot be separated.")
        print("\n   Cause: checkpoints were not retained beyond the job's life,"
              "\n   so a resubmitted job had nothing to restore from. Fixed by"
              "\n   RETAIN_ON_CANCELLATION plus a checkpoint directory on a"
              "\n   named volume (config.CHECKPOINT_DIR).")
        print("\n   To redo the measurement on a clean slate:")
        print("     .\\run.ps1 clean")
        print("     .\\run.ps1 latency-setup")
        print("     then the before / produce / kill / after sequence.")
        print("\n   This finding matters on its own: in production, every"
              "\n   deployment would have re-alerted on the entire retained"
              "\n   topic — transfers that settled days earlier.")
        return

    print("\n1. LOSS — the property that must hold")
    if args.expect is None:
        print(f"   {unique:,} distinct transactions arrived. Pass --expect <n>"
              f"\n   with the number the producer sent to check for loss;"
              f"\n   ClickHouse alone cannot know what was sent.")
    elif args.expect < unique:
        print(f"   --expect {args.expect:,} is below the {unique:,} distinct"
              f"\n   transactions actually stored, so the expectation is wrong"
              f"\n   rather than the pipeline. Use the count the producer"
              f"\n   reported for THIS window.")
    else:
        lost = args.expect - unique
        if lost == 0:
            print(f"   {unique:,} of {args.expect:,} transactions present — "
                  f"NOTHING LOST")
            print("   Checkpointing plus committed Kafka offsets replayed the"
                  "\n   window between the last checkpoint and the kill.")
        else:
            print(f"   {lost:,} transactions MISSING of {args.expect:,} "
                  f"({lost/args.expect:.2%})")
            print("   Before calling this a correctness failure, confirm the"
                  "\n   producer finished and the topic fully drained — a"
                  "\n   still-draining topic looks identical to loss.")

    print("\n2. DUPLICATION — the cost of AT_LEAST_ONCE")
    if dupes == 0:
        print("   None observed. Note this does not prove exactly-once: with a"
              "\n   short checkpoint interval the replay window is small, so a"
              "\n   kill can easily land between checkpoints with nothing in"
              "\n   flight. Repeat under load before concluding anything.")
    else:
        rows = query(f"""
            SELECT decision, count() AS n
            FROM (SELECT transaction_id, any(decision) AS decision
                  FROM {CH_DB}.transactions_scored
                  GROUP BY transaction_id HAVING count() > 1)
            GROUP BY decision ORDER BY n DESC""")
        print("   duplicated transactions by decision:")
        for r in rows:
            print(f"     {r[0]:<10}{int(r[1]):>8,}")
        print("\n   What each duplicate costs:")
        print("     ALLOW   — a duplicate row in the warehouse. Inflates volume"
              "\n               metrics; no operational effect.")
        print("     REVIEW  — a second identical case in the analyst queue."
              "\n               Wasted work, and erodes trust in the queue.")
        print("     BLOCK   — a second alert on an already-blocked transfer."
              "\n               Safe: blocking twice does not double-block, but"
              "\n               it does inflate the reported fraud count.")
        # Whether the copies agree is not assumed — it is queried. Replay is
        # NOT a pure function of the event (see section 4), so copies of one
        # transaction can carry different scores.
        rows = query(f"""
            SELECT count() AS n,
                   countIf(smax - smin > 0.00005) AS differing,
                   round(max(smax - smin), 4) AS worst,
                   countIf(n_decisions > 1) AS decision_flips
            FROM (SELECT transaction_id,
                         min(final_score) AS smin, max(final_score) AS smax,
                         uniqExact(decision) AS n_decisions
                  FROM {CH_DB}.transactions_scored
                  GROUP BY transaction_id HAVING count() > 1)""")
        if rows:
            n, differing, worst, flips = (int(rows[0][0]), int(rows[0][1]),
                                          float(rows[0][2]), int(rows[0][3]))
            print("\n   Are the copies identical?")
            print(f"     duplicated transactions      : {n:,}")
            print(f"     with a DIFFERENT final_score : {differing:,}")
            print(f"     largest score divergence     : {worst}")
            print(f"     where the DECISION changed   : {flips:,}")
            if differing:
                print("\n   Re-scoring is not reproducing the original score. The"
                      "\n   event is identical, so the difference comes from state"
                      "\n   that did not roll back with the checkpoint — see"
                      "\n   section 4. A duplicate is therefore not merely a"
                      "\n   redundant row: it is a second, differently-computed"
                      "\n   opinion about the same transfer.")
            if flips:
                print("\n   DECISION CHANGED on replay for"
                      f" {flips:,} transaction(s). Two rows"
                      "\n   for one transfer disagree about what to do with it,"
                      "\n   and which one an operator sees depends on query order.")

    print("\n3. WHAT WOULD MAKE THIS EXACTLY-ONCE")
    print("""   Not attempted here, and the reasons are worth stating:

     a) ClickHouse ReplacingMergeTree keyed on transaction_id. Cheapest fix,
        but deduplication is eventual — a query before the merge still sees
        both rows.
     b) DeliveryGuarantee.EXACTLY_ONCE on the Kafka sink. Transactional
        writes, at the cost of consumers reading only committed data, which
        adds the checkpoint interval to end-to-end latency — measured at
        2 s here, against a 300 ms budget. That trade is why AT_LEAST_ONCE
        was chosen.
     c) Idempotent sink keyed on transaction_id. Correct and latency-free,
        but requires a deduplication store sized to the replay window.

   For a fraud pipeline (b) is the wrong trade: duplicate alerts are cheap,
   and a 2-second detection delay is not. (a) or (c) are the defensible
   routes, and the audit chain in sink-writer/integrity.py already gives a
   way to identify duplicates after the fact.""")

    print("\n4. THE FAN-IN STORE — WHY THE COPIES DISAGREE")
    print("""   Redis is external to the Flink checkpoint, so it does NOT roll
   back on restore. The WRITE side is idempotent: the sorted-set member encodes
   transaction_id, so a replayed record() is a no-op. (Before that fix the
   member was time|sender|amount, under which two genuinely distinct transfers
   sharing all three collapsed into one — and identical amounts from one sender
   in the same second is precisely what structuring and mule runs look like.
   See test_receiver_store.py.)

   The READ side is not idempotent, and that is the finding. fraud_job.py does:

       receiver_state = self._receivers.load(...)   # read the payee's window
       result         = evaluate(..., receiver_state)
       self._receivers.record(event, ...)           # then append this transfer

   On replay the window returned by load() already contains this transaction —
   written during the first pass — together with every later transaction that
   was also processed before the kill. features.py then computes

       rcv_inflow  = sum(amounts in window) + amount
       rcv_senders = |{senders in window} u {this sender}|

   so the transfer's own amount is counted twice and the payee looks busier than
   it was. The feature vector on the second pass is not the vector of the first,
   and the model returns a different probability from an identical event.

   The drift is one-directional — replay can only ADD members to the window,
   never remove them — so a replayed transfer looks riskier, not safer. That is
   the benign direction for a fraud system, but it is a property of the fan-in
   store rather than a guarantee: the model is not monotonicity-constrained, and
   nothing stops a score crossing FINAL_REVIEW_THRESHOLD on the second pass.

   Consequences worth stating plainly:
     * "at-least-once means duplicate rows" understates it here. It means
       duplicate rows that may disagree.
     * Deduplicating by transaction_id (ReplacingMergeTree, or an idempotent
       sink) silently picks one of two different answers unless the version
       column is chosen deliberately.
     * Any receiver-side feature computed from an external store has this
       property. Only state inside the Flink checkpoint replays exactly.""")


if __name__ == "__main__":
    main()
