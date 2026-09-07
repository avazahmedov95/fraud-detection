"""Break one thing, and measure what the pipeline silently stops doing.

Two arms of one experiment, and they were two files that shared their ClickHouse
access, their before/after state machinery, and a `query()` identical down to its
comment:

  --service scorer   kills the SCORER mid-stream: how many transactions are lost,
                     how many duplicated, and where the duplicates disagree.
                     docs/irp-framing.md 5, point 6.
  --service redis | neo4j | clickhouse | kafka
                     kills what the scorer LEANS ON, and asks the question that
                     matters for each: not "did it crash" but "what did it
                     silently stop doing". docs/irp-framing.md 7.7.
  --service control  breaks nothing. It is the reference the other arms are
                     measured against.

Every dependency here fails open by design, which means every one of them
degrades without an error. The point of the measurement is to say what each
degradation costs, in rules that stop firing and rows that stop arriving.

  python outage.py --phase before
  #   ... produce traffic, kill the taskmanager, let it recover ...
  python outage.py --phase after --expect 1000

  python outage.py --service redis --phase before
  #   ... stop the container, produce traffic, start it again ...
  python outage.py --service redis --phase after --expect 1000

Loss is what was OFFERED to the topic minus what the warehouse holds. For the
kafka arm the outage stops the producer too, so that arm also needs --sent.
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

# Anchored to the package: .gitignore names both state files at
# stream-processor/, and an experiment half-finished before this move must
# still find the baseline it wrote.
_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = os.path.join(_PKG, "fault_injection_state.json")
#: Where the dependency arms kept their baselines while they were a second
#: script. Read if present, never written: a half-finished experiment must not
#: lose its `before` pass because the two harnesses were merged underneath it.
LEGACY_STATE = os.path.join(_PKG, "dependency_failure_state.json")

#: What each dependency is expected to take away. Written down BEFORE the run so
#: the result is a test of a prediction rather than a description of whatever
#: happened - and so a degradation nobody predicted stands out.
EXPECTED = {
    "scorer": (
        "the scorer itself, killed mid-stream. Expect NOTHING lost - "
        "checkpointing plus committed offsets replay the window between the "
        "last checkpoint and the kill - and a small number of duplicates that "
        "may DISAGREE with one another, because the fan-in store does not roll "
        "back with the checkpoint. This arm reports loss and duplication in "
        "full rather than as an alert-mix delta."),
    "control": (
        "nothing. This pass runs the same slice with every service healthy, and "
        "its alert mix is the REFERENCE the other arms are measured against. "
        "Without it the only available comparison is against the whole table, "
        "which answers a different question - what is in this slice - and "
        "answers it identically for every arm."),
    "redis": (
        "enrichment cache, the payee's inbound window and the population "
        "baseline all live here. Expect: no loss, receiver_age unknown, and "
        "MULE_FAN_IN to stop firing entirely - the fan-in features read zero "
        "when the store is unreachable."),
    "neo4j": (
        "the account-age lookup and the alert graph. Expect: no loss, "
        "FRESH_RECEIVER to stop firing once the Redis age cache expires, and "
        "graph writes to be DISCARDED with a running total in the log."),
    "clickhouse": (
        "the warehouse and the audit trail. Expect: scoring unaffected and "
        "alerts still published, but rows discarded - and Kafka offsets advance "
        "regardless, so those rows are unrecoverable. This is the one failure "
        "that loses data by design."),
    "kafka": (
        "the transport itself, taken out MID-STREAM - stopping it first would "
        "only stop the producer. Expect the job to restart and resume from its "
        "committed offsets: nothing lost of what was delivered, latency spiking "
        "to the length of the outage for whatever was in flight. The producer "
        "is hit too, so it may deliver fewer than asked; that shortfall is not "
        "loss, which is why this arm is scored against --sent."),
}


def query(sql):
    url = (f"http://{CH_HOST}:{CH_PORT}/?"
           + urllib.parse.urlencode({"query": sql + " FORMAT TabSeparated",
                                     "user": CH_USER, "password": CH_PASSWORD}))
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            body = resp.read().decode()
    except Exception as exc:                           # noqa: BLE001
        raise SystemExit(f"ClickHouse unreachable at {CH_HOST}:{CH_PORT} - {exc}")
    # strip("\n"), not strip(): a bare .strip() eats the LEADING TAB of the first
    # row when its first column is empty - and predicted_type is empty for every
    # alert no rule explains. The row then parses as one field and the caller
    # gets an IndexError on a query that returned perfectly good data.
    return [line.split("\t") for line in body.strip("\n").splitlines() if line]


def snapshot():
    """Counts, plus which rules were firing. The rule mix is the measurement:
    a dependency going away removes signals, and the row count alone cannot
    see that."""
    n = query(f"SELECT count(), uniqExact(transaction_id) FROM {CH_DB}.transactions_scored")
    total, distinct = (int(n[0][0]), int(n[0][1])) if n else (0, 0)
    hits = query(f"SELECT predicted_type, count() FROM {CH_DB}.transactions_scored "
                 f"WHERE decision != 'ALLOW' GROUP BY predicted_type")
    return {"total": total, "distinct": distinct,
            "types": {r[0] or "(none)": int(r[1]) for r in hits if len(r) == 2}}


REFERENCE = "_reference"


def pass_mix(before, snap):
    """Alert types produced by THIS pass - the delta, not the table's total."""
    return {t: snap["types"].get(t, 0) - before["types"].get(t, 0)
            for t in set(before["types"]) | set(snap["types"])}


def report_mix(mix, ref):
    """Print this arm's alert types beside the healthy pass's, and name what
    the outage silenced."""
    # Comparing a degraded pass against the reference pass over the SAME
    # transactions is the whole point. The first version compared each arm's
    # delta against the whole-table baseline, so what it actually reported was
    # "which types occur in rows 0-1000 of the CSV" - identical for every arm,
    # including the arm that predicted no degradation at all. An effect that
    # shows up in the control is not an effect.
    types = sorted(set(mix) | set(ref), key=lambda t: -max(ref.get(t, 0), mix.get(t, 0)))
    print("\n  alert types, this arm vs the healthy pass on the same slice:")
    print(f"    {'type':<16}{'healthy':>9}{'this arm':>10}{'change':>9}")
    silenced, reduced = [], []
    for t in types:
        r, m = ref.get(t, 0), mix.get(t, 0)
        if not r and not m:
            continue
        d = m - r
        note = ""
        if r and not m:
            note = "  SILENCED"
            silenced.append(t)
        elif r and d < 0:
            note = f"  -{-d / r:.0%}"
            reduced.append(t)
        print(f"    {t:<16}{r:>9}{m:>10}{d:>+9}{note}")
    if silenced:
        print(f"\n  STOPPED FIRING: {', '.join(silenced)}")
    if reduced:
        print(f"  reduced but still firing: {', '.join(reduced)}")
    if not silenced and not reduced:
        print("\n  no type lost ground against the healthy pass")


def offered_count(args):
    """How many messages actually reached the topic - the denominator for loss.

    Returns (count, how it is known), or (None, why it cannot be).
    """
    # --expect is what the producer was ASKED to send, which is not what was
    # offered for exactly one arm. Redis, Neo4j and ClickHouse sit downstream of
    # the topic - the producer talks to Kafka and to nothing else - so stopping
    # them leaves the offer intact, and a missing row is a row the pipeline
    # dropped. Stopping KAFKA stops the offer itself, and counting unsent
    # messages as lost would invent a failure the system never had. The first
    # version of this branch refused to call EITHER case a loss, which hid the
    # one arm that loses data by design.
    if args.sent is not None and args.sent >= 0:
        return args.sent, "the producer reported delivering this many"
    if args.service == "kafka":
        return None, ("stopping kafka stops the producer as well as the "
                      "pipeline, so a missing row may never have been sent. "
                      "Re-run with --sent N from the producer's "
                      "`produced N messages` line; without it this arm is "
                      "silent on loss.")
    if args.expect is None:
        return None, "neither --expect nor --sent given: nothing to compare"
    if args.service == "control":
        return args.expect, "--expect: nothing was stopped in this pass"
    return args.expect, (f"--expect: stopping {args.service} cannot stop the "
                         f"producer, which talks only to Kafka")


def _load_state():
    """Baselines, keyed by service.

    The scorer arm kept a bare {"total", "distinct"} while it was a separate
    script with one arm. A file in that shape is read as the scorer's baseline
    rather than rejected: it is gitignored run state, and failing an experiment
    over the format of its own scratch file would be a small silent failure of
    exactly the kind this script exists to find.
    """
    state = {}
    if os.path.exists(LEGACY_STATE):
        with open(LEGACY_STATE, encoding="utf-8") as fh:
            state.update(json.load(fh))
    if os.path.exists(STATE):
        with open(STATE, encoding="utf-8") as fh:
            blob = json.load(fh)
        state.update({"scorer": blob} if "total" in blob else blob)
    return state


def _save_state(state):
    with open(STATE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def _report_scorer(base, snap, expect):
    written = snap["total"] - base["total"]
    unique = snap["distinct"] - base["distinct"]
    dupes = written - unique

    # Nothing arrived between the two phases. Reporting that as "100% lost"
    # would be a false alarm about the most serious property the script checks,
    # so it is caught before anything else is claimed.
    if written <= 0:
        print("=" * 66)
        print("NO DATA - the experiment did not run")
        print("=" * 66)
        print(f"  rows at baseline : {base['total']:,}")
        print(f"  rows now         : {snap['total']:,}")
        print("\nNothing was written between the two phases, so there is nothing"
              "\nto measure - this says nothing about loss or duplication.\n")
        print("The sequence needs traffic flowing WHILE the worker is killed:")
        print("\n  1. python outage.py --phase before")
        print("  2. in another terminal:  .\\run.ps1 produce-stream-docker")
        print("     leave it running")
        print("  3. after ~30 s, in a third terminal:  .\\run.ps1 kill-worker")
        print("  4. let the producer run another minute, then stop it (Ctrl+C)")
        print("  5. wait ~30 s for the job to recover and drain the topic")
        print("  6. python outage.py --phase after --expect <sent>")
        print("\n`--expect` is the number the PRODUCER sent during this window,"
              "\nnot the size of the CSV. produce-stream is paced, so a few"
              "\nminutes sends a few thousand - check with:")
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
    # is void - say so rather than reporting a number that means nothing.
    if unique == 0 and written > 0:
        ratio = snap["total"] / max(snap["distinct"], 1)
        print("\n" + "!" * 66)
        print("MEASUREMENT VOID - the job is reprocessing, not progressing")
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
              "\n   topic - transfers that settled days earlier.")
        return

    print("\n1. LOSS - the property that must hold")
    if expect is None:
        print(f"   {unique:,} distinct transactions arrived. Pass --expect <n>"
              f"\n   with the number the producer sent to check for loss;"
              f"\n   ClickHouse alone cannot know what was sent.")
    elif expect < unique:
        print(f"   --expect {expect:,} is below the {unique:,} distinct"
              f"\n   transactions actually stored, so the expectation is wrong"
              f"\n   rather than the pipeline. Use the count the producer"
              f"\n   reported for THIS window.")
    else:
        lost = expect - unique
        if lost == 0:
            print(f"   {unique:,} of {expect:,} transactions present - "
                  f"NOTHING LOST")
            print("   Checkpointing plus committed Kafka offsets replayed the"
                  "\n   window between the last checkpoint and the kill.")
        else:
            print(f"   {lost:,} transactions MISSING of {expect:,} "
                  f"({lost/expect:.2%})")
            print("   Before calling this a correctness failure, confirm the"
                  "\n   producer finished and the topic fully drained - a"
                  "\n   still-draining topic looks identical to loss.")

    print("\n2. DUPLICATION - the cost of AT_LEAST_ONCE")
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
        print("     ALLOW   - a duplicate row in the warehouse. Inflates volume"
              "\n               metrics; no operational effect.")
        print("     REVIEW  - a second identical case in the analyst queue."
              "\n               Wasted work, and erodes trust in the queue.")
        print("     BLOCK   - a second alert on an already-blocked transfer."
              "\n               Safe: blocking twice does not double-block, but"
              "\n               it does inflate the reported fraud count.")
        # Whether the copies agree is not assumed - it is queried. Replay is
        # NOT a pure function of the event (see section 3), so copies of one
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
                      "\n   that did not roll back with the checkpoint - see"
                      "\n   section 3 below. A duplicate is therefore not merely a"
                      "\n   redundant row: it is a second, differently-computed"
                      "\n   opinion about the same transfer.")
            if flips:
                print("\n   DECISION CHANGED on replay for"
                      f" {flips:,} transaction(s). Two rows"
                      "\n   for one transfer disagree about what to do with it,"
                      "\n   and which one an operator sees depends on query order.")

    print("\n3. WHY THIS IS NOT EXACTLY-ONCE, AND WHAT THE COPIES COST")
    print("   Three routes to exactly-once, why AT_LEAST_ONCE is chosen over"
          "\n   them, and the mechanism that makes duplicate copies DISAGREE"
          "\n   - the fan-in store is outside the checkpoint and its READS are"
          "\n   not idempotent: docs/irp-framing.md 6, point 6.")


def _report_dependency(args, before, snap):
    # ROWS, not distinct transaction ids. The producer replays the same CSV
    # from the top every run, so the ids repeat and uniqExact does not move at
    # all - the first version of this reported "1000 LOST" for four services in
    # a row, including two that were never touched, because it was counting a
    # quantity that cannot grow. The row delta is the loss measurement; the
    # distinct delta is only a duplication indicator.
    stored = snap["total"] - before["total"]
    dup = stored - (snap["distinct"] - before["distinct"])
    heading = "after the pass" if args.service == "control" else "after the outage"
    print(f"\n=== {args.service}: {heading} ===")
    print(f"  expected  {args.expect if args.expect is not None else '?'}")
    print(f"  rows      {stored:,}")
    if dup:
        print(f"  of which repeats of ids already stored: {dup:,} "
              f"(the producer replays the same CSV)")
    offered, how = offered_count(args)
    if offered is None:
        print(f"\n  INCONCLUSIVE. {how}")
    else:
        print(f"  offered   {offered:,}  ({how})")
        lost = offered - stored
        if lost <= 0:
            print("  lost      0")
        else:
            print(f"  LOST      {lost:,}  -  {lost / offered:.1%} of what was "
                  f"put on the topic never reached the warehouse")
            if stored == 0:
                print("            nothing arrived at all: the outage did not "
                      "degrade the warehouse, it emptied it")
            print("            corroborate: docker compose logs sink-writer "
                  "| Select-String DISCARDED")

    mix = pass_mix(before, snap)
    state = json.load(open(STATE, encoding="utf-8"))

    if args.service == "control":
        state[REFERENCE] = {"types": mix, "rows": stored}
        json.dump(state, open(STATE, "w", encoding="utf-8"), indent=2)
        print("\n  alert types produced by the healthy pass (the reference):")
        for t, c in sorted(mix.items(), key=lambda kv: -kv[1]):
            if c:
                print(f"    {t:<16}{c:>9}")
        print("\n  Stored as the reference. Every other arm is now measured "
              "against these counts on these same transactions.")
    else:
        ref = state.get(REFERENCE)
        if ref is None:
            print("\n  NO REFERENCE PASS. Run --service control first; without "
                  "it the alert mix can only be compared against the whole "
                  "table, which measures the slice and not the outage.")
        elif stored == 0:
            print("\n  no rows stored, so this arm has no observable alert mix "
                  "- its result is the loss column above.")
        else:
            report_mix(mix, ref["types"])

    if args.service == "control":
        return 0
    print(f"\nPredicted:\n  {EXPECTED[args.service]}")
    print("\nCompare the two. A degradation that was predicted is a design "
          "working; one that was not is a finding.")
    return 0


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--service", choices=sorted(EXPECTED), default="scorer",
                    help="what to break; the default is the scorer itself")
    ap.add_argument("--phase", choices=["before", "after"], required=True)
    ap.add_argument("--expect", type=int, default=None,
                    help="how many transactions the producer was asked to send")
    ap.add_argument("--sent", type=int, default=None,
                    help="how many it reported delivering. Only the kafka arm "
                         "needs it: stopping kafka stops the offer too, and "
                         "unsent is not lost")
    args = ap.parse_args()

    snap = snapshot()
    state = _load_state()

    if args.phase == "before":
        state[args.service] = snap
        _save_state(state)
        if args.service == "scorer":
            print(f"baseline recorded: {snap['total']:,} rows, "
                  f"{snap['distinct']:,} distinct transaction_id\n")
            print("Now, with a paced producer running, kill the worker:")
            print("    docker compose kill taskmanager && docker compose start taskmanager")
            print("\nThen re-run with --phase after --expect <transactions sent>.")
            return 0
        print(f"\n=== {args.service}: baseline ===")
        print(f"  rows {snap['total']:,}, distinct {snap['distinct']:,}")
        print(f"\nExpected degradation:\n  {EXPECTED[args.service]}")
        if args.service == "control":
            print("\nNow:  produce the slice with everything healthy, then "
                  "--phase after.")
        elif args.service == "kafka":
            print("\nNow:  start producing, then stop kafka MID-STREAM and "
                  "bring it back while the producer is still running")
            print("      python outage.py --service kafka "
                  "--phase after --expect 1000 --sent N")
        else:
            print(f"\nNow:  docker compose stop {args.service}")
            print(f"      produce the slice, and let the topic DRAIN while "
                  f"{args.service} is still down - restarting first means the "
                  f"tail of the queue is scored healthy")
            print(f"      docker compose start {args.service}")
            print(f"      python outage.py --service {args.service} "
                  f"--phase after --expect 1000 --sent N")
        return 0

    before = state.get(args.service)
    if before is None:
        raise SystemExit("no baseline - run --phase before first"
                         if args.service == "scorer"
                         else f"no baseline for {args.service}")

    if args.service == "scorer":
        _report_scorer(before, snap, args.expect)
        return 0
    return _report_dependency(args, before, snap)


if __name__ == "__main__":
    sys.exit(main())
