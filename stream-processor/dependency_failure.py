"""What the pipeline still does when one of its dependencies is gone.

fault_injection.py kills the SCORER. This kills the things the scorer leans on -
Redis, Neo4j, ClickHouse, Kafka - and asks the question that matters for each:
not "did it crash" but "what did it silently stop doing".

Every one of these fails open by design, which means every one of them degrades
without an error. The point of the measurement is to say what each degradation
costs, in rules that stop firing and rows that stop arriving.

  python dependency_failure.py --service redis --phase before
  #   ... kill the container, produce traffic, restart it ...
  python dependency_failure.py --service redis --phase after --expect 1000

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

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "dependency_failure_state.json")

#: What each dependency is expected to take away. Written down BEFORE the run so
#: the result is a test of a prediction rather than a description of whatever
#: happened - and so a degradation nobody predicted stands out.
EXPECTED = {
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
    # strip("\n"), not strip(): a bare .strip() eats the LEADING TAB of the
    # first row when its first column is empty - and predicted_type is empty for
    # every alert no rule explains. The row then parses as one field and the
    # caller gets an IndexError on a query that returned good data.
    return [l.split("\t") for l in body.strip("\n").splitlines() if l]


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


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--service", choices=sorted(EXPECTED), required=True)
    ap.add_argument("--phase", choices=["before", "after"], required=True)
    ap.add_argument("--expect", type=int, default=None,
                    help="how many transactions the producer was asked to send")
    ap.add_argument("--sent", type=int, default=None,
                    help="how many it reported delivering. Only the kafka arm "
                         "needs it: stopping kafka stops the offer too, and "
                         "unsent is not lost")
    args = ap.parse_args()

    snap = snapshot()

    if args.phase == "before":
        state = {}
        if os.path.exists(STATE):
            state = json.load(open(STATE, encoding="utf-8"))
        state[args.service] = snap
        json.dump(state, open(STATE, "w", encoding="utf-8"), indent=2)
        print(f"\n=== {args.service}: baseline ===")
        print(f"  rows {snap['total']:,}, distinct {snap['distinct']:,}")
        print(f"\nExpected degradation:\n  {EXPECTED[args.service]}")
        if args.service == "control":
            print("\nNow:  produce the slice with everything healthy, then "
                  "--phase after.")
        elif args.service == "kafka":
            print("\nNow:  start producing, then stop kafka MID-STREAM and "
                  "bring it back while the producer is still running")
            print("      python dependency_failure.py --service kafka "
                  "--phase after --expect 1000 --sent N")
        else:
            print(f"\nNow:  docker compose stop {args.service}")
            print(f"      produce the slice, and let the topic DRAIN while "
                  f"{args.service} is still down - restarting first means the "
                  f"tail of the queue is scored healthy")
            print(f"      docker compose start {args.service}")
            print(f"      python dependency_failure.py --service {args.service} "
                  f"--phase after --expect 1000 --sent N")
        return 0

    if not os.path.exists(STATE):
        raise SystemExit("no baseline - run --phase before first")
    before = json.load(open(STATE, encoding="utf-8")).get(args.service)
    if before is None:
        raise SystemExit(f"no baseline for {args.service}")

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


if __name__ == "__main__":
    sys.exit(main())
