"""End-to-end latency against the 300 ms target, from the three wall-clock
stamps a scored record carries: nearest-rank order statistics and a
distribution-free CI for the median. Figures: docs/irp-framing.md 7.

    python latency.py [--since-minutes N]   one run
    python latency.py throughput            one row per rate in a sweep
"""

import argparse
import json
import math
import os

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")   # matches .env

QUERY = """
SELECT
    (toUnixTimestamp64Milli(scored_at)     - toUnixTimestamp64Milli(ingested_at))    AS end_to_end_ms,
    (toUnixTimestamp64Milli(scored_at_job) - toUnixTimestamp64Milli(ingested_at))    AS to_scored_ms,
    (toUnixTimestamp64Milli(scored_at)     - toUnixTimestamp64Milli(scored_at_job))  AS sink_ms,
    scoring_ms,
    toUnixTimestamp64Milli(scored_at)                                                AS scored_at_ms,
    toUnixTimestamp64Milli(ingested_at)                                              AS ingested_at_ms
FROM fraud.transactions_scored
WHERE toUnixTimestamp64Milli(ingested_at) > 0
  {since}
FORMAT TabSeparated
"""


def fetch(since_minutes=None, epoch_from=None, epoch_to=None):
    """Rows for a window. `since_minutes` filters on WRITE time; the epoch pair
    filters on INGEST time, which is what a back-to-back sweep needs - arms are
    seconds apart and the write time of one overlaps the ingest of the next."""
    import urllib.parse
    import urllib.request
    if epoch_from is not None:
        clause = (f"AND toUnixTimestamp64Milli(ingested_at) >= {int(epoch_from * 1000)} "
                  f"AND toUnixTimestamp64Milli(ingested_at) <  {int(epoch_to * 1000)}")
    else:
        clause = ("AND scored_at > now() - INTERVAL %d MINUTE" % since_minutes
                  if since_minutes else "")
    url = (f"http://{CH_HOST}:{CH_PORT}/?"
           + urllib.parse.urlencode({"query": QUERY.format(since=clause),
                                     "user": CH_USER, "password": CH_PASSWORD}))
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            body = resp.read().decode()
    except Exception as exc:                           # noqa: BLE001
        raise SystemExit(
            f"could not query ClickHouse at {CH_HOST}:{CH_PORT} — {exc}\n"
            "Is the stack up? (make up && make produce && make submit-job)")

    rows = []
    for line in body.strip().splitlines():
        parts = line.split("\t")
        if len(parts) == 6:
            try:
                rows.append(tuple(float(p) for p in parts))
            except ValueError:
                continue
    return rows


def quantile(sorted_values, q):
    """Nearest-rank order statistic — no interpolation, no distributional
    assumption. The reported value is one actually observed."""
    if not sorted_values:
        return float("nan")
    idx = max(0, min(len(sorted_values) - 1,
                     int(math.ceil(q * len(sorted_values))) - 1))
    return sorted_values[idx]


def median_ci(sorted_values, conf=0.95):
    """Distribution-free CI for the median (order statistics), since latency is not
    normal."""
    n = len(sorted_values)
    if n < 2:
        return float("nan"), float("nan")
    z = 1.96 if conf == 0.95 else 2.576
    half = z * math.sqrt(n) / 2.0
    lo = max(0, int(math.floor(n / 2.0 - half)))
    hi = min(n - 1, int(math.ceil(n / 2.0 + half)))
    return sorted_values[lo], sorted_values[hi]


def describe(name, values, target_ms=None):
    vals = sorted(v for v in values if v == v)         # drop NaN
    if not vals:
        print(f"{name:<22} no data")
        return
    med = quantile(vals, 0.50)
    lo, hi = median_ci(vals)
    print(f"{name:<22}{len(vals):>8}"
          f"{med:>10.1f}{f'[{lo:.0f},{hi:.0f}]':>13}"
          f"{quantile(vals, 0.95):>10.1f}{quantile(vals, 0.99):>10.1f}"
          f"{vals[-1]:>10.1f}")
    if target_ms is not None:
        breaches = sum(1 for v in vals if v > target_ms)
        share = breaches / len(vals)
        verdict = "MET" if share <= 0.01 else "NOT MET"
        print(f"{'':<22}target {target_ms:.0f} ms: {breaches} of {len(vals)} "
              f"over ({share:.2%}) -> {verdict} at the 99th percentile")


# The longest wait a configured buffer can impose (fetch wait + buffer timeout,
# with room to spare); anything beyond it is a queue.
MAX_BUFFER_MS = 5_000.0


def is_saturated(rows):
    """Was the job falling behind, or merely buffering? An absolute bound first - no
    configured timer holds a message for minutes - then the ratio and trend tests."""
    if len(rows) < 20:
        return False
    waiting = sorted(r[1] for r in rows)
    if quantile(waiting, 0.50) > MAX_BUFFER_MS:
        return True
    in_order = [r[0] for r in sorted(rows, key=lambda r: r[4])]
    tenth = max(1, len(in_order) // 10)
    first = sum(in_order[:tenth]) / tenth
    last = sum(in_order[-tenth:]) / tenth
    return first > 0 and last / first > 3.0


def clock_skew_ms():
    """Offset between this machine's clock and the container's, which drifts on
    Windows and macOS and lands directly in ingest -> decision. Returns (skew_ms,
    round_trip_ms); a skew well inside the round trip is noise."""
    import time as _t
    import urllib.parse
    import urllib.request
    q = "SELECT toUnixTimestamp64Milli(now64(3)) FORMAT TabSeparated"
    url = (f"http://{CH_HOST}:{CH_PORT}/?"
           + urllib.parse.urlencode({"query": q, "user": CH_USER,
                                     "password": CH_PASSWORD}))
    try:
        t0 = _t.time()
        with urllib.request.urlopen(url, timeout=10) as resp:
            container_ms = float(resp.read().decode().strip())
        t1 = _t.time()
    except Exception:                                  # noqa: BLE001
        return None, None
    host_mid_ms = (t0 + t1) / 2 * 1000.0
    return container_ms - host_mid_ms, (t1 - t0) * 1000.0


def throughput(rows):
    """Events per second, from the span of sink write times."""
    stamps = sorted(r[4] for r in rows)
    span_s = (stamps[-1] - stamps[0]) / 1000.0
    if span_s <= 0:
        return None
    return len(rows) / span_s


def cmd_single_run(args):
    rows = fetch(args.since_minutes)
    if not rows:
        raise SystemExit(
            "no instrumented rows found.\n"
            "Rows written before the latency stamps were added have "
            "ingested_at = 0 and are excluded. Re-run the producer.")

    scope = (f" written in the last {args.since_minutes} min"
             if args.since_minutes else "")
    print(f"\n{len(rows):,} scored transactions with latency stamps{scope}\n")

    # The target applies to when the DECISION exists (published to fraud.alerts), not
    # to the warehouse write. Publishing is not enforcement: nothing here declines a
    # transfer.
    print("DECISION PATH - what the <%.0f ms target is about" % args.target_ms)
    print(f"{'stage':<22}{'n':>8}{'median':>10}{'95% CI':>13}"
          f"{'p95':>10}{'p99':>10}{'max':>10}   (ms)")
    describe("ingest -> decision", [r[1] for r in rows], args.target_ms)
    describe("  scoring work only", [r[3] for r in rows])

    print("\nWAREHOUSE PATH - analytical durability, no real-time requirement")
    describe("decision -> ClickHouse", [r[2] for r in rows])
    describe("end-to-end (t0->t2)", [r[0] for r in rows])

    tput = throughput(rows)
    if tput:
        print(f"\nthroughput: {tput:,.0f} events/s sustained over this run")

    if is_saturated(rows):
        print("\n" + "=" * 74)
        print("SATURATED RUN - this is a throughput measurement, not a latency one.")
        print("=" * 74)
        print("Latency grew steadily through the run: the producer filled the "
              "topic faster\nthan the job drains it, so most of the figure is "
              "queue depth. It would grow\nwith the size of the input file and "
              "shrink on a faster machine, and says\nnothing about how quickly "
              "one transaction can be scored.")
        print("\nQuote throughput and 'scoring work only' from this run. A "
              "latency measurement\nneeds a topic with no backlog in it - and "
              "`produce` dumps the whole file at\nonce, which creates one that "
              "takes minutes to drain. Narrowing the time window\ndoes not help: "
              "it just samples the middle of the queue.")
        print("\nStart clean and never run `produce`:")
        print("\n    .\\run.ps1 latency-setup         (clean, up, graph, job - no batch dump)")
        print("    .\\run.ps1 produce-stream        (Ctrl+C after a few minutes)")
        print("    python latency.py --since-minutes 5")
    else:
        decision = sorted(r[1] for r in rows)
        scoring = sorted(r[3] for r in rows)
        buffering = quantile(decision, 0.50) - quantile(scoring, 0.50)
        if buffering > 50:
            print("\n" + "-" * 74)
            print("Latency is dominated by BUFFERING, not by work or backlog.")
            print("-" * 74)
            print(f"Scoring takes {quantile(scoring, 0.50):.1f} ms; reaching the "
                  f"scorer takes {quantile(decision, 0.50):.0f} ms.\nThe run was "
                  "not saturated, so the difference is buffer intervals waiting "
                  "to fill:\nKafka fetch waits, Flink's network buffer timeout, "
                  "and the sink's flush timer\n(SINK_FLUSH_INTERVAL_S, default "
                  "5s) for the warehouse path.")
            print("\nThese are throughput/latency trade-offs set by "
                  "configuration, not limits of\nthe design. Meeting the target "
                  "is a tuning exercise; the compute headroom is\nalready there "
                  f"({quantile(scoring, 0.99):.0f} ms at p99 against a "
                  f"{args.target_ms:.0f} ms budget).")

    print("\nQuantiles are nearest-rank order statistics and the median CI is "
          "distribution-free:\nlatency is heavy-tailed and right-skewed, so a "
          "mean +/- sd would misdescribe it.")
    skew, rtt = clock_skew_ms()
    if skew is not None:
        print(f"\nhost-to-container clock offset right now: {skew:+.0f} ms "
              f"(HTTP round trip {rtt:.0f} ms)")
        if abs(skew) > max(50.0, rtt):
            print("  ^ a real offset, not noise - and it drifts: readings "
                  "minutes apart have\n    differed by ~500 ms as the VM clock "
                  "resynced.")
            print("\n  This invalidates the decision path ONLY if the producer "
                  "ran on the host,\n  since `ingested_at` would then come from "
                  "a different clock than the rest.\n  If it was started with "
                  "produce-stream-docker, every stamp comes from inside\n  the "
                  "container and the figures above are unaffected.")
    print("\nSynthetic traffic on prototype hardware. This measures the "
          "prototype, not the\neventual system.")


# --------------------------------------------------------------------------
# Latency against offered load - one row per rate in a throughput sweep
# --------------------------------------------------------------------------

# Anchored to the package, not to this directory: .gitignore names it at
# stream-processor/throughput_windows.json, and run.ps1 writes it from there.
WINDOWS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "throughput_windows.json")


def arm(w, target_ms):
    """One offered rate: what the pipeline did with it."""
    rows = fetch(epoch_from=w["from"], epoch_to=w["to"])
    if not rows:
        return None
    # r[1], ingest -> DECISION, is the headline; r[0] includes up to 5 s of sink batching.
    decision = sorted(r[1] for r in rows)
    e2e = sorted(r[0] for r in rows)
    # r[1] is queueing plus work, r[3] the work alone: if only r[1] grows, the pipeline
    # queues faster than it drains; if both grow, the work is the limit.
    work = sorted(r[3] for r in rows)
    ingest = sorted(r[5] for r in rows)          # ingest stamps, ms
    span_s = (ingest[-1] - ingest[0]) / 1000.0
    return {
        "requested": w["rate"],
        "n": len(rows),
        # Measured from the ingest stamps, not from the producer's own report:
        # this is the rate the PIPELINE actually saw, after any client buffering.
        "achieved": (len(rows) - 1) / span_s if span_s > 0 else float("inf"),
        "p50": quantile(decision, 0.50),
        "p95": quantile(decision, 0.95),
        "p99": quantile(decision, 0.99),
        "over": sum(1 for v in decision if v > target_ms),
        "e2e50": quantile(e2e, 0.50),
        "work50": quantile(work, 0.50),
        "saturated": is_saturated(rows),
    }


def cmd_throughput(args):
    if not os.path.exists(args.windows):
        raise SystemExit(f"{args.windows} not found - run `.\\run.ps1 measure-throughput` first")
    # utf-8-sig: PowerShell 5.1 writes a byte-order mark, which json.load rejects.
    with open(args.windows, encoding="utf-8-sig") as fh:
        windows = json.load(fh)
    # ConvertTo-Json collapses a one-element array into an object, so a sweep of
    # a single rate would arrive as a dict rather than a list.
    if isinstance(windows, dict):
        windows = [windows]

    print(f"\n{len(windows)} arms, target {args.target_ms:.0f} ms\n")
    print(f"{'offered':>9}{'achieved':>10}{'n':>8}{'p50':>8}{'p95':>8}{'p99':>8}"
          f"{'over':>7}{'work':>8}{'e2e':>9}   state")
    print(f"{'ev/s':>9}{'ev/s':>10}{'':>8}"
          f"{'-- ingest to DECISION --':^24}{'':>7}{'p50':>8}{'p50':>9}")
    print(f"{'':>27}{'ms':>8}{'ms':>8}{'ms':>8}{'':>7}{'ms':>8}{'ms':>9}")

    knee = None
    for w in windows:
        a = arm(w, args.target_ms)
        if a is None:
            print(f"{w['rate']:>9,.0f}{'no rows':>10}")
            continue
        # Falling short of the offered rate is the CLIENT; a rising tail at an achieved
        # rate is the pipeline.
        short = a["achieved"] < a["requested"] * 0.95
        breach = a["p99"] > args.target_ms
        state = ("client-limited" if short else
                 "SATURATED" if a["saturated"] else
                 "p99 over target" if breach else "ok")
        if knee is None and not short and (a["saturated"] or breach):
            knee = a["requested"]
        print(f"{a['requested']:>9,.0f}{a['achieved']:>10,.0f}{a['n']:>8,}"
              f"{a['p50']:>8.0f}{a['p95']:>8.0f}{a['p99']:>8.0f}"
              f"{a['over']:>7}{a['work50']:>8.1f}{a['e2e50']:>9.0f}   {state}")

    print()
    if knee:
        print(f"The pipeline stops meeting the {args.target_ms:.0f} ms target "
              f"somewhere at or below {knee:,.0f} events/s.")
    else:
        print(f"No arm breached the target. The sweep did not find the limit - "
              f"either it did not reach far enough, or the client was the "
              f"constraint before the pipeline was.")
    print("\np50/p95/p99 are ingest -> DECISION, which is what the target is "
          "about. 'e2e' additionally contains the warehouse write, where the "
          "sink batches 500 rows or 5 s - that path has no real-time "
          "requirement and must not be read against the target.")
    print("'work' is the scoring inside process_element. Work flat while the "
          "decision time grows is QUEUEING: the worker cannot drain the offered "
          "rate. The enrichment lookup is synchronous - enrichment.py says so, "
          "and production would use Flink async I/O.")
    print("\nRead 'client-limited' as a fact about the harness, not the system: "
          "the producer could not offer that rate, so the row measures the "
          "producer. Only rows that achieved their offered rate say anything "
          "about the pipeline.")
    print("This deployment is tuned AGAINST throughput on purpose - "
          "python.fn-execution.bundle.time=50ms, bundle.size=100, "
          "buffer.timeout=5ms - because the requirement is a decision before "
          "settlement. The curve is where that trade stops paying.")



def main():
    ap = argparse.ArgumentParser(
        description="Latency against the 300 ms target: one run, or a sweep.")
    ap.add_argument("--target-ms", type=float, default=300.0)
    ap.add_argument("--since-minutes", type=int, default=None,
                    help="only rows written in the last N minutes, to measure a "
                         "single run rather than everything ever produced")
    sub = ap.add_subparsers(dest="cmd")
    p = sub.add_parser("throughput", help="one row per rate in a sweep")
    p.add_argument("--windows", default=WINDOWS)
    p.set_defaults(fn=cmd_throughput)
    args = ap.parse_args()
    (getattr(args, "fn", None) or cmd_single_run)(args)


if __name__ == "__main__":
    main()
