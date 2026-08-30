"""
End-to-end latency against the design target.

Reads the wall-clock stamps written by the live pipeline and reports the
distribution of time from "event entered Kafka" to "decision durable in
ClickHouse".

    t0  ingested_at    producer, at send
    t1  scored_at_job  Flink, after fusion
    t2  scored_at      ClickHouse, at write (column default now64)

    end-to-end = t2 - t0
    scoring    = scoring_ms, the work inside process_element

Reported with order statistics — median, p95, p99, max — and a distribution-free
confidence interval for the median. Latency distributions are heavy-tailed and
strongly right-skewed; a mean and standard deviation describe them badly, and
the tail is the part a real-time claim lives or dies on.

    python latency_report.py                    read from ClickHouse
    python latency_report.py --target-ms 300    change the design target

Requires the stack to be running and traffic to have been produced:

    make up && make produce && make submit-job
"""

import argparse
import math
import os
import sys

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
    toUnixTimestamp64Milli(scored_at)                                                AS scored_at_ms
FROM fraud.transactions_scored
WHERE toUnixTimestamp64Milli(ingested_at) > 0
  {since}
FORMAT TabSeparated
"""


def fetch(since_minutes=None):
    import urllib.parse
    import urllib.request
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
        if len(parts) == 5:
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
    """Distribution-free CI for the median (order-statistic / sign-test based).

    Valid for any continuous distribution, which matters here: latency is not
    normal, and a CI derived from a standard error would assume it is.
    """
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


# The longest wait any configured buffer can impose before the scorer: Kafka
# fetch wait (~500 ms) plus Flink's network buffer timeout (~100 ms), with room
# to spare. Anything beyond this is a queue, not a timer.
MAX_BUFFER_MS = 5_000.0


def is_saturated(rows):
    """Was the job falling behind, or merely buffering?

    Both look like waiting, and this check has now been wrong twice:

    1. Comparing waiting against scoring time - a ratio that is large whenever
       the pipeline buffers at all. It called a run at 2% of capacity saturated.
    2. Testing only for an upward trend - which misses a window opened in the
       middle of a long backlog, where latency is already high and merely
       creeping. It called a 180-second queue healthy.

    So both tests, and an absolute bound first: buffer intervals are bounded by
    configuration, and no legitimate timer holds a message for minutes. A queue
    is the only thing that can.
    """
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
    """Offset between this machine's clock and the container's.

    `ingested_at` is stamped by the producer on the host; `scored_at_job` and
    `scored_at` come from inside Docker. On Windows and macOS those containers
    run in a virtual machine with its own clock, which drifts from the host -
    noticeably after the machine sleeps. Any offset lands directly in the
    ingest->decision figure, and would be invisible without checking.

    Returns (skew_ms, round_trip_ms). A skew well inside the round trip is
    indistinguishable from measurement noise.
    """
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-ms", type=float, default=300.0)
    ap.add_argument("--since-minutes", type=int, default=None,
                    help="only rows written in the last N minutes, to measure a "
                         "single run rather than everything ever produced")
    args = ap.parse_args()

    rows = fetch(args.since_minutes)
    if not rows:
        raise SystemExit(
            "no instrumented rows found.\n"
            "Rows written before the latency stamps were added have "
            "ingested_at = 0 and are excluded. Re-run the producer.")

    scope = (f" written in the last {args.since_minutes} min"
             if args.since_minutes else "")
    print(f"\n{len(rows):,} scored transactions with latency stamps{scope}\n")

    # The design target is about blocking a transfer before settlement, so it
    # applies to when the DECISION exists - not to when the row is durable in
    # the analytical warehouse. Those are different paths: the decision goes to
    # the fraud.alerts topic for the switch to act on, while ClickHouse is where
    # it is later queried. Holding the warehouse write to the same target would
    # be measuring the reporting stack against a real-time requirement.
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
        print("    python latency_report.py --since-minutes 5")
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


if __name__ == "__main__":
    main()
