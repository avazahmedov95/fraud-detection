"""Latency against offered load: one row per rate in a throughput sweep.

A single-rate latency figure says nothing about where a pipeline breaks. This
reads the windows recorded by `run.ps1 measure-throughput` and reports, for each
offered rate, what was actually achieved and what the tail did.

  python throughput_report.py [--windows throughput_windows.json]
"""

import argparse
import json
import os

import latency_report as L

WINDOWS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "throughput_windows.json")


def arm(w, target_ms):
    """One offered rate: what the pipeline did with it."""
    rows = L.fetch(epoch_from=w["from"], epoch_to=w["to"])
    if not rows:
        return None
    # r[1], ingest -> DECISION, is the headline. r[0] is end-to-end and includes
    # the sink's batching - SINK_BATCH_SIZE=500 / FLUSH_INTERVAL_S=5, so up to
    # five seconds of a row's life is a warehouse write that has no real-time
    # requirement at all. latency_report.py splits these for exactly this reason
    # and the first version of this report headlined the wrong one, which is why
    # every arm read as 4-10 SECONDS.
    decision = sorted(r[1] for r in rows)
    e2e = sorted(r[0] for r in rows)
    # The split that makes the curve mean something. r[1] is ingest -> decision
    # (queueing plus work) and r[3] is the work inside process_element. If the
    # first grows while the second stays flat, the pipeline is queueing at a
    # rate the worker cannot drain; if both grow, the work itself is the limit.
    work = sorted(r[3] for r in rows)
    ingest = sorted(r[5] for r in rows)          # ingest stamps, ms
    span_s = (ingest[-1] - ingest[0]) / 1000.0
    return {
        "requested": w["rate"],
        "n": len(rows),
        # Measured from the ingest stamps, not from the producer's own report:
        # this is the rate the PIPELINE actually saw, after any client buffering.
        "achieved": (len(rows) - 1) / span_s if span_s > 0 else float("inf"),
        "p50": L.quantile(decision, 0.50),
        "p95": L.quantile(decision, 0.95),
        "p99": L.quantile(decision, 0.99),
        "over": sum(1 for v in decision if v > target_ms),
        "e2e50": L.quantile(e2e, 0.50),
        "work50": L.quantile(work, 0.50),
        "saturated": L.is_saturated(rows),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--windows", default=WINDOWS)
    ap.add_argument("--target-ms", type=float, default=300.0)
    args = ap.parse_args()

    if not os.path.exists(args.windows):
        raise SystemExit(f"{args.windows} not found - run `.\\run.ps1 measure-throughput` first")
    # utf-8-sig, not utf-8: Windows PowerShell 5.1 writes `-Encoding utf8` WITH
    # a byte-order mark, and json.load rejects it. Same family as the CRLF rule
    # in .gitattributes - Windows tooling adds invisible bytes, and the reader
    # is the cheaper place to be tolerant. utf-8-sig also parses a plain file.
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
        # Two different failures, and they must not be conflated. Falling short
        # of the offered rate means the CLIENT could not generate it; a rising
        # tail at a rate that WAS achieved means the pipeline is the constraint.
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


if __name__ == "__main__":
    main()
