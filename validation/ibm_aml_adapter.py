"""Replays the deployed rules over IBM's AML transaction sets (Altman et al.,
NeurIPS 2023 Datasets and Benchmarks). Why this dataset and what it settles:
README.md 4.

It exists to retest one thing. The AMLSim run found MULE_FAN_IN anti-correlated
with the label and traced it to a clock: fan_in patterns there span a median of 363
days against a one-hour window, so a null result was ambiguous between "the rule
does not transfer" and "the window is shorter than the pattern". This dataset runs
at MINUTE resolution over 17 days, which is not our hour but is roughly two orders
of magnitude closer, and section D of the report measures the remaining gap from
the data instead of assuming it.

Everything downstream of a translated event is in harness.py, shared with the other
two adapters.
"""

import argparse
import os

import pandas as pd

import harness as RP
from harness import C, Event                                   # noqa: F401


#: The file's own headers contain spaces, and itertuples renames any such column to
#: a POSITIONAL name (_3, _8 ...) - reading fields by those is a landmine this
#: project has already been bitten by once. Renamed once, on load, to identifiers.
#: pandas de-duplicates the file's two `Account` columns into Account / Account.1.
COLUMNS = {"Account": "sender", "Account.1": "receiver", "Amount Paid": "amount",
           "Payment Currency": "currency", "Payment Format": "fmt",
           "Is Laundering": "label"}

#: Rows whose sender and receiver are the same account. 12% of the file, almost all
#: of them `Reinvestment`. They are not transfers between two parties: counted as
#: such they inflate every per-sender history and give each account a fan-in edge to
#: itself. Dropped, and the count is printed - a silent filter on 600,000 rows would
#: be the kind of thing that makes a result unreproducible.
DROP_SELF_TRANSFERS = True


def load(path, formats=None, limit=None):
    d = pd.read_csv(path, dtype={"Account": str, "Account.1": str,
                                 "From Bank": str, "To Bank": str})
    d.columns = [c.strip() for c in d.columns]
    missing = [c for c in COLUMNS if c not in d.columns]
    if missing:
        raise SystemExit(f"{path} is missing {missing}; expected the released "
                         f"IBM AML schema.")
    d = d.rename(columns=COLUMNS)
    n_all = len(d)

    d["ts"] = pd.to_datetime(d["Timestamp"], format="%Y/%m/%d %H:%M")
    if formats:
        d = d[d.fmt.isin(formats)]
    self_tx = d.sender == d.receiver
    n_self = int(self_tx.sum())
    if DROP_SELF_TRANSFERS:
        d = d[~self_tx]
    d = d.sort_values("ts").reset_index(drop=True)
    if limit:
        d = d.head(limit)
    return d, n_all, n_self


def _scales(d):
    """One scale factor PER CURRENCY, each from that currency's own median.

    The file carries 15 currencies and this project's absolute thresholds are
    written in UZS, so a single global factor would put a yen amount and a dollar
    amount on different sides of the structuring threshold for no reason but their
    denomination. Per-currency medians make "just under the limit" mean the same
    thing in each. Still a unit conversion and still not tuning: every factor is
    fixed by a median, none is chosen to make a rule fire.
    """
    return {cur: RP.scale_factor(g.amount) for cur, g in d.groupby("currency")}


def to_events(d, scales, typologies=None):
    typologies = typologies or {}
    for r in d.itertuples(index=False):
        yield Event(
            ev={"amount_uzs": float(r.amount) * scales.get(r.currency, 1.0),
                "sender_pinfl": r.sender,
                "receiver_pinfl": r.receiver},
            ts=int(r.ts.timestamp()),
            label=int(r.label),
            typology=typologies.get((r.sender, r.receiver), ""))


def read_patterns(path):
    """Map (sender, receiver) -> typology from a `*_Patterns.txt` sidecar.

    The released transaction CSVs carry only `Is Laundering`; WHICH typology each
    row belongs to lives in this separate file, and the Hugging Face mirror of the
    transactions does not include it. Without it section B is empty and the run
    still answers the aggregate question - so it is optional, and its absence is
    reported rather than worked around.

    Format: blocks introduced by a `BEGIN LAUNDERING ATTEMPT - <TYPE>` line,
    followed by transaction rows in the same column order as the CSV.
    """
    typ, current = {}, None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("BEGIN LAUNDERING ATTEMPT"):
                # Split on the FIRST " - " only: the typology names are themselves
                # hyphenated (FAN-IN, GATHER-SCATTER), and splitting on every "-"
                # turns FAN-IN into "in" - which still parses, still populates the
                # table, and quietly merges FAN-IN with every other *-IN typology.
                current = line.split(" - ", 1)[-1].strip().lower()
            elif line.startswith("END LAUNDERING ATTEMPT"):
                current = None
            elif current and "," in line:
                parts = line.split(",")
                if len(parts) >= 5:
                    typ[(parts[2].strip(), parts[4].strip())] = current
    return typ


def window_stats(d):
    """How far the collection stage outruns RECEIVER_WINDOW_S, measured here.

    The AMLSim report had to state this as a caveat because the mismatch there was
    ~10^4 and swamped everything. Measuring it makes the caveat quantitative, and
    it is the number that says how much of each pattern the rule can even see.
    """
    span = d[d.label == 1].groupby("receiver").ts.agg(["min", "max", "size"])
    span = span[span["size"] > 1]
    if span.empty:
        return None
    hours = (span["max"] - span["min"]).dt.total_seconds() / 3600
    return dict(receivers=len(span), median_h=float(hours.median()),
                p25_h=float(hours.quantile(.25)),
                p75_h=float(hours.quantile(.75)),
                within_window=float((hours <= C.RECEIVER_WINDOW_S / 3600).mean()))


BY_TYPOLOGY_NOTES = (
    "The typologies IBM injects include fan-in and gather-scatter - a collection",
    "stage, which PaySim has none of and which is the one design claim in this",
    "project with no external validation. Recall on those rows is the answer this",
    "dataset was fetched for.",
)


def report(res, hits, stats):
    RP.section_lift(res, hits, positive="laundering", width=72)
    print()
    RP.section_by_group(res, "B. BY TYPOLOGY - the collection stage",
                        BY_TYPOLOGY_NOTES, width=72)
    print()
    RP.section_decision(res, hits, positive="laundering", width=72)

    print("\n" + "=" * 72)
    print("D. THE WINDOW, MEASURED RATHER THAN ASSUMED")
    print("=" * 72)
    print(f"  RECEIVER_WINDOW_S is {C.RECEIVER_WINDOW_S:,} s "
          f"({C.RECEIVER_WINDOW_S/3600:.0f} h).")
    if not stats:
        print("  Too few multi-transaction laundering receivers to measure a span.")
        return
    print(f"  Laundering receivers with more than one inbound laundering "
          f"transaction: {stats['receivers']:,}")
    print(f"  Their collection spans, in hours: p25 {stats['p25_h']:.1f}  "
          f"median {stats['median_h']:.1f}  p75 {stats['p75_h']:.1f}")
    print(f"  Share collecting entirely inside the deployed window: "
          f"{stats['within_window']:.1%}")
    ratio = stats["median_h"] / (C.RECEIVER_WINDOW_S / 3600)
    print(f"\n  The window is {ratio:.0f}x shorter than the median pattern, so the")
    print("  rule still sees a fraction of each one. That is the same direction as")
    print("  the AMLSim run and roughly two orders of magnitude smaller: there the")
    print("  median fan_in spanned 363 DAYS against the same hour. A weak recall")
    print("  here is therefore still partly a window effect - but the lift table")
    print("  above is threshold-free and window-limited in the same direction, so")
    print("  a POSITIVE lift here cannot be explained away by the window, while on")
    print("  AMLSim the lift inverted and could not be rescued by widening it.")
    print("\n  The window is not widened to fit. Doing so would be tuning on the")
    print("  validation set; a longer-window run is a separate experiment.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True,
                    help="HI-Small_Trans.csv or another of the six released sets")
    ap.add_argument("--patterns", default=None,
                    help="optional *_Patterns.txt sidecar; without it the "
                         "per-typology section is empty")
    ap.add_argument("--formats", default=None,
                    help="comma-separated Payment Format values to keep. Left "
                         "unset ON PURPOSE: laundering here concentrates in ACH, "
                         "so defaulting to ACH would be selecting rows by the "
                         "label. The per-format rates are printed instead.")
    ap.add_argument("--limit", type=int, default=None,
                    help="first N rows after sorting (the full file is 5.1M)")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    # Same profile PaySim forced: account identifiers, amounts and a clock, and
    # nothing else this project uses. receiver_age is off here too - unlike AMLSim,
    # the released files carry no account-opening date.
    RP.capability_profile("receiver_age", "myid_kinship", "device_telemetry",
                          "geo_telemetry", "session_telemetry")

    formats = ([f.strip() for f in args.formats.split(",")]
               if args.formats else None)
    d, n_all, n_self = load(args.file, formats, args.limit)

    print(f"{len(d):,} transactions of {n_all:,} in the file "
          f"({n_self:,} self-transfers dropped)")
    print(f"span {d.ts.min()} -> {d.ts.max()}  "
          f"({(d.ts.max()-d.ts.min()).days} days, minute resolution)")
    n_l = int(d.label.sum())
    print(f"laundering: {n_l:,} ({n_l/max(len(d),1):.4%})")
    print("\nlaundering rate by payment format "
          "(printed, not filtered on - see --formats):")
    by_fmt = d.groupby("fmt").label.agg(["size", "sum", "mean"])
    for fmt, row in by_fmt.sort_values("mean", ascending=False).iterrows():
        print(f"  {fmt:<14}{int(row['size']):>10,}{int(row['sum']):>8,}"
              f"{row['mean']:>10.4%}")

    scales = _scales(d)
    print(f"\namount scale factors, one per currency ({len(scales)} present):")
    for cur, s in sorted(scales.items(), key=lambda kv: -kv[1])[:4]:
        print(f"  {cur:<16}{s:>12,.1f}x")
    if len(scales) > 4:
        print(f"  ... and {len(scales)-4} more")

    typologies = {}
    if args.patterns:
        typologies = read_patterns(args.patterns)
        print(f"\n{len(typologies):,} labelled pattern edges read from "
              f"{os.path.basename(args.patterns)}")
    else:
        print("\nNo --patterns file: section B will be empty. The released "
              "transaction CSV\ncarries only Is Laundering; the typology of each "
              "row lives in the sidecar.")
    print()

    res, hits = RP.replay(to_events(d, scales, typologies), total=len(d))
    report(res, hits, window_stats(d))

    print("\nWhat this does and does not show:")
    print("  - Synthetic, like the other two: a multi-agent simulation of banks,")
    print("    businesses and individuals, not production data.")
    print("  - Independent: built by IBM Research for a graph-learning benchmark,")
    print("    with no knowledge of this system. The rules run on it unchanged.")
    print("  - INTERBANK laundering, not consumer card P2P. A result here is about")
    print("    the shape of the rules, never about their thresholds.")
    print("  - It is the first foreign dataset whose clock is finer than this")
    print("    project's own windows, which is the whole reason it was fetched.")


if __name__ == "__main__":
    main()
