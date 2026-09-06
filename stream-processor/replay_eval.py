"""Replays a generated CSV through the deployed rule engine, offline.

Three questions, one replay loop. They were three files, and the loop was
written out once per file; the copies had already drifted, two of them extracting
a dataset name with different and one of them broken path handling.

The event dict that feeds `rules.evaluate` is no longer here at all. It had a
fourth copy in ml/dataset.py, on the path to the deployed model, so it now lives
in `features.event_from` beside `truthy` - one mapping, for the same reason there
is one coercion.

    python replay_eval.py                                  what the CEP layer alone does
    python replay_eval.py fan-in-mode --files 'out_seed*/transactions.csv'
    python replay_eval.py payee-seeding --files 'out_seed*/transactions.csv'

Results: fan-in-mode is docs/irp-framing.md 6, third RQ3 result; payee-seeding
bounds how much is_new_payee owes to the generator rather than to behaviour.
"""

import argparse
import glob
import math
import os
import statistics as st
from collections import defaultdict, Counter

import pandas as pd

import config as C
import features as F
from features import event_from, age_or_none
from rules import SenderState, ReceiverState, PopulationBaseline, evaluate


def replay(path, population=None, count_hits=False):
    """Score every row through the deployed `rules.evaluate`.

    `population` is passed straight through: None means MULE_FAN_IN falls back to
    its absolute threshold, which is what the payee-seeding arm wants and what
    the other two must NOT get by accident.

    Returns (frame with `decision` and `flagged`, rule-hit counter or None).
    """
    df = pd.read_csv(path).sort_values("event_time").reset_index(drop=True)
    has_labels = "label_is_fraud" in df.columns
    states, rstates = defaultdict(SenderState), defaultdict(ReceiverState)
    decisions, hit_counter = [], defaultdict(Counter) if count_hits else None

    for row in df.itertuples(index=False):
        r = row._asdict()
        ev = event_from(r)
        res = evaluate(ev,
                       receiver_age_days=age_or_none(r.get("receiver_account_age_days")),
                       state=states[r["sender_card"]],
                       now=pd.Timestamp(r["event_time"]).timestamp(),
                       receiver_state=rstates[F.payee_key(ev)],
                       population=population)
        decisions.append(res["decision"])
        if count_hits:
            bucket = ("fraud" if has_labels and int(r["label_is_fraud"]) == 1
                      else "legit")
            for h in res["rule_hits"]:
                hit_counter[bucket][h] += 1

    df["decision"] = decisions
    df["flagged"] = df["decision"].isin(["REVIEW", "BLOCK"])
    return df, hit_counter


def ci(vals):
    """95% t interval for the mean. One copy: two files carried the same table."""
    n = len(vals)
    if n < 2:
        return float("nan"), float("nan")
    m, sem = st.mean(vals), st.stdev(vals) / math.sqrt(n)
    # t for 95%, small-n; falls back to 1.96 beyond the table
    t = {2: 12.71, 3: 4.30, 4: 3.18, 5: 2.78, 6: 2.57, 7: 2.45,
         8: 2.36, 9: 2.31, 10: 2.26}.get(n, 1.96)
    return m - t * sem, m + t * sem


def _expand(patterns):
    return [f for pat in patterns for f in sorted(glob.glob(pat))] or patterns


def _name(path):
    """The seed directory, however the shell spelled the separator. The two
    files this replaces did this differently and one of them did it wrongly."""
    return os.path.basename(os.path.dirname(os.path.abspath(path)))


# --------------------------------------------------------------------------
# 1. what the CEP layer alone would have done
# --------------------------------------------------------------------------

def cmd_summary(args):
    population = PopulationBaseline()
    df, hits = replay(args.file, population=population, count_hits=True)
    has_labels = "label_is_fraud" in df.columns

    thr = population.threshold(C.MULE_FAN_IN_QUANTILE, C.MULE_FAN_IN_MIN_SENDERS)
    print(f"MULE_FAN_IN mode: {C.MULE_FAN_IN_MODE}"
          + (f"  (q={C.MULE_FAN_IN_QUANTILE}, threshold settled at "
             f"{thr} senders/h over {population.n:,} observations)"
             if C.MULE_FAN_IN_MODE == "relative"
             else f"  (fixed at {C.MULE_FAN_IN_MIN_SENDERS} senders/h)"))

    print(f"events scored : {len(df):,}")
    print("\noverall decisions:")
    print(df["decision"].value_counts().reindex(
        ["ALLOW", "REVIEW", "BLOCK"]).fillna(0).astype(int).to_string())

    if not has_labels:
        print("\n(no labels in file - run the producer/CSV with labels for a "
              "fraud/legit breakdown)")
        return

    fraud = df["label_is_fraud"] == 1
    legit = ~fraud
    fr_flagged = (df["flagged"] & fraud).sum()
    lg_flagged = (df["flagged"] & legit).sum()
    print("\nfraud vs legit (flagged = REVIEW or BLOCK):")
    print(f"  fraud flagged : {fr_flagged:>6} / {fraud.sum():<6}  "
          f"({fr_flagged / max(fraud.sum(), 1):.1%})")
    print(f"  legit flagged : {lg_flagged:>6} / {legit.sum():<6}  "
          f"({lg_flagged / max(legit.sum(), 1):.2%})   <- false positives")

    print("\nflagged rate by fraud type:")
    for ftype, grp in df[fraud].groupby("label_fraud_type"):
        print(f"  {ftype:<12} {grp['flagged'].mean():.1%}  (n={len(grp)})")

    for bucket in ("fraud", "legit"):
        print(f"\ntop rule hits among {bucket}:")
        for rule, c in hits[bucket].most_common(8):
            print(f"  {rule:<22} {c}")

    print("\nNote: design behaviour of the CEP layer on synthetic data - tuning "
          "targets, not validated production metrics. ML fusion (phase 6) lifts "
          "recall further.")


# --------------------------------------------------------------------------
# 2. absolute vs population-relative MULE_FAN_IN, paired within seed
# --------------------------------------------------------------------------

def _score_mode(path, mode, quantile):
    """One dataset, one mode. Returns the numbers the modes differ on."""
    saved = C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE
    C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE = mode, quantile
    try:
        pop = PopulationBaseline()
        df, _ = replay(path, population=pop)
        fraud = df["label_is_fraud"].astype(int) == 1
        mule = fraud & (df.get("label_fraud_type", "") == "MULE")
        return {
            "mule_recall": (float(df.loc[mule, "flagged"].mean())
                            if mule.any() else float("nan")),
            "fraud_recall": float(df.loc[fraud, "flagged"].mean()),
            "fp_rate": float(df.loc[~fraud, "flagged"].mean()),
            "threshold": pop.threshold(quantile, C.MULE_FAN_IN_MIN_SENDERS),
        }
    finally:
        C.MULE_FAN_IN_MODE, C.MULE_FAN_IN_QUANTILE = saved


def cmd_fan_in_mode(args):
    files = _expand(args.files)
    print(f"{len(files)} dataset(s), quantile {args.quantile}, "
          f"absolute threshold {C.MULE_FAN_IN_MIN_SENDERS}\n")
    print(f"{'dataset':<26}{'thr':>5}{'MULE abs':>10}{'MULE rel':>10}{'delta':>9}"
          f"{'FP abs':>9}{'FP rel':>9}")

    rows = []
    for f in files:
        a = _score_mode(f, "absolute", args.quantile)
        r = _score_mode(f, "relative", args.quantile)
        rows.append((a, r))
        print(f"{_name(f):<26}{r['threshold']:>5}{a['mule_recall']:>10.1%}"
              f"{r['mule_recall']:>10.1%}{r['mule_recall'] - a['mule_recall']:>+9.1%}"
              f"{a['fp_rate']:>9.2%}{r['fp_rate']:>9.2%}")

    print("\n" + "=" * 72)
    for key, label in (("mule_recall", "MULE recall"),
                       ("fraud_recall", "overall fraud recall"),
                       ("fp_rate", "false-positive rate")):
        d = [r[key] - a[key] for a, r in rows if not math.isnan(a[key])]
        if not d:
            continue
        lo, hi = ci(d)
        pos = sum(1 for x in d if x > 0)
        verdict = "real" if (len(d) > 1 and (lo > 0 or hi < 0)) else "unresolved"
        print(f"{label:<24} delta {st.mean(d):+.4f}"
              + (f"  95% CI [{lo:+.4f}, {hi:+.4f}]" if len(d) > 1 else "")
              + f"  sign {pos}/{len(d)}  -> {verdict}")

    print("\nRead the false-positive row first. A recall gain bought with alerts")
    print("is not a gain; it is a threshold move, and the decision layer already")
    print("has a knob for that. The claim worth making is a recall delta whose")
    print("interval excludes zero WHILE the false-positive interval contains it.")
    if len(files) < 3:
        print("\n!! Fewer than three datasets. The interval is decoration at this n.")


# --------------------------------------------------------------------------
# 3. seeded vs unseeded APP episodes
# --------------------------------------------------------------------------

MAX_SEED_LAG_DAYS = 21     # must match maybe_seed_payee in fraud_patterns.py


def _with_stream_new(path):
    """Replay, plus a per-row flag for whether the payee was new to the stream.

    No population baseline: this arm measures the new-payee control, and letting
    MULE_FAN_IN move underneath it would confound the two.
    """
    df, _ = replay(path)
    # Resolved through features.payee_key rather than by naming a column: this
    # recomputation exists to be compared against what the RULE layer sees, and
    # a cross-check keyed on a different identity than the thing it checks is
    # not a cross-check.
    payees = [F.payee_key({"receiver_pinfl": p, "receiver_card": c})
              for p, c in zip(df["receiver_pinfl"], df["receiver_card"])]
    seen, stream_new = defaultdict(set), []
    for card, rcv in zip(df["sender_card"], payees):
        stream_new.append(0 if rcv in seen[card] else 1)
        seen[card].add(rcv)
    df["stream_new"] = stream_new
    # ISO8601: the generator omits microseconds when they are zero, so the
    # column is not one fixed format.
    df["t"] = pd.to_datetime(df["event_time"], format="ISO8601")
    return df


def cmd_payee_seeding(args):
    files = _expand(args.files)
    print(f"{len(files)} dataset(s); APP episodes after day "
          f"{MAX_SEED_LAG_DAYS} only\n")
    print(f"{'dataset':<22}{'n new':>7}{'recall':>9}{'n seeded':>10}"
          f"{'recall':>9}{'delta':>9}")

    deltas, pooled = [], []
    for f in files:
        df = _with_stream_new(f)
        app = df[(df["label_is_fraud"] == 1)
                 & (df["label_fraud_type"] == "APP")].copy()
        app["day"] = (app["t"] - df["t"].min()).dt.days
        late = app[app["day"] >= MAX_SEED_LAG_DAYS]
        new, seeded = late[late.stream_new == 1], late[late.stream_new == 0]
        if len(new) == 0 or len(seeded) == 0:
            print(f"{f:<22}  skipped - one group empty")
            continue
        d = seeded.flagged.mean() - new.flagged.mean()
        deltas.append(d)
        pooled.append((int(new.flagged.sum()), len(new),
                       int(seeded.flagged.sum()), len(seeded)))
        print(f"{_name(f):<22}{len(new):>7}{new.flagged.mean():>9.1%}"
              f"{len(seeded):>10}{seeded.flagged.mean():>9.1%}{d:>+9.1%}")

    if not deltas:
        raise SystemExit("nothing measured")

    lo, hi = ci(deltas)
    neg = sum(1 for d in deltas if d < 0)
    hn, nn, hs, ns = (sum(x[i] for x in pooled) for i in range(4))
    print("\n" + "=" * 66)
    print(f"pooled   new payee {hn}/{nn} = {hn/nn:.1%}   "
          f"seeded {hs}/{ns} = {hs/ns:.1%}")
    print(f"delta    {st.mean(deltas):+.1%}"
          + (f"  95% CI [{lo:+.1%}, {hi:+.1%}]" if len(deltas) > 1 else "")
          + f"  sign {neg}/{len(deltas)} negative")
    print("\nA negative delta is the evasion working: the same episode, with one")
    print("small prior transfer, is detected less often. The threat model called")
    print("this control cheap to evade from reasoning alone; this is the price.")
    if len(deltas) < 3:
        print("\n!! Fewer than three seeds. The interval is decoration at this n.")


def main():
    ap = argparse.ArgumentParser(
        description="Replay a dataset through the deployed CEP rule engine.")
    sub = ap.add_subparsers(dest="cmd")

    ap.add_argument("--file", default="../data-generator/out/transactions.csv",
                    help="dataset for the default summary")

    p = sub.add_parser("fan-in-mode",
                       help="absolute vs population-relative MULE_FAN_IN")
    p.add_argument("--files", nargs="+", required=True)
    p.add_argument("--quantile", type=float, default=C.MULE_FAN_IN_QUANTILE)
    p.set_defaults(fn=cmd_fan_in_mode)

    p = sub.add_parser("payee-seeding",
                       help="seeded vs unseeded APP episodes")
    p.add_argument("--files", nargs="+", required=True)
    p.set_defaults(fn=cmd_payee_seeding)

    args = ap.parse_args()
    (getattr(args, "fn", None) or cmd_summary)(args)


if __name__ == "__main__":
    main()
