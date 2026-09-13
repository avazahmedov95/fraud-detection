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
           "Is Laundering": "label", "From Bank": "from_bank",
           "To Bank": "to_bank"}

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
                "receiver_pinfl": r.receiver,
                # The bank on each side. cross_network compares the issuers behind
                # the two cards, and an interbank transfer is the same distinction;
                # left out, the feature was a constant zero on a file naming both.
                "sender_network": r.from_bank,
                "receiver_network": r.to_bank},
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

    UNVERIFIED against a real sidecar - written from the dataset's description,
    because the Hugging Face mirror carries only the transactions and the file
    itself is behind a Kaggle account. The column indices ARE confirmed: IBM's own
    Multi-GNN loader reads the accounts from positions 2 and 4, which is what this
    reads. The marker lines are not. So the first real run must check that section B
    is non-empty and that the typology names look like typologies - an empty table
    here means "the file was not parsed", not "the system missed everything", and
    those two must not be confused.
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


def extract_matrix(path, cache, limit=None):
    """Run the deployed extractor over the file once and cache what a model needs.

    The rules replay over these 4.49M rows took 5.5 hours, because the extractor
    re-scans each sender's day of history per event and this file holds senders
    with 26,365 transactions in one day (README.md 4). Every fit reads the cache, so
    a fit that fails costs minutes rather than the extraction again.

    Saved beside the features: the label, the timestamp for a temporal split, and
    the file's own payment format and currency - columns this project's contract has
    no place for, kept so a configuration WITH them can be compared, as PaySim's
    transaction type was.
    """
    import numpy as np
    d, n_all, n_self = load(path, None, limit)
    print(f"{len(d):,} transactions ({n_self:,} self-transfers dropped), "
          f"{int(d.label.sum()):,} laundering")
    X, y, ts = RP.extract_features(to_events(d, _scales(d)), total=len(d))
    idx, names = RP.available_features()
    fmt = d.fmt.astype("category")
    cur = d.currency.astype("category")
    np.savez_compressed(
        cache, X=X[:, idx], y=y, ts=ts, names=np.array(names),
        fmt=fmt.cat.codes.values.astype("int8"),
        fmt_names=np.array([str(c) for c in fmt.cat.categories]),
        currency=cur.cat.codes.values.astype("int8"),
        currency_names=np.array([str(c) for c in cur.cat.categories]))
    print(f"cached {X.shape[0]:,} x {len(idx)} features to {cache}")
    print(f"  kept: {', '.join(names)}")


#: Published minority-class F1 (%) on HI-Small, on the split `our_model` uses:
#: the earliest 60% of transactions train, the next 20% validate, the last 20% test.
#: arXiv:2402.08593 (Graph Feature Preprocessor), Table 4, mean +/- sd over runs;
#: for GFP the better of its two batch settings. The paper does not say how the
#: threshold behind each F1 was chosen - see `our_model` for the rule used here.
PUBLISHED_F1 = (
    ("XGBoost, the file's own columns", 19.75, 0.89),
    ("LightGBM, the file's own columns", 21.30, 0.30),
    ("GIN (graph neural network)", 28.70, 1.13),
    ("GIN+EU", 47.73, 7.86),
    ("PNA (graph neural network)", 56.77, 2.41),
    ("GFP + LightGBM (graph features)", 62.86, 0.25),
    ("GFP + XGBoost (graph features)", 64.77, 0.47),
)

RCV = ("rcv_distinct_senders_1h", "rcv_inflow_1h")


def _ci95(xs):
    """Half-width of the t-based 95% interval for a mean: the convention
    ml/experiments/ablate_seeds.py uses for paired ablation deltas. NaN below two
    observations, where no interval exists."""
    import math
    from scipy import stats
    n = len(xs)
    if n < 2:
        return float("nan")
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return float(stats.t.ppf(0.975, n - 1) * sd / math.sqrt(n))


def _fit_score(Xtr, ytr, Xva, yva, Xte, yte, weighted, seed):
    """One fit of train.py's recipe; test metrics at a threshold chosen on validation.

    The threshold is the one that maximises F1 on the VALIDATION slice, then applied
    unchanged to the test slice. The published table does not state its rule, so
    F1 at 0.5 is reported beside it: the gap between the two is the size of that
    uncertainty, not something to be hidden by picking the kinder one.
    """
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import (average_precision_score, f1_score,
                                 precision_recall_curve, roc_auc_score)
    spw = ((ytr == 0).sum() / max(int(ytr.sum()), 1)) if weighted else 1.0
    m = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8,
                           min_child_samples=30, scale_pos_weight=spw,
                           random_state=seed, n_jobs=-1, verbose=-1)
    m.fit(Xtr, ytr)
    pva = m.predict_proba(Xva)[:, 1]
    prec, rec, thr = precision_recall_curve(yva, pva)
    f1s = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    t = float(thr[int(np.argmax(f1s))]) if len(thr) else 0.5
    pte = m.predict_proba(Xte)[:, 1]
    return {"f1_tuned": 100 * f1_score(yte, pte >= t, zero_division=0),
            "f1_05": 100 * f1_score(yte, pte >= 0.5, zero_division=0),
            "pr_auc": average_precision_score(yte, pte),
            "roc_auc": roc_auc_score(yte, pte)}


def our_model(cache, seeds=3):
    """This project's features and training recipe on IBM AML, scored on the
    published split and metric so it sits beside published models on one axis.

    Nothing is tuned to this dataset: train.py's hyperparameters, the deployed
    extractor's features, the capability profile the data supports. What is new is
    only the evaluation - minority-class F1 on a temporal 60/20/20 split, the
    convention of the IBM benchmark - because a number on a different split or
    metric could not be compared with anything.

    The second configuration is the reason to run it at all: receiver-side
    aggregation is this project's largest measured effect, PaySim could not test it
    (no collection stage, and the sign reversed), and this dataset has one.
    """
    import warnings
    import numpy as np
    # sklearn warns once per fit that LightGBM was fitted without feature names;
    # sixty identical warnings bury the table they interrupt.
    warnings.filterwarnings("ignore", message="X does not have valid feature names")
    z = np.load(cache, allow_pickle=False)
    X, y = z["X"], z["y"].astype("int8")
    names = [str(n) for n in z["names"]]
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)      # load() sorted by time; split by count
    fmt, fnames = z["fmt"], [str(c) for c in z["fmt_names"]]
    own = np.column_stack([(fmt == k).astype("float32") for k in range(len(fnames))]
                          + [z["currency"].astype("float32")])
    keep = [i for i, c in enumerate(names) if c not in RCV]
    configs = (("this project, all it can compute", X),
               ("  without receiver aggregation", X[:, keep]),
               ("  + the file's own format and currency", np.hstack([X, own])))
    print(f"{n:,} transactions, {int(y.sum()):,} laundering; split by time "
          f"60/20/20: train {a:,}, validate {b - a:,}, test {n - b:,} "
          f"({int(y[b:].sum()):,} laundering in test)")
    print(f"features: {len(names)} ({', '.join(names)})\n")

    results = {}
    print(f"  {'configuration':<40}{'recipe':>10}{'F1 %':>14}{'F1@0.5 %':>12}"
          f"{'PR-AUC':>9}{'ROC-AUC':>9}")
    for label, M in configs:
        for weighted in (True, False):
            runs = [_fit_score(M[:a], y[:a], M[a:b], y[a:b], M[b:], y[b:],
                               weighted, seed) for seed in range(seeds)]
            agg = {k: (float(np.mean([r[k] for r in runs])),
                       float(np.std([r[k] for r in runs])))
                   for k in runs[0]}
            results[(label.strip(), weighted)] = (agg, runs)
            recipe = "weighted" if weighted else "unweighted"
            print(f"  {label:<40}{recipe:>10}"
                  f"{agg['f1_tuned'][0]:>8.2f} +/-{agg['f1_tuned'][1]:>4.2f}"
                  f"{agg['f1_05'][0]:>12.2f}{agg['pr_auc'][0]:>9.3f}"
                  f"{agg['roc_auc'][0]:>9.3f}")

    print("\n  published on the same split (minority-class F1 %, arXiv:2402.08593):")
    for name, f1, sd in PUBLISHED_F1:
        print(f"    {name:<38}{f1:>8.2f} +/-{sd:>5.2f}")

    print("\n  receiver aggregation, paired by seed (full minus without):")
    for weighted in (True, False):
        full = results[("this project, all it can compute", weighted)][1]
        less = results[("without receiver aggregation", weighted)][1]
        for k in ("f1_tuned", "pr_auc"):
            d = [f[k] - l[k] for f, l in zip(full, less)]
            h = _ci95(d)
            verdict = ("" if h != h else "  - excludes zero" if abs(np.mean(d)) > h
                       else "  - includes zero")
            print(f"    {'weighted' if weighted else 'unweighted':<11}{k:<9}"
                  f"{np.mean(d):+.3f} +/- {h:.3f} (95% CI, n={len(d)}){verdict}")
    return results


def _bootstrap_delta(y, p_full, p_less, t_full, t_less, boots, seed=0):
    """Paired Poisson bootstrap over the test rows: the SAME resampling weights for
    both models, so what varies is the test set, not the comparison.

    Returns the 2.5th/97.5th percentiles of the F1 and PR-AUC deltas (full minus
    less) and the share of resamples in which the delta is not positive. PR-AUC is
    computed from one fixed ordering per model with weighted cumulative sums, which
    ignores score ties - immaterial for an interval, and the point estimates are
    taken with sklearn separately.
    """
    import numpy as np
    rng = np.random.default_rng(seed)

    def f1_w(pred, w):
        tp = float(np.sum(w * y * pred))
        fp = float(np.sum(w * (1 - y) * pred))
        fn = float(np.sum(w * y * (1 - pred)))
        return 200.0 * tp / max(2 * tp + fp + fn, 1e-12)

    def ap_w(order, w):
        ys, ws = y[order], w[order]
        tp = np.cumsum(ws * ys)
        seen = np.cumsum(ws)
        prec = np.where(seen > 0, tp / np.maximum(seen, 1e-12), 0.0)
        total = tp[-1]
        return float(np.sum(ws * ys * prec) / total) if total > 0 else 0.0

    pf, pl = (p_full >= t_full).astype("int8"), (p_less >= t_less).astype("int8")
    of, ol = np.argsort(-p_full, kind="stable"), np.argsort(-p_less, kind="stable")
    d_f1, d_ap = [], []
    for _ in range(boots):
        w = rng.poisson(1.0, len(y)).astype("float64")
        d_f1.append(f1_w(pf, w) - f1_w(pl, w))
        d_ap.append(ap_w(of, w) - ap_w(ol, w))
    d_f1, d_ap = np.array(d_f1), np.array(d_ap)
    return {"f1_ci": tuple(np.percentile(d_f1, [2.5, 97.5])),
            "ap_ci": tuple(np.percentile(d_ap, [2.5, 97.5])),
            "f1_share_not_positive": float(np.mean(d_f1 <= 0)),
            "ap_share_not_positive": float(np.mean(d_ap <= 0))}


def receiver_ablation(cache, seeds=20, boots=1000):
    """Is receiver aggregation worth anything to the model here? Asked so that seed
    noise cannot answer it.

    Ten seeds put the paired F1 delta at +2.1, 95% CI [-1.5, +5.6]: the model's own
    fit-to-fit spread, about 4 F1 points, is wider than the effect. So the question
    is asked two ways, both fixed before the run and both reported whatever they
    say:

    1. per seed, paired, with a t-based 95% interval - the ablate_seeds convention,
       at twenty seeds instead of ten;
    2. on the seed-AVERAGED model - the mean of every seed's probabilities, which
       removes most of the fit-to-fit noise - with a paired bootstrap over the test
       rows, which measures the uncertainty that remains, the test set's own.

    Decision rule, stated in advance: the effect counts as established only if BOTH
    intervals exclude zero for F1 on the fourteen-feature configuration, which is
    this project's own feature set. PR-AUC and the configuration with the file's own
    format and currency are reported beside it and do not change the verdict.
    Unweighted recipe only: the weighted one collapses at this base rate (README 4).
    """
    import warnings
    import numpy as np
    from sklearn.metrics import average_precision_score, f1_score
    warnings.filterwarnings("ignore", message="X does not have valid feature names")
    z = np.load(cache, allow_pickle=False)
    X, y = z["X"], z["y"].astype("int8")
    names = [str(n) for n in z["names"]]
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    fmt, fnames = z["fmt"], [str(c) for c in z["fmt_names"]]
    own = np.column_stack([(fmt == k).astype("float32") for k in range(len(fnames))]
                          + [z["currency"].astype("float32")])
    keep = [i for i, c in enumerate(names) if c not in RCV]
    pairs = (("fourteen features", X, X[:, keep]),
             ("plus format and currency", np.hstack([X, own]),
              np.hstack([X[:, keep], own])))
    yte = y[b:]

    def fit(M, seed):
        import lightgbm as lgb
        m = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31,
                               subsample=0.8, colsample_bytree=0.8,
                               min_child_samples=30, scale_pos_weight=1.0,
                               random_state=seed, n_jobs=-1, verbose=-1)
        m.fit(M[:a], y[:a])
        return m.predict_proba(M[a:b])[:, 1], m.predict_proba(M[b:])[:, 1]

    def tuned(pva):
        from sklearn.metrics import precision_recall_curve
        prec, rec, thr = precision_recall_curve(y[a:b], pva)
        f1s = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
        return float(thr[int(np.argmax(f1s))]) if len(thr) else 0.5

    def score(pva, pte):
        t = tuned(pva)
        return t, (100 * f1_score(yte, pte >= t, zero_division=0),
                   average_precision_score(yte, pte))

    out = {}
    print(f"test: {n - b:,} rows, {int(yte.sum()):,} laundering; {seeds} seeds, "
          f"{boots} bootstrap resamples; unweighted recipe\n")
    for label, Mf, Ml in pairs:
        per_f1, per_ap = [], []
        sva_f = np.zeros(b - a); ste_f = np.zeros(n - b)
        sva_l = np.zeros(b - a); ste_l = np.zeros(n - b)
        for seed in range(seeds):
            vf, tf = fit(Mf, seed)
            vl, tl = fit(Ml, seed)
            _, (f1f, apf) = score(vf, tf)
            _, (f1l, apl) = score(vl, tl)
            per_f1.append(f1f - f1l); per_ap.append(apf - apl)
            sva_f += vf; ste_f += tf; sva_l += vl; ste_l += tl
        tf_, (ef1f, eapf) = score(sva_f / seeds, ste_f / seeds)
        tl_, (ef1l, eapl) = score(sva_l / seeds, ste_l / seeds)
        bs = _bootstrap_delta(yte, ste_f / seeds, ste_l / seeds, tf_, tl_, boots)
        h1, h2 = _ci95(per_f1), _ci95(per_ap)
        m1, m2 = float(np.mean(per_f1)), float(np.mean(per_ap))
        out[label] = {"per_seed_f1": (m1, h1), "per_seed_ap": (m2, h2),
                      "positive_seeds_f1": int(sum(d > 0 for d in per_f1)),
                      "ensemble_f1": (ef1f, ef1l), "ensemble_ap": (eapf, eapl),
                      **bs}
        print(f"  {label}")
        print(f"    per seed, paired     F1 {m1:+.2f} [{m1 - h1:+.2f}, {m1 + h1:+.2f}]"
              f"   PR-AUC {m2:+.4f} [{m2 - h2:+.4f}, {m2 + h2:+.4f}]"
              f"   ({out[label]['positive_seeds_f1']}/{seeds} seeds positive)")
        print(f"    seed-averaged model  F1 {ef1f:.2f} vs {ef1l:.2f} = {ef1f - ef1l:+.2f} "
              f"[{bs['f1_ci'][0]:+.2f}, {bs['f1_ci'][1]:+.2f}]   PR-AUC "
              f"{eapf:.4f} vs {eapl:.4f} = {eapf - eapl:+.4f} "
              f"[{bs['ap_ci'][0]:+.4f}, {bs['ap_ci'][1]:+.4f}]")
    r = out["fourteen features"]
    lo_seed = r["per_seed_f1"][0] - r["per_seed_f1"][1]
    established = bool(lo_seed > 0 and r["f1_ci"][0] > 0)
    print("\n  verdict (rule fixed in advance: both F1 intervals on the fourteen "
          "features exclude zero):")
    print(f"    {'ESTABLISHED' if established else 'NOT ESTABLISHED'}")
    out["established"] = established
    return out


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
    ap.add_argument("--extract-only", dest="extract_only", action="store_true",
                    help="run the deployed extractor over the whole file and cache "
                         "the feature matrix to --cache, for the model fits")
    ap.add_argument("--cache", default="ibm_features.npz",
                    help="where --extract-only writes the feature matrix")
    ap.add_argument("--our-model", dest="our_model", action="store_true",
                    help="fit this project's recipe on the cached features and "
                         "score it on the published 60/20/20 temporal split")
    ap.add_argument("--seeds", type=int, default=3,
                    help="fits per configuration, for the spread")
    ap.add_argument("--receiver-ablation", dest="receiver_ablation",
                    action="store_true",
                    help="the receiver-aggregation delta, per seed and on the "
                         "seed-averaged model with a paired bootstrap")
    ap.add_argument("--boots", type=int, default=1000,
                    help="bootstrap resamples for --receiver-ablation")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    # Same profile PaySim forced: account identifiers, amounts and a clock, and
    # nothing else this project uses. receiver_age is off here too - unlike AMLSim,
    # the released files carry no account-opening date.
    RP.capability_profile("receiver_age", "myid_kinship", "device_telemetry",
                          "geo_telemetry", "session_telemetry")

    if args.extract_only:
        return extract_matrix(args.file, args.cache, args.limit)
    if args.our_model:
        if not os.path.exists(args.cache):
            raise SystemExit(f"{args.cache} not found - run --extract-only first")
        return our_model(args.cache, args.seeds)
    if args.receiver_ablation:
        if not os.path.exists(args.cache):
            raise SystemExit(f"{args.cache} not found - run --extract-only first")
        return receiver_ablation(args.cache, args.seeds, args.boots)

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
