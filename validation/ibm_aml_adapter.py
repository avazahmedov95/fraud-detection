"""Replays the deployed rules over IBM's AML transaction sets (Altman et al.,
NeurIPS 2023), at minute resolution over 17 days - to retest MULE_FAN_IN, whose
null result on AMLSim was ambiguous with a day-long clock (README.md 4), and to
score this project's model on a published benchmark. Section D measures the
remaining window gap. The shared replay is in harness.py.
"""

import argparse
import os

import pandas as pd

import harness as RP
from harness import C, Event                                   # noqa: F401


#: The headers contain spaces, which itertuples turns into positional names, so
#: they are renamed on load (pandas splits the two `Account` columns into
#: Account / Account.1).
COLUMNS = {"Account": "sender", "Account.1": "receiver", "Amount Paid": "amount",
           "Payment Currency": "currency", "Payment Format": "fmt",
           "Is Laundering": "label"}

#: Self-transfers (12% of the file, mostly `Reinvestment`) are not transfers
#: between two parties: dropped, and the count printed.
DROP_SELF_TRANSFERS = True


def load(path, formats=None, limit=None):
    d = pd.read_csv(path, dtype={"Account": str, "Account.1": str})
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
    """One scale factor PER CURRENCY, from that currency's median, so "just under the
    limit" means the same in yen and in dollars. Fixed by medians, not chosen."""
    return {cur: RP.scale_factor(g.amount) for cur, g in d.groupby("currency")}


def to_events(d, scales, typologies=None):
    typologies = typologies or {}
    for r in d.itertuples(index=False):
        # read_patterns' key: the minute as the sidecar prints it, and both accounts.
        key = (f"{r.ts:%Y/%m/%d %H:%M}", r.sender, r.receiver)
        yield Event(
            ev={"amount_uzs": float(r.amount) * scales.get(r.currency, 1.0),
                "sender_pinfl": r.sender,
                "receiver_pinfl": r.receiver},
            ts=int(r.ts.timestamp()),
            label=int(r.label),
            typology=typologies.get(key, ""))


def read_patterns(path):
    """Map (minute, sender, receiver) -> typology from a `*_Patterns.txt` sidecar.

    Blocks open with `BEGIN LAUNDERING ATTEMPT - <TYPE>`, some with a suffix
    (`FAN-OUT:  Max 16-degree Fan-Out`); rows follow in the CSV's column order. The
    minute is in the key because one account pair recurs across attempts of
    different types. Optional: without it section B is empty.
    """
    typ, current = {}, None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("BEGIN LAUNDERING ATTEMPT"):
                # The first " - " only (the names are hyphenated), and up to a ":" - a
                # suffix like "Max 16-degree Fan-Out" is not part of the name.
                current = line.split(" - ", 1)[-1].split(":", 1)[0].strip().lower()
            elif line.startswith("END LAUNDERING ATTEMPT"):
                current = None
            elif current and "," in line:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 5:
                    typ[(parts[0], parts[2], parts[4])] = current
    return typ


def window_stats(d):
    """How far the collection stage outruns RECEIVER_WINDOW_S - the share of each
    pattern the rule can see."""
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
    """Run the deployed extractor over the file once and cache what a model needs:
    features, label, timestamp, and the file's payment format and currency. The
    replay takes hours (senders with 26,365 transactions in a day), so every fit
    reads the cache."""
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


#: Published minority-class F1 (%) on HI-Small, on the split `our_model` uses
#: (60% train, 20% validation, 20% test): arXiv:2402.08593, Table 4, mean +/- sd.
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
#: The counterparty counters (capabilities.py, adopted 2026-09-20). This file is
#: the only dataset here with a collection stage on a minute clock, which is the
#: shape they were built for, so it is the only place their multi-day columns can
#: be read against real laundering.
CPH = ("payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
       "sender_payees_7d", "secs_since_sender_inbound")


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


def _fit_and_cut(Xtr, ytr, Xva, yva, weighted, seed):
    """One fit of train.py's recipe, with the cut that maximises F1 on validation.
    The recipe lives here once, for every mode that needs a fitted model."""
    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import precision_recall_curve
    spw = ((ytr == 0).sum() / max(int(ytr.sum()), 1)) if weighted else 1.0
    m = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8,
                           min_child_samples=30, scale_pos_weight=spw,
                           random_state=seed, n_jobs=-1, verbose=-1)
    m.fit(Xtr, ytr)
    pva = m.predict_proba(Xva)[:, 1]
    prec, rec, thr = precision_recall_curve(yva, pva)
    f1s = 2 * prec[:-1] * rec[:-1] / np.maximum(prec[:-1] + rec[:-1], 1e-12)
    return m, (float(thr[int(np.argmax(f1s))]) if len(thr) else 0.5)


def _fit_score(Xtr, ytr, Xva, yva, Xte, yte, weighted, seed):
    """Test metrics at that cut, with F1 at 0.5 beside it since the paper does not
    state its rule."""
    from sklearn.metrics import (average_precision_score, f1_score, roc_auc_score)
    m, t = _fit_and_cut(Xtr, ytr, Xva, yva, weighted, seed)
    pte = m.predict_proba(Xte)[:, 1]
    return {"f1_tuned": 100 * f1_score(yte, pte >= t, zero_division=0),
            "f1_05": 100 * f1_score(yte, pte >= 0.5, zero_division=0),
            "pr_auc": average_precision_score(yte, pte),
            "roc_auc": roc_auc_score(yte, pte)}


def _open_cache(cache):
    """The cached matrix, refused if it was built from another feature set. Caches of
    different vintages hold the same row count, so the alignment check in
    `typology_recall` cannot tell them apart and a stale one would be read in
    silence, with a model fitted on columns that are not the ones named."""
    import numpy as np
    z = np.load(cache, allow_pickle=False)
    have = [str(n) for n in z["names"]]
    want = list(RP.available_features()[1])
    if have != want:
        z.close()
        raise SystemExit(
            f"{cache} was built from a different feature set - re-run --extract-only."
            f"\n  cached : {', '.join(have)}\n  current: {', '.join(want)}")
    return z


def our_model(cache, seeds=3):
    """This project's features and recipe on IBM AML, scored on the published split
    and metric; nothing is tuned to the dataset. Two configurations drop what only
    this dataset can test: the receiver-side aggregation, and the counterparty
    counters, whose collection stage PaySim has none of."""
    import warnings
    import numpy as np
    # sklearn warns once per fit that LightGBM was fitted without feature names;
    # sixty identical warnings bury the table they interrupt.
    warnings.filterwarnings("ignore", message="X does not have valid feature names")
    z = _open_cache(cache)
    X, y = z["X"], z["y"].astype("int8")
    names = [str(n) for n in z["names"]]
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)      # load() sorted by time; split by count
    fmt, fnames = z["fmt"], [str(c) for c in z["fmt_names"]]
    own = np.column_stack([(fmt == k).astype("float32") for k in range(len(fnames))]
                          + [z["currency"].astype("float32")])
    keep = [i for i, c in enumerate(names) if c not in RCV]
    keep_cph = [i for i, c in enumerate(names) if c not in CPH]
    configs = (("this project, all it can compute", X),
               ("  without receiver aggregation", X[:, keep]),
               ("  without the counterparty counters", X[:, keep_cph]),
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

    for what, dropped in (("receiver aggregation", "without receiver aggregation"),
                          ("the counterparty counters",
                           "without the counterparty counters")):
        print(f"\n  {what}, paired by seed (full minus without):")
        for weighted in (True, False):
            full = results[("this project, all it can compute", weighted)][1]
            less = results[(dropped, weighted)][1]
            for k in ("f1_tuned", "pr_auc"):
                d = [f[k] - l[k] for f, l in zip(full, less)]
                h = _ci95(d)
                verdict = ("" if h != h else "  - excludes zero"
                           if abs(np.mean(d)) > h else "  - includes zero")
                print(f"    {'weighted' if weighted else 'unweighted':<11}"
                      f"{k:<9}{np.mean(d):+.3f} +/- {h:.3f} "
                      f"(95% CI, n={len(d)}){verdict}")
    return results


def typology_recall(path, patterns, cache, seeds=3):
    """Recall per laundering typology, for the recipe fitted on this file.

    The replay takes hours, so the sidecar's labels are joined onto the matrix
    --extract-only cached rather than measured again. The join is positional, so it
    is checked first: a cache built from another file, or another sort, would hang
    a typology on the wrong transaction and the table would still print. This is
    the MODEL's recall; the rules' is section B of a replay with --patterns.
    """
    import warnings
    import numpy as np
    warnings.filterwarnings("ignore", message="X does not have valid feature names")

    d, lab, n_edges = _typology_labels(path, patterns)
    print(f"{len(d):,} transactions, {int(d.label.sum()):,} laundering, "
          f"{int((lab != '').sum()):,} of them named by the sidecar "
          f"({n_edges:,} edges read)")

    z = _open_cache(cache)
    X, y, ts = z["X"], z["y"].astype("int8"), z["ts"]
    if len(y) != len(d):
        raise SystemExit(f"the cache holds {len(y):,} rows and this file {len(d):,} "
                         f"- they are not the same run")
    file_ts = d.ts.values.astype("datetime64[s]").astype("int64")
    if (int((ts.astype("int64") != file_ts).sum())
            or int((y != d.label.values.astype("int8")).sum())):
        raise SystemExit("the cache is not row-aligned with this file - re-run "
                         "--extract-only before joining labels onto it")

    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    yte, lte = y[b:], lab[b:]
    print(f"split by time 60/20/20: test {n - b:,} rows, {int(yte.sum()):,} "
          f"laundering, {int((lte != '').sum()):,} of them named\n")

    flags = []
    for seed in range(seeds):
        # Unweighted: train.py's rule below 0.5% fraud, and this file is at 0.102%.
        m, t = _fit_and_cut(X[:a], y[:a], X[a:b], y[a:b], False, seed)
        f = m.predict_proba(X[b:])[:, 1] >= t
        flags.append(f)
        print(f"  seed {seed}: cut {t:.4f} from validation, {int(f.sum()):,} alerts, "
              f"{int((f & (yte == 1)).sum()):,} of them laundering")

    def row(name, sel):
        k = int(sel.sum())
        if not k:
            return
        rs = [float((f & sel).sum()) / k for f in flags]
        print(f"  {name:<22}{k:>9,}{np.mean(rs):>9.1%}"
              f"{min(rs):>11.1%}-{max(rs):.1%}")

    print(f"\n  {'typology':<22}{'in test':>9}{'recall':>9}{'across seeds':>17}")
    for g in sorted({str(v) for v in lte[yte == 1] if v}):
        row(g, (yte == 1) & (lte == g))
    row("(unnamed)", (yte == 1) & (lte == ""))
    row("ALL laundering", yte == 1)

def _bootstrap_delta(y, p_full, p_less, t_full, t_less, boots, seed=0):
    """Paired Poisson bootstrap over the test rows - the same weights for both models.
    Returns the 2.5/97.5 percentiles of the F1 and PR-AUC deltas (full minus less)
    and the share of resamples where the delta is not positive."""
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


def _typology_labels(path, patterns):
    """The file, and one typology name per row - empty where the sidecar names none.
    The join is positional on the cached matrix, so both readers share it rather than
    writing the key twice: `(minute, sender, receiver)`, looked up for laundering rows
    only, is the whole of the correspondence with the sidecar."""
    import numpy as np
    typ = read_patterns(patterns)
    d, _, _ = load(path)
    lab = np.array([""] * len(d), dtype=object)
    mins = d.ts.dt.strftime("%Y/%m/%d %H:%M").values
    sender, receiver = d.sender.values, d.receiver.values
    for i in np.flatnonzero(d.label.values == 1):
        lab[i] = typ.get((mins[i], sender[i], receiver[i]), "")
    return d, lab, len(typ)


BUDGETS = (0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.10)

#: Neither side is a hub: at most twenty distinct counterparties in the week before,
#: on the two sides this project's extractor counts. `ml/README.md` screened the same
#: way with the columns of the time (twenty over 96 hours). IBM AML carries no account
#: type, so this is a screen this project defines, never a fact the file states - and
#: the counts include the transfer itself, so twenty means nineteen others.
INDIVIDUAL_MAX = 20


def _people_like(X, names):
    import numpy as np
    return ((X[:, names.index("payee_payers_7d")] <= INDIVIDUAL_MAX)
            & (X[:, names.index("sender_payees_7d")] <= INDIVIDUAL_MAX))


def _top_of(pte, sel, frac):
    """Indices of the most suspicious `frac` of the rows `sel` keeps, and how many."""
    import numpy as np
    idx = np.flatnonzero(sel)
    k = max(int(round(frac * len(idx))), 1)
    return idx[np.argpartition(-pte[idx], k - 1)[:k]], k


def alert_budgets(cache, seeds=5, path=None, patterns=None, individuals=False):
    """How much laundering sits inside the most suspicious 0.1%, 1%, 2% ... of the
    test slice. A catch rate means nothing without the queue length that bought it,
    and the F1 cut `our_model` reports lands on a different queue every seed - 1,364
    alerts on one, 4,980 on another - so it cannot answer "how much would we catch if
    an analyst reviewed N of them". With --patterns the laundering the sidecar names
    is read beside the whole of it, and per typology at the two middle budgets; with
    --individuals the same questions are asked again of the transfers that look like
    people, where both the queue and the recall are drawn from those rows only."""
    import warnings
    import numpy as np
    warnings.filterwarnings("ignore", message="X does not have valid feature names")
    z = _open_cache(cache)
    X, y = z["X"], z["y"].astype("int8")
    names = [str(n) for n in z["names"]]
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    fmt, fnames = z["fmt"], [str(c) for c in z["fmt_names"]]
    own = np.column_stack([(fmt == k).astype("float32") for k in range(len(fnames))]
                          + [z["currency"].astype("float32")])
    yte = y[b:]
    lab = None
    if patterns:
        d, lab_all, _ = _typology_labels(path, patterns)
        if len(d) != n:
            raise SystemExit(f"the cache holds {n:,} rows and this file {len(d):,} - "
                             "re-run --extract-only before joining labels onto it")
        lab = lab_all[b:]

    views = [("all test rows", np.ones(len(yte), bool))]
    if individuals:
        views.append((f"people-like only (neither side over {INDIVIDUAL_MAX} "
                      f"counterparties in 7 d)", _people_like(X[b:], names)))

    print(f"test slice {n - b:,} rows, {int(yte.sum()):,} laundering "
          f"({yte.mean():.3%}); {seeds} seeds, unweighted")
    for label, M in (("this project, all it can compute", X),
                     ("+ the file's own format and currency", np.hstack([X, own]))):
        runs = [_fit_and_cut(M[:a], y[:a], M[a:b], y[a:b], False, seed)[0]
                .predict_proba(M[b:])[:, 1] for seed in range(seeds)]
        for view, sel in views:
            fraud = (yte == 1) & sel
            named = fraud & (lab != "") if lab is not None else None
            print(f"\n  {label}\n  {view}: {int(sel.sum()):,} rows, "
                  f"{int(fraud.sum()):,} laundering"
                  + (f", {int(named.sum()):,} of them named" if named is not None else ""))
            print(f"    {'budget':>8}{'alerts':>10}{'of laundering':>15}"
                  f"{'across seeds':>19}" + (f"{'of named':>11}" if lab is not None else ""))
            for frac in BUDGETS:
                rs, ns, k = [], [], 0
                for pte in runs:
                    top, k = _top_of(pte, sel, frac)
                    rs.append(float(fraud[top].sum()) / max(int(fraud.sum()), 1))
                    if named is not None:
                        ns.append(float(named[top].sum()) / max(int(named.sum()), 1))
                print(f"    {frac:>7.1%}{k:>10,}{np.mean(rs):>14.1%}"
                      f"{min(rs):>11.1%}-{max(rs):<7.1%}"
                      + (f"{np.mean(ns):>10.1%}" if ns else ""))
            if lab is None:
                continue
            shown = (0.01, 0.02)
            tops = {f: [_top_of(pte, sel, f)[0] for pte in runs] for f in shown}
            print(f"    {'per typology':<18}{'rows':>7}" +
                  "".join(f"{f'at {f:.0%}':>12}" for f in shown))
            for g in sorted({str(v) for v in lab[fraud] if v}):
                gm = fraud & (lab == g)
                cells = [f"{np.mean([float(gm[t].sum()) / max(int(gm.sum()), 1) for t in tops[f]]):>11.1%}"
                         for f in shown]
                print(f"    {g:<18}{int(gm.sum()):>7,}" + "".join(cells))
    print()


def receiver_ablation(cache, seeds=20, boots=1000):
    """Is receiver aggregation worth anything to the model here, beyond seed noise?
    Asked two ways, fixed before the run: per seed with a t-interval (twenty seeds),
    and on the seed-averaged model with a paired bootstrap over the test rows. It
    counts as established only if BOTH F1 intervals on this project's own columns
    configuration exclude zero. Unweighted recipe only (README 4)."""
    import warnings
    import numpy as np
    from sklearn.metrics import average_precision_score, f1_score
    warnings.filterwarnings("ignore", message="X does not have valid feature names")
    z = _open_cache(cache)
    X, y = z["X"], z["y"].astype("int8")
    names = [str(n) for n in z["names"]]
    n = len(y)
    a, b = int(n * 0.6), int(n * 0.8)
    fmt, fnames = z["fmt"], [str(c) for c in z["fmt_names"]]
    own = np.column_stack([(fmt == k).astype("float32") for k in range(len(fnames))]
                          + [z["currency"].astype("float32")])
    keep = [i for i, c in enumerate(names) if c not in RCV]
    own_label = f"{len(names)} features"
    pairs = ((own_label, X, X[:, keep]),
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
    r = out[own_label]
    lo_seed = r["per_seed_f1"][0] - r["per_seed_f1"][1]
    established = bool(lo_seed > 0 and r["f1_ci"][0] > 0)
    print("\n  verdict (rule fixed in advance: both F1 intervals on this "
          "project's own columns exclude zero):")
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
    ap.add_argument("--typology-recall", dest="typology_recall",
                    action="store_true",
                    help="recall per laundering typology from --patterns and the "
                         "cached matrix, without re-running the replay")
    ap.add_argument("--budgets", dest="budgets", action="store_true",
                    help="recall at fixed alert budgets from the cached "
                         "matrix; with --patterns, also over the named rows")
    ap.add_argument("--individuals", dest="individuals", action="store_true",
                    help="with --budgets, ask the same of transfers where "
                         "neither side looks like a hub")
    ap.add_argument("--boots", type=int, default=1000,
                    help="bootstrap resamples for --receiver-ablation")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found")

    # The PaySim profile: accounts, amounts and a clock; no account-opening date either.
    RP.capability_profile("myid_kinship",
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
    if args.budgets:
        if not os.path.exists(args.cache):
            raise SystemExit(f"{args.cache} not found - run --extract-only first")
        return alert_budgets(args.cache, args.seeds, args.file, args.patterns,
                             args.individuals)
    if args.typology_recall:
        if not os.path.exists(args.cache):
            raise SystemExit(f"{args.cache} not found - run --extract-only first")
        if not args.patterns:
            raise SystemExit("--typology-recall needs --patterns: the released CSV "
                             "carries no typology of its own")
        return typology_recall(args.file, args.patterns, args.cache, args.seeds)

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
