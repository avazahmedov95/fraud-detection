"""Retrains the model on what the running system served and what people decided, and
says whether the new model should replace the served one. The retrainer service runs
it daily; nothing changes until a person promotes it (.\\run.ps1 promote-model).

Rows: the job's logged decisions with their features (fraud.transactions_scored),
added to the served model's training rows. Labels: fraud where an analyst confirmed
it or a client reported it (fraud.cases). The logged rows split as in train.py: the
earliest 64% train, the next 16% set the cut-off at the served model's number of
alerts, the last 20% decide between the two. Drift is read first; the outcome goes to
models/retrain_status.json for the demo page.

    python retrain.py [--cache models_matrix.npz] [--every-hours 24]
"""
import argparse
import json
import os
import time
import traceback

import joblib
import numpy as np
from sklearn.metrics import average_precision_score

import dataset as D
import train as T

CANDIDATE_DIR = os.path.join(T.MODELS_DIR, "candidate")
STATUS = os.path.join(T.MODELS_DIR, "retrain_status.json")
#: Fewer known frauds in the deciding rows, and a comparison is noise.
MIN_FRAUD = 30
#: Population stability index over which a feature has moved (under 0.1: stable).
PSI_MOVED = 0.25
#: Fewer recent decisions, and a PSI reads the sample, not the traffic.
MIN_DRIFT_ROWS = 1000
#: The newest decisions read, so memory stays bounded.
MAX_ROWS = 1_000_000


def warehouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"], password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.getenv("CLICKHOUSE_DB", "fraud"))


def logged(client):
    """The newest decisions with a whole feature vector, one per transfer, in the
    job's order, labelled by what people called fraud."""
    n = len(D.FEATURE_NAMES)
    rows = client.query(
        "SELECT transaction_id, features FROM ("
        "SELECT transaction_id, features, scored_at_job FROM transactions_scored "
        "WHERE length(features) = %(n)s "
        "ORDER BY scored_at_job DESC LIMIT 1 BY transaction_id LIMIT %(cap)s) "
        "ORDER BY scored_at_job", parameters={"n": n, "cap": MAX_ROWS}).result_rows
    fraud = {r[0] for r in client.query(
        "SELECT transaction_id FROM cases FINAL "
        "WHERE disposition = 'CONFIRMED_FRAUD'").result_rows}
    X = np.array([r[1] for r in rows], dtype="float32").reshape(-1, n)
    y = np.array([r[0] in fraud for r in rows], dtype="int8")
    return X, y


def split(n):
    """Where fitting ends and where the cut-off rows end, as in train.py."""
    cut = T.cut_index(n)
    return int(cut * T.FIT_SHARE), cut


def same_workload_cut(new, old, old_cut):
    """The new model's cut-off: as many alerts as the served model's on these rows."""
    k = int((old >= old_cut).sum())
    return float(np.sort(new)[::-1][k - 1]) if k else float(new.max()) + 1e-9


def read(y, p, cut):
    """The alerts at a cut-off and what they make of the known frauds."""
    alert = p >= cut
    n, caught, fraud = int(alert.sum()), int((alert & (y == 1)).sum()), int(y.sum())
    prec = caught / n if n else 0.0
    rec = caught / fraud if fraud else 0.0
    return {"alerts": n, "caught": caught, "fraud": fraud, "precision": prec, "recall": rec,
            "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
            "pr_auc": float(average_precision_score(y, p)) if fraud else None}


def psi(ref, cur, bins=10):
    """How far a feature moved from `ref` to `cur` (population stability index) over
    `ref`'s deciles; NaN is a bin of its own."""
    seen = ref[~np.isnan(ref)]
    edges = np.unique(np.quantile(seen, np.linspace(0, 1, bins + 1)[1:-1])) if len(seen) else []

    def shares(x):
        nan = np.isnan(x)
        counts = np.bincount(np.searchsorted(edges, x[~nan], side="right"),
                             minlength=len(edges) + 1)
        counts = np.append(counts, nan.sum())
        return (counts + 0.5) / (counts.sum() + 0.5 * len(counts))

    p, q = shares(ref), shares(cur)
    return float(np.sum((q - p) * np.log(q / p)))


def drift(ref, cur):
    """Every feature's PSI, the most moved first."""
    out = {name: psi(ref[:, j], cur[:, j]) for j, name in enumerate(D.FEATURE_NAMES)}
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def run(cache):
    """One retrain: too_few_rows, too_few_fraud, better or not_better, with figures."""
    with open(os.path.join(T.MODELS_DIR, "thresholds.json"), encoding="utf-8") as fh:
        served_cut = json.load(fh)["review"]
    served = joblib.load(os.path.join(T.MODELS_DIR, "model.joblib"))
    Xh, yh = D.cached_matrix(T.CSV, cache)
    # The rows the served model was fitted and tuned on; its test slice is left out.
    Xh, yh = Xh[:T.cut_index(len(yh))], yh[:T.cut_index(len(yh))]
    X, y = logged(warehouse())
    fit, cut = split(len(y))
    status = {"logged": int(len(y)), "known_fraud": int(y.sum()),
              "needed": {"rows": MIN_DRIFT_ROWS, "fraud": MIN_FRAUD}}
    print(f"logged decisions: {len(y):,}, {int(y.sum())} of them called fraud by people")
    if len(y) - fit < MIN_DRIFT_ROWS:
        print(f"too few to read drift or compare ({len(y) - fit} recent of the "
              f"{MIN_DRIFT_ROWS} needed): keep the served model")
        return dict(status, outcome="too_few_rows", recent=int(len(y) - fit))

    moved = drift(Xh, X[fit:])
    print(f"\n=== drift: the latest {len(y) - fit:,} decisions against the served "
          f"model's training rows (PSI; over {PSI_MOVED} = moved) ===")
    for name, v in list(moved.items())[:5]:
        print(f"  {name:<32}{v:7.3f}{'  MOVED' if v > PSI_MOVED else ''}")
    rate_then = float((served.predict(Xh[int(len(Xh) * T.FIT_SHARE):]) >= served_cut).mean())
    rate_now = float((served.predict(X[fit:]) >= served_cut).mean())
    print(f"  served model's alert rate: {rate_then:.3%} on its own cut-off rows, "
          f"{rate_now:.3%} now")
    status.update(recent=int(len(y) - fit), alert_rate={"then": rate_then, "now": rate_now},
                  moved=[n for n, v in moved.items() if v > PSI_MOVED])

    known = int(y[cut:].sum())
    if known < MIN_FRAUD:
        print(f"\nThe deciding rows hold {known} known frauds, under the {MIN_FRAUD} a "
              f"comparison needs: no candidate. Keep the served model.")
        return dict(status, outcome="too_few_fraud", deciding_fraud=known)
    model, _ = T.fit_committee(np.vstack([Xh, X[:fit]]), np.concatenate([yh, y[:fit]]))
    new_cut = same_workload_cut(model.predict(X[fit:cut]), served.predict(X[fit:cut]),
                                served_cut)
    old = read(y[cut:], served.predict(X[cut:]), served_cut)
    new = read(y[cut:], model.predict(X[cut:]), new_cut)
    print(f"\n=== the latest {len(y) - cut:,} decisions, {known} known frauds, "
          f"neither model trained on them ===")
    for label, r in (("served", old), ("retrained", new)):
        print(f"  {label:<10} alerts {r['alerts']:>6}  caught {r['caught']:>5}  "
              f"precision {r['precision']:.1%}  recall {r['recall']:.1%}  F1 {r['f1']:.1%}  "
              f"PR-AUC {r['pr_auc']:.3f}")
    better = new["f1"] > old["f1"]
    print("\nThe retrained model is better here: promote it with .\\run.ps1 promote-model."
          if better else "\nThe retrained model is not better here: keep the served one.")

    os.makedirs(CANDIDATE_DIR, exist_ok=True)
    joblib.dump(model, os.path.join(CANDIDATE_DIR, "model.joblib"))
    with open(os.path.join(CANDIDATE_DIR, "feature_names.json"), "w") as fh:
        json.dump(list(D.FEATURE_NAMES), fh, indent=2)
    with open(os.path.join(CANDIDATE_DIR, "thresholds.json"), "w") as fh:
        json.dump(dict(review=new_cut, chosen_on=dict(rows=cut - fit, fraud=int(y[fit:cut].sum()),
                                                       alerts_as_served=True),
                       committee=len(T.COMMITTEE_SEEDS)), fh, indent=2)
    with open(os.path.join(CANDIDATE_DIR, "metrics.json"), "w") as fh:
        json.dump(dict(pr_auc=new["pr_auc"], thresholds=dict(review=new_cut), at_review=new,
                       by_fraud_type={}, served=old, better=better, drift=moved,
                       measured_on=f"the latest {len(y) - cut} logged decisions, "
                                   f"labelled by analysts' verdicts and clients' reports"),
                  fh, indent=2)
    print(f"candidate written to {CANDIDATE_DIR}/")
    return dict(status, outcome="better" if better else "not_better", deciding_fraud=known,
                served=old, retrained=new)


def write_status(status, every_hours):
    """The latest run's outcome, for the demo page."""
    with open(STATUS, "w", encoding="utf-8") as fh:
        json.dump(dict(status, at=time.time(), every_hours=every_hours), fh, indent=2)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", help="npz of the training matrix (X, y, names)")
    ap.add_argument("--every-hours", type=float,
                    help="run again every so many hours, for ever: the retrainer service")
    a = ap.parse_args()
    while True:
        try:
            outcome = run(a.cache)
        except Exception as exc:                       # noqa: BLE001 - the schedule goes on
            traceback.print_exc()
            outcome = {"outcome": "failed", "error": f"{type(exc).__name__}: {exc}"[:300]}
        write_status(outcome, a.every_hours)
        if not a.every_hours:
            break
        print(f"next retrain in {a.every_hours:g} h", flush=True)
        time.sleep(a.every_hours * 3600)
