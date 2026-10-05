"""Retrains the model on what the running system served and what people decided, and
says whether the new model should replace the served one. Nothing changes until a
person promotes it: .\\run.ps1 promote-model.

The rows: every decision the job logged with its feature values
(fraud.transactions_scored, `features`), once per transfer, in the order the job made
them, added to the rows the served model was trained on. The labels: fraud where an
analyst confirmed it or a client reported it (fraud.cases), not fraud everywhere
else - a fraud nobody reported stays a wrong label, as it does in a bank.

The logged rows split as train.py splits its data: the earliest 64% join the training
rows; the next 16% set the new model's cut-off at as many alerts as the served model
raises there, so the analysts' workload stays the same; the last 20%, which neither
model trained on, decide between the two. First it says which features have moved
since the served model was trained: the drift a retrain answers.

    python retrain.py [--cache models_matrix.npz]
"""
import argparse
import json
import os

import joblib
import numpy as np
from sklearn.metrics import average_precision_score

import dataset as D
import train as T

CANDIDATE_DIR = os.path.join(T.MODELS_DIR, "candidate")
#: Below this many known frauds in the deciding rows a comparison is noise, and no
#: candidate is written.
MIN_FRAUD = 30
#: Population stability index over which a feature counts as moved: under 0.1 is
#: read as stable, 0.1-0.25 as some change, over 0.25 as a large one.
PSI_MOVED = 0.25
#: Fewer recent decisions than this, and a PSI reads the sample, not the traffic.
MIN_DRIFT_ROWS = 1000


def warehouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"], password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.getenv("CLICKHOUSE_DB", "fraud"))


def logged(client):
    """The decisions logged with a whole feature vector, the first one per transfer
    (a second look adds another), in the order the job made them, labelled by what
    people called fraud."""
    n = len(D.FEATURE_NAMES)
    rows = client.query(
        "SELECT transaction_id, features FROM transactions_scored "
        "WHERE length(features) = %(n)s "
        "ORDER BY scored_at_job LIMIT 1 BY transaction_id", parameters={"n": n}).result_rows
    fraud = {r[0] for r in client.query(
        "SELECT transaction_id FROM cases FINAL "
        "WHERE disposition = 'CONFIRMED_FRAUD'").result_rows}
    X = np.array([r[1] for r in rows], dtype="float32").reshape(-1, n)
    y = np.array([r[0] in fraud for r in rows], dtype="int8")
    return X, y


def split(n):
    """train.py's shares over n time-ordered rows: where fitting ends, where the
    cut-off rows end."""
    cut = T.cut_index(n)
    return int(cut * T.FIT_SHARE), cut


def same_workload_cut(new, old, old_cut):
    """The new model's cut-off: as many alerts as the served model raises on the same
    rows."""
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
    """How far one feature's distribution in `cur` has moved from `ref` (population
    stability index), over the deciles of `ref`; a value never computed (NaN) is a
    bin of its own."""
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


def main(cache):
    with open(os.path.join(T.MODELS_DIR, "thresholds.json"), encoding="utf-8") as fh:
        served_cut = json.load(fh)["review"]
    served = joblib.load(os.path.join(T.MODELS_DIR, "model.joblib"))
    Xh, yh = D.cached_matrix(T.CSV, cache)
    # The rows the served model was fitted and tuned on; its test slice is left out.
    Xh, yh = Xh[:T.cut_index(len(yh))], yh[:T.cut_index(len(yh))]
    X, y = logged(warehouse())
    fit, cut = split(len(y))
    print(f"logged decisions: {len(y):,}, {int(y.sum())} of them called fraud by people")
    if len(y) - fit < MIN_DRIFT_ROWS:
        print(f"too few to read drift or compare ({len(y) - fit} recent of the "
              f"{MIN_DRIFT_ROWS} needed): keep the served model")
        return

    moved = drift(Xh, X[fit:])
    print(f"\n=== drift: the latest {len(y) - fit:,} decisions against the served "
          f"model's training rows (PSI; over {PSI_MOVED} = moved) ===")
    for name, v in list(moved.items())[:5]:
        print(f"  {name:<32}{v:7.3f}{'  MOVED' if v > PSI_MOVED else ''}")
    rate_then = float((served.predict(Xh[int(len(Xh) * T.FIT_SHARE):]) >= served_cut).mean())
    rate_now = float((served.predict(X[fit:]) >= served_cut).mean())
    print(f"  served model's alert rate: {rate_then:.3%} on its own cut-off rows, "
          f"{rate_now:.3%} now")

    known = int(y[cut:].sum())
    if known < MIN_FRAUD:
        print(f"\nThe deciding rows hold {known} known frauds, under the {MIN_FRAUD} a "
              f"comparison needs: no candidate. Keep the served model.")
        return
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


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", help="npz of the training matrix (X, y, names)")
    main(ap.parse_args().cache)
