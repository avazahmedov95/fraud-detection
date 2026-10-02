"""Would a graph store help the served model? Four columns one could serve, computed
offline from the transfers before each one - asking needs no Neo4j - and read the way
this project gates a feature: the realistic profile under twenty generator seeds,
the served recipe fitted with and without them, paired by seed.

  payee_flagged           the payee received a confirmed fraud
  sender_flagged          so did the sender
  payee_flagged_contacts  confirmed-fraud accounts the payee dealt with in the week
  payee_common_funder     the most of the payee's payers in the last day that one
                          account had itself paid in the day before them - money
                          converging through intermediaries

The first three spread confirmed cases along the graph: a fraud counts as confirmed
a day after it happened (LABEL_DELAY_S), every one of them - the generous case, as
if each were reported. The fourth needs no label, only the graph.

The rule, fixed before the run: a graph store is worth building only if all four
raise validation PR-AUC with a 95% interval clear of zero over the twenty seeds
(the gate the counterparty counters passed) and do not lower test F1.

    python experiments/graph_features.py            # resumes
    python experiments/graph_features.py --report
"""
import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter, defaultdict, deque

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import models as M                                      # noqa: E402
from ablate_seeds import GEN_DIR, _contract_fingerprint, ci95  # noqa: E402
from recall import DEFAULT_SEEDS                        # noqa: E402

STATE = os.path.join(M._PKG, "models", "ablation", "graph_features.json")
SCRATCH = "/tmp/graph_features"
FEATURES = ("payee_flagged", "sender_flagged", "payee_flagged_contacts",
            "payee_common_funder")
LABEL_DELAY_S = 86_400
CONTACT_WINDOW_S = 7 * 86_400
FUNDER_WINDOW_S = 86_400
#: The columns each fit adds to the served 21: confirmed cases spread along the
#: graph, the graph's shape alone, and both.
VARIANTS = {"model": (), "cases": (0, 1, 2), "shape": (3,), "both": (0, 1, 2, 3)}


def _recent(queue, since):
    while queue and queue[0][0] < since:
        queue.popleft()
    return queue


def _common_funder(payers, payee, t):
    """The most of the payee's payers in the last day that one account had paid in
    the day before."""
    funded = Counter()
    for p in {p for _, p in _recent(payers[payee], t - FUNDER_WINDOW_S)}:
        funded.update({f for _, f in _recent(payers[p], t - FUNDER_WINDOW_S)})
    return max(funded.values(), default=0)


def graph_features(senders, receivers, times, labels, cap=None):
    """FEATURES for each transfer, from the transfers strictly before it. `cap` keeps
    only an account's latest so many contacts and payers, for files whose hub
    accounts deal with thousands a week; this project's people never reach it."""
    out = np.zeros((len(times), len(FEATURES)), "float32")
    flagged, pending = set(), deque()        # pending: (when it is known, account)
    contacts = defaultdict(lambda: deque(maxlen=cap))   # account -> (t, the other side)
    payers = defaultdict(lambda: deque(maxlen=cap))     # account -> (t, who paid it)
    for i, (s, r, t) in enumerate(zip(senders, receivers, times)):
        while pending and pending[0][0] <= t:
            flagged.add(pending.popleft()[1])
        week = _recent(contacts[r], t - CONTACT_WINDOW_S)
        out[i] = (r in flagged, s in flagged,
                  len({o for _, o in week if o in flagged}),
                  _common_funder(payers, r, t))
        contacts[r].append((t, s))
        contacts[s].append((t, r))
        payers[r].append((t, s))
        if labels[i]:
            pending.append((t + LABEL_DELAY_S, r))
    return out


def _rates(y, p, cut):
    alert = p >= cut
    caught = int(y[alert].sum())
    prec, rec = caught / max(int(alert.sum()), 1), caught / max(int(y.sum()), 1)
    return {"pr_auc": float(average_precision_score(y, p)), "recall": rec,
            "precision": prec, "f1": 2 * prec * rec / max(prec + rec, 1e-12)}


def run_seed(seed):
    """Generate, featurise, add the graph's columns and fit every variant."""
    out = os.path.join(SCRATCH, f"seed{seed}")
    os.makedirs(out, exist_ok=True)
    started = time.time()
    subprocess.run([sys.executable, "generator.py", "--profile", "realistic",
                    "--seed", str(seed), "--out", out],
                   cwd=GEN_DIR, check=True, capture_output=True)
    csv = os.path.join(out, "transactions.csv")
    df = M.D.build_matrix(csv)
    raw = pd.read_csv(csv).sort_values("event_time").reset_index(drop=True)   # its order
    if not (np.array_equal(raw["event_time"].values, df["event_time"].values)
            and np.array_equal(raw["label_is_fraud"].values, df["label"].values)):
        raise SystemExit(f"seed {seed}: the graph's rows are not the matrix's")
    times = pd.to_datetime(raw["event_time"], format="ISO8601").astype("int64") // 10**9
    y = df["label"].values.astype("int8")
    G = graph_features(raw["sender_card"].values, raw["receiver_card"].values,
                       times.values, y)
    X = df[list(M.D.FEATURE_NAMES)].astype("float32").values
    for name in ("transactions.csv", "persons.csv"):
        os.remove(os.path.join(out, name))

    cut = M.T.cut_index(len(y))
    fit = int(cut * M.T.FIT_SHARE)
    spw = M.T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum()))
    result = {"frauds": int(y[cut:].sum()),
              # How clean the flag is here: test transfers to a flagged payee, by truth.
              "flagged_fraud": int(((G[cut:, 0] > 0) & (y[cut:] == 1)).sum()),
              "flagged_legit": int(((G[cut:, 0] > 0) & (y[cut:] == 0)).sum())}
    for name, cols in VARIANTS.items():
        Xv = np.hstack([X, G[:, cols]]) if cols else X
        members = M._committee(Xv[:fit], y[:fit], spw)
        pva = M._committee_score(members, Xv[fit:cut])
        pte = M._committee_score(members, Xv[cut:])
        review = M.T.choose_review_cutoff(y[fit:cut], pva)
        result[name] = {"val_pr_auc": float(average_precision_score(y[fit:cut], pva)),
                        **_rates(y[cut:], pte, review)}
    print(f"seed {seed}: {time.time() - started:.0f} s, PR-AUC "
          + ", ".join(f"{k} {result[k]['pr_auc']:.3f}" for k in VARIANTS), flush=True)
    return result


def _load():
    if not os.path.exists(STATE):
        return {}
    with open(STATE, encoding="utf-8") as fh:
        blob = json.load(fh)
    if blob["_contract"] != _contract_fingerprint():
        raise SystemExit(f"{STATE} was measured under another feature set or generator")
    return blob["seeds"]


def report(results):
    runs = list(results.values())
    if not runs:
        print("nothing stored yet")
        return
    print(f"{len(runs)} seeds, {sum(r['frauds'] for r in runs):,} held-out frauds; held-out "
          f"transfers to a flagged payee: {sum(r['flagged_fraud'] for r in runs)} fraud, "
          f"{sum(r['flagged_legit'] for r in runs)} legitimate\n")
    keys = ("val_pr_auc", "pr_auc", "recall", "precision", "f1")
    print(f"{'variant':<8}" + "".join(f"{k:>17}" for k in keys))
    for v in VARIANTS:
        print(f"{v:<8}" + "".join(f"{np.mean([r[v][k] for r in runs]):>11.3f} +/-"
                                  f"{ci95([r[v][k] for r in runs]):.3f}" for k in keys))
    print("\nagainst the model alone, paired by seed: mean, 95% CI, seeds higher")
    for v in ("cases", "shape", "both"):
        for k in keys:
            d = [r[v][k] - r["model"][k] for r in runs]
            m, h = float(np.mean(d)), ci95(d)
            print(f"  {v:<6}{k:<12}{m:>+8.4f} [{m - h:+.4f}, {m + h:+.4f}]"
                  f"  {sum(x > 0 for x in d)} of {len(d)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seeds", default=",".join(map(str, DEFAULT_SEEDS)))
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()
    results = _load()
    if not args.report:
        for seed in (int(s) for s in args.seeds.split(",")):
            if str(seed) in results:
                continue
            results[str(seed)] = run_seed(seed)
            with open(STATE, "w", encoding="utf-8") as fh:
                json.dump({"_contract": _contract_fingerprint(), "seeds": results}, fh,
                          indent=1)
    report(results)


if __name__ == "__main__":
    main()
