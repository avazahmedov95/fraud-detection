"""The second look's artefacts, written beside the served model. TabPFN reads the
transfers just under the review cut-off and holds the ones it calls fraud
(second-look/README.md). Everything is chosen on the cutoff rows; the held-out
slice is not read:

  second_look.json  the band - served score from `from` up to `review` - and
                    TabPFN's cut-off in it. The job reads `from` only while `review`
                    is the cut-off it serves: a band chosen for one model means
                    nothing under another.
  second_look.npz   TabPFN's context: the pieces of the training slice it reads.

Runs where TabPFN is installed (.venv-models), after train.py and export_onnx.py:

    cd ml
    python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>
"""
import argparse
import hashlib
import json
import os

import lightgbm as lgb
import numpy as np
from scipy.special import expit
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import dataset as D
import train as T

#: The band: the highest-scored cutoff rows under the review cut-off.
BAND = 100
#: TabPFN refuses more than this many context rows on a CPU.
TABPFN_ROWS = 5_000
#: Each piece is a gigabyte in the service: TabPFN cannot share its weights between
#: members that cache their context. Two held as much of the cutoff band's fraud as
#: five, with no more alerts.
PIECES = 2
#: The F1 peak sits exactly on one fraud row's score, and the service's cached
#: TabPFN answers within 1e-5 of the uncached one it was chosen on - enough to drop
#: that row under its own cut-off. Set this far below it, the row stays held.
TIE = 1e-4


def matrix(cache=None):
    """train.py's matrix, read from `cache` when it holds these columns."""
    if cache and os.path.exists(cache):
        with np.load(cache, allow_pickle=False) as z:
            if [str(n) for n in z["names"]] != list(D.FEATURE_NAMES):
                raise SystemExit(f"{cache} holds other columns - delete it and re-run")
            return z["X"], z["y"]
    df = D.build_matrix(T.CSV)
    X = df[list(D.FEATURE_NAMES)].astype("float32").values
    y = df["label"].values.astype("int8")
    if cache:
        np.savez_compressed(cache, X=X, y=y, names=np.array(list(D.FEATURE_NAMES)))
    return X, y


def pieces(yfit, rows=TABPFN_ROWS, seed=42):
    """The training slice in pieces TabPFN can read: every fraud row in each piece
    and the ordinary rows dealt out between them, none above `rows`."""
    rng = np.random.default_rng(seed)
    fraud = np.flatnonzero(yfit == 1)
    legit = rng.permutation(np.flatnonzero(yfit == 0))
    count = -(-len(legit) // (rows - len(fraud)))
    return [np.concatenate([fraud, part]) for part in np.array_split(legit, count)]


def tabpfn_scores(Xfit, yfit, X, context, checkpoint):
    """TabPFN reading each piece as a one-member model; the mean over the pieces."""
    from tabpfn import TabPFNClassifier
    total = np.zeros(len(X))
    for k, rows in enumerate(context, 1):
        model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                              TabPFNClassifier(model_path=checkpoint, n_estimators=1,
                                               random_state=k))
        model.fit(Xfit[rows], yfit[rows])
        total += model.predict_proba(X)[:, 1]
    return total / len(context)


def _sha256(path):
    """The weights the cut-off was chosen with; the service refuses any others."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the training matrix (X, y, names)")
    ap.add_argument("--tabpfn-model", dest="tabpfn_model", required=True,
                    help="the TabPFN checkpoint the second-look service will load")
    args = ap.parse_args()

    with open(os.path.join(T.MODELS_DIR, "thresholds.json"), encoding="utf-8") as fh:
        review = json.load(fh)["review"]
    X, y = matrix(args.cache)
    cut = T.cut_index(len(y))
    fit = int(cut * T.FIT_SHARE)
    Xfit, yfit, Xva, yva = X[:fit], y[:fit], X[fit:cut], y[fit:cut]
    booster = lgb.Booster(model_file=os.path.join(T.MODELS_DIR, "model.txt"))
    served = expit(booster.predict(Xva, raw_score=True))
    below = np.flatnonzero(served < review)
    band = below[np.argsort(-served[below])][:BAND]
    context = pieces(yfit)[:PIECES]
    tab = tabpfn_scores(Xfit, yfit, Xva[band], context, args.tabpfn_model)

    spec = {"from": float(served[band[-1]]), "review": review,
            "cut": T.choose_review_cutoff(yva[band], tab) - TIE, "pieces": len(context),
            "checkpoint": os.path.basename(args.tabpfn_model),
            "checkpoint_sha256": _sha256(args.tabpfn_model),
            "chosen_on": {"rows": len(yva), "band": len(band),
                          "band_fraud": int(yva[band].sum())}}
    rows = np.concatenate(context)
    np.savez_compressed(os.path.join(T.MODELS_DIR, "second_look.npz"), X=Xfit[rows],
                        y=yfit[rows],
                        piece=np.repeat(np.arange(len(context)), [len(p) for p in context]))
    with open(os.path.join(T.MODELS_DIR, "second_look.json"), "w", encoding="utf-8") as fh:
        json.dump(spec, fh, indent=2)
    print(f"band from {spec['from']:.4f} to {review:.4f}, TabPFN holding at "
          f"{spec['cut']:.4f}; {len(rows):,} context rows in {len(context)} pieces")


if __name__ == "__main__":
    main()
