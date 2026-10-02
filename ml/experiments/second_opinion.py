"""Would TabPFN help as a second opinion after the served model? Read on the
cutoff rows; the test slice stays for the decision itself.

1. The analyst's queue is ordered by amount (case-manager's store): how much
   sooner would its fraud be found in TabPFN's order?
2. How much fraud sits just under the served cut-off, and how far under it is
   TabPFN worth asking - better than simply lowering the cut-off?
3. What one answer costs on this machine.

With --test, the band the cutoff rows chose is read once on the held-out slice;
with --served as well, as the second-look service runs it - the same reading the
public datasets get (validation/README.md). Read-only like models.py: it fits its
own models and writes nothing the system serves.

    cd ml
    python experiments/second_opinion.py --cache models_matrix.npz --tabpfn-model <checkpoint>
"""
import argparse
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import models as M  # noqa: E402

#: The curve in ml/README.md: one piece is already within 0.003 PR-AUC of all 73.
PIECES = 5
#: Bands under the cut-off, as the highest-scored rows below it.
BANDS = (100, 250, 500, 1000, 2000)
#: The band the cutoff rows chose, for the held-out read (--test).
BAND = 100


def queue(y, served, tab, log_amount):
    """The queue is ordered by amount on purpose - money at risk first - so each
    order is read both ways: fraud found, and the share of the fraud money."""
    money = np.expm1(log_amount) * y
    print(f"1. The queue: {len(y)} alerts, {int(y.sum())} of them fraud\n")
    print(f"{'ordered by':<20}{'first half: fraud':>19}{'its money':>11}"
          f"{'first quarter':>15}{'its money':>11}")
    for name, order in (("amount, as today", np.lexsort((-served, -log_amount))),
                        ("the served score", np.argsort(-served)),
                        ("TabPFN", np.argsort(-tab))):
        half, quarter = order[:round(len(y) / 2)], order[:round(len(y) / 4)]
        print(f"{name:<20}{int(y[half].sum()):>19}{money[half].sum() / money.sum():>11.0%}"
              f"{int(y[quarter].sum()):>15}{money[quarter].sum() / money.sum():>11.0%}")
    low = np.argsort(tab)[:len(y) // 3]
    print(f"TabPFN's lowest third: {int((y[low] == 0).sum())} false alarms, "
          f"{int(y[low].sum())} fraud\n")


def under_the_cut(y, served, tab, total, missed):
    """Rows come sorted by the served score, highest first."""
    print("2. Under the cut-off: TabPFN alerts at its own F1 peak in each band, against "
          "lowering\n   the cut-off far enough to raise as many alerts\n")
    print(f"{'band':<12}{'of rows':>9}{'score from':>12}{'fraud':>7}{'TabPFN finds':>14}"
          f"{'false':>7}{'lower cut finds':>17}")
    for n in BANDS:
        yb, tb = y[:n], tab[:n]
        flag = tb >= M._cut(yb, tb) if yb.any() else np.zeros(n, bool)
        m, hit = int(flag.sum()), int(yb[flag].sum())
        print(f"{'top ' + format(n, ','):<12}{n / total:>9.2%}{served[n - 1]:>12.4f}"
              f"{int(yb.sum()):>7}{hit:>14}{m - hit:>7}{int(yb[:m].sum()):>17}")
    print(f"missed fraud further down, out of TabPFN's reach here: "
          f"{missed - int(y.sum())}\n")


def cost(Xfit, yfit, X, tabpfn_model):
    from tabpfn import TabPFNClassifier

    rows = M._pieces(yfit)[0]
    print("3. One alert, one piece of context:")
    for mode in ("fit_preprocessors", "fit_with_cache"):
        model = M._scaled(TabPFNClassifier(model_path=tabpfn_model or "auto",
                                           n_estimators=1, fit_mode=mode))
        started = time.time()
        model.fit(Xfit[rows], yfit[rows])
        fitted = time.time() - started
        model.predict_proba(X[:1])                     # the first call warms up
        started = time.time()
        model.predict_proba(X[1:2])
        print(f"   {mode:<18} {time.time() - started:6.2f} s an answer, "
              f"after {fitted:.0f} s to take the context")


def band_of(served, cut, n=BAND):
    """The n highest-scored rows under the cut-off, highest first. ml/second_look.py
    exports the band this chooses on the cutoff rows."""
    below = np.flatnonzero(served < cut)
    return below[np.argsort(-served[below])][:n]


def held_out(Xfit, yfit, Xva, yva, Xte, yte, served_va, served_te, cut, tabpfn_model,
             band=BAND, pieces=PIECES, tie=0.0):
    """The first band, read where nothing was chosen. The band's floor and TabPFN's
    threshold in it are fixed on the cutoff rows first; the held-out slice only
    answers whether TabPFN still finds more there than a lowered cut-off would."""
    band_va = band_of(served_va, cut, band)
    floor = served_va[band_va[-1]]
    band_te = np.flatnonzero((served_te >= floor) & (served_te < cut))
    band_te = band_te[np.argsort(-served_te[band_te])]
    *_, (_, tab, _) = M._in_pieces(Xfit, yfit, np.concatenate([Xva[band_va], Xte[band_te]]),
                                   M._pieces(yfit)[:pieces], tabpfn_model)
    t = M._cut(yva[band_va], tab[:len(band_va)]) - tie
    print(f"fixed on the cutoff rows: the band of {len(band_va)} rows from {floor:.4f} to "
          f"the cut-off {cut:.4f}, TabPFN on {pieces} pieces alerting at {t:.4f}\n")
    y, flag = yte[band_te], tab[len(band_va):] >= t
    m, hit = int(flag.sum()), int(y[flag].sum())
    raised, caught, total = int((served_te >= cut).sum()), int(yte[served_te >= cut].sum()), int(yte.sum())
    print(f"held-out slice: {len(yte):,} rows, {total} fraud; the served model raises "
          f"{raised} alerts and catches {caught}")
    print(f"the band holds {len(band_te)} rows ({len(band_te) / len(yte):.2%}), "
          f"{int(y.sum())} of them fraud")
    print(f"TabPFN raises {m} alerts there: {hit} fraud, {m - hit} false")
    print(f"a cut-off lowered to raise {m} alerts would catch {int(y[:m].sum())}")
    print(f"recall {caught / total:.1%} -> {(caught + hit) / total:.1%}, "
          f"alerts {raised} -> {raised + m}")


def as_served(Xfit, yfit, Xva, yva, Xte, yte, served_va, served_te, cut, tabpfn_model):
    """held_out with the second look as the service runs it (ml/second_look.py): its
    pieces and its tie, and a band holding the share of the cutoff rows it holds on
    this project's own data - so another dataset is read the same way, untuned."""
    import second_look as SL
    with open(os.path.join(M._PKG, "models", "second_look.json"), encoding="utf-8") as fh:
        chosen = json.load(fh)["chosen_on"]
    band = max(1, round(len(yva) * chosen["band"] / chosen["rows"]))
    held_out(Xfit, yfit, Xva, yva, Xte, yte, served_va, served_te, cut, tabpfn_model,
             band=band, pieces=SL.PIECES, tie=SL.TIE)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed matrix (X, y, names)")
    ap.add_argument("--tabpfn-model", dest="tabpfn_model", default=None,
                    help="the TabPFN checkpoint, as for models.py")
    ap.add_argument("--test", action="store_true",
                    help="read the band the cutoff rows chose on the held-out slice")
    ap.add_argument("--served", action="store_true",
                    help="with --test: as the second-look service runs it (as_served)")
    args = ap.parse_args()

    Xfit, yfit, Xva, yva, Xte, yte = M._slices(args.cache)
    spw = M.T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    members = M._committee(Xfit, yfit, spw)
    served = M._committee_score(members, Xva)
    with open(os.path.join(M._PKG, "models", "thresholds.json"), encoding="utf-8") as fh:
        cut = json.load(fh)["review"]
    if args.test:
        return (as_served if args.served else held_out)(
            Xfit, yfit, Xva, yva, Xte, yte, served, M._committee_score(members, Xte), cut,
            args.tabpfn_model)
    alert = np.flatnonzero(served >= cut)
    below = np.flatnonzero(served < cut)
    near = below[np.argsort(-served[below])][:max(BANDS)]
    print(f"served cut-off {cut:.4f} (refitted here: {M._cut(yva, served):.4f}); "
          f"{len(yva):,} cutoff rows, {int(yva.sum())} fraud; TabPFN reads "
          f"{PIECES} pieces\n")

    rows = np.concatenate([alert, near])
    *_, (_, tab, _) = M._in_pieces(Xfit, yfit, Xva[rows], M._pieces(yfit)[:PIECES],
                                   args.tabpfn_model)
    queue(yva[alert], served[alert], tab[:len(alert)],
          Xva[alert, list(M.D.FEATURE_NAMES).index("log_amount")])
    under_the_cut(yva[near], served[near], tab[len(alert):], len(yva), int(yva[below].sum()))
    cost(Xfit, yfit, Xva[near], args.tabpfn_model)


if __name__ == "__main__":
    main()
