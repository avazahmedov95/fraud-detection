"""The second look's artefacts, written beside the served model. TabPFN reads the
transfers just under the review cut-off and holds the ones it calls fraud
(ml/README.md, TabPFN as a second opinion). Everything is chosen on the cutoff
rows, as experiments/second_opinion.py chose it; the held-out slice is not read:

  second_look.json  the band - served score from `from` up to `review` - and
                    TabPFN's cut-off in it. The job reads `from` only while `review`
                    is the cut-off it serves: a band chosen for one model means
                    nothing under another.
  second_look.npz   TabPFN's context: the pieces of the training slice it reads.

Runs where TabPFN is installed (.venv-models):

    cd ml
    python second_look.py --cache models_matrix.npz --tabpfn-model <checkpoint>
"""
import argparse
import hashlib
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments"))
import models as M            # noqa: E402  the slices, the pieces, the served recipe
import second_opinion as S    # noqa: E402  the band

#: The F1 peak sits exactly on one fraud row's score, and the service's cached
#: TabPFN answers within 1e-5 of the uncached one it was chosen on - enough to drop
#: that row under its own cut-off. Set this far below it, the row stays held.
TIE = 1e-4
#: Each piece is a gigabyte in the service: TabPFN cannot share its weights between
#: members that cache their context. Chosen on the cutoff band by a rule fixed
#: before the run - the fewest pieces that hold as much of its fraud as five, with
#: no more alerts: two (9 of 11 at 22 alerts; five held 9 at 23).
PIECES = 2


def _sha256(path):
    """The weights the cut-off was chosen with; the service refuses any others."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", help="npz of the deployed matrix (X, y, names)")
    ap.add_argument("--tabpfn-model", dest="tabpfn_model", required=True,
                    help="the TabPFN checkpoint the second-look service will load")
    args = ap.parse_args()

    out = os.path.join(M._PKG, "models")
    with open(os.path.join(out, "thresholds.json"), encoding="utf-8") as fh:
        review = json.load(fh)["review"]
    Xfit, yfit, Xva, yva, _, _ = M._slices(args.cache)
    spw = M.T.class_weight(int(yfit.sum()), int((yfit == 0).sum()))
    served = M._committee_score(M._committee(Xfit, yfit, spw), Xva)
    band = S.band_of(served, review)
    pieces = M._pieces(yfit)[:PIECES]
    *_, (_, tab, _) = M._in_pieces(Xfit, yfit, Xva[band], pieces, args.tabpfn_model)

    spec = {"from": float(served[band[-1]]), "review": review,
            "cut": float(M._cut(yva[band], tab)) - TIE, "pieces": len(pieces),
            "checkpoint": os.path.basename(args.tabpfn_model),
            "checkpoint_sha256": _sha256(args.tabpfn_model),
            "chosen_on": {"rows": len(yva), "band": len(band),
                          "band_fraud": int(yva[band].sum())}}
    rows = np.concatenate(pieces)
    np.savez_compressed(os.path.join(out, "second_look.npz"), X=Xfit[rows], y=yfit[rows],
                        piece=np.repeat(np.arange(len(pieces)), [len(p) for p in pieces]))
    with open(os.path.join(out, "second_look.json"), "w", encoding="utf-8") as fh:
        json.dump(spec, fh, indent=2)
    print(f"band from {spec['from']:.4f} to {review:.4f}, TabPFN holding at "
          f"{spec['cut']:.4f}; {len(rows):,} context rows in {len(pieces)} pieces")


if __name__ == "__main__":
    main()
