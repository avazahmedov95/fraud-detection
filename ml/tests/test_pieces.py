"""TabPFN's context, cut into pieces it can read (experiments/models.py)."""

import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "experiments"))
import models as M  # noqa: E402


def test_a_capped_piece_draws_its_own_fraud_and_the_ordinary_rows_are_dealt_once():
    y = np.zeros(60_000, "int8")
    y[::20] = 1                                            # 3,000 fraud rows
    assert all(y[p].sum() == 3_000 for p in M._pieces(y))  # uncapped: all of it, every piece
    capped = M._pieces(y, fraud_rows=500)
    assert all(y[p].sum() == 500 and len(p) <= M.TABPFN_ROWS for p in capped)
    assert not set(capped[0]) & set(capped[1])
    legit = np.concatenate([p[y[p] == 0] for p in capped])
    assert np.array_equal(np.sort(legit), np.flatnonzero(y == 0))
