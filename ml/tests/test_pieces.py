"""TabPFN's context, cut into pieces it can read (second_look.py)."""

import numpy as np

import second_look as SL


def test_every_piece_holds_all_the_fraud_and_the_ordinary_rows_are_dealt_once():
    y = np.zeros(60_000, "int8")
    y[::20] = 1                                            # 3,000 fraud rows
    pieces = SL.pieces(y)
    assert all(y[p].sum() == 3_000 and len(p) <= SL.TABPFN_ROWS for p in pieces)
    legit = np.concatenate([p[y[p] == 0] for p in pieces])
    assert np.array_equal(np.sort(legit), np.flatnonzero(y == 0))
