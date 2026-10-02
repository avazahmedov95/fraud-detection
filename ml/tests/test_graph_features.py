"""The graph's columns (experiments/graph_features.py), on a few transfers whose
answers are known."""

import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "experiments"))
import graph_features as G  # noqa: E402

DAY = G.LABEL_DELAY_S


def test_each_column_reads_only_what_came_before_it():
    rows = [("A", "X", 0, 1),            # a fraud into X
            ("B", "X", 10, 0),           # X not confirmed yet: a day has not passed
            ("C", "X", DAY + 1, 0),      # now it is
            ("X", "D", DAY + 2, 0),      # X sends
            ("E", "D", DAY + 3, 0),      # D has dealt with X this week
            ("F", "P", DAY + 4, 0), ("F", "Q", DAY + 5, 0),   # one account funds two
            ("P", "Z", DAY + 6, 0), ("Q", "Z", DAY + 7, 0),   # who both pay Z
            ("W", "Z", DAY + 8, 0)]
    s, r, t, y = (np.array(c) for c in zip(*rows))
    out = G.graph_features(s, r, t, y)
    col = {name: out[:, k] for k, name in enumerate(G.FEATURES)}
    assert list(col["payee_flagged"][:3]) == [0, 0, 1]
    assert col["sender_flagged"][3] == 1
    assert col["payee_flagged_contacts"][4] == 1
    # Z's second payer is not counted on its own transfer, only after it.
    assert list(col["payee_common_funder"][7:]) == [0, 1, 2]
