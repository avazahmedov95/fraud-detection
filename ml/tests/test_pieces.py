"""TabPFN's context, cut into pieces it can read (second_look.py)."""

import numpy as np

import second_look as SL


def test_the_matrix_cache_is_read_only_for_the_data_it_was_built_from(tmp_path, monkeypatch):
    import pandas as pd
    import dataset as D
    built = []

    def build(path):
        built.append(path)
        return pd.DataFrame({**{n: [1.0] for n in D.FEATURE_NAMES}, "label": [0]})
    monkeypatch.setattr(D, "build_matrix", build)
    csv, cache = tmp_path / "t.csv", str(tmp_path / "m.npz")
    csv.write_text("a\n1\n")
    D.cached_matrix(str(csv), cache)
    D.cached_matrix(str(csv), cache)
    assert len(built) == 1
    csv.write_text("a\n2\n")                     # regenerated
    D.cached_matrix(str(csv), cache)
    assert len(built) == 2


def test_every_piece_holds_all_the_fraud_and_the_ordinary_rows_are_dealt_once():
    y = np.zeros(60_000, "int8")
    y[::20] = 1                                            # 3,000 fraud rows
    pieces = SL.pieces(y)
    assert all(y[p].sum() == 3_000 and len(p) <= SL.TABPFN_ROWS for p in pieces)
    legit = np.concatenate([p[y[p] == 0] for p in pieces])
    assert np.array_equal(np.sort(legit), np.flatnonzero(y == 0))
