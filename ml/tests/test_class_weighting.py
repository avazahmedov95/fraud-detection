"""The class weighting in train.py: weighted where that recipe was validated,
unweighted below the line, where every weight measured collapsed the ranking."""

import pytest

import train as T


def test_weighted_at_the_rate_the_recipe_was_validated_at():
    # 1.5% fraud, this project's own data: a weight near 65.
    assert T.class_weight(15, 985, "auto") == pytest.approx(985 / 15)


def test_below_the_line_the_default_fits_unweighted():
    # 0.1%, IBM AML's rate, where every weighted fit collapsed.
    assert T.class_weight(1, 999, "auto") == 1.0


def test_off_fits_unweighted():
    assert T.class_weight(1, 999, "off") == 1.0


def test_on_weights_regardless():
    assert T.class_weight(1, 999, "on") == 999.0


def test_an_unknown_setting_is_refused_not_read_as_auto():
    with pytest.raises(ValueError, match="auto, on or off"):
        T.class_weight(15, 985, "yes")


def test_cutoffs_review_maximises_f1_and_block_demands_precision():
    import numpy as np
    y = np.array([0] * 90 + [1] * 10)
    p = np.concatenate([np.linspace(0, 0.5, 90), np.linspace(0.4, 1.0, 10)])
    review, block = T.choose_cutoffs(y, p, block_precision=0.9)
    assert 0.4 <= review <= 1.0
    assert block is None or block >= review


def test_without_a_precise_enough_cutoff_there_is_no_block():
    import numpy as np
    y = np.array([1, 0] * 50)          # the top score is a legitimate row, so no
    p = np.linspace(0, 1, 100)         # cutoff reaches beyond half precision
    assert T.choose_cutoffs(y, p, block_precision=0.99)[1] is None
