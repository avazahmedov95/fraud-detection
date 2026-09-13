"""The class-weighting guard in train.py: weighted where the recipe was validated,
a stop below the line unless whoever trains says how to weight."""

import pytest

import train as T


def test_weighted_at_the_rate_the_recipe_was_validated_at():
    # 1.5% fraud, this project's own data: a weight near 65.
    assert T.class_weight(15, 985, "auto") == pytest.approx(985 / 15)


def test_below_the_line_training_stops_and_says_how_to_go_on():
    # 0.1%, IBM AML's rate, where the weighted recipe collapsed.
    with pytest.raises(SystemExit, match="CLASS_WEIGHTING=off"):
        T.class_weight(1, 999, "auto")


def test_off_fits_unweighted():
    assert T.class_weight(1, 999, "off") == 1.0


def test_on_weights_regardless():
    assert T.class_weight(1, 999, "on") == 999.0


def test_an_unknown_setting_is_refused_not_read_as_auto():
    with pytest.raises(ValueError, match="auto, on or off"):
        T.class_weight(15, 985, "yes")
