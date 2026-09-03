"""The feature contract must not depend on how the caller typed its input.

A defect that lived only in production: a Kafka record built from csv.DictReader
carries every field as TEXT, so `active_call` travelled as the string "False" -
non-empty, therefore true to `1 if v else 0`. The live job scored
active_call = 1 on 100% of events while the model had been trained on 3.5%.
Measured cost, scoring the dataset both ways through the deployed model: false
positives 20 -> 459, a 23x increase, for five additional true positives. It was
invisible offline because every offline caller converted the flag itself -
replay_eval via _as_bool, ml/dataset.py via pandas' bool dtype - so the one path
without a private conversion was the live one. The coercion now lives in
features.py, which every caller reaches the model through.
"""

import pytest

import features as F
import rules as R


def _ev(**over):
    ev = {"amount_uzs": 500_000, "receiver_pinfl": "R1",
          "receiver_card": "8600330000000002", "sender_pinfl": "S1",
          "sender_card": "8600330000000001", "device_id": "dev-1",
          "sender_region": "Tashkent City"}
    ev.update(over)
    return ev


# --- the exact failure -------------------------------------------------------

@pytest.mark.parametrize("false_value", [
    "False", "false", "FALSE", "f", "no", "0", "", "None", "nan", "  false  ",
])
def test_textual_false_is_false(false_value):
    """All non-empty strings except "" - and `1 if v else 0` called them all true."""
    f = F.extract(_ev(active_call=false_value), 800, R.SenderState(), now=1000)
    assert f["active_call"] == 0, f"{false_value!r} read as an active call"


@pytest.mark.parametrize("true_value", ["True", "true", "1", "yes", "Y"])
def test_textual_true_is_true(true_value):
    f = F.extract(_ev(active_call=true_value), 800, R.SenderState(), now=1000)
    assert f["active_call"] == 1


def test_native_types_are_unchanged():
    """Coercion must not disturb the offline path that was always correct."""
    for v, expected in ((True, 1), (False, 0), (1, 1), (0, 0), (None, 0)):
        f = F.extract(_ev(active_call=v), 800, R.SenderState(), now=1000)
        assert f["active_call"] == expected, f"{v!r}"


def test_a_missing_flag_is_absence_not_presence():
    """No session telemetry must read as "no call" - the USSD and ATM channels."""
    f = F.extract(_ev(), 800, R.SenderState(), now=1000)
    assert f["active_call"] == 0


def test_the_same_hazard_on_the_kinship_flag():
    """is_family_transfer travels the same way when myid_kinship is on."""
    f = F.extract(_ev(is_family_transfer="False"), 800, R.SenderState(), now=1000)
    assert f["is_family"] == 0


# --- the property, stated once ----------------------------------------------

def test_a_wire_shaped_event_extracts_like_a_typed_one():
    """An event whose every field is a string - what csv.DictReader and a lenient
    JSON producer deliver - must extract to the same vector as the typed one."""
    typed = _ev(amount_uzs=500_000, active_call=False,
                secs_login_to_confirm=41.2)
    wire = _ev(amount_uzs="500000", active_call="False",
               secs_login_to_confirm="41.2")
    a = F.extract(typed, 800, R.SenderState(), now=1000)
    b = F.extract(wire, 800, R.SenderState(), now=1000)
    assert F.to_vector(a) == F.to_vector(b)
