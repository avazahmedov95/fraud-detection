"""The feature contract must not depend on how the caller typed its input.

A Kafka record built from csv.DictReader carries every field as text, and
`active_call` = "False" once scored 1 on every live event. The coercion lives in
features.py (`truthy`), which every caller reaches the model through.
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
    f = F.extract(_ev(active_call=false_value), R.SenderState(), now=1000)
    assert f["active_call"] == 0, f"{false_value!r} read as an active call"


@pytest.mark.parametrize("true_value", ["True", "true", "1", "yes", "Y"])
def test_textual_true_is_true(true_value):
    f = F.extract(_ev(active_call=true_value), R.SenderState(), now=1000)
    assert f["active_call"] == 1


def test_native_types_are_unchanged():
    """Coercion must not disturb the offline path that was always correct."""
    for v, expected in ((True, 1), (False, 0), (1, 1), (0, 0), (None, 0)):
        f = F.extract(_ev(active_call=v), R.SenderState(), now=1000)
        assert f["active_call"] == expected, f"{v!r}"


def test_a_missing_flag_is_absence_not_presence():
    """No session telemetry must read as "no call" - the USSD and ATM channels."""
    f = F.extract(_ev(), R.SenderState(), now=1000)
    assert f["active_call"] == 0


def test_the_same_hazard_on_the_kinship_flag():
    """is_family_transfer travels the same way when myid_kinship is on."""
    f = F.extract(_ev(is_family_transfer="False"), R.SenderState(), now=1000)
    assert f["is_family"] == 0


# --- the property, stated once ----------------------------------------------

def test_a_wire_shaped_event_extracts_like_a_typed_one():
    """An event whose every field is a string - what csv.DictReader and a lenient
    JSON producer deliver - must extract to the same vector as the typed one."""
    typed = _ev(amount_uzs=500_000, active_call=False,
                secs_login_to_confirm=41.2)
    wire = _ev(amount_uzs="500000", active_call="False",
               secs_login_to_confirm="41.2")
    a = F.extract(typed, R.SenderState(), now=1000)
    b = F.extract(wire, R.SenderState(), now=1000)
    assert F.to_vector(a) == F.to_vector(b)


# --- an event the extractor cannot score ---------------------------------------

@pytest.mark.parametrize("bad", [
    {"amount_uzs": None}, {"amount_uzs": ""}, {"amount_uzs": "abc"},
    {"amount_uzs": [1, 2]}, {"amount_uzs": float("nan")},
    {"amount_uzs": float("inf")}, {"amount_uzs": -500_000},
    {"secs_login_to_confirm": "abc"}, {"secs_login_to_confirm": float("nan")},
    {"secs_login_to_confirm": -5},
])
def test_an_unscorable_event_is_named(bad):
    """Each of these raised in the extractor - a task failing on every replay of the
    record - or, for NaN and infinity, poisoned the sender's baseline for good."""
    assert F.unusable(_ev(**bad))


def test_a_missing_amount_is_unscorable():
    ev = _ev()
    del ev["amount_uzs"]
    assert F.unusable(ev)


@pytest.mark.parametrize("fine", [
    {}, {"amount_uzs": "500000"}, {"amount_uzs": 0}, {"secs_login_to_confirm": "41.2"},
])
def test_a_scorable_event_passes(fine):
    assert F.unusable(_ev(**fine)) is None


def test_the_job_checks_every_event_before_scoring_it():
    """fraud_job.py needs PyFlink, absent on the host, so this reads its SOURCE."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent.parent / "fraud_job.py"
           ).read_text(encoding="utf-8")
    assert "F.unusable(" in src, "the job no longer checks events before scoring"
    assert src.index("F.unusable(") < src.index("evaluate(event"), (
        "the check must run before the event reaches the extractor")
