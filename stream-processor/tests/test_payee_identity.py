"""What the payee is: receiver-side state is keyed by CARD by default (the bank
sees a destination PAN); these pin that default, its symmetry and its limit."""

import pytest

import capabilities as CAP
import features as F
import rules as R
from conftest import payee_card


@pytest.fixture
def mode():
    original = CAP.MODES["payee_identity"]
    yield lambda m: CAP.MODES.__setitem__("payee_identity", m)
    CAP.MODES["payee_identity"] = original


def _ev(pinfl="P1", card=None, amount=100_000):
    return {"amount_uzs": amount, "receiver_pinfl": pinfl,
            "receiver_card": card or payee_card(pinfl),
            "sender_pinfl": "S1", "sender_card": payee_card("S1"),
            "device_id": "dev-1", "sender_region": "Tashkent City"}


# --- the default ------------------------------------------------------------

def test_default_is_the_card():
    """What a bank holds, not what it would like to hold."""
    assert CAP.BY_KEY["payee_identity"].default == "card"


def test_resolver_follows_the_mode(mode):
    ev = _ev(pinfl="P1")
    mode("card")
    assert F.payee_key(ev) == ev["receiver_card"]
    mode("pinfl")
    assert F.payee_key(ev) == "P1"


def test_no_per_transfer_mode():
    """Resolving to PINFL only where the bank can was measured and rejected: it
    makes the key depend on the SENDER's bank, splitting one payee's inbound
    window in two and losing 17.4% of MULE_FAN_IN's true positives."""
    assert set(CAP.BY_KEY["payee_identity"].modes) == {"card", "pinfl"}


# --- symmetry: the read and the write must resolve identically ---------------

def test_state_write_and_read_use_the_same_key(mode):
    """When the write and read keys disagree, is_new_payee reads 1 forever."""
    for m in ("card", "pinfl"):
        mode(m)
        st = R.SenderState()
        ev = _ev(pinfl="P1")
        assert F.extract(ev, st, now=1000)["is_new_payee"] == 1
        F.update_state(st, ev, now=1000)
        assert F.extract(ev, st, now=2000)["is_new_payee"] == 0


def test_distinct_payees_stay_distinct(mode):
    mode("card")
    st = R.SenderState()
    F.update_state(st, _ev(pinfl="P1"), now=1000)
    assert F.extract(_ev(pinfl="P2"), st, now=2000)["is_new_payee"] == 1


# --- the limitation, asserted ------------------------------------------------

def test_one_person_two_cards_is_two_payees_by_card(mode):
    """The measured cost of the honest default: a person holding two cards is ONE
    payee to the switch and TWO to a bank keying on the PAN, so a mule spreading
    inbound transfers across their own cards is split across that many fan-in
    buckets and MULE_FAN_IN sees a fraction of the true convergence."""
    one, two = _ev(pinfl="P1"), _ev(pinfl="P1", card=payee_card("P1-second"))

    mode("card")
    st = R.SenderState()
    F.update_state(st, one, now=1000)
    assert F.extract(two, st, now=2000)["is_new_payee"] == 1, \
        "by card, the same person's second card is a new payee"

    mode("pinfl")
    st = R.SenderState()
    F.update_state(st, one, now=1000)
    assert F.extract(two, st, now=2000)["is_new_payee"] == 0, \
        "by pinfl, both cards are the same payee"


def test_card_mode_never_reads_the_pinfl(mode):
    """Why receiver_pinfl may still travel: it makes the `pinfl` mode runnable at
    all, the identity being available at switch or platform level. Under the
    default profile nothing reads it - the field REMOVED must score identically."""
    mode("card")
    with_pinfl = _ev(pinfl="P1")
    without = {k: v for k, v in with_pinfl.items() if k != "receiver_pinfl"}

    a = R.evaluate(with_pinfl, R.SenderState(), now=1000)
    b = R.evaluate(without, R.SenderState(), now=1000)
    assert a["features"] == b["features"]
    assert a["cep_score"] == b["cep_score"]
    assert a["rule_hits"] == b["rule_hits"]


# --- the contract ------------------------------------------------------------

def test_switching_mode_does_not_change_the_feature_vector(mode):
    """The capability contributes no columns, so a model trained under one mode
    still LOADS under the other; retraining is for correctness, not to avoid a crash."""
    mode("card")
    card_names = CAP.feature_names()
    mode("pinfl")
    assert CAP.feature_names() == card_names


# --- a source that carries neither identity ---------------------------------

def test_a_missing_card_collapses_every_payee_into_one(mode, caplog):
    """An empty card key puts every payee into ONE ReceiverState and fabricates the
    fan-in MULE_FAN_IN looks for - found on PaySim, which issues no PANs."""
    mode("card")
    F._warned.clear()
    a = {"amount_uzs": 100_000, "receiver_pinfl": "P1", "sender_pinfl": "S1"}
    b = {"amount_uzs": 100_000, "receiver_pinfl": "P2", "sender_pinfl": "S2"}

    import logging
    with caplog.at_level(logging.WARNING, logger="features"):
        ka, kb = F.payee_key(a), F.payee_key(b)

    assert ka == kb == "", "two different payees must be seen to collapse"
    assert any("payee key is empty" in r.message or "EVERY" in r.message
               for r in caplog.records), \
        "collapsing every payee into one key must not be silent"


def test_pinfl_mode_is_the_answer_for_such_a_source(mode):
    """PaySim's accounts ARE the payee identity, so the mode exists for it."""
    mode("pinfl")
    a = {"amount_uzs": 100_000, "receiver_pinfl": "P1", "sender_pinfl": "S1"}
    b = {"amount_uzs": 100_000, "receiver_pinfl": "P2", "sender_pinfl": "S2"}
    assert F.payee_key(a) == "P1"
    assert F.payee_key(b) == "P2"
    assert F.payee_key(a) != F.payee_key(b)
