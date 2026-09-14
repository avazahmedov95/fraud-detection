"""money_chains: the receiver-keyed store read a day back, and for the sender."""

import math

import pytest

import capabilities as CAP
import features as F
from conftest import payee_card
from rules import ReceiverState, SenderState


@pytest.fixture
def chains():
    original = CAP.MODES["money_chains"]
    CAP.MODES["money_chains"] = "on"
    yield
    CAP.MODES["money_chains"] = original


def _ev(sender, receiver, amount=500_000):
    return {"amount_uzs": amount, "sender_pinfl": sender, "receiver_pinfl": receiver,
            "sender_card": payee_card(sender), "receiver_card": payee_card(receiver),
            "device_id": "d", "sender_region": "Tashkent City"}


def test_off_by_default_so_no_trained_model_changes():
    assert CAP.MODES["money_chains"] == "off"
    assert "sender_inflow_24h" not in F.FEATURE_NAMES


def test_a_mule_paying_out_reads_what_reached_it(chains):
    mule_in = ReceiverState()
    t0 = 1_000_000.0
    for k in range(5):
        F.update_receiver_state(mule_in, _ev(f"p{k}", "mule", 400_000), t0 + 3600 * k)
    f = F.extract(_ev("mule", "cashout", 1_900_000), None, SenderState(),
                  t0 + 6 * 3600, ReceiverState(), mule_in)
    assert f["sender_distinct_payers_24h"] == 5
    assert f["sender_inflow_24h"] == pytest.approx(math.log1p(2_000_000))


def test_the_day_window_keeps_what_the_hour_window_drops(chains):
    rcv = ReceiverState()
    t0 = 1_000_000.0
    for k in range(4):
        F.update_receiver_state(rcv, _ev(f"p{k}", "r"), t0 + 3 * 3600 * k)
    f = F.extract(_ev("p9", "r"), None, SenderState(), t0 + 10.5 * 3600, rcv)
    assert f["rcv_distinct_senders_24h"] == 5        # the four earlier, and this one
    assert f["rcv_distinct_senders_1h"] == 1         # this one alone


def test_switched_off_the_store_keeps_an_hour():
    rcv = ReceiverState()
    F.update_receiver_state(rcv, _ev("p0", "r"), 0.0)
    F.update_receiver_state(rcv, _ev("p1", "r"), 2 * 3600.0)
    assert len(rcv.inbound) == 1


def test_the_payer_key_is_the_key_the_sender_was_paid_under():
    ev = _ev("mule", "x")
    assert F.payer_key(ev) == F.payee_key(_ev("someone", "mule"))


def test_the_running_totals_agree_with_a_scan_of_the_window(chains):
    import random
    rng = random.Random(4)
    rcv, t = ReceiverState(), 0.0
    for k in range(400):
        t += rng.uniform(0, 900)
        ev = _ev(f"p{rng.randrange(30)}", "hub", rng.randrange(1, 10) * 100_000)
        want_day = [e for e in rcv.inbound if t - e[0] <= 86400]
        f = F.extract(ev, None, SenderState(), t, rcv, rcv)
        assert f["rcv_distinct_senders_24h"] == len({e[1] for e in want_day} | {ev["sender_pinfl"]})
        assert f["sender_inflow_24h"] == pytest.approx(math.log1p(sum(e[2] for e in want_day)))
        F.update_receiver_state(rcv, ev, t)


def test_a_state_read_from_the_store_is_scanned_not_trusted(chains):
    from collections import deque
    read = ReceiverState(inbound=deque([(0.0, "p1", 100.0), (10.0, "p2", 50.0)]))
    f = F.extract(_ev("x", "y"), None, SenderState(), 20.0, ReceiverState(), read)
    assert f["sender_distinct_payers_24h"] == 2
