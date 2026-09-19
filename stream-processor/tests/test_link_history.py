"""link_history: four days of who paid whom, read for the sender and the payee."""

import random
from collections import defaultdict, deque

import pytest

import capabilities as CAP
import features as F
from conftest import payee_card
from rules import ReceiverState, SenderState

HOUR = 3600.0


@pytest.fixture
def links():
    original = CAP.MODES["link_history"]
    CAP.MODES["link_history"] = "on"
    yield
    CAP.MODES["link_history"] = original


def _ev(sender, receiver, amount=500_000):
    return {"amount_uzs": amount, "sender_pinfl": sender, "receiver_pinfl": receiver,
            "sender_card": payee_card(sender), "receiver_card": payee_card(receiver),
            "device_id": "d", "sender_region": "Tashkent City"}


class World:
    """The stores the live job keeps, in process: inbound under the payee, outbound
    under the sender, both by the keys fraud_job reads them with."""

    def __init__(self):
        self.inbound = defaultdict(ReceiverState)
        self.outbound = defaultdict(ReceiverState)

    def read(self, ev, now):
        payer, payee = F.payer_key(ev), F.payee_key(ev)
        return F.extract(ev, None, SenderState(), now, self.inbound[payee],
                         self.inbound[payer], self.outbound[payer], self.outbound[payee])

    def send(self, ev, now):
        f = self.read(ev, now)
        F.update_receiver_state(self.inbound[F.payee_key(ev)], ev, now)
        F.update_outbound_state(self.outbound[F.payer_key(ev)], ev, now)
        return f


def test_on_by_default_and_last_in_the_vector():
    assert CAP.MODES["link_history"] == "on"
    assert F.FEATURE_NAMES[-5:] == ["payee_payers_96h", "sender_payers_96h",
                                    "sender_payees_96h", "payee_payees_96h",
                                    "money_back_96h"]


def test_a_mule_collecting_for_days_then_paying_on(links):
    w, t0 = World(), 1_000_000.0
    for k in range(5):
        w.send(_ev(f"p{k}", "mule"), t0 + 12 * HOUR * k)
    f = w.send(_ev("mule", "cashout"), t0 + 60 * HOUR)
    assert f["sender_payers_96h"] == 5
    assert f["sender_payees_96h"] == 0
    assert f["payee_payers_96h"] == 0
    g = w.send(_ev("mule", "cashout2"), t0 + 61 * HOUR)
    assert g["sender_payees_96h"] == 1


def test_money_going_straight_back(links):
    w, t0 = World(), 1_000_000.0
    w.send(_ev("a", "b"), t0)
    f = w.send(_ev("b", "a"), t0 + HOUR)
    assert f["money_back_96h"] == 1
    assert f["payee_payees_96h"] == 1          # a paid b
    assert f["payee_payers_96h"] == 0          # nobody paid a


def test_four_days_and_not_five(links):
    w, t0 = World(), 1_000_000.0
    w.send(_ev("p", "r"), t0)
    assert w.read(_ev("q", "r"), t0 + 95 * HOUR)["payee_payers_96h"] == 1
    assert w.read(_ev("q", "r"), t0 + 97 * HOUR)["payee_payers_96h"] == 0


def test_strictly_before_the_transfer(links):
    w, t0 = World(), 1_000_000.0
    w.send(_ev("p", "r"), t0)
    assert w.send(_ev("q", "r"), t0)["payee_payers_96h"] == 0
    assert w.read(_ev("s", "r"), t0 + 1)["payee_payers_96h"] == 2


def test_running_counts_equal_a_scan(links):
    rng = random.Random(7)
    w, now = World(), 1_000_000.0
    for _ in range(2000):
        now += rng.choice([0.0, 60.0, 3 * HOUR, 20 * HOUR])
        ev = _ev(f"a{rng.randrange(12)}", f"a{rng.randrange(12)}")
        f = w.send(ev, now)
        for store in (w.inbound, w.outbound):
            for key, state in store.items():
                scanned = ReceiverState(inbound=deque(state.inbound))   # n=0: scan path
                assert F._recent(state, now, key) == F._recent(scanned, now, key)
        assert 0 <= f["money_back_96h"] <= 20


def test_an_unreachable_store_reads_as_zero(links):
    f = F.extract(_ev("a", "b"), None, SenderState(), 1_000_000.0, None, None, None, None)
    assert (f["payee_payers_96h"], f["sender_payers_96h"], f["sender_payees_96h"],
            f["payee_payees_96h"], f["money_back_96h"]) == (0, 0, 0, 0, 0)


def test_switched_off_the_store_keeps_an_hour():
    original = CAP.MODES["link_history"]
    CAP.MODES["link_history"] = "off"
    try:
        _keeps_an_hour()
    finally:
        CAP.MODES["link_history"] = original


def _keeps_an_hour():
    rcv = ReceiverState()
    F.update_receiver_state(rcv, _ev("p0", "r"), 0.0)
    F.update_receiver_state(rcv, _ev("p1", "r"), 2 * HOUR)
    assert len(rcv.inbound) == 1
