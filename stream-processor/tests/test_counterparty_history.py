"""The counterparty counters: distinct payers and payees over a day and a week, and
the interval since the sender's own account was last paid. The windows are what the
CBU's internal-control rules count over (docs/related-work.md 6e), shortened to what
a 30-day dataset can carry."""

import pytest

import capabilities as CAP
import config as C
import features as F
from rules import ReceiverState, SenderState
from conftest import payee_card


@pytest.fixture
def on():
    """The capability is off by default; these tests are about what it adds."""
    original = dict(CAP.MODES)
    CAP.MODES["counterparty_history"] = "on"
    yield
    CAP.MODES.clear()
    CAP.MODES.update(original)


def _ev(payee="rcv", sender="S1", amount=150_000):
    return {"amount_uzs": amount, "sender_pinfl": sender,
            "receiver_pinfl": payee, "sender_region": "Tashkent City",
            "sender_card": payee_card(sender), "receiver_card": payee_card(payee)}


def _f(event, state=None, now=1_000_000.0, receiver_state=None, sender_inbound_ts=None):
    return F.extract(event, state or SenderState(), now, receiver_state,
                     sender_inbound_ts=sender_inbound_ts)


# --- the vector ------------------------------------------------------------

def test_columns_are_absent_until_the_capability_is_on(on):
    names = CAP.feature_names()
    assert names[-5:] == ["payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
                          "sender_payees_7d", "secs_since_sender_inbound"], \
        "the capability must append to the vector, not shift trained columns"
    CAP.MODES["counterparty_history"] = "off"
    assert not [n for n in CAP.feature_names() if n.startswith(("payee_payers",
                                                                "sender_payees",
                                                                "secs_since_sender"))]


# --- the payee's payers ----------------------------------------------------

def test_payers_are_counted_over_both_windows(on):
    now = 1_000_000.0
    rs = ReceiverState()
    rs.payers = {"a": now - 3600, "b": now - 2 * C.LINK_DAY_S, "c": now - 8 * 86400}
    f = _f(_ev(sender="S9"), receiver_state=rs, now=now)
    assert f["payee_payers_24h"] == 2, "a, plus this sender"
    assert f["payee_payers_7d"] == 3, "a and b, plus this sender; c is eight days old"


def test_the_same_payer_twice_is_one_counterparty(on):
    now = 1_000_000.0
    rs = ReceiverState()
    for _ in range(5):
        F.update_receiver_state(rs, _ev(sender="S1"), now - 60)
    f = _f(_ev(sender="S1"), receiver_state=rs, now=now)
    assert f["payee_payers_24h"] == 1, "five transfers from one person are one payer"


def test_a_store_that_is_down_reads_as_nothing_seen(on):
    """Fail open exactly like the fan-in features: a missing store is not a count."""
    f = _f(_ev(), receiver_state=None)
    assert f["payee_payers_24h"] == 0 and f["payee_payers_7d"] == 0


# --- the sender's payees ---------------------------------------------------

def test_sender_payees_count_distinct_payees_in_each_window(on):
    now = 1_000_000.0
    st = SenderState()
    F.update_state(st, _ev(payee="p1"), now - 3600)
    F.update_state(st, _ev(payee="p2"), now - 3 * C.LINK_DAY_S)
    F.update_state(st, _ev(payee="p3"), now - 9 * 86400)
    f = _f(_ev(payee="p4"), state=st, now=now)
    assert f["sender_payees_24h"] == 2, "p1, plus the payee of this transfer"
    assert f["sender_payees_7d"] == 3, "p1 and p2, plus this one; p3 is nine days old"


def test_sender_payees_need_no_store(on):
    """Sender-keyed state is Flink's own, so these two columns survive a Redis outage."""
    st = SenderState()
    F.update_state(st, _ev(payee="p1"), 1_000_000.0 - 60)
    f = _f(_ev(payee="p2"), state=st, receiver_state=None, now=1_000_000.0)
    assert f["sender_payees_24h"] == 2


# --- money in, money straight out ------------------------------------------

def test_transit_interval_is_seconds_since_the_sender_was_paid(on):
    now = 1_000_000.0
    f = _f(_ev(), now=now, sender_inbound_ts=now - 45)
    assert f["secs_since_sender_inbound"] == pytest.approx(45.0)


def test_never_paid_and_no_store_read_the_same_capped_value(on):
    f = _f(_ev(), sender_inbound_ts=None)
    assert f["secs_since_sender_inbound"] == float(C.LINK_WEEK_S)
    old = _f(_ev(), now=1_000_000.0, sender_inbound_ts=1_000_000.0 - 30 * 86400)
    assert old["secs_since_sender_inbound"] == float(C.LINK_WEEK_S), "capped, never NaN"


def test_update_receiver_state_records_when_the_account_was_paid(on):
    """What the store reads back as last_inbound for that account."""
    rs = ReceiverState()
    F.update_receiver_state(rs, _ev(), 1_234.0)
    assert rs.last_inbound_ts == 1_234.0


# --- pruning is memory, not meaning ----------------------------------------

def test_pruning_cannot_change_a_count(on):
    """The window filter decides; pruning only bounds the map."""
    now = 1_000_000.0
    rs = ReceiverState()
    for i in range(C.LINK_PRUNE_AT + 10):
        F.update_receiver_state(rs, _ev(sender=f"S{i}"), now - 60)
    assert len(rs.payers) == C.LINK_PRUNE_AT + 10, "nothing here is older than a week"
    f = _f(_ev(sender="S0"), receiver_state=rs, now=now)
    assert f["payee_payers_24h"] == C.LINK_PRUNE_AT + 10
