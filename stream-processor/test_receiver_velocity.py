"""
Unit tests for the receiver-side (fan-in) signals.

Every other feature is computed from per-sender state. These are the only ones
that look at the payee's inbound history, which is what a mule's fan-in shape
actually is.

Run: python -m pytest test_receiver_velocity.py -q
"""

import pytest

import capabilities as CAP
import config as C
import features as F
from rules import SenderState, ReceiverState, evaluate
from conftest import bank_card, payee_card

needs_cap = pytest.mark.skipif(
    not CAP.enabled("receiver_velocity"),
    reason="receiver velocity is disabled by CAP_RECEIVER_VELOCITY=off")


def _ev(sender, amount=500_000, receiver="mule"):
    return {"amount_uzs": amount, "sender_pinfl": sender,
            "receiver_pinfl": receiver, "device_id": f"dev-{sender}",
            "sender_region": "Tashkent City",
            "sender_card": bank_card("BankA"),
            "receiver_card": payee_card(receiver)}


def _fan_in(n_senders, receiver_state, spacing_s=60, now0=1000):
    """Drive n distinct senders into one payee; return the last result."""
    res = None
    for i in range(n_senders):
        res = evaluate(_ev(f"s{i}"), 800, SenderState(), now0 + i * spacing_s,
                       receiver_state)
    return res


@needs_cap
def test_fan_in_from_many_senders_is_flagged():
    rs = ReceiverState()
    res = _fan_in(C.MULE_FAN_IN_MIN_SENDERS, rs)
    assert "MULE_FAN_IN" in res["rule_hits"]


@needs_cap
def test_fan_in_below_threshold_is_not_flagged():
    rs = ReceiverState()
    res = _fan_in(C.MULE_FAN_IN_MIN_SENDERS - 1, rs)
    assert "MULE_FAN_IN" not in res["rule_hits"]


@needs_cap
def test_repeat_transfers_from_one_sender_are_not_fan_in():
    """Ten transfers from one person is a habit; one each from ten people is a
    collection point. The rule counts distinct senders for that reason."""
    rs = ReceiverState()
    res = None
    for i in range(C.MULE_FAN_IN_MIN_SENDERS + 4):
        res = evaluate(_ev("same-sender"), 800, SenderState(), 1000 + i * 60, rs)
    assert "MULE_FAN_IN" not in res["rule_hits"]


@needs_cap
def test_fan_in_spread_over_days_is_not_flagged():
    """The window is what makes it a burst rather than a popular payee."""
    rs = ReceiverState()
    res = _fan_in(C.MULE_FAN_IN_MIN_SENDERS, rs,
                  spacing_s=C.RECEIVER_WINDOW_S)
    assert "MULE_FAN_IN" not in res["rule_hits"]


@needs_cap
def test_inbound_features_reach_the_vector():
    rs = ReceiverState()
    _fan_in(3, rs)
    f = F.extract(_ev("s99"), 800, SenderState(), now=1400, receiver_state=rs)
    assert f["rcv_distinct_senders_1h"] == 4      # three prior + this one
    assert f["rcv_inflow_1h"] > 0


def test_missing_receiver_state_fails_open():
    """If the shared store is unreachable the pipeline must keep scoring on the
    remaining signals, not stall or crash."""
    res = evaluate(_ev("s1"), 800, SenderState(), now=1000, receiver_state=None)
    assert "MULE_FAN_IN" not in res["rule_hits"]
    assert res["decision"] in ("ALLOW", "REVIEW", "BLOCK")


def test_receiver_state_is_pruned_to_the_window():
    """Unbounded growth would be a memory leak in the shared store."""
    rs = ReceiverState()
    _fan_in(5, rs, spacing_s=C.RECEIVER_WINDOW_S // 2)
    assert all(rs.inbound[-1][0] - ts <= C.RECEIVER_WINDOW_S
               for ts, _, _ in rs.inbound)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
