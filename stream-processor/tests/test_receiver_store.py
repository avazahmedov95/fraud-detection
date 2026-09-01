"""
Unit tests for the Redis-backed receiver store.

The store lives OUTSIDE Flink's checkpoint, so it does not roll back when the
job restores. Under AT_LEAST_ONCE delivery that means every event between the
last checkpoint and a failure is recorded twice. These tests pin the two
properties that makes acceptable.

Run: python -m pytest test_receiver_store.py -q
"""

import pytest

import config as C
from receiver_store import ReceiverStore
import features as F
from conftest import payee_card


class FakeRedis:
    """Enough of a Redis sorted set to exercise the store's semantics."""

    def __init__(self):
        self.sets = {}
        self.expiries = {}

    def ping(self):
        return True

    def pipeline(self):
        return FakePipeline(self)

    def zrangebyscore(self, key, lo, hi):
        return [m for m, s in sorted(self.sets.get(key, {}).items(),
                                     key=lambda kv: kv[1]) if lo <= s <= hi]

    def close(self):
        pass


class FakePipeline:
    def __init__(self, redis):
        self._r, self._ops = redis, []

    def zadd(self, key, mapping):
        self._ops.append(("zadd", key, mapping)); return self

    def zremrangebyscore(self, key, lo, hi):
        self._ops.append(("zrem", key, lo, hi)); return self

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl)); return self

    def execute(self):
        for op in self._ops:
            if op[0] == "zadd":
                self._r.sets.setdefault(op[1], {}).update(op[2])
            elif op[0] == "zrem":
                s = self._r.sets.get(op[1], {})
                lo = float("-inf") if op[2] == "-inf" else op[2]
                for m in [m for m, sc in s.items() if lo <= sc <= op[3]]:
                    del s[m]
            elif op[0] == "expire":
                self._r.expiries[op[1]] = op[2]
        self._ops = []


@pytest.fixture
def store():
    s = ReceiverStore("h", 1)
    s._redis = FakeRedis()
    return s


def _ev(txid, sender="S1", amount=100_000.0, receiver="R1"):
    # Carries BOTH identities. The store keys on whichever the payee_identity
    # capability selects (features.payee_key), so an event that named only one
    # of them would silently exercise a single mode.
    return {"transaction_id": txid, "sender_pinfl": sender,
            "receiver_pinfl": receiver, "receiver_card": payee_card(receiver),
            "amount_uzs": amount}


def _payee(receiver="R1"):
    """The identity the store keys this payee by under the active mode.

    load() takes an ALREADY-RESOLVED key - the job calls it with
    features.payee_key(event) - so the test has to resolve it the same way.
    Writing "R1" here instead is precisely the read/write key mismatch the
    store's design exists to prevent, and it silently reads an empty window.
    """
    return F.payee_key(_ev("probe", receiver=receiver))


def test_replay_of_the_same_event_is_idempotent(store):
    """The at-least-once case: the store is not checkpointed, so replayed events
    call record() again. A repeat must not inflate the payee's inflow."""
    store.record(_ev("tx-1"), now=1000)
    store.record(_ev("tx-1"), now=1000)          # replayed after restart
    state = store.load(_payee(), now=1000)
    assert len(state.inbound) == 1


def test_distinct_transfers_sharing_time_sender_amount_are_both_kept(store):
    """The bug this key format fixes. Identical amounts from one sender in the
    same second is what a structuring or mule run looks like; under the previous
    time|sender|amount key the second transfer vanished."""
    store.record(_ev("tx-1"), now=1000)
    store.record(_ev("tx-2"), now=1000)          # different transaction
    state = store.load(_payee(), now=1000)
    assert len(state.inbound) == 2
    assert sum(a for _, _, a in state.inbound) == 200_000.0


def test_distinct_senders_are_counted_separately(store):
    for i in range(4):
        store.record(_ev(f"tx-{i}", sender=f"S{i}"), now=1000)
    state = store.load(_payee(), now=1000)
    assert len({s for _, s, _ in state.inbound}) == 4


def test_entries_outside_the_window_are_dropped(store):
    store.record(_ev("tx-old"), now=1000)
    store.record(_ev("tx-new"), now=1000 + C.RECEIVER_WINDOW_S + 10)
    state = store.load(_payee(), now=1000 + C.RECEIVER_WINDOW_S + 10)
    assert len(state.inbound) == 1


def test_a_ttl_is_always_set(store):
    """Without it an idle payee's key would sit in memory for ever."""
    store.record(_ev("tx-1"), now=1000)
    assert store._redis.expiries[f"rcv:{_payee()}"] >= C.RECEIVER_WINDOW_S


def test_unavailable_redis_fails_open_rather_than_raising():
    s = ReceiverStore("h", 1)          # never opened, _redis is None
    assert s.load("R1", now=1000) is None
    s.record(_ev("tx-1"), now=1000)    # must not raise


def test_load_distinguishes_unavailable_from_empty(store):
    """None means 'not computed' and is failed open; an empty window is a real
    observation about a payee nobody has paid. Collapsing them would let a
    broken store read as a quiet payee."""
    assert store.load("never-paid", now=1000).inbound == \
        type(store.load("never-paid", now=1000).inbound)()
    s = ReceiverStore("h", 1)
    assert s.load("never-paid", now=1000) is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
