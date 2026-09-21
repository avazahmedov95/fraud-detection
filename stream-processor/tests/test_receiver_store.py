"""Unit tests for the Redis-backed receiver store, which lives outside Flink's
checkpoint: replays record events twice, and these pin why that is acceptable."""

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

    def zrangebyscore(self, key, lo, hi, withscores=False):
        rows = [(m, s) for m, s in sorted(self.sets.get(key, {}).items(),
                                          key=lambda kv: kv[1]) if lo <= s <= hi]
        return rows if withscores else [m for m, _ in rows]

    def zrevrange(self, key, start, stop, withscores=False):
        rows = sorted(self.sets.get(key, {}).items(), key=lambda kv: -kv[1])
        rows = rows[start:stop + 1] if stop >= 0 else rows[start:]
        return rows if withscores else [m for m, _ in rows]

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
    # Carries BOTH identities: the store keys on whichever payee_identity selects
    # (features.payee_key), so naming one would silently exercise a single mode.
    return {"transaction_id": txid, "sender_pinfl": sender,
            "receiver_pinfl": receiver, "receiver_card": payee_card(receiver),
            "amount_uzs": amount}


def _payee(receiver="R1"):
    """The identity the store keys this payee by under the active mode. load() takes
    an ALREADY-RESOLVED key, so writing "R1" here is the read/write key mismatch
    the store exists to prevent, and it silently reads an empty window."""
    return F.payee_key(_ev("probe", receiver=receiver))


def test_replay_of_the_same_event_is_idempotent(store):
    """A replayed event must not inflate the payee's inflow."""
    store.record(_ev("tx-1"), now=1000)
    store.record(_ev("tx-1"), now=1000)          # replayed after restart
    state = store.load(_payee(), now=1000)
    assert len(state.inbound) == 1


def test_distinct_transfers_sharing_time_sender_amount_are_both_kept(store):
    """The bug this key format fixes: identical amounts from one sender in the same
    second is a mule run, and the previous time|sender|amount key lost the second."""
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
    """Collapsing 'not computed' with an empty window makes a broken store quiet."""
    assert store.load("never-paid", now=1000).inbound == \
        type(store.load("never-paid", now=1000).inbound)()
    s = ReceiverStore("h", 1)
    assert s.load("never-paid", now=1000) is None


# --- the counterparty counters (counterparty_history) -----------------------

@pytest.fixture
def counters_on():
    import capabilities as CAP
    original = dict(CAP.MODES)
    CAP.MODES["counterparty_history"] = "on"
    yield
    CAP.MODES.clear(); CAP.MODES.update(original)


def test_one_member_per_payer_not_per_transfer(store, counters_on):
    """The key grows with counterparties, which is what makes a week affordable."""
    for i in range(4):
        store.record(_ev(f"t{i}", sender="S1"), 1000 + i)
    key = f"cp:in:{F.payee_key(_ev('t0'))}"
    assert list(store._redis.sets[key]) == ["S1"]
    assert store._redis.sets[key]["S1"] == 1003, "the score is when they last paid"


def test_load_reads_back_the_payers_and_the_last_inbound(store, counters_on):
    store.record(_ev("t1", sender="S1"), 1000)
    store.record(_ev("t2", sender="S2"), 1500)
    state = store.load(F.payee_key(_ev("t1")), 2000)
    assert state.payers == {"S1": 1000.0, "S2": 1500.0}
    assert state.last_inbound_ts == 1500.0


def test_last_inbound_is_the_newest_payment_to_that_account(store, counters_on):
    store.record(_ev("t1", sender="S1", receiver="MULE"), 1000)
    store.record(_ev("t2", sender="S2", receiver="MULE"), 1400)
    account = F.payee_key(_ev("t1", receiver="MULE"))
    assert store.last_inbound(account, 1500) == 1400.0
    assert store.last_inbound("never-paid", 1500) is None


def test_nothing_is_written_while_the_capability_is_off(store):
    """Off is off: a capability nobody switched on costs no writes and no reads."""
    import capabilities as CAP
    original = dict(CAP.MODES)
    CAP.MODES["counterparty_history"] = "off"
    try:
        store.record(_ev("t1"), 1000)
        assert not [k for k in store._redis.sets if k.startswith("cp:in:")]
        assert store.last_inbound(F.payee_key(_ev("t1")), 1000) is None
    finally:
        CAP.MODES.clear(); CAP.MODES.update(original)


def test_the_store_path_and_the_in_process_replay_agree(store, counters_on):
    """Parity, the condition the gate in ml/README.md names first: the same events
    through Redis and through plain objects give the same columns. The extractor is
    shared, so this pins what the store has to reproduce - including the counters."""
    import random
    from collections import defaultdict
    from rules import ReceiverState, SenderState

    senders_a, receivers_a = defaultdict(SenderState), defaultdict(ReceiverState)
    senders_b = defaultdict(SenderState)
    rng, rows_a, rows_b = random.Random(7), [], []
    for i in range(120):
        ev = _ev(f"t{i}", sender=f"S{rng.randrange(6)}", receiver=f"R{rng.randrange(4)}")
        ts = 1_000_000.0 + i * 600
        pk, sk = F.payee_key(ev), F.sender_key(ev)

        paid = receivers_a.get(sk)                    # .get: never invent a state
        rows_a.append(F.to_vector(F.extract(
            ev, senders_a[ev["sender_pinfl"]], ts, receivers_a[pk],
            sender_inbound_ts=(paid.last_inbound_ts if paid else None))))
        F.update_state(senders_a[ev["sender_pinfl"]], ev, ts)
        F.update_receiver_state(receivers_a[pk], ev, ts)

        rows_b.append(F.to_vector(F.extract(
            ev, senders_b[ev["sender_pinfl"]], ts, store.load(pk, ts),
            sender_inbound_ts=store.last_inbound(sk, ts))))
        F.update_state(senders_b[ev["sender_pinfl"]], ev, ts)
        store.record(ev, ts)

    assert rows_a == rows_b
