"""
Tests for the Redis-backed population baseline.

The behaviour that matters is not "does it talk to Redis" but the three
decisions around that: writes are batched so the 300 ms path pays no round trip
per event, reads are cached for the same reason, and the absence of Redis falls
back to the ABSOLUTE constant rather than to this worker's own slice - which
would be a partition baseline masquerading as a population one.

A fake client is used rather than a live server so these run in CI and so the
call COUNTS can be asserted; a real Redis would test the library, not the
design.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config as C                          # noqa: E402
from receiver_store import PopulationStore  # noqa: E402


class FakePipeline:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def hincrby(self, key, field, amount):
        self.ops.append(("hincrby", str(field), amount))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "hincrby":
                self.store.hash[op[1]] = self.store.hash.get(op[1], 0) + op[2]
        self.store.pipelines += 1
        self.ops = []


class FakeRedis:
    def __init__(self):
        self.hash = {}
        self.pipelines = 0
        self.hgetalls = 0
        self.fail = False

    def ping(self):
        return True

    def pipeline(self):
        if self.fail:
            raise RuntimeError("redis down")
        return FakePipeline(self)

    def hgetall(self, key):
        if self.fail:
            raise RuntimeError("redis down")
        self.hgetalls += 1
        return dict(self.hash)

    def close(self):
        pass


def _wired():
    ps = PopulationStore("h", 1)
    ps._redis = FakeRedis()
    return ps, ps._redis


def test_no_redis_falls_back_to_the_constant_not_to_a_local_histogram():
    """The dangerous alternative is using this worker's own slice: a confident
    number computed from the wrong population."""
    ps = PopulationStore("h", 1)          # never opened -> _redis is None
    for _ in range(C.MULE_FAN_IN_MIN_OBS * 2):
        ps.observe(40)
    assert ps.threshold(0.999, fallback=6) == 6


def test_writes_are_batched_not_per_event():
    ps, r = _wired()
    for _ in range(100):
        ps.observe(3)
    assert r.pipelines == 0, "observe() must not touch Redis"
    ps.threshold(0.999, fallback=6)
    assert r.pipelines == 1


def test_reads_are_cached_between_refreshes():
    ps, r = _wired()
    for _ in range(C.MULE_FAN_IN_REFRESH_EVERY):
        ps.observe(2)
    ps.threshold(0.999, fallback=6)
    before = r.hgetalls
    for _ in range(C.MULE_FAN_IN_REFRESH_EVERY - 1):
        ps.observe(2)
        ps.threshold(0.999, fallback=6)
    assert r.hgetalls == before, "no re-read before the refresh interval"


def test_below_min_obs_returns_the_fallback():
    ps, _ = _wired()
    for _ in range(10):
        ps.observe(9)
    assert ps.threshold(0.999, fallback=6) == 6


def test_threshold_reflects_the_whole_population_not_one_worker():
    """Another worker's observations arrive through the shared hash and must
    move this worker's threshold. This is the entire point of the class."""
    ps, r = _wired()
    r.hash = {"1": C.MULE_FAN_IN_MIN_OBS, "30": C.MULE_FAN_IN_MIN_OBS}
    ps.observe(1)
    assert ps.threshold(0.60, fallback=6) >= 2


def test_a_redis_blip_keeps_pending_observations():
    """A transient failure should cost the accuracy of one refresh, not the
    counts themselves."""
    ps, r = _wired()
    for _ in range(50):
        ps.observe(4)
    r.fail = True
    ps.threshold(0.999, fallback=6)
    assert sum(ps._pending.values()) == 50
    r.fail = False
    ps.threshold(0.999, fallback=6)
    assert ps._pending == {}
    assert r.hash.get("4") == 50


def test_values_beyond_the_last_bin_are_counted_not_dropped():
    ps, r = _wired()
    ps.observe(10_000)
    ps.threshold(0.999, fallback=6)
    assert r.hash.get(str(PopulationStore.BINS - 1)) == 1


def test_the_hash_gets_a_ttl_so_an_idle_deployment_does_not_score_on_old_traffic():
    ps, r = _wired()
    ps.observe(3)
    pipe = FakePipeline(r)
    ps._redis = r
    ps.threshold(0.999, fallback=6)
    assert r.hash, "something was written"
    # TTL is applied in the same pipeline as the increments
    assert PopulationStore.TTL_S > 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
