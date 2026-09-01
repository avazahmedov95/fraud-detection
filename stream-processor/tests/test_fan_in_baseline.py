"""
Tests for the population-relative MULE_FAN_IN threshold.

The behaviour under test is a threshold that adapts to the traffic it sees, so
the tests are about the ADAPTATION, not about a number: that it reproduces the
absolute rule on an unremarkable population, that it does not fire on an
estimate built from nothing, and that a denser population moves it.

Why this exists at all: running the deployed rules on IBM AMLSim showed
MULE_FAN_IN firing on 3.12% of legitimate traffic and 0.0% of the fan_in
typology, because the constant 6 encodes the in-degree density of this
project's own generator. See validation/README.md 3 and MULE_FAN_IN_MODE in
config.py.
"""

import os
import sys

import pytest

# sys.path is set by tests/conftest.py.

import config as C          # noqa: E402
from rules import PopulationBaseline   # noqa: E402


@pytest.fixture
def relative_mode():
    """MULE_FAN_IN_MODE is read from config at call time, so tests flip it and
    put it back rather than relying on the environment."""
    saved = C.MULE_FAN_IN_MODE
    C.MULE_FAN_IN_MODE = "relative"
    yield
    C.MULE_FAN_IN_MODE = saved


def _fill(pop, values):
    for v in values:
        pop.observe(v)
    return pop


def test_falls_back_until_enough_observations():
    """A quantile estimated from a handful of events is not a baseline. Firing
    on one would swap a wrong constant for a random one."""
    pop = _fill(PopulationBaseline(), [0] * 10)
    assert pop.n < C.MULE_FAN_IN_MIN_OBS
    assert pop.threshold(0.999, fallback=6) == 6


def test_a_quiet_population_still_needs_two_senders():
    """If almost every receiver sees zero or one sender, the raw quantile lands
    at 1 and the rule would fire on ordinary traffic. The floor is a statement
    about what 'concentration' means, not a tuning constant."""
    pop = _fill(PopulationBaseline(), [0, 1] * C.MULE_FAN_IN_MIN_OBS)
    assert pop.threshold(0.999, fallback=6) == 2


def test_a_denser_population_raises_the_threshold():
    """The point of the whole exercise: the same rule against busier traffic
    must not flag the busier traffic."""
    quiet = _fill(PopulationBaseline(), ([1] * 99 + [3]) * 200)
    dense = _fill(PopulationBaseline(), ([8] * 99 + [40]) * 200)
    assert dense.threshold(0.99, fallback=6) > quiet.threshold(0.99, fallback=6)


def test_threshold_is_monotone_in_the_quantile():
    pop = _fill(PopulationBaseline(), [i % 12 for i in range(C.MULE_FAN_IN_MIN_OBS * 2)])
    ts = [pop.threshold(q, fallback=6) for q in (0.50, 0.90, 0.99, 0.999)]
    assert ts == sorted(ts)


def test_values_above_the_last_bin_do_not_escape():
    """A receiver with more senders than the histogram has bins must still be
    counted, or the tail this rule exists to find would be invisible."""
    pop = PopulationBaseline()
    _fill(pop, [0] * C.MULE_FAN_IN_MIN_OBS + [10_000] * 10)
    assert pop.n == C.MULE_FAN_IN_MIN_OBS + 10
    assert sum(pop.counts) == pop.n


def test_cache_refreshes_and_does_not_freeze_the_threshold():
    """The threshold is cached for MULE_FAN_IN_REFRESH_EVERY observations. A
    cache that never refreshed would silently pin the rule to the warm-up
    period's traffic."""
    pop = _fill(PopulationBaseline(), [1] * (C.MULE_FAN_IN_MIN_OBS * 2))
    first = pop.threshold(0.999, fallback=6)
    _fill(pop, [30] * (C.MULE_FAN_IN_MIN_OBS * 2))
    assert pop.threshold(0.999, fallback=6) > first


def test_absolute_mode_ignores_the_baseline():
    """Default behaviour is unchanged: with MULE_FAN_IN_MODE left at
    'absolute', a wildly different population must not move the rule."""
    assert C.MULE_FAN_IN_MODE == "absolute"
    from rules import SenderState, ReceiverState, evaluate
    pop = _fill(PopulationBaseline(), [50] * (C.MULE_FAN_IN_MIN_OBS * 2))
    ev = {"amount_uzs": 100_000.0, "sender_pinfl": "A", "receiver_pinfl": "B"}
    res = evaluate(ev, None, SenderState(), 1_700_000_000.0,
                   ReceiverState(), population=pop)
    assert "MULE_FAN_IN" not in res["rule_hits"]


def test_baseline_is_observed_after_the_decision(relative_mode):
    """An event must not be part of the baseline it is judged against."""
    from rules import SenderState, ReceiverState, evaluate
    pop = PopulationBaseline()
    before = pop.n
    ev = {"amount_uzs": 100_000.0, "sender_pinfl": "A", "receiver_pinfl": "B"}
    evaluate(ev, None, SenderState(), 1_700_000_000.0, ReceiverState(),
             population=pop)
    assert pop.n == before + 1


def test_missing_baseline_falls_back_rather_than_failing(relative_mode):
    """Same posture as the Redis and Neo4j lookups: if the shared state is not
    there, degrade to known behaviour instead of stalling."""
    from rules import SenderState, ReceiverState, evaluate
    ev = {"amount_uzs": 100_000.0, "sender_pinfl": "A", "receiver_pinfl": "B"}
    res = evaluate(ev, None, SenderState(), 1_700_000_000.0, ReceiverState(),
                   population=None)
    assert res["decision"] in ("ALLOW", "REVIEW", "BLOCK")
