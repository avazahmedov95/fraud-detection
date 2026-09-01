"""
Unit tests for travel simulation.

The point of these is the negative control: legitimate journeys must never be
constructible as physically impossible, or the IMPOSSIBLE_TRAVEL rule would be
validated against data that guarantees its own success.

Run: python -m pytest test_travel.py -q
"""

import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pytest

import travel as T

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "stream-processor"))
import config as SC          # noqa: E402  — the detector's thresholds
import geo as G              # noqa: E402


class _P:
    def __init__(self, pinfl="p1", region="Tashkent City"):
        self.pinfl, self.region = pinfl, region


def test_travel_speed_is_below_the_detector_ceiling():
    """Ground travel must never approach what the rule calls impossible."""
    assert T.TRAVEL_SPEED_KMH < SC.MAX_PLAUSIBLE_KMH


def test_journey_duration_matches_the_distance():
    hours = T.travel_hours("Tashkent City", "Karakalpakstan")
    dist = G.region_distance_km("Tashkent City", "Karakalpakstan")
    assert abs(hours - dist / T.TRAVEL_SPEED_KMH) < 1e-9


def test_unknown_region_has_no_journey():
    assert T.travel_hours("Tashkent City", "Narnia") == 0.0


def test_person_is_at_home_outside_any_trip():
    p = _P()
    ts = datetime(2026, 1, 1, 12)
    region, transit = T.locate(p, {}, ts)
    assert region == p.region and transit is False


def test_person_is_at_the_destination_during_the_stay():
    p = _P()
    depart = datetime(2026, 1, 1, 8)
    arrive = depart + timedelta(hours=T.travel_hours(p.region, "Samarkand"))
    back = arrive + timedelta(hours=24)
    trips = {p.pinfl: [(depart, arrive, "Samarkand", back,
                        back + timedelta(hours=4))]}
    region, transit = T.locate(p, trips, arrive + timedelta(hours=2))
    assert region == "Samarkand" and transit is False


def test_mid_journey_is_reported_as_transit():
    p = _P()
    depart = datetime(2026, 1, 1, 8)
    arrive = depart + timedelta(hours=T.travel_hours(p.region, "Samarkand"))
    back = arrive + timedelta(hours=24)
    trips = {p.pinfl: [(depart, arrive, "Samarkand", back,
                        back + timedelta(hours=4))]}
    _, transit = T.locate(p, trips, depart + timedelta(minutes=30))
    assert transit is True


def test_transit_timestamps_are_moved_past_arrival():
    p = _P()
    rng = np.random.default_rng(0)
    depart = datetime(2026, 1, 1, 8)
    arrive = depart + timedelta(hours=T.travel_hours(p.region, "Samarkand"))
    back = arrive + timedelta(hours=24)
    trips = {p.pinfl: [(depart, arrive, "Samarkand", back,
                        back + timedelta(hours=4))]}
    settled = T.settle_after_transit(p, trips, depart + timedelta(minutes=10), rng)
    assert settled >= arrive
    assert T.locate(p, trips, settled)[1] is False


def test_planned_trips_are_never_physically_impossible():
    """The core negative-control guarantee, over a full simulated population."""
    rng = np.random.default_rng(7)
    people = [_P(f"p{i}", r) for i, r in enumerate(list(G.REGION_COORDS) * 8)]
    start = datetime(2026, 1, 1)
    trips = T.plan_trips(people, rng, start, days=30)
    assert trips, "no trips planned — the control would be vacuous"

    for p in people:
        for depart, arrive, dest, back_depart, back_arrive in trips.get(p.pinfl, ()):
            for a, b, r1, r2 in ((depart, arrive, p.region, dest),
                                 (back_depart, back_arrive, dest, p.region)):
                hours = (b - a).total_seconds() / 3600
                dist = G.region_distance_km(r1, r2)
                assert dist / max(hours, 1e-9) <= SC.MAX_PLAUSIBLE_KMH


# --- the positive case ------------------------------------------------------

def test_hijack_origin_is_genuinely_unreachable():
    rng = np.random.default_rng(1)
    for _ in range(50):
        origin = T.hijack_origin("Tashkent City", minutes_available=10, rng=rng)
        assert origin is not None
        dist = G.region_distance_km("Tashkent City", origin)
        assert dist / (10 / 60) > SC.MAX_PLAUSIBLE_KMH


def test_hijack_origin_respects_the_detector_distance_floor():
    """Below the floor the rule ignores the move regardless of speed, so a
    hijack placed there would be undetectable by construction."""
    rng = np.random.default_rng(1)
    for _ in range(50):
        origin = T.hijack_origin("Tashkent City", minutes_available=10, rng=rng)
        assert G.region_distance_km("Tashkent City", origin) >= SC.MIN_TRAVEL_DISTANCE_KM


def test_hijack_origin_gives_up_rather_than_inventing_a_journey():
    """With a week available, every region is reachable — the pattern must not
    be forced into data whose geography cannot support it."""
    rng = np.random.default_rng(1)
    assert T.hijack_origin("Tashkent City", minutes_available=7 * 24 * 60,
                           rng=rng) is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
