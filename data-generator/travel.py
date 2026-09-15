"""Moves people between regions over time, so IMPOSSIBLE_TRAVEL has something real
to contradict rather than flagging any inter-region transfer."""

import importlib.util
import os
import sys
from datetime import timedelta


_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")


def _load_from_detector(module_name):
    """Load a stream-processor module by explicit path, under a private name: both
    packages have a `config`, so putting the detector on sys.path would shadow ours."""
    spec = importlib.util.spec_from_file_location(
        f"_detector_{module_name}", os.path.join(_SP, f"{module_name}.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


G = _load_from_detector("geo")          # shared reference data, single source
_DC = _load_from_detector("config")
MAX_PLAUSIBLE_KMH = _DC.MAX_PLAUSIBLE_KMH
MIN_TRAVEL_DISTANCE_KM = _DC.MIN_TRAVEL_DISTANCE_KM

# Ground travel between Uzbek regional centres. Deliberately slow: longer journeys
# make the negative control HARDER (more chance of looking impossible), not easier.
TRAVEL_SPEED_KMH = 70.0

TRIP_SHARE = 0.18          # share of people who travel at all in the period
MAX_TRIPS = 2
STAY_HOURS = (12, 96)      # how long a trip lasts


def travel_hours(region_a, region_b):
    dist = G.region_distance_km(region_a, region_b)
    if dist is None:
        return 0.0
    return dist / TRAVEL_SPEED_KMH


def plan_trips(persons, rng, start_dt, days):
    """Give a share of people one or two real journeys, as
    {pinfl: [(depart, arrive, region, back_depart, back_arrive)]}; home otherwise."""
    regions = list(G.REGION_COORDS)
    span = days * 24 * 3600
    trips = {}
    for p in persons:
        if rng.random() >= TRIP_SHARE:
            continue
        plans = []
        for _ in range(int(rng.integers(1, MAX_TRIPS + 1))):
            dest = str(rng.choice(regions))
            if dest == p.region:
                continue
            hours = travel_hours(p.region, dest)
            if hours <= 0:
                continue
            depart = start_dt + timedelta(seconds=float(rng.random() * span))
            arrive = depart + timedelta(hours=hours)
            back_depart = arrive + timedelta(
                hours=float(rng.uniform(*STAY_HOURS)))
            back_arrive = back_depart + timedelta(hours=hours)
            plans.append((depart, arrive, dest, back_depart, back_arrive))
        if plans:
            trips[p.pinfl] = plans
    return trips


def locate(person, trips, ts):
    """(region, in_transit) for the person at `ts`. The caller re-times in-transit
    events, so legitimate traffic never makes an impossible journey."""
    for depart, arrive, dest, back_depart, back_arrive in trips.get(person.pinfl, ()):
        if depart <= ts < arrive or back_depart <= ts < back_arrive:
            return person.region, True
        if arrive <= ts < back_depart:
            return dest, False
    return person.region, False


def settle_after_transit(person, trips, ts, rng):
    """Move a timestamp out of any transit window, to just after arrival."""
    for depart, arrive, dest, back_depart, back_arrive in trips.get(person.pinfl, ()):
        if depart <= ts < arrive:
            return arrive + timedelta(minutes=float(rng.uniform(5, 180)))
        if back_depart <= ts < back_arrive:
            return back_arrive + timedelta(minutes=float(rng.uniform(5, 180)))
    return ts


def hijack_origin(home_region, minutes_available, rng):
    """A region the account holder could not reach by any transport, judged with the
    detector's jet-speed limits, so the rule can see it; the result to quote is the
    false-positive rate on legitimate journeys. None when no region is far enough."""
    hours = max(minutes_available, 1) / 60.0
    candidates = []
    for r in G.REGION_COORDS:
        if r == home_region:
            continue
        dist = G.region_distance_km(home_region, r) or 0.0
        if dist >= MIN_TRAVEL_DISTANCE_KM and dist / hours > MAX_PLAUSIBLE_KMH:
            candidates.append(r)
    if not candidates:
        return None
    return str(rng.choice(candidates))
