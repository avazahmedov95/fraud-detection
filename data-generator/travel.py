"""Moves people between regions over time, so IMPOSSIBLE_TRAVEL has something real
to contradict rather than flagging any inter-region transfer."""

import importlib.util
import os
import sys
from datetime import timedelta

import numpy as np

_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")


def _load_from_detector(module_name):
    """Load a stream-processor module by explicit path, under a private name.

    NOT `sys.path.insert(_SP)`: both packages have a `config`, so the detector's dir on the
    path shadows ours depending on import order. It did break - importing travel before
    config made persons.py read the detector's config and fail on a missing constant.
    """
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
    """Where the person is at `ts`, and whether they are mid-journey.

    Returns (region, in_transit). In-transit events are re-timed by the caller: origin then
    destination minutes later would manufacture an impossible journey in legitimate traffic.
    """
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
    """A region the account holder could not be in, by any means of transport.

    Judged with the DETECTOR's `MAX_PLAUSIBLE_KMH` / `MIN_TRAVEL_DISTANCE_KM` (jet cruise),
    not road speed `TRAVEL_SPEED_KMH`: a hijack built against the slower figure would be
    unreachable by car yet reachable by plane, and the rule would rightly ignore it. That
    makes it detectable by construction, so its detection rate is NOT the result to quote -
    the meaningful measurement is the false-positive rate on the legitimate journeys
    generated alongside it. Returns None when no region is far enough.
    """
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
