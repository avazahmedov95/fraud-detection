"""
Legitimate travel, and the journeys that are not legitimate.

The IMPOSSIBLE_TRAVEL rule claims to separate a hijacked session from an
ordinary trip. That claim is only testable if the data contains both. Injecting
the fraud pattern alone would guarantee the rule detects it — the rule would be
finding what the generator was told to plant, which proves nothing.

So this module does two things:

  * `plan_trips` sends ordinary people on real journeys. Their transactions
    appear in another region, and the pipeline must NOT flag them. This is the
    negative control, and it is the more important half.
  * `hijack_origin` picks a region far enough from the victim that reaching it
    in the available time is physically impossible. This is the positive case:
    a session continuing from somewhere the account holder cannot be.

Both use the same coordinate table the detector uses (`stream-processor/geo.py`),
so the simulation cannot disagree with the detector about how far apart two
regions are.
"""

import importlib.util
import os
import sys
from datetime import timedelta

import numpy as np

_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")


def _load_from_detector(module_name):
    """Load a stream-processor module by explicit path, under a private name.

    Deliberately NOT `sys.path.insert(_SP)` + plain import. Both packages have a
    module called `config`, so putting the detector's directory on the path
    shadows this package's own config for every later import — and whether it
    breaks depends on import order, which is the worst kind of dependency.
    (It did break: importing travel before config made persons.py read the
    detector's config and fail on a missing constant.)

    Loading by path keeps the detector's modules addressable without touching
    how anything else resolves.
    """
    spec = importlib.util.spec_from_file_location(
        f"_detector_{module_name}", os.path.join(_SP, f"{module_name}.py"))
    module = importlib.util.module_from_spec(spec)
    # Registered so the module is importable by its private name if something
    # holds a reference; the public name `config` is left untouched.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


G = _load_from_detector("geo")          # shared reference data, single source
_DC = _load_from_detector("config")
MAX_PLAUSIBLE_KMH = _DC.MAX_PLAUSIBLE_KMH
MIN_TRAVEL_DISTANCE_KM = _DC.MIN_TRAVEL_DISTANCE_KM

# Ground travel between Uzbek regional centres. Deliberately conservative: a
# slower assumed speed means longer journeys, which makes the negative control
# HARDER (more chance of looking impossible), not easier.
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
    """Give a share of people one or two real journeys.

    Returns {pinfl: [(depart, arrive, region, back_depart, back_arrive)]}, times
    as datetimes. A person is at home outside these windows.
    """
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

    Returns (region, in_transit). Transactions in transit are re-timed by the
    caller rather than placed: putting one at the origin and the next at the
    destination minutes later would manufacture an impossible journey inside
    legitimate traffic — the exact artefact this module exists to avoid.
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

    Reachability is judged with the DETECTOR's constants (`MAX_PLAUSIBLE_KMH`,
    `MIN_TRAVEL_DISTANCE_KM`), not the ground-travel speed used for real trips.
    Those two answer different questions: `TRAVEL_SPEED_KMH` is how fast people
    actually go by road, while the detector's ceiling is jet cruise speed — the
    line past which no journey is possible at all. A hijack generated against
    the slower figure would sit in the gap between them, unreachable by car yet
    perfectly reachable by plane, and the rule would rightly ignore it.

    This does make the injected pattern detectable by construction, which is why
    the detection rate on it is NOT the result to quote. The meaningful
    measurement is the false-positive rate on the legitimate journeys generated
    alongside it: those are built from independent physics and the rule has to
    leave them alone.

    Returns None when no region is far enough — the caller then leaves the
    session where it is rather than inventing a journey the geography cannot
    support.
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
