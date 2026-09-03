"""Uzbekistan's 14 regions as administrative-centre coordinates, and great-circle
distance. IMPOSSIBLE_TRAVEL compensates for that approximation with
MIN_TRAVEL_DISTANCE_KM."""

import math

# Administrative centre; Tashkent City and Tashkent Region are separate units.
REGION_COORDS = {
    "Andijan":         (40.7821, 72.3442),
    "Bukhara":         (39.7747, 64.4286),
    "Fergana":         (40.3864, 71.7864),
    "Jizzakh":         (40.1158, 67.8422),
    "Karakalpakstan":  (42.4600, 59.6100),   # Nukus
    "Kashkadarya":     (38.8606, 65.7891),   # Karshi
    "Khorezm":         (41.5500, 60.6333),   # Urgench
    "Namangan":        (40.9983, 71.6726),
    "Navoi":           (40.0844, 65.3792),
    "Samarkand":       (39.6542, 66.9597),
    "Surkhandarya":    (37.2242, 67.2783),   # Termez
    "Syrdarya":        (40.4897, 68.7842),   # Gulistan
    "Tashkent City":   (41.2995, 69.2401),
    "Tashkent Region": (41.0167, 69.3500),   # Nurafshon
}

EARTH_RADIUS_KM = 6371.0088          # IUGG mean radius


def haversine_km(a: tuple, b: tuple) -> float:
    """Great-circle distance in km between two (lat, lon) pairs."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def region_distance_km(r1: str, r2: str):
    """Distance between two named regions, or None if either is unknown."""
    p1, p2 = REGION_COORDS.get(r1), REGION_COORDS.get(r2)
    if p1 is None or p2 is None:
        return None
    return haversine_km(p1, p2)


def implied_speed_kmh(r1: str, r2: str, elapsed_s: float):
    """Speed needed to be in r2 `elapsed_s` after r1; None if a region is unknown.
    An unknown region must never be treated as a zero-distance move."""
    dist = region_distance_km(r1, r2)
    if dist is None:
        return None
    # max(..., 1.0): a raw division would raise on same-second events, and one
    # second is the finest resolution event_time has.
    return dist / (max(float(elapsed_s), 1.0) / 3600.0)
