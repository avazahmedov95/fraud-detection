"""
Unit tests for the geographic reference table and distance maths.
Run: python -m pytest test_geo.py -q
"""

import geo as G
import config as C


def test_all_regions_have_coordinates_inside_uzbekistan():
    """Bounding box of Uzbekistan, as a guard against transposed lat/lon."""
    assert len(G.REGION_COORDS) == 14
    for name, (lat, lon) in G.REGION_COORDS.items():
        assert 37.0 <= lat <= 46.0, f"{name} latitude outside Uzbekistan"
        assert 55.0 <= lon <= 74.0, f"{name} longitude outside Uzbekistan"


def test_known_distance_tashkent_samarkand():
    """Tashkent-Samarkand great-circle distance is ~270 km (road is ~300 km)."""
    d = G.region_distance_km("Tashkent City", "Samarkand")
    assert 250 < d < 290, d


def test_known_distance_tashkent_nukus():
    """The country's longest domestic hop, ~800 km great-circle."""
    d = G.region_distance_km("Tashkent City", "Karakalpakstan")
    assert 750 < d < 850, d


def test_distance_is_symmetric_and_zero_on_self():
    assert G.region_distance_km("Bukhara", "Bukhara") == 0.0
    ab = G.region_distance_km("Andijan", "Navoi")
    ba = G.region_distance_km("Navoi", "Andijan")
    assert abs(ab - ba) < 1e-9


def test_unknown_region_returns_none():
    assert G.region_distance_km("Tashkent City", "Narnia") is None
    assert G.implied_speed_kmh("Narnia", "Bukhara", 3600) is None


def test_implied_speed_matches_distance_over_time():
    d = G.region_distance_km("Tashkent City", "Samarkand")
    assert abs(G.implied_speed_kmh("Tashkent City", "Samarkand", 3600) - d) < 1e-6


def test_same_second_move_is_not_infinite():
    """Zero elapsed time must clamp, not divide by zero — and must still be
    far above the plausible-speed ceiling."""
    v = G.implied_speed_kmh("Tashkent City", "Karakalpakstan", 0)
    assert v > C.MAX_PLAUSIBLE_KMH


def test_driving_speed_is_below_the_ceiling():
    """A five-hour Tashkent-Samarkand drive must never look impossible."""
    v = G.implied_speed_kmh("Tashkent City", "Samarkand", 5 * 3600)
    assert v < C.MAX_PLAUSIBLE_KMH


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\n{len(fns)} tests passed")
