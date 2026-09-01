"""
Unit tests for the capability registry.

Run: python -m pytest test_capabilities.py -q
"""

import pytest

import capabilities as CAP


@pytest.fixture
def set_mode():
    """Set capability modes, restoring the configured ones afterwards."""
    original = dict(CAP.MODES)
    yield lambda **kw: CAP.MODES.update(kw)
    CAP.MODES.clear()
    CAP.MODES.update(original)


# --- registry integrity -----------------------------------------------------

def test_every_feature_is_declared_exactly_once():
    """Two capabilities owning one feature would duplicate a vector column."""
    seen = [f for cap in CAP.REGISTRY for f in cap.features]
    assert len(seen) == len(set(seen))


def test_every_rule_is_declared_exactly_once():
    seen = [r for cap in CAP.REGISTRY for r in cap.rules]
    assert len(seen) == len(set(seen))


#: Capabilities that legitimately have no "off", with the reason. A capability
#: that SELECTS between data sources has no null option: switching it off would
#: not model a poorer deployment, it would model one that cannot key its state
#: at all. Exemptions are listed rather than the rule weakened, so a new
#: capability without "off" still has to be argued for here.
_NO_OFF_MODE = {
    "payee_identity": "selects which identity the payee is keyed by; there is "
                      "no deployment that keys receiver-side state on nothing",
}


def test_off_is_available_wherever_it_makes_sense():
    for cap in CAP.REGISTRY:
        if cap.always_on or cap.key in _NO_OFF_MODE:
            continue
        assert "off" in cap.modes, f"{cap.key} cannot be switched off"


def test_exempt_capabilities_contribute_no_features():
    """The exemption above is only safe while such a capability adds no columns:
    a feature that can never be removed would be an undeclared requirement."""
    for key in _NO_OFF_MODE:
        assert CAP.BY_KEY[key].features == ()


def test_core_history_cannot_be_switched_off():
    """It is the input stream itself, not an integration."""
    core = CAP.BY_KEY["core_history"]
    assert core.always_on
    assert CAP._configured(core) == "on"


def test_core_history_ignores_the_environment(monkeypatch):
    monkeypatch.setenv("CAP_CORE_HISTORY", "off")
    assert CAP._configured(CAP.BY_KEY["core_history"]) == "on"


def test_every_capability_documents_why_it_may_be_missing():
    for cap in CAP.REGISTRY:
        assert cap.requires, f"{cap.key} declares no data source"
        assert cap.rationale, f"{cap.key} has no rationale"


# --- derived contract -------------------------------------------------------

def test_disabling_a_capability_drops_its_features(set_mode):
    # Set both states explicitly: these tests must not depend on whatever the
    # ambient CAP_* environment happens to be.
    set_mode(geo_telemetry="on")
    before = CAP.feature_names()
    assert "geo_is_anomaly" in before

    set_mode(geo_telemetry="off")
    after = CAP.feature_names()
    assert "geo_is_anomaly" not in after
    assert len(after) == len(before) - 1


def test_disabling_a_capability_drops_its_rules(set_mode):
    set_mode(geo_telemetry="on")
    assert CAP.rule_enabled("GEO_ANOMALY")
    assert CAP.rule_enabled("IMPOSSIBLE_TRAVEL")

    set_mode(geo_telemetry="off")
    assert not CAP.rule_enabled("GEO_ANOMALY")
    assert not CAP.rule_enabled("IMPOSSIBLE_TRAVEL")


def test_core_rules_stay_enabled_when_integrations_are_off(set_mode):
    """A bank with no telemetry at all still runs its own-history rules."""
    set_mode(geo_telemetry="off", device_telemetry="off",
             session_telemetry="off", receiver_age="off")
    for rule in ("VELOCITY", "STRUCTURING", "AMOUNT_DEVIATION",
                 "DAILY_LIMIT_BREACH", "DISTINCT_PAYEE_BURST",
                 "NEW_PAYEE_HIGH_AMOUNT"):
        assert CAP.rule_enabled(rule)


def test_undeclared_rule_fails_open():
    """Failing closed would silently disable detection on a naming slip."""
    assert CAP.rule_enabled("SOME_NEW_RULE_NOT_IN_REGISTRY")


def test_myid_kinship_is_off_by_default(set_mode):
    """Most banks have no MyID integration; the default must not assume one."""
    assert CAP.BY_KEY["myid_kinship"].default == "off"
    set_mode(myid_kinship="off")
    assert "is_family" not in CAP.feature_names()
    set_mode(myid_kinship="on")
    assert "is_family" in CAP.feature_names()


def test_feature_order_is_stable_across_calls():
    assert CAP.feature_names() == CAP.feature_names()


def test_minimal_deployment_still_has_a_usable_contract(set_mode):
    """With every optional integration off, the contract is exactly the features
    computable from the bank's own transaction stream."""
    set_mode(**{cap.key: "off" for cap in CAP.REGISTRY if not cap.always_on})
    names = CAP.feature_names()
    assert names == list(CAP.BY_KEY["core_history"].features)
    assert len(names) == 12


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
