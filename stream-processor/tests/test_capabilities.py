"""Unit tests for the capability registry."""

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


#: Capabilities that legitimately have no "off". One that SELECTS between data
#: sources has no null option: off would model a deployment that cannot key its
#: state at all. Listed as exemptions rather than weakening the rule, so a new
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
    """The exemption is safe only while such a capability adds no columns."""
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
    # Set both states explicitly: no dependence on the ambient CAP_* environment.
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
    set_mode(geo_telemetry="off", session_telemetry="off")
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
    """With every optional integration off, only own-stream features remain."""
    set_mode(**{cap.key: "off" for cap in CAP.REGISTRY if "off" in cap.modes})
    names = CAP.feature_names()
    assert names == list(CAP.BY_KEY["core_history"].features)
    assert len(names) == 11


# --- a write that configures nothing must fail -----------------------------

def test_an_unknown_capability_cannot_be_set(set_mode):
    with pytest.raises(KeyError):
        set_mode(channel="off")
    with pytest.raises(KeyError):
        CAP.MODES["no_such_capability"] = "off"


def test_a_mode_outside_the_declared_set_cannot_be_set(set_mode):
    """A mode outside the declared set must fail too: payee_identity has no "off"."""
    assert "off" not in CAP.BY_KEY["payee_identity"].modes
    with pytest.raises(ValueError):
        set_mode(payee_identity="off")
    assert CAP.enabled("payee_identity")


def test_update_is_guarded_too(set_mode):
    """dict.update is C code that skips a subclass's __setitem__, and the fixtures
    set profiles that way - so the guard has to cover it explicitly."""
    with pytest.raises(KeyError):
        CAP.MODES.update({"channel": "off"})


def test_restoring_a_saved_profile_still_works(set_mode):
    """clear() + update(saved) is how every fixture here restores state; guarding
    writes must not break it."""
    saved = dict(CAP.MODES)
    CAP.MODES.clear()
    CAP.MODES.update(saved)
    assert CAP.MODES == saved
