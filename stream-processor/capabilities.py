"""What the bank can observe: each capability names its data source, its model
features and its rules; features.py and rules.py follow it. Set with CAP_<KEY>;
changing one changes the feature vector, so retrain afterwards."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Capability:
    key: str
    requires: str
    features: tuple = ()
    rules: tuple = ()
    modes: tuple = ("on", "off")        # richest first

    @property
    def default(self):
        return self.modes[0]

    @property
    def always_on(self):
        return self.modes == ("on",)


# The order is the feature vector's: reordering breaks every trained model.
REGISTRY = (
    Capability(
        key="core_history",
        requires="the bank's own transaction stream",
        modes=("on",),
        features=("log_amount", "amount_to_mean", "amount_z", "is_new_payee",
                  "vel_10m", "vel_1h", "distinct_payees_10m", "sub_threshold_1h",
                  "secs_since_last", "daily_sum_ratio", "hour"),
        rules=("NEW_PAYEE_HIGH_AMOUNT", "VELOCITY", "STRUCTURING",
               "DISTINCT_PAYEE_BURST", "AMOUNT_DEVIATION", "DAILY_LIMIT_BREACH"),
    ),
    Capability(
        key="receiver_velocity",
        requires="a receiver-keyed counter shared across the cluster (Redis)",
        features=("rcv_distinct_senders_1h", "rcv_inflow_1h"),
        rules=("MULE_FAN_IN",),
    ),
    Capability(
        key="geo_telemetry",
        requires="the region the operation originated from",
        features=("geo_is_anomaly",),
        rules=("GEO_ANOMALY", "IMPOSSIBLE_TRAVEL"),
    ),
    Capability(
        key="session_telemetry",
        requires="mobile-app session signals (call state, login-to-confirm time)",
        features=("active_call", "secs_login_z"),
        rules=("COACHED_SESSION",),
    ),
    Capability(
        key="payee_identity",
        requires="resolution of the destination PAN to the person behind it",
        modes=("card", "pinfl"),        # pinfl only offline: it is not on the wire
        features=(), rules=(),
    ),
    Capability(
        key="counterparty_history",
        requires="who paid each card over a week, and when the sender was last paid (Redis)",
        features=("payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
                  "sender_payees_7d", "secs_since_sender_inbound"),
        rules=(),
    ),
    Capability(
        key="confirmed_cases",
        requires="the confirmed fraud accounts and each card's contacts of the week (Redis)",
        features=("payee_flagged", "sender_flagged", "payee_flagged_contacts"),
        rules=(),
    ),
)

BY_KEY = {c.key: c for c in REGISTRY}


def _configured(cap: Capability) -> str:
    """A capability's mode from the environment."""
    if cap.always_on:
        return cap.default
    raw = os.getenv(f"CAP_{cap.key.upper()}")
    if raw is None:
        return cap.default
    mode = raw.strip().lower()
    if mode not in cap.modes:
        raise ValueError(
            f"CAP_{cap.key.upper()} must be one of {cap.modes}, got {raw!r}")
    return mode


class _Modes(dict):
    """The live profile; a misspelt capability or mode fails instead of switching
    nothing (dict.update bypasses __setitem__, hence update)."""

    def __setitem__(self, key, value):
        cap = BY_KEY.get(key)
        if cap is None:
            raise KeyError(f"no capability {key!r}; the registry declares "
                           f"{sorted(BY_KEY)}")
        if value not in cap.modes:
            raise ValueError(f"{key} must be one of {cap.modes}, got {value!r}")
        super().__setitem__(key, value)

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items():
            self[key] = value


MODES = _Modes({c.key: _configured(c) for c in REGISTRY})


def mode(key: str) -> str:
    return MODES[key]


def enabled(key: str) -> bool:
    """True unless switched off; payee_identity has no "off"."""
    return MODES[key] != "off"


def feature_names() -> list:
    """The model's feature vector, in registry order."""
    names = []
    for cap in REGISTRY:
        if not enabled(cap.key):
            continue
        names.extend(cap.features)
    return names


RULE_CAPABILITY = {rule: cap.key for cap in REGISTRY for rule in cap.rules}


def _rule_enabled_in(modes: dict, rule: str) -> bool:
    key = RULE_CAPABILITY.get(rule)
    return True if key is None else modes[key] != "off"


def rule_enabled(rule: str) -> bool:
    """True when the data behind a rule is available; a rule no capability names runs
    on the core stream."""
    return _rule_enabled_in(MODES, rule)


# The rules that fire together on one scheme.
PATTERN_SIGNATURES = {
    "APP":         ("NEW_PAYEE_HIGH_AMOUNT", "COACHED_SESSION"),
    "ATO":         ("GEO_ANOMALY", "IMPOSSIBLE_TRAVEL", "VELOCITY"),
    "STRUCTURING": ("STRUCTURING", "VELOCITY"),
    "MULE":        ("MULE_FAN_IN", "NEW_PAYEE_HIGH_AMOUNT"),
}


def _rule_weights():
    """Rule -> weight; a renamed constant fails here instead of weighing zero."""
    import config as C
    explicit = {
        "NEW_PAYEE_HIGH_AMOUNT": "W_NEW_PAYEE_HIGH",
        "VELOCITY": "W_VELOCITY",
        "STRUCTURING": "W_STRUCTURING",
        "DISTINCT_PAYEE_BURST": "W_DISTINCT_BURST",
        "GEO_ANOMALY": "W_GEO_ANOMALY",
        "IMPOSSIBLE_TRAVEL": "W_IMPOSSIBLE_TRAVEL",
        "AMOUNT_DEVIATION": "W_AMOUNT_DEVIATION",
        "COACHED_SESSION": "W_COACHED_SESSION",
        "DAILY_LIMIT_BREACH": "W_DAILY_LIMIT",
        "MULE_FAN_IN": "W_MULE_FAN_IN",
    }
    return {rule: getattr(C, const) for rule, const in explicit.items()}


def _full_modes() -> dict:
    """Every capability at its richest: the profile the thresholds were set on."""
    return {c.key: "on" if "on" in c.modes else c.modes[0] for c in REGISTRY}


def reachable_score(pattern: str, modes: dict = None) -> float:
    """The highest rule score a scheme can reach under a profile."""
    m = MODES if modes is None else modes
    weights = _rule_weights()
    fired = [weights.get(r, 0.0) for r in PATTERN_SIGNATURES.get(pattern, ())
             if _rule_enabled_in(m, r)]
    return min(1.0, sum(fired))


def weakest_reachable(modes: dict = None) -> float:
    return min(reachable_score(p, modes) for p in PATTERN_SIGNATURES)


def scaled_threshold(base_threshold: float, base_weakest: float = None) -> float:
    """A threshold set on every capability, scaled by how much of the weakest
    scheme's score the current profile can still reach; otherwise, with rules off,
    the rule layer goes silent (on PaySim it never flagged)."""
    if base_weakest is None:
        base_weakest = weakest_reachable(_full_modes())
    if base_weakest <= 0:
        return base_threshold
    return round(base_threshold * (weakest_reachable() / base_weakest), 4)


def describe() -> str:
    lines = ["capability        mode      features  rules"]
    for cap in REGISTRY:
        m = MODES[cap.key]
        n_feats = len([f for f in feature_names() if f in cap.features])
        n_rules = len(cap.rules) if enabled(cap.key) else 0
        lines.append(f"{cap.key:<18}{m:<10}{n_feats:>8}{n_rules:>7}")
    lines.append(f"{'total':<18}{'':<10}{len(feature_names()):>8}"
                 f"{sum(1 for r in RULE_CAPABILITY if rule_enabled(r)):>7}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(describe())
