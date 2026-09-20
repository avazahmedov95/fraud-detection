"""Registry of what the deploying bank can observe: each capability declares the
integration it needs, the model features it contributes and the CEP rules it enables.
features.py and rules.py derive from it, so switching one off follows through the
whole pipeline. Configure with CAP_<KEY> env vars - changing any of them changes the
feature contract: retrain and re-export afterwards.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Capability:
    key: str
    requires: str
    features: tuple = ()
    rules: tuple = ()
    modes: tuple = ("on", "off")        # allowed settings, richest first
    rationale: str = ""                 # why it may be unavailable

    @property
    def default(self):
        return self.modes[0]

    @property
    def always_on(self):
        return self.modes == ("on",)


# Declaration order fixes the feature-vector order, so it must stay stable:
# reordering silently invalidates every previously trained model.
REGISTRY = (
    Capability(
        key="core_history",
        requires="the bank's own transaction stream",
        modes=("on",),                  # cannot be switched off: it IS the input
        features=("log_amount", "amount_to_mean", "amount_z", "is_new_payee",
                  "vel_10m", "vel_1h", "distinct_payees_10m", "sub_threshold_1h",
                  "secs_since_last", "daily_sum_ratio", "hour"),
        rules=("NEW_PAYEE_HIGH_AMOUNT", "VELOCITY", "STRUCTURING",
               "DISTINCT_PAYEE_BURST", "AMOUNT_DEVIATION", "DAILY_LIMIT_BREACH"),
        rationale="Every bank has its own payment history; switching this off "
                  "would model nothing except declining to look at it.",
    ),
    Capability(
        key="myid_kinship",
        requires="MyID integration exposing verified family relationships",
        modes=("off", "on"),            # default off: not every bank has it
        features=("is_family",),
        rules=(),
        rationale="Only banks integrated with MyID can tell whether the payee is "
                  "a verified relative of the sender.",
    ),
    Capability(
        key="receiver_velocity",
        requires="a receiver-keyed counter shared across the cluster (Redis)",
        features=("rcv_distinct_senders_1h", "rcv_inflow_1h"),
        rules=("MULE_FAN_IN",),
        rationale="Every other feature here is computed from per-SENDER state, "
                  "because the stream is keyed by sender. A mule's signature is "
                  "the opposite shape — many distinct senders converging on one "
                  "receiver — and is invisible from any single sender's history. "
                  "Seeing it needs state keyed by receiver, which in a "
                  "partitioned stream means an external store.",
    ),
    Capability(
        key="geo_telemetry",
        requires="the region the operation originated from",
        features=("geo_is_anomaly",),
        rules=("GEO_ANOMALY", "IMPOSSIBLE_TRAVEL"),
        rationale="Depends on the channel reporting a location; not all "
                  "acquirer integrations pass it through.",
    ),
    Capability(
        key="session_telemetry",
        requires="mobile-app session signals (call state, login-to-confirm time)",
        features=("active_call", "secs_login_z"),
        rules=("COACHED_SESSION",),
        rationale="Only observable inside the bank's own mobile app; absent for "
                  "USSD, ATM and third-party channels.",
    ),
    Capability(
        key="payee_identity",
        requires="resolution of the destination PAN to the person behind it",
        modes=("card", "pinfl"),        # default card: what a bank actually has
        features=(), rules=(),
        rationale="A card-to-card transfer reaches the sending bank as a "
                  "destination PAN. Resolving it to a person needs either the "
                  "switch or the CBU platform; a bank can do it only for its "
                  "own clients, which is 6.9% of transfers at the measured "
                  "market concentration. So receiver-side state is keyed by "
                  "CARD by default. The cost is real and one-directional: a "
                  "mule spreading inbound transfers across several of their own "
                  "cards is split across as many fan-in buckets. Resolving "
                  "per-transfer where possible was measured and REJECTED - it "
                  "makes the key depend on the sender's bank, fragmenting one "
                  "payee's window in two and losing 17.4% of MULE_FAN_IN's true "
                  "positives. Mode 'pinfl' models a switch-level or "
                  "platform-level deployment, where the resolution exists for "
                  "everyone rather than for 7%. It is reachable only OFFLINE: "
                  "receiver_pinfl is not on the wire, so the live job falls "
                  "back to the card and says so once. The harnesses that read "
                  "the generated CSV can run it.",
    ),
    Capability(
        key="counterparty_history",
        requires="the shared store keeping, per card, who paid it and when, for a "
                 "week rather than an hour, and the sender's own last inbound",
        modes=("on", "off"),            # on since 2026-09-20: both gates passed
        features=("payee_payers_24h", "payee_payers_7d", "sender_payees_24h",
                  "sender_payees_7d", "secs_since_sender_inbound"),
        rules=(),
        rationale="The CBU's internal-control rules define P2P activity subject "
                  "to control as counts of distinct counterparties over up to 30 "
                  "days (docs/related-work.md 6e); the one-hour fan-in window "
                  "sees a slice of that. A mule collects over days and passes "
                  "the money on, so the interval since the sender's own account "
                  "was last paid is the other half of the shape. A month cannot "
                  "be measured on 30-day datasets, so the windows are a day and "
                  "a week. Last in the registry, so switching it on appends to "
                  "the vector instead of shifting every trained model's columns.",
    ),)

BY_KEY = {c.key: c for c in REGISTRY}


def _configured(cap: Capability) -> str:
    """Resolve a capability's mode from the environment, validating it."""
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
    """The live capability profile. Every write is checked against the registry, so
    a misspelt or deleted capability fails instead of silently switching nothing.
    `update` is overridden because dict.update bypasses __setitem__."""

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
    """Current mode of a capability."""
    return MODES[key]


def enabled(key: str) -> bool:
    """True unless the capability is switched off entirely. One with no "off" mode
    (payee_identity) is therefore always enabled: it selects BETWEEN data sources
    and adds no features, so the vector is unaffected."""
    return MODES[key] != "off"


def feature_names() -> list:
    """The model's feature vector, in registry order (derived, never hand-written)."""
    names = []
    for cap in REGISTRY:
        if not enabled(cap.key):
            continue
        names.extend(cap.features)
    return names


RULE_CAPABILITY = {rule: cap.key for cap in REGISTRY for rule in cap.rules}


def _rule_enabled_in(modes: dict, rule: str) -> bool:
    """As rule_enabled, against an explicit profile."""
    key = RULE_CAPABILITY.get(rule)
    return True if key is None else modes[key] != "off"


def rule_enabled(rule: str) -> bool:
    """True when the data behind a CEP rule is available. Unknown rules are enabled:
    a rule with no declared dependency runs on the core stream, and failing open
    here would silently disable detection."""
    return _rule_enabled_in(MODES, rule)


# Rules that plausibly fire TOGETHER on one episode - distinct from
# fusion._TYPE_PRIORITY, which names an alert from whichever rule fired first.
PATTERN_SIGNATURES = {
    "APP":         ("NEW_PAYEE_HIGH_AMOUNT", "COACHED_SESSION"),
    "ATO":         ("GEO_ANOMALY", "IMPOSSIBLE_TRAVEL", "VELOCITY"),
    "STRUCTURING": ("STRUCTURING", "VELOCITY"),
    "MULE":        ("MULE_FAN_IN", "NEW_PAYEE_HIGH_AMOUNT"),
}


def _rule_weights():
    """Rule -> weight, read from config by naming convention. A renamed weight
    constant shows up as missing here rather than silently contributing zero."""
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
    return {rule: getattr(C, const) for rule, const in explicit.items()
            if hasattr(C, const)}


def _full_modes() -> dict:
    """Every capability at its richest setting - the profile the hand-calibrated
    thresholds were set against, and the denominator every rescale divides by."""
    return {c.key: (c.modes[0] if c.always_on
                    else ("on" if "on" in c.modes else c.modes[0]))
            for c in REGISTRY}


def reachable_score(pattern: str, modes: dict = None) -> float:
    """Highest CEP score this fraud pattern can reach, under the active profile or
    a hypothetical `modes` - answered without installing it."""
    m = MODES if modes is None else modes
    weights = _rule_weights()
    fired = [weights.get(r, 0.0) for r in PATTERN_SIGNATURES.get(pattern, ())
             if _rule_enabled_in(m, r)]
    return min(1.0, sum(fired))


def weakest_reachable(modes: dict = None) -> float:
    """The hardest-to-score pattern under a profile. Zero if none can score."""
    if not PATTERN_SIGNATURES:
        return 0.0
    return min(reachable_score(p, modes) for p in PATTERN_SIGNATURES)


def scaled_threshold(base_threshold: float, base_weakest: float = None) -> float:
    """Re-express a hand-calibrated threshold for the current capability profile:

        threshold = base_threshold x (weakest_now / weakest_at_full_capability)

    An additive threshold states how many rules must agree, so held fixed while rules
    are switched off the rule layer goes silent - on PaySim it never flagged. At full
    capability this returns base_threshold unchanged."""
    if base_weakest is None:
        base_weakest = weakest_reachable(_full_modes())
    if base_weakest <= 0:
        return base_threshold
    return round(base_threshold * (weakest_reachable() / base_weakest), 4)


def describe() -> str:
    """Human-readable summary of the active deployment profile."""
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
