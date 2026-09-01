"""
Capability registry — what the deploying bank can actually observe.

A detection feature is only as deployable as the data behind it. A bank with a
mobile app can see whether the customer was on a call while confirming; one
operating through USSD cannot. A bank integrated with MyID can check whether the
payee is a verified relative; one without that integration cannot. Neither is a
better or worse antifraud system — they have different inputs.

This module is the single source of truth for that. Each capability declares the
integration it needs, the model features it contributes and the CEP rules it
enables. `features.py` assembles FEATURE_NAMES from it and `rules.py` gates each
rule on it, so a capability can be switched off in one place and the whole
pipeline follows — including the train/serve feature contract, which is derived
rather than maintained by hand.

Configure per capability via environment variable, e.g.

    CAP_GEO_TELEMETRY=off
    CAP_RECEIVER_AGE=on_us
    CAP_MYID_KINSHIP=on

Changing any of these changes the FEATURE CONTRACT: retrain and re-export the
model afterwards. `ml/ablation.py` sweeps configurations and does that for you.
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Capability:
    key: str
    requires: str                       # the integration or data source needed
    features: tuple = ()                # model features it contributes
    rules: tuple = ()                   # CEP rules it enables
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
                  "secs_since_last", "daily_sum_ratio", "hour", "cross_network"),
        rules=("NEW_PAYEE_HIGH_AMOUNT", "VELOCITY", "STRUCTURING",
               "DISTINCT_PAYEE_BURST", "AMOUNT_DEVIATION", "DAILY_LIMIT_BREACH"),
        rationale="Every bank has its own payment history; switching this off "
                  "would model nothing except declining to look at it.",
    ),
    Capability(
        key="receiver_age",
        requires="core-banking lookup of the payee's account, or an inter-bank "
                 "exchange of that field",
        modes=("always", "on_us", "off"),
        features=("receiver_age", "receiver_is_fresh"),
        rules=("FRESH_RECEIVER",),
        rationale="The sending bank can only resolve its own clients' accounts. "
                  "The UzCard / HUMO switch carries no account-age field, so on "
                  "inter-bank transfers this is unobtainable ('on_us' models "
                  "exactly that; it adds a receiver_age_known flag).",
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
        key="device_telemetry",
        requires="a stable device identifier from the channel",
        features=("device_is_new",),
        rules=("DEVICE_CHANGE",),
        rationale="Available in app and web channels; ATM and USSD traffic "
                  "carries no device identity.",
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
        key="channel",
        requires="the channel the transfer came through",
        features=("ch_mobile_app", "ch_ussd", "ch_web", "ch_atm"),
        rules=(),
        rationale="Present in the switch message; separable mainly to test how "
                  "much channel identity contributes on its own.",
    ),
)

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


MODES = {c.key: _configured(c) for c in REGISTRY}


def mode(key: str) -> str:
    """Current mode of a capability."""
    return MODES[key]


def enabled(key: str) -> bool:
    """True unless the capability is switched off entirely.

    A capability with no "off" among its modes (payee_identity) is therefore
    always enabled: it selects BETWEEN data sources rather than declaring one
    absent. It contributes no features, so this does not affect the vector.
    """
    return MODES[key] != "off"


def feature_names() -> list:
    """The model's feature vector, in registry order.

    Derived rather than hand-maintained: a capability that is off contributes no
    columns, so the train/serve contract cannot drift from the configuration.
    """
    names = []
    for cap in REGISTRY:
        if not enabled(cap.key):
            continue
        names.extend(cap.features)
        # 'on_us' cannot distinguish "new account" from "not our client" without
        # saying which it is, so the flag only exists in that mode.
        if cap.key == "receiver_age" and MODES[cap.key] == "on_us":
            names.append("receiver_age_known")
    return names


RULE_CAPABILITY = {rule: cap.key for cap in REGISTRY for rule in cap.rules}


def rule_enabled(rule: str) -> bool:
    """True when the data behind a CEP rule is available.

    Unknown rules are enabled: a rule with no declared dependency runs on the
    core stream, and failing open here would silently disable detection.
    """
    key = RULE_CAPABILITY.get(rule)
    return True if key is None else enabled(key)


# Rules that characterise each fraud pattern — the ones that plausibly fire
# TOGETHER on one episode. Distinct from fusion._TYPE_PRIORITY, which names an
# alert from whichever rule fired first: this is the full co-firing signature,
# and it is what determines how much score a pattern can actually accumulate.
PATTERN_SIGNATURES = {
    "APP":         ("NEW_PAYEE_HIGH_AMOUNT", "FRESH_RECEIVER", "COACHED_SESSION"),
    "ATO":         ("DEVICE_CHANGE", "GEO_ANOMALY", "IMPOSSIBLE_TRAVEL", "VELOCITY"),
    "STRUCTURING": ("STRUCTURING", "VELOCITY"),
    "MULE":        ("MULE_FAN_IN", "FRESH_RECEIVER", "NEW_PAYEE_HIGH_AMOUNT"),
}


def _rule_weights():
    """Rule -> weight, read from config by naming convention.

    Kept as a lookup rather than a second hand-maintained table: a rule whose
    weight constant is renamed shows up as missing here rather than silently
    contributing zero to the reachability calculation.
    """
    import config as C
    explicit = {
        "NEW_PAYEE_HIGH_AMOUNT": "W_NEW_PAYEE_HIGH",
        "FRESH_RECEIVER": "W_FRESH_RECEIVER",
        "VELOCITY": "W_VELOCITY",
        "STRUCTURING": "W_STRUCTURING",
        "DISTINCT_PAYEE_BURST": "W_DISTINCT_BURST",
        "DEVICE_CHANGE": "W_DEVICE_CHANGE",
        "GEO_ANOMALY": "W_GEO_ANOMALY",
        "IMPOSSIBLE_TRAVEL": "W_IMPOSSIBLE_TRAVEL",
        "AMOUNT_DEVIATION": "W_AMOUNT_DEVIATION",
        "COACHED_SESSION": "W_COACHED_SESSION",
        "DAILY_LIMIT_BREACH": "W_DAILY_LIMIT",
        "MULE_FAN_IN": "W_MULE_FAN_IN",
    }
    return {rule: getattr(C, const) for rule, const in explicit.items()
            if hasattr(C, const)}


def reachable_score(pattern: str) -> float:
    """Highest CEP score this fraud pattern can reach under the current profile.

    A pattern whose signature rules are mostly disabled cannot accumulate score,
    however obvious the fraud is. This is the quantity a fixed threshold ignores.
    """
    weights = _rule_weights()
    fired = [weights.get(r, 0.0) for r in PATTERN_SIGNATURES.get(pattern, ())
             if rule_enabled(r)]
    return min(1.0, sum(fired))


def weakest_reachable() -> float:
    """The hardest-to-score pattern under this profile. Zero if none can score."""
    if not PATTERN_SIGNATURES:
        return 0.0
    return min(reachable_score(p) for p in PATTERN_SIGNATURES)


def scaled_threshold(base_threshold: float, base_weakest: float = None) -> float:
    """Re-express a hand-calibrated threshold for the current capability profile.

    The CEP score is additive, so a threshold is implicitly a statement about how
    many rules must agree. Hold that statement fixed while the available rules
    change, and a reduced deployment does not get a slightly worse rule layer —
    it gets a silent one. Measured on PaySim: with two rules available the
    highest score any fraud reached was 0.35, against a 0.40 cutoff. Nothing was
    ever flagged, while the rules themselves separated the classes 4:1.

    So the threshold is carried across as a PROPORTION of what the weakest
    pattern can reach:

        threshold = base_threshold x (weakest_now / weakest_at_full_capability)

    At full capability this returns base_threshold unchanged, so the calibrated
    operating point is preserved and only reduced deployments move.

    This rescales sensitivity, it does not recover it: a profile that cannot see
    a pattern still cannot see it. It only stops the layer from going mute.
    """
    if base_weakest is None:
        saved = dict(MODES)
        try:
            for cap in REGISTRY:
                MODES[cap.key] = cap.modes[0] if cap.always_on else (
                    "on" if "on" in cap.modes else cap.modes[0])
            base_weakest = weakest_reachable()
        finally:
            MODES.clear(); MODES.update(saved)
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
