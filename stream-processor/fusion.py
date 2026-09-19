"""Turns two scores into one decision - ALLOW / REVIEW - plus reason codes.

Fused at the decision, never blended: every blend tried degraded ranking. The
rule layer reaches the decision only through MANDATORY_REVIEW_RULES and the
fallback cutoff in `review_cutoff()`.
"""

import config as C
import capabilities as CAP


def final_score(cep_score, ml_score) -> float:
    """Graded risk = model probability when present; CEP score as fallback."""
    raw = cep_score if ml_score is None else ml_score
    return min(1.0, max(0.0, float(raw)))


_CUTOFF_CACHE = {}


def review_cutoff(cep_only: bool):
    """The REVIEW cutoff for the score being decided on. The model's comes from
    thresholds.json; only the CEP-only fallback scales with capability, because an
    additive rule score goes silent as rules are switched off
    (capabilities.scaled_threshold). At full capability the constant stands."""
    if not cep_only:
        # The model's own cutoff, shipped with it (config._model_review_threshold).
        return C.MODEL_REVIEW_THRESHOLD
    if not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.FINAL_REVIEW_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _CUTOFF_CACHE:
        _CUTOFF_CACHE[key] = CAP.scaled_threshold(C.FINAL_REVIEW_THRESHOLD)
    return _CUTOFF_CACHE[key]


def score_and_decide(cep_score, ml_score, rule_hits):
    """Score and decide in one call - the only form the job should use - so whether
    the score is a probability is derived here, once, not by each caller."""
    final = final_score(cep_score, ml_score)
    return final, decide(final, rule_hits, cep_only=ml_score is None)


def decide(score: float, rule_hits, cep_only: bool = False) -> str:
    """ALLOW or REVIEW - never BLOCK: every alert goes to a person. `cep_only`: the
    score is the rule layer's because no model is loaded."""
    mandatory = any(r in C.MANDATORY_REVIEW_RULES for r in rule_hits)
    if score >= review_cutoff(cep_only) or mandatory:
        return "REVIEW"
    return "ALLOW"


#: Alert label from the first pattern whose rules fired. MULE is named only by
#: MULE_FAN_IN - money converging, which a takeover does not do; the burst rules
#: name ATO, since a fast run of outbound transfers fits either pattern.
_TYPE_PRIORITY = (
    ("STRUCTURING", ("STRUCTURING",)),
    ("ATO",         ("GEO_ANOMALY", "VELOCITY", "DISTINCT_PAYEE_BURST")),
    ("MULE",        ("MULE_FAN_IN",)),
    ("APP",         ("NEW_PAYEE_HIGH_AMOUNT", "AMOUNT_DEVIATION")),
)


def classify_type(rule_hits):
    """Rule-pattern fraud-type label for the alert (None if nothing salient)."""
    hits = set(rule_hits)
    for label, triggers in _TYPE_PRIORITY:
        if any(t in hits for t in triggers):
            return label
    return None
