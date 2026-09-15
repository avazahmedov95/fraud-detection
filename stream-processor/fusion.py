"""Turns two scores into one decision - ALLOW / REVIEW / BLOCK - plus reason codes.

Fused at the decision, never blended: every blend tried degraded ranking. The
rule layer reaches the decision only through MANDATORY_REVIEW_RULES and the
fallback cutoffs in `cutoffs()`.
"""

import config as C
import capabilities as CAP


def final_score(cep_score, ml_score) -> float:
    """Graded risk = model probability when present; CEP score as fallback."""
    raw = cep_score if ml_score is None else ml_score
    return min(1.0, max(0.0, float(raw)))


_CUTOFF_CACHE = {}


def cutoffs(cep_only: bool):
    """REVIEW / BLOCK cutoffs for the score being decided on. The model's come from
    thresholds.json; only the CEP-only fallback scales with capability, because an
    additive rule score goes silent as rules are switched off
    (capabilities.scaled_threshold). At full capability the constants stand."""
    if not cep_only:
        # The model's own cutoffs, shipped with it (config._model_thresholds).
        return C.MODEL_REVIEW_THRESHOLD, C.MODEL_BLOCK_THRESHOLD
    if not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _CUTOFF_CACHE:
        _CUTOFF_CACHE[key] = (CAP.scaled_threshold(C.FINAL_REVIEW_THRESHOLD),
                              CAP.scaled_threshold(C.FINAL_BLOCK_THRESHOLD))
    return _CUTOFF_CACHE[key]


def score_and_decide(cep_score, ml_score, rule_hits):
    """Score and decide in one call - the only form the job should use - so whether
    the score is a probability is derived here, once, not by each caller."""
    final = final_score(cep_score, ml_score)
    return final, decide(final, rule_hits, cep_only=ml_score is None)


def decide(score: float, rule_hits, cep_only: bool = False) -> str:
    """ALLOW / REVIEW / BLOCK. `cep_only`: the score is the rule layer's because no
    model is loaded."""
    review_at, block_at = cutoffs(cep_only)
    if score >= block_at:
        return "BLOCK"
    mandatory = any(r in C.MANDATORY_REVIEW_RULES for r in rule_hits)
    if score >= review_at or mandatory:
        return "REVIEW"
    return "ALLOW"


#: Alert label from the first pattern whose rules fired. MULE is named only by
#: MULE_FAN_IN - money converging, which a takeover does not do; the burst rules
#: name ATO, since a fast run of outbound transfers fits either pattern.
_TYPE_PRIORITY = (
    ("STRUCTURING", ("STRUCTURING",)),
    ("ATO",         ("DEVICE_CHANGE", "GEO_ANOMALY", "VELOCITY",
                     "DISTINCT_PAYEE_BURST")),
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
