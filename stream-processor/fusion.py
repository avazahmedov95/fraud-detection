"""The decision from the model's score and the rules, and the alert's type. Not a
blend (every blend tried ranked worse): the rules decide only through
MANDATORY_REVIEW_RULES, and with no model loaded through the fallback cut-off."""

import config as C
import capabilities as CAP


def final_score(cep_score, ml_score) -> float:
    """The model's probability; the rule score when no model is loaded."""
    raw = cep_score if ml_score is None else ml_score
    return min(1.0, max(0.0, float(raw)))


_CUTOFF_CACHE = {}


def review_cutoff(cep_only: bool):
    """The model's cut-off (thresholds.json), or with no model the rule score's,
    scaled to the capability profile (capabilities.scaled_threshold)."""
    if not cep_only:
        return C.MODEL_REVIEW_THRESHOLD
    if not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.REVIEW_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _CUTOFF_CACHE:
        _CUTOFF_CACHE[key] = CAP.scaled_threshold(C.REVIEW_THRESHOLD)
    return _CUTOFF_CACHE[key]


def score_and_decide(cep_score, ml_score, rule_hits):
    """The final score and the decision; whether the score is a probability is
    settled here, once."""
    final = final_score(cep_score, ml_score)
    return final, decide(final, rule_hits, cep_only=ml_score is None)


def decide(score: float, rule_hits, cep_only: bool = False) -> str:
    """ALLOW, REVIEW or SECOND_LOOK, never BLOCK: REVIEW holds the transfer until a
    person blocks or releases it; SECOND_LOOK, a model score just under the cut-off,
    waits for TabPFN (second-look/). `cep_only`: no model is loaded."""
    mandatory = any(r in C.MANDATORY_REVIEW_RULES for r in rule_hits)
    if score >= review_cutoff(cep_only) or mandatory:
        return "REVIEW"
    if not cep_only and C.SECOND_LOOK_FROM is not None and score >= C.SECOND_LOOK_FROM:
        return "SECOND_LOOK"
    return "ALLOW"


#: The first scheme whose rules fired names the alert; MULE needs money converging.
_TYPE_PRIORITY = (
    ("STRUCTURING", ("STRUCTURING",)),
    ("ATO",         ("GEO_ANOMALY", "VELOCITY", "DISTINCT_PAYEE_BURST")),
    ("MULE",        ("MULE_FAN_IN",)),
    ("APP",         ("NEW_PAYEE_HIGH_AMOUNT", "AMOUNT_DEVIATION")),
)


def classify_type(rule_hits):
    """The alert's type, or None."""
    hits = set(rule_hits)
    for label, triggers in _TYPE_PRIORITY:
        if any(t in hits for t in triggers):
            return label
    return None
