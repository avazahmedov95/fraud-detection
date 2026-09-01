"""Turns two scores into one decision: ALLOW / REVIEW / BLOCK, plus reason codes.

Fusion happens at the DECISION layer, not by blending the scores - every blend
tried degraded ranking. Pure: no Flink, onnx or numpy here.
Rationale and the measured comparison: docs/irp-framing.md 7.
"""

import config as C


def final_score(cep_score, ml_score) -> float:
    """Graded risk = model probability when present; CEP score as fallback.

    Deliberately NOT an average of the two — blending was shown to hurt ranking.
    """
    raw = cep_score if ml_score is None else ml_score
    return min(1.0, max(0.0, float(raw)))


def decide(score: float, rule_hits) -> str:
    if score >= C.FINAL_BLOCK_THRESHOLD:
        return "BLOCK"
    mandatory = any(r in C.MANDATORY_REVIEW_RULES for r in rule_hits)
    if score >= C.FINAL_REVIEW_THRESHOLD or mandatory:
        return "REVIEW"
    return "ALLOW"


# Priority order: the first pattern whose triggers fired names the alert.
_TYPE_PRIORITY = (
    ("STRUCTURING", ("STRUCTURING",)),
    ("ATO",         ("DEVICE_CHANGE", "GEO_ANOMALY")),
    ("MULE",        ("DISTINCT_PAYEE_BURST", "VELOCITY")),
    ("APP",         ("NEW_PAYEE_HIGH_AMOUNT", "AMOUNT_DEVIATION")),
)


def classify_type(rule_hits):
    """Rule-pattern fraud-type label for the alert (None if nothing salient)."""
    hits = set(rule_hits)
    for label, triggers in _TYPE_PRIORITY:
        if any(t in hits for t in triggers):
            return label
    return None
