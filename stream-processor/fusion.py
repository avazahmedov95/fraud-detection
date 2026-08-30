"""
Score fusion + decisioning — shared by the Flink job and the offline eval.

Design (evidence-driven): we evaluated naive score blends (noisy-OR, weighted
average, ML-augmented-by-CEP) and every one DEGRADED ranking versus the model
alone (PR-AUC 0.953 -> ~0.91-0.94), because the rule score is lower-resolution
and dilutes a strong model. We therefore fuse at the DECISION layer:

  * final_score = the model probability (graded risk) when the model is available,
    falling back to the CEP score only if the model is down;
  * the CEP layer adds DETERMINISTIC regulatory must-flags (structuring, daily
    limit) that force at least REVIEW regardless of the model score — high-
    precision on synthetic data (38 fraud vs 2 legit) — plus per-alert reason
    codes (rule_hits) and the model-down fallback.

This both beats CEP-only and ML-only at the chosen operating point and matches how
regulated systems actually combine rules with ML. Pure: no Flink/onnx/numpy here.

`predicted_type` is NOT an ML output (the model is a binary classifier); it is a
rule-pattern label explaining *why* an alert fired.
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
