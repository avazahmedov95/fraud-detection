"""Turns two scores into one decision: ALLOW / REVIEW / BLOCK, plus reason codes.

Fusion is at the DECISION layer, never a blend - every blend tried degraded ranking
(measured comparison: docs/irp-framing.md 7). The rule layer's own verdict is
discarded here, and that too is measured rather than assumed: honouring it as a
FLOOR - which can only raise a decision, so unlike a blend it cannot degrade
ranking at all - still costs precision 0.847 -> 0.457 for two additional true
positives (docs/irp-framing.md 8, fifteenth). The rules reach the decision only
through MANDATORY_REVIEW_RULES, and the fallback cutoffs in `cutoffs()`.
"""

import config as C
import capabilities as CAP


def final_score(cep_score, ml_score) -> float:
    """Graded risk = model probability when present; CEP score as fallback."""
    raw = cep_score if ml_score is None else ml_score
    return min(1.0, max(0.0, float(raw)))


_CUTOFF_CACHE = {}


def cutoffs(cep_only: bool):
    """REVIEW / BLOCK cutoffs for the score actually being decided on.

    Only the FALLBACK path scales with capability, and the asymmetry is the point:

    - A model probability needs no scaling. Switching a capability off changes the
      feature contract (capabilities.feature_names), so the model is retrained and
      its output recalibrated by construction.
    - An additive CEP score does: it states how many rules must agree, so held
      fixed as the rules shrink the layer goes silent rather than degrading.
      capabilities.scaled_threshold has the measurement.

    At full capability both branches return the constants unchanged, so the
    validated operating point does not move.
    """
    if not cep_only or not C.SCALE_THRESHOLDS_BY_CAPABILITY:
        return C.FINAL_REVIEW_THRESHOLD, C.FINAL_BLOCK_THRESHOLD
    key = tuple(sorted(CAP.MODES.items()))
    if key not in _CUTOFF_CACHE:
        _CUTOFF_CACHE[key] = (CAP.scaled_threshold(C.FINAL_REVIEW_THRESHOLD),
                              CAP.scaled_threshold(C.FINAL_BLOCK_THRESHOLD))
    return _CUTOFF_CACHE[key]


def score_and_decide(cep_score, ml_score, rule_hits):
    """Both steps at once, and the only form the job should use.

    `decide` needs to know whether the score it was handed is a probability or an
    additive CEP score, and `final_score` is where that is decided - so a caller
    doing the two steps separately has to RE-DERIVE a fact this module already
    knows. That re-derivation is a `cep_only=` keyword with a safe default, which
    means dropping it restores the old behaviour silently, and at full capability
    the old and new behaviour are identical. A regression there would be invisible
    in exactly the profile anyone would check it in - which is the defect this
    whole path exists to fix (docs/irp-framing.md 8, fifteenth).

    So the derivation lives here, once, and cannot be got wrong by a caller.
    """
    final = final_score(cep_score, ml_score)
    return final, decide(final, rule_hits, cep_only=ml_score is None)


def decide(score: float, rule_hits, cep_only: bool = False) -> str:
    """ALLOW / REVIEW / BLOCK.

    `cep_only` says the score came from the rule layer because no model was
    loaded. It defaults False because the fused path is the normal one, and
    because a caller that forgets it gets today's behaviour rather than a
    silently rescaled cutoff.
    """
    review_at, block_at = cutoffs(cep_only)
    if score >= block_at:
        return "BLOCK"
    mandatory = any(r in C.MANDATORY_REVIEW_RULES for r in rule_hits)
    if score >= review_at or mandatory:
        return "REVIEW"
    return "ALLOW"


# Priority order: the first pattern whose triggers fired names the alert.
#: Alert label from whichever rule fired, first match wins.
#:
#: Reassigned on 08.09.2026, in two steps, and the second one matters more.
#:
#: MULE used to be named by DISTINCT_PAYEE_BURST and VELOCITY alone - two
#: SENDER-side burst rules - and MULE_FAN_IN was not in this table at all.
#: irp-framing.md 9 had noticed that from the other side: an outage experiment
#: predicted MULE_FAN_IN would stop firing and read a column that could not see
#: it, recorded as a category error in the metric. It was also a defect here.
#:
#: Adding MULE_FAN_IN gave the label back (30 alerts, 30 correct). Then the
#: generator was corrected so A2 stops evading VELOCITY for free, both burst
#: rules started firing - and 7 of 28 MULE alerts turned out to be account
#: takeovers. A fast run of outbound transfers does NOT say which pattern it is:
#: a drained account and a mule paying out look the same from the sender's side.
#:
#: So the burst rules moved to ATO, where threat-model.md 3 puts them - "A2's
#: window is short by nature", the takeover operator cannot slow down - and MULE
#: keeps only MULE_FAN_IN. Fan-in is money CONVERGING, which is the one thing a
#: mule does that a takeover does not, and it is the only rule here that
#: identifies the pattern rather than its tempo. On the current dataset: 21 MULE
#: alerts, all 21 on true mule fraud, none misattributed.
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
