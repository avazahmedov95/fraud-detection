"""What every external-dataset run does identically.

Three public datasets have three shapes, so there are three adapters. What they must
NOT have is three copies of the measurement: a change to how lift is computed would
then land in one report and silently not in the others, and the three results are
only comparable because they are the same measurement.

So each adapter owns exactly its dataset's shape - which file, which columns, which
identifiers, which capabilities the data can support - and everything downstream of
"a translated event" lives here: the unit conversion, the replay over the DEPLOYED
rule engine, and the report sections.
"""

import os
import sys
from collections import Counter, defaultdict
from typing import NamedTuple, Optional

import pandas as pd

_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")
if _SP not in sys.path:
    sys.path.insert(0, _SP)

import capabilities as CAP                                     # noqa: E402
import config as C                                             # noqa: E402
import rules as _R                                             # noqa: E402
from rules import ReceiverState, SenderState, evaluate          # noqa: E402


#: Median legitimate amount on this project's own generated data, in UZS. Foreign
#: amounts are rescaled onto it so that rules carrying ABSOLUTE thresholds
#: (STRUCTURING_THRESHOLD, NEW_PAYEE_ABS_FLOOR, LIMIT_DAILY) can fire at all.
OUR_MEDIAN_UZS = 138_740.0


def scale_factor(amounts, our_median_uzs=OUR_MEDIAN_UZS):
    """One multiplier for the whole dataset, taken from the medians.

    A unit conversion, not tuning. It is deliberately a single number rather than a
    per-rule adjustment: tuning would mean choosing it to make a rule fire, and one
    factor fixed by the medians cannot be chosen that way.
    """
    med = float(amounts.median())
    return our_median_uzs / med if med > 0 else 1.0


class Event(NamedTuple):
    """One foreign row, translated into what the extractor expects.

    `ev` carries only fields the dataset actually has; anything absent stays absent
    and its capability is switched off, so no rule fires on a fabricated zero.
    `typology` is an optional per-row pattern label, printed by
    `section_by_group`.
    """
    ev: dict
    ts: int
    label: int
    receiver_age: Optional[int] = None
    typology: str = ""


def replay(events):
    """Run the deployed rule engine over translated events, in stream order.

    Receiver state is keyed by `receiver_pinfl` because that is the identifier these
    datasets carry; the payee_identity capability, which chooses between PAN and
    person on this project's own rail, has nothing to choose between here.
    """
    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    rows, hits_by_class = [], defaultdict(Counter)

    for e in events:
        res = evaluate(e.ev, e.receiver_age, senders[e.ev["sender_pinfl"]],
                       e.ts, receivers[e.ev["receiver_pinfl"]])
        rows.append((e.label, res["cep_score"], res["decision"], e.typology))
        for hit in res["rule_hits"]:
            hits_by_class["fraud" if e.label else "legit"][hit] += 1

    return (pd.DataFrame(rows,
                         columns=["label", "cep_score", "decision", "typology"]),
            hits_by_class)


def capabilities_off(*keys):
    """Switch off what the dataset cannot supply, and print the resulting profile.

    Switching OFF rather than leaving a field absent is the point: an absent field
    reaches the extractor as a zero, and a rule firing on that measures the adapter,
    not the data. An unknown key raises - see test_capabilities.py.
    """
    for key in keys:
        CAP.MODES[key] = "off"
    print("capability profile for this run:")
    print(CAP.describe())
    print()


def _head(title, width):
    print("=" * width)
    print(title)
    print("=" * width)


def section_lift(res, hits, positive="fraud", width=70):
    """A. Per-rule lift - the measure that survives a wrong threshold.

    Threshold-free, and therefore the section that actually answers "does this rule
    carry signal on foreign data". A rule that fires more often on the positive class
    than on the negative one is discriminating, whatever the decision layer does.
    """
    n_pos = int((res.label == 1).sum())
    n_neg = int((res.label == 0).sum())

    _head("A. PER-RULE LIFT - does each rule carry signal on foreign data?", width)
    print(f"{'rule':<26}{'on ' + positive:>12}{'on legit':>12}{'lift':>9}")
    lifts = []
    for rule in sorted(set(hits["fraud"]) | set(hits["legit"])):
        f = hits["fraud"][rule] / max(n_pos, 1)
        l = hits["legit"][rule] / max(n_neg, 1)
        lifts.append((rule, f, l, (f / l) if l > 0 else float("inf")))
    for rule, f, l, lift in sorted(lifts, key=lambda x: -x[3]):
        shown = "inf" if lift == float("inf") else f"{lift:.1f}x"
        print(f"{rule:<26}{f:>11.2%}{l:>12.2%}{shown:>9}")
    return lifts


def section_by_group(res, title, notes, width=70):
    """B. Recall split by the dataset's own typology labels, when it has them.

    Only a dataset that says WHICH pattern each positive row belongs to can answer
    "which pattern does this system miss", and that is a different question from
    aggregate recall.
    """
    _head(title, width)
    for line in notes:
        print(line)
    print()
    labelled = res[(res.label == 1) & (res.typology != "")]
    if labelled.empty:
        print("  No typology labels available; this section is empty.")
        return
    print(f"{'typology':<20}{'n':>8}{'flagged':>10}{'recall':>10}")
    for name, g in labelled.groupby("typology"):
        flagged = g.decision.isin(["REVIEW", "BLOCK"])
        print(f"{name:<20}{len(g):>8,}{int(flagged.sum()):>10,}"
              f"{flagged.mean():>10.1%}")


def section_decision(res, hits, positive="fraud", width=70):
    """C. What the deployed decision layer did with those rules.

    Reported after the lift table on purpose: the threshold is calibrated for this
    project's own capability profile and base rate, so on foreign data it is the
    least transferable thing here. A weak result in this section beside a strong one
    in A is a statement about calibration, not about the features.
    """
    n_pos = int((res.label == 1).sum())
    n_neg = int((res.label == 0).sum())
    flagged = res.decision.isin(["REVIEW", "BLOCK"])
    pos_rate = flagged[res.label == 1].mean() if n_pos else 0.0
    neg_rate = flagged[res.label == 0].mean() if n_neg else 0.0

    _head("C. DECISION LAYER - does the deployed threshold still work?", width)
    print(f"  {positive + ' flagged':<17}: {int(flagged[res.label==1].sum()):>7,}"
          f" / {n_pos:<9,} ({pos_rate:.1%})")
    print(f"  {'legit flagged':<17}: {int(flagged[res.label==0].sum()):>7,}"
          f" / {n_neg:<9,} ({neg_rate:.2%})")

    scores = res.cep_score
    if n_pos:
        print(f"\n  cep_score, {positive:<12}: max {scores[res.label==1].max():.2f}"
              f"   mean {scores[res.label==1].mean():.3f}")
    if n_neg:
        print(f"  cep_score, {'legit':<12}: max {scores[res.label==0].max():.2f}"
              f"   mean {scores[res.label==0].mean():.3f}")

    # The threshold actually applied - under capability scaling this is not the
    # configured constant, and printing the constant hid the mechanism.
    review_at, block_at = _R._thresholds()
    if abs(review_at - C.REVIEW_THRESHOLD) > 1e-9:
        print(f"\n  REVIEW threshold : {review_at:.2f}  "
              f"(scaled from {C.REVIEW_THRESHOLD:.2f} for this profile)")
        print(f"  BLOCK  threshold : {block_at:.2f}  "
              f"(scaled from {C.BLOCK_THRESHOLD:.2f})")
    else:
        print(f"\n  REVIEW threshold : {review_at:.2f}")

    if pos_rate > 0 and neg_rate > 0:
        print(f"\n  decision-layer lift: {pos_rate/neg_rate:.1f}x")

    top = scores[res.label == 1].max() if n_pos else 0.0
    n_rules = len(set(hits["fraud"]) | set(hits["legit"]))
    if n_pos and pos_rate == 0.0 and top > 0:
        print(f"\n  Nothing crossed the threshold: the highest score any "
              f"{positive} reached")
        print(f"  was {top:.2f}, against a REVIEW cutoff of {review_at:.2f}.")
        print("  The CEP score is ADDITIVE, so crossing it normally takes two")
        print("  rules firing together. With this capability profile only")
        print(f"  {n_rules} rule(s) can fire at all, and they rarely co-occur.")
        print("\n  This is a finding about threshold calibration, not about the")
        print("  features: see the lift table above, where the rules do separate")
        print("  the classes. A deployment with fewer integrations does not get a")
        print("  slightly worse rule layer - it gets a silent one.")

    if n_pos and n_neg:
        best = None
        for t in sorted(set(round(v, 2) for v in scores if v > 0)):
            f_at = (scores[res.label == 1] >= t).mean()
            l_at = (scores[res.label == 0] >= t).mean()
            if f_at > 0 and l_at > 0 and f_at / l_at > (best[3] if best else 0):
                best = (t, f_at, l_at, f_at / l_at)
        if best:
            t, f_at, l_at, lift = best
            print(f"\n  Best cutoff on this data: {t:.2f} -> flags {f_at:.1%} of "
                  f"{positive}, {l_at:.2%} of legit ({lift:.1f}x lift).")
            print("  Reported to size the gap, NOT adopted - tuning a threshold on")
            print("  the validation set is what this exercise exists to avoid.")
