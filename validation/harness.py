"""What every external-dataset run does identically: the unit conversion, the
replay over the DEPLOYED rule engine, and the report sections. Each adapter owns
only its dataset's shape - file, columns, identifiers, and which capabilities the
data can support - so the results stay one measurement.

Not `replay.py`: stream-processor/experiments/replay.py does this for the
project's own CSV, and pytest imports modules by bare name.
"""

import os
import sys
import time
from collections import Counter, defaultdict
from typing import NamedTuple

import pandas as pd

_SP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "stream-processor")
if _SP not in sys.path:
    sys.path.insert(0, _SP)

import capabilities as CAP                                     # noqa: E402
import config as C                                             # noqa: E402
import rules as _R                                             # noqa: E402
import features as F                                            # noqa: E402
from rules import ReceiverState, SenderState, evaluate          # noqa: E402


#: Median legitimate amount on this project's own data, in UZS: foreign amounts are
#: rescaled onto it so rules with absolute thresholds can fire at all.
OUR_MEDIAN_UZS = 138_740.0


def scale_factor(amounts, our_median_uzs=OUR_MEDIAN_UZS):
    """One multiplier for the whole dataset, from the medians - a unit conversion,
    not a per-rule tuning."""
    med = float(amounts.median())
    return our_median_uzs / med if med > 0 else 1.0


class Event(NamedTuple):
    """One foreign row, translated: `ev` carries only fields the dataset has."""
    ev: dict
    ts: int
    label: int


def replay(events, total=None):
    """Run the deployed rule engine over translated events, in stream order.
    Receiver state is keyed by `receiver_pinfl`, the identifier these datasets
    carry. `total` prints progress on stderr, keeping a redirected report clean."""
    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    rows, hits_by_class = [], defaultdict(Counter)
    checked, started = False, time.time()

    for n, e in enumerate(events, 1):
        if not checked:
            _require_a_payee_key(e.ev)
            checked = True
        res = evaluate(e.ev, senders[e.ev["sender_pinfl"]],
                       e.ts, receivers[e.ev["receiver_pinfl"]])
        rows.append((e.label, res["cep_score"], res["decision"]))
        for hit in res["rule_hits"]:
            hits_by_class["fraud" if e.label else "legit"][hit] += 1
        if n % PROGRESS_EVERY == 0:
            _progress(n, total, started)
    if total:
        _progress(n, total, started, final=True)

    return (pd.DataFrame(rows,
                         columns=["label", "cep_score", "decision"]),
            hits_by_class)


def capability_profile(*off, payee_identity="pinfl"):
    """The profile a foreign dataset gets: what it cannot supply switched OFF - an
    absent field would reach the extractor as a zero - and the payee keyed by
    account ("pinfl"), since these datasets name accounts and issue no PANs."""
    for key in off:
        CAP.MODES[key] = "off"
    CAP.MODES["payee_identity"] = payee_identity
    print("capability profile for this run:")
    print(CAP.describe())
    print()


def _require_a_payee_key(ev):
    """Refuse to replay a stream whose payee key resolves empty: every payee would
    share one key, so is_new_payee, DISTINCT_PAYEE_BURST and NEW_PAYEE_HIGH_AMOUNT
    would all measure the adapter. Raises rather than warns."""
    if not F.payee_key(ev):
        raise SystemExit(
            "the payee key resolves empty on this stream. features.payee_key falls "
            "back to receiver_card and this dataset issues no PANs, so every sender "
            "would have exactly one payee forever. Build the profile with "
            "replay.capability_profile(...), which sets payee_identity=pinfl.")


#: Rows between progress lines. Large enough that the print costs nothing, small
#: enough that a stalled run is obvious within seconds.
PROGRESS_EVERY = 100_000


def _progress(n, total, started, final=False):
    if not total:
        return
    secs = time.time() - started
    rate = n / secs if secs > 0 else 0.0
    left = (total - n) / rate if rate > 0 else 0.0
    print(f"\r  replayed {n:,} / {total:,} ({n/total:.0%})  "
          f"{rate:,.0f} rows/s  {left/60:.0f} min left    ",
          end="\n" if final else "", file=sys.stderr, flush=True)


def _head(title, width):
    print("=" * width)
    print(title)
    print("=" * width)


def section_lift(res, hits, positive="fraud", width=70):
    """A. Per-rule lift: threshold-free, so it answers whether a rule carries signal
    on foreign data whatever the decision layer does."""
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


def section_decision(res, hits, positive="fraud", width=70):
    """B. What the deployed decision layer did. Last on purpose: the threshold is
    calibrated for this project's profile and base rate, the least transferable part."""
    n_pos = int((res.label == 1).sum())
    n_neg = int((res.label == 0).sum())
    flagged = res.decision == "REVIEW"
    pos_rate = flagged[res.label == 1].mean() if n_pos else 0.0
    neg_rate = flagged[res.label == 0].mean() if n_neg else 0.0

    _head("B. DECISION LAYER - does the deployed threshold still work?", width)
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
    review_at = _R._review_threshold()
    if abs(review_at - C.REVIEW_THRESHOLD) > 1e-9:
        print(f"\n  REVIEW threshold : {review_at:.2f}  "
              f"(scaled from {C.REVIEW_THRESHOLD:.2f} for this profile)")
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


def available_features():
    """(indices, names) of the contract columns the active profile can supply;
    FEATURE_NAMES is fixed at import, so switched-off columns are dropped here."""
    import features as F
    off = {f for cap in CAP.REGISTRY if CAP.MODES.get(cap.key) == "off"
           for f in cap.features}
    idx = [i for i, n in enumerate(F.FEATURE_NAMES) if n not in off]
    return idx, [F.FEATURE_NAMES[i] for i in idx]


def extract_features(events, total):
    """This project's model inputs over translated events, computed by the deployed
    extractor as fraud_job computes them. Returns (X, y, ts), one column per
    `features.FEATURE_NAMES`; `available_features()` says which to keep."""
    import numpy as np
    import features as F
    X = np.zeros((total, len(F.FEATURE_NAMES)), dtype="float32")
    y = np.zeros(total, dtype="int8")
    ts = np.zeros(total, dtype="int64")
    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    started, n = time.time(), 0
    for n, e in enumerate(events, 1):
        if n == 1:
            _require_a_payee_key(e.ev)
        key = F.payee_key(e.ev)
        sender = senders[e.ev["sender_pinfl"]]
        X[n - 1] = F.to_vector(F.extract(e.ev, sender, e.ts,
                                         receivers[key]))
        F.update_state(sender, e.ev, e.ts)
        F.update_receiver_state(receivers[key], e.ev, e.ts)
        y[n - 1], ts[n - 1] = e.label, e.ts
        if n % PROGRESS_EVERY == 0:
            _progress(n, total, started)
    _progress(n, total, started, final=True)
    return X[:n], y[:n], ts[:n]

