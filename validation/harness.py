"""What both public-dataset adapters share: the unit conversion, the profile a
foreign dataset gets, the deployed feature extractor over its rows, and one fit of
train.py's recipe scored on the held-out rows."""

import os
import sys
import time
import warnings
from collections import defaultdict
from typing import NamedTuple

import numpy as np

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _path in (os.path.join(_ROOT, "stream-processor"), os.path.join(_ROOT, "ml")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import capabilities as CAP                                     # noqa: E402
import features as F                                            # noqa: E402
import train as T                                               # noqa: E402
from rules import ReceiverState, SenderState                    # noqa: E402

# LightGBM's sklearn wrapper names the columns it was fitted on, then warns on
# every prediction from the same unnamed array.
warnings.filterwarnings("ignore", message="X does not have valid feature names")


#: Median legitimate amount on this project's own data, in UZS: foreign amounts are
#: rescaled onto it, a unit conversion rather than a tuning.
OUR_MEDIAN_UZS = 138_740.0


def scale_factor(amounts, our_median_uzs=OUR_MEDIAN_UZS):
    """One multiplier for the whole dataset, from the medians."""
    med = float(amounts.median())
    return our_median_uzs / med if med > 0 else 1.0


class Event(NamedTuple):
    """One foreign row, translated: `ev` carries only the fields the dataset has."""
    ev: dict
    ts: int
    label: int


def capability_profile(*off, payee_identity="pinfl"):
    """What the dataset cannot supply switched OFF - an absent field would reach the
    extractor as a zero - and the payee keyed by account, since these datasets name
    accounts and issue no PANs. No analyst confirmed their frauds either, so
    confirmed_cases is off on every one."""
    for key in off + ("confirmed_cases",):
        CAP.MODES[key] = "off"
    CAP.MODES["payee_identity"] = payee_identity


def available_features():
    """(indices, names) of the columns the active profile can supply."""
    off = {f for cap in CAP.REGISTRY if CAP.MODES.get(cap.key) == "off"
           for f in cap.features}
    idx = [i for i, n in enumerate(F.FEATURE_NAMES) if n not in off]
    return idx, [F.FEATURE_NAMES[i] for i in idx]


def extract_features(events, total):
    """The deployed extractor over translated events, as fraud_job runs it.
    Returns (X, y, ts), one column per `features.FEATURE_NAMES`."""
    X = np.zeros((total, len(F.FEATURE_NAMES)), dtype="float32")
    y = np.zeros(total, dtype="int8")
    ts = np.zeros(total, dtype="int64")
    senders, receivers = defaultdict(SenderState), defaultdict(ReceiverState)
    started, n = time.time(), 0
    for n, e in enumerate(events, 1):
        if n == 1 and not F.payee_key(e.ev):
            # Every payee would share one key: build the profile with
            # capability_profile(...), which keys the payee by account.
            raise SystemExit("the payee key resolves empty on this stream")
        key = F.payee_key(e.ev)
        sender = senders[e.ev["sender_pinfl"]]
        paid_sender = receivers.get(e.ev["sender_pinfl"])
        X[n - 1] = F.to_vector(F.extract(
            e.ev, sender, e.ts, receivers[key],
            sender_inbound_ts=(paid_sender.last_inbound_ts if paid_sender else None)))
        F.update_state(sender, e.ev, e.ts)
        F.update_receiver_state(receivers[key], e.ev, e.ts)
        y[n - 1], ts[n - 1] = e.label, e.ts
        if n % 200_000 == 0 or n == total:
            rate = n / max(time.time() - started, 1e-9)
            print(f"\r  {n:,} / {total:,} rows, {rate:,.0f}/s", end="", file=sys.stderr,
                  flush=True)
    print(file=sys.stderr)
    return X[:n], y[:n], ts[:n]


def cached_matrix(cache, build):
    """The feature matrix, from `cache` when it holds the columns the active profile
    computes; otherwise built by `build()` (hours on these files) and cached."""
    idx, names = available_features()
    if os.path.exists(cache):
        with np.load(cache, allow_pickle=False) as z:
            if [str(n) for n in z["names"]] == names:
                return z["X"], z["y"].astype("int8")
        raise SystemExit(f"{cache} was built from other columns - delete it and re-run")
    X, y = build()
    np.savez_compressed(cache, X=X[:, idx], y=y, names=np.array(names))
    return X[:, idx], y


def fit_and_score(X, y, fit, cut, seed=0):
    """train.py's recipe fitted on rows [:fit], its cut-off - the F1 peak - chosen on
    [fit:cut], and the rows from `cut` on scored at it. Weighted only above 0.5%
    fraud, as train.py fits."""
    model = T.make_model(T.class_weight(int(y[:fit].sum()), int((y[:fit] == 0).sum())),
                         random_state=seed)
    model.fit(X[:fit], y[:fit])
    threshold = T.choose_review_cutoff(y[fit:cut], model.predict_proba(X[fit:cut])[:, 1])
    alert = model.predict_proba(X[cut:])[:, 1] >= threshold
    yte = y[cut:]
    caught, alerts, frauds = int(yte[alert].sum()), int(alert.sum()), int(yte.sum())
    recall, precision = caught / max(frauds, 1), caught / max(alerts, 1)
    return {"frauds": frauds, "alerts": alerts, "caught": caught, "recall": recall,
            "precision": precision,
            "f1": 2 * precision * recall / max(precision + recall, 1e-12)}


def print_scores(s):
    print(f"\nthis project's model on the held-out rows ({s['frauds']:,} fraud):")
    print(f"  recall     {s['recall']:.1%}   ({s['caught']:,} caught)")
    print(f"  precision  {s['precision']:.1%}   ({s['alerts']:,} alerts)")
    print(f"  F1         {s['f1']:.1%}")
