"""Turns a model-only alert into words: the top tree contributions, phrased as
findings an analyst can read out.

Refuses to speak rather than risk a wrong reason - see Explainer.explain.
Why it runs here and not on the scoring path: docs/irp-framing.md 9.2.
"""

import logging
import os

log = logging.getLogger("explain")

#: Statuses an explanation can carry. Stored beside it so a missing explanation
#: is never mistaken for "nothing was notable".
OK = "OK"
NO_MODEL = "NO_MODEL"                 # artefact absent from the image
NO_FEATURES = "NO_FEATURES"           # the alert predates feature publication
MODEL_MISMATCH = "MODEL_MISMATCH"     # joblib disagrees with what scored
FAILED = "FAILED"

#: Max |recomputed - recorded| before the explanation is refused. ONNX and the
#: joblib booster agree to 3.3e-07 across 50k events; 1e-4 flags a real
#: divergence (a stale artefact, a retrain that was not re-exported) without
#: firing on float noise.
TOLERANCE = 1e-4

TOP_N = 3

_HERE = os.path.dirname(os.path.abspath(__file__))

#: A bare LightGBM Booster, written by ml/export_onnx.py from the same object
#: it converts to ONNX. Not model.joblib: unpickling an LGBMClassifier drags
#: scikit-learn (~30 MB) into a service that only reads trees.
_MODEL_CANDIDATES = (
    os.path.join(_HERE, "model.txt"),
    os.path.join(_HERE, "..", "ml", "models", "model.txt"),
)

#: The booster itself carries no usable names - it was trained on a bare numpy
#: array, so its feature_name() is ["Column_0", ...]. The real order lives in
#: feature_names.json, the same artefact `serve-prep` copies next to the Flink
#: job. Labelling a contribution with the wrong feature would be the worst
#: available outcome here: a confident, specific, wrong reason. So the count is
#: checked against the booster and mislabelling fails loudly.
_NAMES_CANDIDATES = (
    os.path.join(_HERE, "feature_names.json"),
    os.path.join(_HERE, "..", "ml", "models", "feature_names.json"),
)


def _uzs(v):
    return f"{int(round(v)):,}".replace(",", " ")


#: feature -> (phrase, value formatter). The phrase reads as a finding, not as a
#: column name: an analyst acts on "payee's account is 2 days old", not on
#: "receiver_age = 2.0". Anything absent here falls back to the raw name, which
#: is ugly but never wrong.
_PHRASES = {
    "log_amount":             ("amount",                        lambda v: _uzs(pow(2.718281828, v) - 1) + " UZS"),
    "amount_to_mean":         ("amount vs this sender's average", lambda v: f"{v:.1f}x"),
    "amount_z":               ("amount vs this sender's usual spread", lambda v: f"{v:+.1f} sigma"),
    "is_new_payee":           ("payee never paid before",       lambda v: "yes" if v else "no"),
    "vel_10m":                ("transfers in the last 10 min",  lambda v: f"{int(v)}"),
    "vel_1h":                 ("transfers in the last hour",    lambda v: f"{int(v)}"),
    "distinct_payees_10m":    ("distinct payees in 10 min",     lambda v: f"{int(v)}"),
    "sub_threshold_1h":       ("just-under-threshold transfers in an hour", lambda v: f"{int(v)}"),
    "secs_since_last":        ("time since the sender's last transfer", lambda v: f"{v/60:.0f} min"),
    "daily_sum_ratio":        ("share of the daily limit used", lambda v: f"{v:.0%}"),
    "hour":                   ("hour of day (UTC)",             lambda v: f"{int(v):02d}:00"),
    "cross_network":          ("UzCard <-> HUMO transfer",      lambda v: "yes" if v else "no"),
    "receiver_age":           ("payee's account age",           lambda v: "unknown" if v != v or v < 0 else f"{int(v)} days"),
    "receiver_is_fresh":      ("payee's account is newly opened", lambda v: "unknown" if v != v else ("yes" if v else "no")),
    "rcv_distinct_senders_1h": ("distinct senders paying this payee in an hour", lambda v: f"{int(v)}"),
    "rcv_inflow_1h":          ("money into this payee in an hour", lambda v: _uzs(pow(2.718281828, v) - 1) + " UZS"),
    "device_is_new":          ("device never seen for this sender", lambda v: "yes" if v else "no"),
    "geo_is_anomaly":         ("operation away from the sender's usual region", lambda v: "yes" if v else "no"),
    "active_call":            ("phone call active while confirming", lambda v: "yes" if v else "no"),
    "secs_login_z":           ("hesitation before confirming vs this sender's habit", lambda v: f"{v:+.1f} sigma"),
    "ch_mobile_app":          ("channel: mobile app",           lambda v: "yes" if v else "no"),
    "ch_ussd":                ("channel: USSD",                 lambda v: "yes" if v else "no"),
    "ch_web":                 ("channel: web",                  lambda v: "yes" if v else "no"),
    "ch_atm":                 ("channel: ATM",                  lambda v: "yes" if v else "no"),
}


def phrase(name: str, value: float, contribution: float) -> str:
    """One finding, as a line an analyst can read out to a customer."""
    label, fmt = _PHRASES.get(name, (name, lambda v: f"{v:g}"))
    try:
        shown = fmt(value)
    except Exception:                                  # noqa: BLE001
        shown = f"{value:g}"
    return f"{label}: {shown} ({contribution:+.2f})"


def _load_names():
    path = next((p for p in _NAMES_CANDIDATES if p and os.path.exists(p)), None)
    if path is None:
        return None
    import json
    with open(path, encoding="utf-8") as fh:
        names = json.load(fh)
    return list(names) if isinstance(names, list) else None


class Explainer:
    """Lazily loaded. Absent artefacts disable explanation; they never crash the
    consumer, because an unexplained case is still a case worth queueing."""

    def __init__(self, feature_names=None):
        self._booster = None
        self._names = feature_names
        self._loaded = False
        self._logged_missing = False

    def _load(self):
        if self._loaded:
            return
        self._loaded = True
        path = next((p for p in _MODEL_CANDIDATES if p and os.path.exists(p)), None)
        if path is None:
            log.warning("no model.txt found; alerts will be queued without an "
                        "explanation (status %s). Run ml/export_onnx.py.", NO_MODEL)
            return
        try:
            import lightgbm as lgb
            self._booster = lgb.Booster(model_file=path)
            if self._names is None:
                self._names = _load_names()
            n_model = self._booster.num_feature()
            if self._names is None or len(self._names) != n_model:
                log.error("feature names (%s) do not match the model (%d "
                          "features); refusing to explain rather than label "
                          "contributions with the wrong feature.",
                          len(self._names) if self._names else "missing", n_model)
                self._booster = None
                return
            log.info("explanations enabled (%s, %d trees, %d features)",
                     os.path.basename(path), self._booster.num_trees(),
                     len(self._names))
        except Exception as exc:                       # noqa: BLE001
            hint = ""
            if isinstance(exc, OSError) and "shared object" in str(exc):
                # The wheel is installed and the import still fails: a native
                # dependency is missing from the IMAGE, which no amount of
                # pip-checking reveals. Name the fix rather than the symptom.
                hint = (" - this is a missing system library, not a bad model. "
                        "lightgbm needs the OpenMP runtime; install libgomp1 in "
                        "the image (see infra/case-manager/Dockerfile).")
            log.warning("could not load %s, explanations disabled: %s%s",
                        path, exc, hint)
            self._booster = None

    def explain(self, features, recorded_score):
        """(status, [top-N phrases]) for one alert.

        `features` is the vector the JOB scored on, republished in the alert -
        not recomputed here. Recomputing would need the sender's streaming
        state, which does not exist in this process, and would explain a
        different event than the one that alerted.
        """
        # Features first: checking the model first made NO_MODEL mask whether
        # the alert carried a vector at all, hiding the upstream problem.
        if not features:
            return NO_FEATURES, []
        self._load()
        if self._booster is None:
            return NO_MODEL, []
        try:
            import numpy as np
            x = np.asarray([features], dtype=np.float64)
            contrib = self._booster.predict(x, pred_contrib=True)[0]
            margin = float(contrib.sum())              # log-odds incl. the bias
            proba = 1.0 / (1.0 + pow(2.718281828, -margin))
            if recorded_score is not None and \
                    abs(proba - float(recorded_score)) > TOLERANCE:
                # Say nothing rather than say something plausible about the
                # wrong model. A confident-sounding wrong reason is worse than
                # an admitted absence.
                log.error("explanation model disagrees with the scorer "
                          "(%.6f vs %.6f) - refusing to explain. model.joblib "
                          "and model.onnx are probably out of step; re-export.",
                          proba, recorded_score)
                return MODEL_MISMATCH, []
            pairs = sorted(zip(self._names, features, contrib[:len(features)]),
                           key=lambda t: -abs(t[2]))
            # Only what pushed the score UP. A negative contribution is a reason
            # the model was less suspicious, which is not what the case is about.
            top = [p for p in pairs if p[2] > 0][:TOP_N]
            return OK, [phrase(n, v, c) for n, v, c in top]
        except Exception as exc:                       # noqa: BLE001
            log.warning("explanation failed: %s", exc)
            return FAILED, []
