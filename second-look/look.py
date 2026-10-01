"""TabPFN over its context pieces, read the way ml/experiments/models.py reads them:
one member per piece, each with the median imputer and the scaler fitted on its
piece, the members' probabilities averaged. Each member caches its context once,
at start, so an answer does not reprocess five thousand rows."""

import hashlib
import json
import os
import tempfile

import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _named(checkpoint, name):
    """TabPFN reads the model's version from the file's NAME - "v3.5" in it - and
    without one silently takes the oldest version, with another configuration and
    other answers. The mount has a name of its own, so a link restores the name the
    cut-off was chosen under."""
    link = os.path.join(tempfile.gettempdir(), name)
    if not os.path.lexists(link):
        os.symlink(checkpoint, link)
    return link


class SecondLook:
    def __init__(self, models_dir, checkpoint):
        from tabpfn import TabPFNClassifier

        with open(os.path.join(models_dir, "second_look.json"), encoding="utf-8") as fh:
            self.spec = json.load(fh)
        if _sha256(checkpoint) != self.spec["checkpoint_sha256"]:
            raise ValueError(f"{checkpoint} is not {self.spec['checkpoint']}, the weights "
                             f"the second look's cut-off was chosen with")
        checkpoint = _named(checkpoint, self.spec["checkpoint"])
        context = np.load(os.path.join(models_dir, "second_look.npz"))
        self.members = []
        for k in range(self.spec["pieces"]):
            rows = context["piece"] == k
            member = make_pipeline(
                SimpleImputer(strategy="median"), StandardScaler(),
                # A full-precision cache: the cut-off was chosen on uncached answers.
                TabPFNClassifier(model_path=checkpoint, n_estimators=1, random_state=k + 1,
                                 fit_mode="fit_with_cache", kv_cache_precision="auto"))
            member.fit(context["X"][rows], context["y"][rows])
            self.members.append(member)
        # Every member has built its own copy from the raw checkpoint, which TabPFN
        # keeps in a cache of its own: a gigabyte nothing reads again.
        from tabpfn.model_loading import _load_checkpoint_cached
        _load_checkpoint_cached.cache_clear()

    def score(self, features):
        """TabPFN's probability of fraud for the vector the job scored; None is a gap."""
        x = np.array([[np.nan if v is None else v for v in features]], dtype=np.float32)
        return float(np.mean([m.predict_proba(x)[0, 1] for m in self.members]))
