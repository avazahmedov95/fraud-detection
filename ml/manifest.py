"""Ties a served model to what produced it: artefacts, dataset, feature contract
and capability profile in one record, checkable against a deployment. Catches a
partial re-export, a capability toggled after training, a regenerated dataset.
"""

import hashlib
import json
import os
import sys

MODELS = os.getenv(
    "MODELS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join(MODELS, "manifest.json")

#: Artefacts whose content identifies the model. Absent ones are recorded as absent,
#: not skipped: "model.txt was not exported" is a fact a later check needs.
ARTEFACTS = ("model.onnx", "model.txt", "model.joblib", "feature_names.json")
DATASET = (("transactions.csv", "data-generator/out/transactions.csv"),
           ("persons.csv", "data-generator/out/persons.csv"))


def _sha256(path):
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _commit():
    """The commit HEAD points at, read from .git directly - not `git rev-parse`, which
    fails differently on every host from inside a build step. Absent is fine.
    """
    head = os.path.join(ROOT, ".git", "HEAD")
    if not os.path.exists(head):
        return None
    ref = open(head, encoding="utf-8").read().strip()
    if not ref.startswith("ref: "):
        return ref
    p = os.path.join(ROOT, ".git", ref[5:])
    return open(p, encoding="utf-8").read().strip() if os.path.exists(p) else None


def _contract():
    sys.path.insert(0, os.path.join(ROOT, "stream-processor"))
    import capabilities as CAP
    names = CAP.feature_names()
    return {
        "features": names,
        # One value to compare, so a check need not diff two lists to say the contract moved.
        "features_sha256": hashlib.sha256("\n".join(names).encode()).hexdigest(),
        "capability_profile": dict(CAP.MODES),
    }


def build():
    import datetime
    man = {
        "exported_at": datetime.datetime.now(datetime.timezone.utc)
                               .isoformat(timespec="seconds"),
        "commit": _commit(),
        "artefacts": {a: _sha256(os.path.join(MODELS, a)) for a in ARTEFACTS},
        "dataset": {name: {"sha256": _sha256(os.path.join(ROOT, rel)),
                           "path": rel} for name, rel in DATASET},
        "contract": _contract(),
    }
    metrics = os.path.join(MODELS, "metrics.json")
    if os.path.exists(metrics):
        m = json.load(open(metrics, encoding="utf-8"))
        # A pointer, not a copy: metrics.json is the record of the numbers.
        man["metrics"] = {k: m[k] for k in ("roc_auc", "pr_auc") if k in m}
    return man


def write():
    man = build()
    with open(PATH, "w", encoding="utf-8") as fh:
        json.dump(man, fh, indent=2)
    print(f"wrote {PATH}")
    print(f"  contract  {man['contract']['features_sha256'][:12]}  "
          f"({len(man['contract']['features'])} features)")
    for a, h in man["artefacts"].items():
        print(f"  {a:<20}{(h or 'ABSENT')[:12]}")
    return man


def check():
    """Compare the current tree against the record. Returns a list of problems."""
    if not os.path.exists(PATH):
        return [f"{PATH} does not exist - run `python ml/export_onnx.py`"]
    rec = json.load(open(PATH, encoding="utf-8"))
    now = build()
    problems = []

    for a, want in rec["artefacts"].items():
        got = now["artefacts"].get(a)
        if want and not got:
            problems.append(f"{a} is in the manifest but missing from {MODELS}")
        elif want and got != want:
            problems.append(
                f"{a} differs from the manifest ({got[:12]} vs {want[:12]}): the "
                f"served artefact is not the one this record describes")

    if now["contract"]["features_sha256"] != rec["contract"]["features_sha256"]:
        problems.append(
            "the feature contract has moved since export. capability profile "
            f"then {rec['contract']['capability_profile']}, now "
            f"{now['contract']['capability_profile']}. The model is being served "
            "a vector it was not fitted to - retrain and re-export.")

    for name, d in rec["dataset"].items():
        got = now["dataset"].get(name, {}).get("sha256")
        if d["sha256"] and got != d["sha256"]:
            problems.append(
                f"{name} is not the file this model was trained on "
                f"({(got or 'absent')[:12]} vs {d['sha256'][:12]}); every "
                f"reported figure describes the other one")
    return problems


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "write":
        write()
        return 0
    problems = check()
    if not problems:
        rec = json.load(open(PATH, encoding="utf-8"))
        print(f"manifest ok - exported {rec['exported_at']}"
              + (f", commit {rec['commit'][:8]}" if rec.get("commit") else ""))
        return 0
    for p in problems:
        print(f"MISMATCH: {p}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
