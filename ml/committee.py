"""A committee of LightGBM fits, served as one booster.

Averaging several fits' log-odds takes the noise out of one fit to rare fraud
(ml/README.md). A boosted model's raw score is the sum of its trees' outputs, so
the mean of K models is one model holding every member's trees with every value
divided by K: merge() writes it, and the ONNX export, model.txt and the tree
contributions all see a single booster.
"""
import re

import lightgbm as lgb
import numpy as np

_VALUES = re.compile(r"^(leaf_value|internal_value)=(.*)$", re.M)


def _parts(text):
    """(header, [tree blocks], tail) of a model string. Each block runs from its
    `Tree=` line to the next one - the span LightGBM's own tree_sizes counts."""
    head, rest = text.split("\nTree=0\n", 1)
    trees, tail = ("Tree=0\n" + rest).split("end of trees", 1)
    return head, [b for b in re.split(r"(?=^Tree=\d+$)", trees, flags=re.M) if b], tail


def _scaled(block, k, index):
    def scale(m):
        return m.group(1) + "=" + " ".join(repr(float(v) / k) for v in m.group(2).split())
    block = re.sub(r"^Tree=\d+$", f"Tree={index}", block, count=1, flags=re.M)
    return _VALUES.sub(scale, block)


def merge(boosters):
    """One booster whose raw score is the mean of `boosters`' raw scores."""
    k = len(boosters)
    parts = [_parts(b.model_to_string()) for b in boosters]
    blocks = [_scaled(block, k, i) for i, block in
              enumerate(b for _, blocks, _ in parts for b in blocks)]
    sizes = " ".join(str(len(b.encode())) for b in blocks)
    head = re.sub(r"^tree_sizes=.*$", "tree_sizes=" + sizes, parts[0][0],
                  count=1, flags=re.M)
    return lgb.Booster(model_str=head + "\n" + "".join(blocks) + "end of trees" + parts[0][2])


def check(merged, boosters, X, tol=1e-6):
    """The merged raw score must be the members' mean - checked, not assumed: a
    model text format that changed under merge() would otherwise produce a
    plausible model that is not the committee."""
    want = np.mean([b.predict(X, raw_score=True) for b in boosters], axis=0)
    worst = float(np.max(np.abs(merged.predict(X, raw_score=True) - want)))
    if worst > tol:
        raise RuntimeError(f"the merged committee is off its members' mean by {worst:.2e}")
    return worst
