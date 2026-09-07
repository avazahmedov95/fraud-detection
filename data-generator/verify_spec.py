"""Checks a generated dataset against the parameters docs/generator-spec.md declares,
so the spec cannot drift from what the generator produces."""

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C          # noqa: E402


def _check(label, observed, expected, tol, unit=""):
    ok = abs(observed - expected) <= tol
    mark = "ok " if ok else "OFF"
    print(f"  [{mark}] {label:<38} spec {expected:>10,.3f}{unit}   "
          f"observed {observed:>10,.3f}{unit}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="out/transactions.csv")
    ap.add_argument("--persons", default="out/persons.csv")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found - run generator.py first")

    d = pd.read_csv(args.file)
    p = pd.read_csv(args.persons) if os.path.exists(args.persons) else None
    cfg = C.GeneratorConfig()
    ok = []

    print(f"\nchecking {len(d):,} events against docs/generator-spec.md\n")

    print("population and class balance")
    ok.append(_check("transactions", len(d), cfg.n_transactions, 0))
    ok.append(_check("fraud rate", d.label_is_fraud.mean(), cfg.fraud_rate, 0.002))

    if p is not None:
        is_fraud_acct = p.is_fraud_account.astype(str).str.lower() == "true"
        legit, fraud = p[~is_fraud_acct], p[is_fraud_acct]
        ok.append(_check("fresh legitimate accounts",
                         (legit.account_age_days < 30).mean(),
                         cfg.new_account_share, 0.03))
        ok.append(_check("aged fraud accounts",
                         (fraud.account_age_days >= 100).mean(), 0.30, 0.06))

    print("\nchannel mix")
    share = d.channel.value_counts(normalize=True)
    for ch, w in zip(C.CHANNELS, C.CHANNEL_WEIGHTS):
        ok.append(_check(f"  {ch}", share.get(ch, 0.0), w, 0.02))

    print("\nsession signals")
    legit_ev = d[d.label_is_fraud == 0]
    app = d[d.label_fraud_type == "APP"]
    ok.append(_check("active_call, legitimate", legit_ev.active_call.mean(),
                     C.ACTIVE_CALL_BASE_RATE, 0.01))
    if len(app):
        ok.append(_check("active_call, APP", app.active_call.mean(),
                         C.ACTIVE_CALL_APP_RATE, 0.08))

    print("\nfraud pattern shapes")
    st = d[d.label_fraud_type == "STRUCTURING"].amount_uzs / C.STRUCTURING_THRESHOLD
    if len(st):
        ok.append(_check("STRUCTURING min fraction of threshold",
                         st.min(), 0.85, 0.02))
        ok.append(_check("STRUCTURING max fraction of threshold",
                         st.max(), 0.99, 0.02))
    ato = d[d.label_fraud_type == "ATO"].groupby("sender_pinfl").size()
    if len(ato):
        within = set(ato.unique()) <= {2, 3, 4}
        print(f"  [{'ok ' if within else 'OFF'}] "
              f"{'ATO events per episode':<38} spec {'{2,3,4}':>10}   "
              f"observed {sorted(ato.unique())}")
        ok.append(within)

    print("\namount distributions")
    ok.append(_check("median legitimate amount",
                     legit_ev.amount_uzs.median(), 133_000, 25_000, " UZS"))
    ok.append(_check("all amounts within bounds",
                     float(d.amount_uzs.between(C.AMOUNT_MIN, C.AMOUNT_MAX).all()),
                     1.0, 0))

    print("\ndevices (legitimate senders must change device - see spec section 3)")
    # Stream-derived, the way features.py computes device_is_new: the FIRST event
    # from a device this sender has not used before. Counting distinct devices per
    # sender would answer a different question and would pass while the model still
    # saw nothing.
    seen, legit_new, fraud_new = {}, 0, 0
    for pinfl, dev, lab in zip(d.sender_pinfl, d.device_id, d.label_is_fraud):
        known = seen.setdefault(pinfl, set())
        if known and dev not in known:
            if lab:
                fraud_new += 1
            else:
                legit_new += 1
        known.add(dev)
    fires = legit_new + fraud_new
    precision = fraud_new / fires if fires else 0.0
    base = d.label_is_fraud.mean()
    print(f"       device_is_new fires on {legit_new} legitimate events "
          f"and {fraud_new} fraudulent")
    print(f"       as a fraud predictor: precision {precision:.1%} against a "
          f"{base:.1%} base rate ({precision/base if base else 0:.1f}x lift)")
    # Two distinct ways this feature has been useless, and each needs its own test.
    # Splittable: 30 is min_child_samples in ml/train.py, and it bounds the LEAF,
    # so what has to clear it is the total - not either class on its own.
    splittable = fires >= 30
    # Not the label: it fired on 25 rows once and every one was fraud, which is
    #100% precision and no information the label does not already carry.
    not_a_proxy = 0.0 < precision < 0.5
    print(f"  [{'ok ' if splittable else 'OFF'}] fires on {fires} rows "
          f"(needs >= 30, or no split can be formed)")
    print(f"  [{'ok ' if not_a_proxy else 'OFF'}] both classes produce it "
          f"(a device change only fraud makes is the label renamed)")
    ok.append(splittable)
    ok.append(not_a_proxy)

    print("\nkinship (must be non-zero on BOTH sides - see spec section 3)")
    fam_legit = legit_ev.is_family_transfer.mean()
    fam_fraud = d[d.label_is_fraud == 1].is_family_transfer.mean()
    print(f"       legitimate {fam_legit:.3f}   fraud {fam_fraud:.3f}")
    both = fam_legit > 0.05 and fam_fraud > 0.005
    print(f"  [{'ok ' if both else 'OFF'}] both sides modelled")
    if not both:
        print("       ^ if either is zero, is_family separates the classes by "
              "construction\n         and any importance it shows is an artefact.")
    ok.append(both)

    failed = len(ok) - sum(ok)
    print(f"\n{sum(ok)}/{len(ok)} checks passed")
    if failed:
        print(f"\n{failed} quantity(ies) moved. Either the generator changed and "
              f"docs/generator-spec.md\nneeds updating, or a change had an "
              f"unintended effect. Both are worth knowing.")
        sys.exit(1)
    print("\nDataset matches the specification.")


if __name__ == "__main__":
    main()
