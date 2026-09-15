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
    ap.add_argument("--profile", choices=("baseline", "realistic"), default="realistic",
                    help="the dataset of record is the realistic profile")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found - run generator.py first")

    d = pd.read_csv(args.file)
    p = pd.read_csv(args.persons) if os.path.exists(args.persons) else None
    cfg = C.realistic() if args.profile == "realistic" else C.GeneratorConfig()
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
                         (fraud.account_age_days >= 100).mean(),
                         cfg.aged_fraud_share, 0.06))

    print("\nsession signals")
    legit_ev = d[d.label_is_fraud == 0]
    app = d[d.label_fraud_type == "APP"]
    ok.append(_check("active_call, legitimate", legit_ev.active_call.mean(),
                     cfg.active_call_base_rate, 0.01))
    if len(app):
        ok.append(_check("active_call, APP", app.active_call.mean(),
                         cfg.active_call_app_rate, 0.08))

    print("\nfraud pattern shapes")
    st = d[d.label_fraud_type == "STRUCTURING"].amount_uzs / C.STRUCTURING_THRESHOLD
    if len(st):
        ok.append(_check("STRUCTURING min fraction of threshold",
                         st.min(), cfg.structuring_fraction[0], 0.02))
        ok.append(_check("STRUCTURING max fraction of threshold",
                         st.max(), cfg.structuring_fraction[1], 0.02))
    # Group by EPISODE, not by victim: one victim can suffer two takeovers, and events
    # within an episode are at most 12 minutes apart, so an hour separates them.
    ato_rows = d[d.label_fraud_type == "ATO"].copy()
    if len(ato_rows):
        ato_rows["t"] = pd.to_datetime(ato_rows.event_time)
        ato_rows = ato_rows.sort_values(["sender_pinfl", "t"])
        gap = ato_rows.groupby("sender_pinfl").t.diff().dt.total_seconds()
        ato_rows["episode"] = (
            (gap.isna() | (gap > 3600)).cumsum())
        ato = ato_rows.groupby("episode").size()
        within = set(ato.unique()) <= set(range(2, 9))
        print(f"  [{'ok ' if within else 'OFF'}] "
              f"{'ATO events per episode':<38} spec {'2..8':>10}   "
              f"observed {sorted(ato.unique())}")
        ok.append(within)

    print("\namount distributions")
    ok.append(_check("median legitimate amount",
                     legit_ev.amount_uzs.median(), 133_000, 25_000, " UZS"))
    ok.append(_check("all amounts within bounds",
                     float(d.amount_uzs.between(C.AMOUNT_MIN, C.AMOUNT_MAX).all()),
                     1.0, 0))

    print("\ndevices (legitimate senders must change device - see spec section 3)")
    # Stream-derived, as features.py computes device_is_new: first use of a device.
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
    # Two ways this feature can be useless, one test each. Splittable: the total must
    # clear min_child_samples (30, ml/train.py), which bounds the leaf.
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

    print("\npayee identity (the two keys must be able to disagree - spec section 2)")
    # If everyone receives on one card, PAN and PINFL keys partition the stream
    # identically and the payee_identity ablation compares a profile with itself.
    by_pinfl = d.groupby("receiver_pinfl").receiver_card.nunique()
    multi = int((by_pinfl > 1).sum())
    rows_to_multi = int(d.receiver_pinfl.isin(by_pinfl[by_pinfl > 1].index).sum())
    print(f"       receivers reachable on >1 card: {multi:,} of {len(by_pinfl):,}"
          f"  ({rows_to_multi:,} rows land on them)")
    # A handful would leave the ablation measuring almost nothing; the point is
    # that a meaningful share of receiver-side state actually splits.
    splits = multi >= 100 and rows_to_multi >= 1000
    print(f"  [{'ok ' if splits else 'OFF'}] the PAN and PINFL keys partition "
          f"differently")
    if not splits:
        print("       ^ with one card per person the two modes are the same "
              "configuration,\n         and ablate_seeds.py reports NOT MEASURED "
              "rather than 'no effect'.")
    ok.append(splits)

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
