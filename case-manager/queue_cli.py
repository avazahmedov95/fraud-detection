"""The analyst surface: look at the held transfers, block or release one, see what
holding costs.

  queue_cli.py list | show ID | resolve ID VERDICT --by WHO | stats"""

import argparse
import logging
import time

import config as C
from case import DISPOSITIONS, held_seconds
from store import CaseStore

#: What each verdict does to the held transfer.
ACTION = {"CONFIRMED_FRAUD": "blocked: the money stays with the payer",
          "FALSE_POSITIVE": "released to the payee"}

logging.basicConfig(level=logging.WARNING)


def _store():
    s = CaseStore(C.CH_HOST, C.CH_PORT, C.CH_USER, C.CH_PASSWORD, C.CH_DB)
    s.open()
    return s


#: Case ids are transaction ids - 36-character UUIDs in live data. The first column
#: was 16 wide and every row overflowed it, pushing the whole table out of alignment.
_ID_W = 38


def _duration(s):
    if s is None:
        return "-"
    return f"{s:.0f} s" if s < 60 else f"{s / 60:.0f} min" if s < 3600 else f"{s / 3600:.1f} h"


def cmd_list(args):
    rows = _store().open_cases(args.limit)
    if not rows:
        print("nothing held (or ClickHouse unreachable - check the log)")
        return
    now = time.time()
    print(f"{'case':<{_ID_W}}{'held for':>9}{'score':>7}{'amount, UZS':>16}"
          f"  {'type':<12}reasons")
    unexplained = 0
    for r in rows:
        reasons = ", ".join(r["rule_hits"])
        if not reasons:
            # No rule fired, but the model has exact tree contributions; show the first.
            model_reasons = r.get("explanation") or []
            if model_reasons:
                reasons = "model: " + model_reasons[0]
            else:
                reasons = f"(unexplained: {r.get('explanation_status') or 'none'})"
                unexplained += 1
        print(f"{r['case_id']:<{_ID_W}}{_duration(held_seconds(r, now)):>9}"
              f"{r['final_score']:>7.3f}{r['amount_uzs']:>16,}"
              f"  {r['predicted_type'] or '-':<12}{reasons}")
    print(f"\n{len(rows)} held transfer(s) shown, largest amount first. Each waits "
          f"until it is blocked or released.")
    if unexplained:
        print(f"{unexplained} case(s) have neither a rule hit nor a model "
              f"explanation. Check the case-manager log: the artefact is "
              f"missing, or the explaining model disagreed with the scorer.")


def cmd_show(args):
    c = _store().get(args.case_id)
    if c is None:
        raise SystemExit(f"no case {args.case_id!r}")
    width = max(len(k) for k in c)
    for k, v in c.items():
        # An unresolved case stores the epoch in resolved_at: the right sentinel in
        # the column, but "1970-01-01" reads to a person as a bug, not as "not yet".
        if k in ("resolved_at", "resolved_by") and c["disposition"] == "NEW":
            v = "-"
        print(f"{k:<{width}}  {v}")
    if not c["rule_hits"]:
        print("\nNo rule fired on this transaction - the decision is the "
              "model's alone.")
        lines = c.get("explanation") or []
        if lines:
            print("What pushed the score up, by exact tree contribution:")
            for ln in lines:
                print(f"  * {ln}")
            print("The number in brackets is the feature's contribution to the "
                  "log-odds, so it is comparable between lines but is not a "
                  "probability.")
        else:
            print(f"No model explanation either (status: "
                  f"{c.get('explanation_status') or 'none'}). This case cannot "
                  f"currently be justified to a customer or an auditor.")


def cmd_resolve(args):
    store = _store()
    ok = store.resolve(args.case_id, args.disposition, args.by)
    if not ok:
        raise SystemExit(f"no case {args.case_id!r} - nothing resolved")
    held = _duration(held_seconds(store.get(args.case_id), time.time()))
    print(f"{args.case_id} -> {args.disposition} (by {args.by}): the transfer is "
          f"{ACTION[args.disposition]}, after {held} held")
    print("Recorded as a label. It is attributable and it is revisable: the "
          "row is versioned, so a later verdict supersedes this one.")


def cmd_stats(args):
    store = _store()
    s, h = store.stats(), store.holds()
    if not s:
        raise SystemExit("ClickHouse unreachable")
    for d in DISPOSITIONS:
        print(f"{d:<16}{s.get(d, 0):>8}")
    print(f"{'resolved':<16}{s['_resolved']:>8}")
    p = s["_precision"]
    if p is None:
        print("\nprecision: undefined - no case has been resolved yet.")
    else:
        print(f"\nprecision on resolved cases: {p:.1%}")
    print("\nexplanation coverage:")
    for status, n in sorted(s.get("_explanation", {}).items(),
                            key=lambda kv: -kv[1]):
        print(f"  {status:<40}{n:>8}")
    print(f"\nmost recent case opened: {s.get('_last_opened') or 'never'}")

    if h:
        held, blocked, released = h["NEW"], h["CONFIRMED_FRAUD"], h["FALSE_POSITIVE"]
        print(f"\nheld now: {held['n']}, the longest for {_duration(held['max_s'])}")
        print(f"blocked as fraud: {blocked['n']}, {blocked['amount']:,} UZS kept "
              f"with the payers")
        print(f"released as false alarms: {released['n']}, after "
              f"{_duration(released['median_s'])} held at the median and "
              f"{_duration(released['max_s'])} at the longest - the cost of holding, "
              f"paid by honest customers")

    print("\nThis is the only figure in the system computed from human verdicts "
          "rather than generated ground truth. Over a small, non-random sample "
          "of resolved cases it is an indication, not a measurement: analysts "
          "work the top of the queue, so the resolved set is biased towards "
          "high scores and this number reads higher than the true precision.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list", help="held transfers, largest amount first")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("show", help="every field of one case")
    p.add_argument("case_id")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("resolve", help="block (CONFIRMED_FRAUD) or release "
                                       "(FALSE_POSITIVE) a held transfer")
    p.add_argument("case_id")
    p.add_argument("disposition",
                   choices=[d for d in DISPOSITIONS if d != "NEW"])
    p.add_argument("--by", required=True,
                   help="who is making this call; a label with no author "
                        "cannot be audited or withdrawn")
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("stats", help="verdicts, the precision they imply, and what "
                                     "holding costs")
    p.set_defaults(func=cmd_stats)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
