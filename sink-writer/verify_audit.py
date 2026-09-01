"""Recomputes the audit hash chain and reports where it breaks.

Anchored heads: docs/audit-anchors.md.
"""

import argparse
import json
import os
import sys

import integrity

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")


def _fetch(limit):
    import urllib.parse
    import urllib.request
    # Ordered by seq so the chain is walked in the order it was built, not in
    # ClickHouse storage order.
    tail = f"WHERE seq >= (SELECT max(seq) - {limit} FROM {CH_DB}.audit_log)" if limit else ""
    query = (f"SELECT seq, prev_hash, record_hash, ingress_hash, payload, "
             f"decision, transaction_id "
             f"FROM {CH_DB}.audit_log {tail} ORDER BY seq FORMAT JSONEachRow")
    url = (f"http://{CH_HOST}:{CH_PORT}/?"
           + urllib.parse.urlencode({"query": query, "user": CH_USER,
                                     "password": CH_PASSWORD}))
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            body = resp.read().decode()
    except Exception as exc:                           # noqa: BLE001
        raise SystemExit(f"could not query ClickHouse at {CH_HOST}:{CH_PORT} — {exc}")
    return [json.loads(line) for line in body.strip().splitlines() if line]


def verify(records):
    """Return a list of findings; empty means the log is intact."""
    findings = []
    prev_hash = integrity.GENESIS
    expected_seq = None

    for r in records:
        seq = int(r["seq"])
        if expected_seq is None:
            expected_seq = seq                         # first record sets the base
        elif seq != expected_seq:
            findings.append(f"seq gap: expected {expected_seq}, found {seq} "
                            f"({expected_seq - seq if seq < expected_seq else seq - expected_seq} "
                            f"record(s) missing or duplicated)")
            expected_seq = seq

        # 1. chain link
        if r["prev_hash"] != prev_hash:
            findings.append(f"seq {seq}: prev_hash does not match the previous "
                            f"record_hash — records reordered or one deleted")
        recomputed = integrity.record_hash(
            r["prev_hash"], seq, [r["ingress_hash"], r["payload"]])
        if recomputed != r["record_hash"]:
            findings.append(f"seq {seq} (txn {r['transaction_id']}): record_hash "
                            f"does not recompute — content was altered")

        # 3. projection vs signed payload
        try:
            payload = json.loads(r["payload"])
            if payload.get("decision", "") != r["decision"]:
                findings.append(f"seq {seq}: decision column '{r['decision']}' "
                                f"disagrees with payload '{payload.get('decision')}'")
            if (payload.get("transaction_id", "") or "") != r["transaction_id"]:
                findings.append(f"seq {seq}: transaction_id column disagrees "
                                f"with payload")
        except (ValueError, TypeError):
            findings.append(f"seq {seq}: payload is not valid JSON")

        prev_hash = r["record_hash"]
        expected_seq += 1

    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="verify only the most recent N records")
    args = ap.parse_args()

    records = _fetch(args.limit)
    if not records:
        print("audit log is empty — nothing to verify.")
        return

    findings = verify(records)
    n = len(records)
    print(f"verified {n:,} audit records "
          f"(seq {records[0]['seq']}..{records[-1]['seq']})\n")

    if not findings:
        print(f"INTACT — chain continuous, no gaps, projections consistent.")
        print(f"head record_hash: {records[-1]['record_hash']}")
        print("\nPublish the head hash somewhere outside the database (a commit, "
              "a timestamping\nservice) to anchor the chain: it closes the one "
              "gap a chain alone leaves —\nan attacker who rewrites every record "
              "from a point onward.")
        return

    print(f"TAMPERING DETECTED — {len(findings)} finding(s):\n")
    for f in findings:
        print(f"  - {f}")
    sys.exit(1)


if __name__ == "__main__":
    main()
