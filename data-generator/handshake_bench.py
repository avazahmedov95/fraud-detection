"""Microbenchmark: the cost of one Kafka connection, plaintext versus mutual TLS. Arms
alternate within each pair, one warm-up pair is discarded; docs/irp-framing.md 7.5a."""

import argparse
import statistics as st
import time

from kafka import KafkaProducer


def _quantile(sorted_vals, q):
    """Nearest-rank order statistic, same convention as latency_report.py."""
    if not sorted_vals:
        return float("nan")
    k = max(1, min(len(sorted_vals), int(-(-q * len(sorted_vals) // 1))))
    return sorted_vals[k - 1]


def one_connection(bootstrap, tls, args):
    cfg = dict(bootstrap_servers=bootstrap,
               key_serializer=lambda k: k.encode(),
               value_serializer=lambda v: v,
               # No retries and a short timeout: a silent reconnect inside the
               # constructor would be measured as a fast handshake.
               reconnect_backoff_ms=0)
    if tls:
        cfg.update(security_protocol="SSL", ssl_check_hostname=True,
                   ssl_cafile=args.ssl_ca, ssl_certfile=args.ssl_cert,
                   ssl_keyfile=args.ssl_key)
    t0 = time.perf_counter()
    p = KafkaProducer(**cfg)
    elapsed = (time.perf_counter() - t0) * 1000.0
    p.close(timeout=5)
    return elapsed


def report(label, vals):
    v = sorted(vals)
    print(f"  {label:<14} n={len(v):<4} median {st.median(v):7.1f} ms   "
          f"p95 {_quantile(v, 0.95):7.1f}   max {max(v):7.1f}   min {min(v):7.1f}")
    return st.median(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=30, help="pairs of connections")
    ap.add_argument("--plain", default="kafka:9092")
    ap.add_argument("--tls", default="kafka:9094")
    ap.add_argument("--ssl-ca", default="/certs/ca.crt")
    ap.add_argument("--ssl-cert", default="/certs/client.crt")
    ap.add_argument("--ssl-key", default="/certs/client.key")
    args = ap.parse_args()

    # One discarded pair: the first connection of a process pays for imports,
    # DNS and the SSL context, none of which recur.
    one_connection(args.plain, False, args)
    one_connection(args.tls, True, args)

    plain, tls = [], []
    for i in range(args.n):
        # Alternate: neither transport always follows the other's teardown.
        if i % 2 == 0:
            plain.append(one_connection(args.plain, False, args))
            tls.append(one_connection(args.tls, True, args))
        else:
            tls.append(one_connection(args.tls, True, args))
            plain.append(one_connection(args.plain, False, args))

    print(f"\nCONNECTION SETUP, {args.n} pairs, alternating\n")
    m_plain = report("plaintext", plain)
    m_tls = report("mutual TLS", tls)

    # Paired differences remove drift shared by both arms.
    diffs = sorted(t - p for t, p in zip(tls, plain))
    print(f"\n  paired difference, TLS minus plaintext:")
    print(f"    median {st.median(diffs):+.1f} ms    "
          f"p95 {_quantile(diffs, 0.95):+.1f}    "
          f"range [{min(diffs):+.1f}, {max(diffs):+.1f}]")
    print(f"    medians differ by {m_tls - m_plain:+.1f} ms")
    print("\n  Absolute figures include the TCP connect, the API-version probe")
    print("  and the metadata fetch, which both arms pay. The DIFFERENCE is the")
    print("  mutual-TLS handshake; the absolutes are not.")
    print(f"\n  Against the 300 ms decision budget, a handshake of "
          f"{st.median(diffs):.1f} ms is paid ONCE PER CONNECTION, not per")
    print("  record. Its weight therefore depends entirely on how often a")
    print("  deployment reconnects - which is what the churn arm measures.")


if __name__ == "__main__":
    main()
