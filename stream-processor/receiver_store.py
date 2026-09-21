"""Receiver-side state in Redis: the payee's inbound window and the population
distribution its threshold is compared against. Both live outside Flink keyed state
because the stream is keyed by SENDER, spreading one payee across every partition.
Fails open."""

import logging

import capabilities as CAP
import config as C
import features as F
from rules import FAN_IN_BINS, ReceiverState, quantile_threshold

log = logging.getLogger("receiver_store")


class ReceiverStore:
    def __init__(self, host, port):
        self._host, self._port = host, port
        self._redis = None

    def open(self):
        try:
            import redis
            self._redis = redis.Redis(host=self._host, port=self._port,
                                      decode_responses=True)
            self._redis.ping()
        except Exception as exc:                       # noqa: BLE001
            log.warning("Redis unavailable, fan-in detection disabled: %s", exc)
            self._redis = None

    def load(self, payee, now):
        """The payee's inbound window, or None when the store is unavailable.
        None and an empty window differ: None means "not being computed", which the
        extractor treats as fail-open; an empty window is a real observation."""
        if self._redis is None or not payee:
            return None
        state = ReceiverState()
        try:
            members = self._redis.zrangebyscore(
                f"rcv:{payee}", now - C.RECEIVER_WINDOW_S, now)
        except Exception as exc:                       # noqa: BLE001
            log.warning("fan-in lookup failed, failing open: %s", exc)
            return None
        if CAP.enabled("counterparty_history"):
            # Who paid this account over the week, and when: the AML counters.
            # A failure here must not cost the fan-in window read above.
            try:
                for member, score in self._redis.zrangebyscore(
                        f"cp:in:{payee}", now - C.LINK_WEEK_S, now,
                        withscores=True):
                    state.payers[member] = float(score)
                    state.last_inbound_ts = max(state.last_inbound_ts,
                                                float(score))
            except Exception as exc:                   # noqa: BLE001
                log.warning("counterparty lookup failed, failing open: %s", exc)
        for m in members:
            try:
                # Left three separators only: the last field is the transaction id.
                parts = m.split("|", 3)
                if len(parts) < 3:
                    continue
                ts, sender, amount = parts[0], parts[1], parts[2]
                state.inbound.append((float(ts), sender, float(amount)))
            except ValueError:                         # malformed member
                continue
        return state

    def record(self, event, now):
        """Append this transfer to the payee's window and prune what expired."""
        if self._redis is None:
            return
        # Same helper load() uses, so a write cannot land under a key the read ignores.
        payee = F.payee_key(event)
        if not payee:
            return
        key = f"rcv:{payee}"
        # The transaction id makes replays idempotent - this store does not roll back
        # with a checkpoint - and keeps two identical transfers distinct.
        txid = event.get("transaction_id") or ""
        member = (f"{now}|{event.get('sender_pinfl', '')}|"
                  f"{float(event['amount_uzs'])}|{txid}")
        try:
            pipe = self._redis.pipeline()
            pipe.zadd(key, {member: now})
            pipe.zremrangebyscore(key, "-inf", now - C.RECEIVER_WINDOW_S)
            # Twice the window: nothing in use expires, idle payees do not accumulate.
            pipe.expire(key, int(C.RECEIVER_WINDOW_S * 2))
            if CAP.enabled("counterparty_history"):
                # One member per PAYER, scored with the last time they paid, so
                # the key grows with counterparties rather than with transfers.
                ckey = f"cp:in:{payee}"
                pipe.zadd(ckey, {event.get("sender_pinfl", ""): now})
                pipe.zremrangebyscore(ckey, "-inf", now - C.LINK_WEEK_S)
                pipe.expire(ckey, int(C.LINK_WEEK_S * 2))
            pipe.execute()
        except Exception as exc:                       # noqa: BLE001
            log.warning("fan-in write failed, continuing: %s", exc)

    def last_inbound(self, account, now):
        """When this account was last paid, or None when that is not being
        computed - the other half of the transit shape: money in, money out."""
        if (self._redis is None or not account
                or not CAP.enabled("counterparty_history")):
            return None
        try:
            top = self._redis.zrevrange(f"cp:in:{account}", 0, 0, withscores=True)
        except Exception as exc:                       # noqa: BLE001
            log.warning("last-inbound lookup failed, failing open: %s", exc)
            return None
        return float(top[0][1]) if top else None

    def close(self):
        if self._redis is not None:
            try:
                self._redis.close()
            except Exception:                          # noqa: BLE001
                pass


class PopulationStore:
    """Population-wide distribution of `rcv_distinct_senders_1h`, shared across Flink
    partitions via Redis (mule:fanin:hist -> {sender-count: times seen}). Writes are
    batched and reads cached to keep Redis off the per-event path; fails closed to
    the absolute constant."""

    KEY = "mule:fanin:hist"
    BINS = FAN_IN_BINS
    #: Long enough to survive normal operation, short enough that a deployment
    #: left idle does not come back scoring against last month's traffic.
    TTL_S = 7 * 24 * 3600

    def __init__(self, host, port):
        self._host, self._port = host, port
        self._redis = None
        self._pending = {}
        self._since_sync = 0
        self._counts = None
        self._total = 0
        self._thr = None
        self._warned = False

    def open(self):
        try:
            import redis
            self._redis = redis.Redis(host=self._host, port=self._port,
                                      decode_responses=True)
            self._redis.ping()
        except Exception as exc:                       # noqa: BLE001
            log.warning("Redis unavailable, MULE_FAN_IN stays on the absolute "
                        "threshold: %s", exc)
            self._redis = None

    def observe(self, senders):
        """Called by rules.evaluate AFTER the decision. Local only, no Redis."""
        k = min(int(senders), self.BINS - 1)
        self._pending[k] = self._pending.get(k, 0) + 1
        self._since_sync += 1

    def threshold(self, q, fallback):
        if self._redis is None:
            if not self._warned:
                self._warned = True
                log.warning("no Redis: MULE_FAN_IN on the absolute threshold "
                            "(%d senders)", fallback)
            return fallback
        if self._counts is None or self._since_sync >= C.MULE_FAN_IN_REFRESH_EVERY:
            self._sync(q)
        if self._total < C.MULE_FAN_IN_MIN_OBS or self._thr is None:
            return fallback
        return self._thr

    def _sync(self, q):
        """Flush what this worker observed, then re-read the whole population and
        cut it at quantile `q`."""
        try:
            if self._pending:
                pipe = self._redis.pipeline()
                for k, v in self._pending.items():
                    pipe.hincrby(self.KEY, k, v)
                pipe.expire(self.KEY, self.TTL_S)
                pipe.execute()
                self._pending.clear()
            self._since_sync = 0
            raw = self._redis.hgetall(self.KEY) or {}
            counts = [0] * self.BINS
            total = 0
            for k, v in raw.items():
                try:
                    i, c = int(k), int(v)
                except (TypeError, ValueError):
                    continue
                if 0 <= i < self.BINS:
                    counts[i] = c
                    total += c
            self._counts, self._total = counts, total
            self._thr = quantile_threshold(counts, total, q) if total else None
        except Exception as exc:                       # noqa: BLE001
            # Do not drop _pending: a blip should cost the next refresh's accuracy, not
            # the observations themselves.
            log.warning("fan-in baseline sync failed, keeping the last "
                        "threshold: %s", exc)

    def close(self):
        if self._redis is not None:
            try:
                if self._pending:
                    self._sync(C.MULE_FAN_IN_QUANTILE)
                self._redis.close()
            except Exception:                          # noqa: BLE001
                pass
