"""
Receiver-side inbound history, shared across Flink partitions via Redis.

Every other piece of state in this job lives in Flink keyed state, which works
because the stream is keyed by sender. Receiver-side aggregation cannot: the
transfers arriving at one payee are spread across every partition, since they
come from different senders. Seeing a mule's fan-in therefore requires state
outside the partitioning — hence Redis.

The window is a sorted set per payee, scored by timestamp:

    rcv:{receiver_pinfl}  ->  ZSET of "ts|sender_pinfl|amount", score = ts

Reads take the window with ZRANGEBYSCORE; writes append and prune expired
members in one pipeline. A TTL on the key means a payee who stops receiving
disappears on its own rather than accumulating forever.

Fails open: if Redis is unreachable the caller gets None and the pipeline scores
without the fan-in signal, exactly as the enrichment lookups degrade.
"""

import logging

import config as C
from rules import ReceiverState, quantile_threshold

log = logging.getLogger("receiver_store")


class ReceiverStore:
    def __init__(self, host, port, window_s=None):
        self._host, self._port = host, port
        self._window_s = window_s or C.RECEIVER_WINDOW_S
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

    def load(self, receiver_pinfl, now):
        """The payee's inbound window, or None when the store is unavailable.

        None and an empty window are deliberately different: None means "this
        signal is not being computed", which the feature extractor treats as
        fail-open, while an empty window is a real observation about a payee
        nobody has paid recently.
        """
        if self._redis is None or not receiver_pinfl:
            return None
        state = ReceiverState()
        try:
            members = self._redis.zrangebyscore(
                f"rcv:{receiver_pinfl}", now - self._window_s, now)
        except Exception as exc:                       # noqa: BLE001
            log.warning("fan-in lookup failed, failing open: %s", exc)
            return None
        for m in members:
            try:
                # Split on the LEFT three separators: the trailing field is the
                # transaction id, which is present only to make the member
                # unique and is not part of the window state. A plain split(2)
                # here silently dropped every entry, because the amount field
                # then carried the id and failed to parse.
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
        receiver = event.get("receiver_pinfl")
        if not receiver:
            return
        key = f"rcv:{receiver}"
        # The member encodes transaction_id, which buys two distinct properties:
        #
        #   Idempotence under replay. This store is external, so it does NOT
        #   roll back with a Flink checkpoint. Under AT_LEAST_ONCE the events
        #   between the last checkpoint and a failure are replayed, and each
        #   replay calls record() again. A member keyed on the transaction makes
        #   the repeat a no-op instead of inflating the payee's inflow.
        #
        #   Correctness for genuine repeats. The previous key was
        #   time|sender|amount, under which two DIFFERENT transfers that share
        #   all three collapsed into one member and the second vanished from
        #   rcv_inflow_1h. Identical amounts from one sender in the same second
        #   is exactly what a structuring or mule run looks like, so the
        #   collision landed on the traffic this store exists to catch.
        txid = event.get("transaction_id") or ""
        member = (f"{now}|{event.get('sender_pinfl', '')}|"
                  f"{float(event['amount_uzs'])}|{txid}")
        try:
            pipe = self._redis.pipeline()
            pipe.zadd(key, {member: now})
            pipe.zremrangebyscore(key, "-inf", now - self._window_s)
            # Twice the window: long enough that nothing in use expires, short
            # enough that idle payees do not accumulate in memory.
            pipe.expire(key, int(self._window_s * 2))
            pipe.execute()
        except Exception as exc:                       # noqa: BLE001
            log.warning("fan-in write failed, continuing: %s", exc)

    def close(self):
        if self._redis is not None:
            try:
                self._redis.close()
            except Exception:                          # noqa: BLE001
                pass


class PopulationStore:
    """The population-wide distribution of `rcv_distinct_senders_1h`, shared
    across Flink partitions via Redis.

    Same argument as ReceiverStore one class up, one level higher. That store
    exists because a payee's inbound transfers are spread across every
    partition, so receiver-side STATE cannot live in Flink keyed state. This one
    exists because the THRESHOLD that state is compared against is a property of
    the whole population, and a worker that built its own histogram would hold a
    partition baseline, not a population one - two workers would derive
    different thresholds from different slices and judge identical transactions
    differently depending on where they landed.

    Topology constrains the feature, then the state, then the threshold. Three
    consequences of one partitioning choice.

        mule:fanin:hist  ->  HASH { sender-count -> times observed }

    WRITES ARE BATCHED. `observe()` only increments a local dict; the flush
    happens on the same call that refreshes the threshold, once every
    MULE_FAN_IN_REFRESH_EVERY observations. A HINCRBY per event would be a Redis
    round trip per event on the 300 ms decision path, which is the kind of cost
    7.3 spent a chapter tracking down.

    READS ARE CACHED for the same reason: HGETALL every event is the same round
    trip wearing a different name. The threshold moves slowly, so a stale one is
    a far smaller error than a latency budget spent on freshness nobody needs.

    Fails CLOSED to the absolute constant, not to a local histogram. Without
    Redis the only baseline available is this worker's own slice, which is
    precisely the wrong quantity - better the known constant than a confident
    wrong number.
    """

    KEY = "mule:fanin:hist"
    BINS = 257
    #: Long enough to survive normal operation, short enough that a deployment
    #: left idle for a week does not come back scoring against last month's
    #: traffic.
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
        """Called by rules.evaluate AFTER the decision. Local only - see the
        class docstring on why this does not touch Redis."""
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
            self._sync()
        if self._total < C.MULE_FAN_IN_MIN_OBS or self._thr is None:
            return fallback
        return self._thr

    def _sync(self):
        """Flush what this worker observed, then re-read the whole population."""
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
            self._thr = (quantile_threshold(counts, total, C.MULE_FAN_IN_QUANTILE)
                         if total else None)
        except Exception as exc:                       # noqa: BLE001
            # Do not drop _pending: a transient Redis blip should cost accuracy
            # of the next refresh, not the observations themselves.
            log.warning("fan-in baseline sync failed, keeping the last "
                        "threshold: %s", exc)

    def close(self):
        if self._redis is not None:
            try:
                if self._pending:
                    self._sync()
                self._redis.close()
            except Exception:                          # noqa: BLE001
                pass
