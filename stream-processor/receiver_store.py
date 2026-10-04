"""Receiver-side state in Redis: the payee's inbound window, its counterparties and
contacts, and the confirmed fraud accounts. It lives outside Flink keyed state
because the stream is keyed by SENDER, spreading one payee across every partition.
Fails open."""

import logging
import time

import capabilities as CAP
import config as C
import features as F
from rules import ReceiverState

log = logging.getLogger("receiver_store")


class ReceiverStore:
    def __init__(self, host, port):
        self._host, self._port = host, port
        self._redis = None
        self._down_until = 0.0

    def open(self):
        import redis
        self._redis = redis.Redis(host=self._host, port=self._port, decode_responses=True,
                                  socket_timeout=C.REDIS_TIMEOUT_S,
                                  socket_connect_timeout=C.REDIS_TIMEOUT_S)
        try:
            self._redis.ping()
        except Exception as exc:                       # noqa: BLE001
            self._failed("connecting", exc)

    def _up(self):
        return self._redis is not None and time.monotonic() >= self._down_until

    def _failed(self, what, exc):
        """Fail open, and fast: the transfers that follow are decided without Redis
        until REDIS_RETRY_AFTER_S has passed, rather than each waiting on it."""
        if time.monotonic() >= self._down_until:
            log.warning("Redis %s failed, deciding without it for %.0f s: %s",
                        what, C.REDIS_RETRY_AFTER_S, exc)
        self._down_until = time.monotonic() + C.REDIS_RETRY_AFTER_S

    def load(self, payee, now):
        """The payee's inbound window, or None when the store is unavailable.
        None and an empty window differ: None means "not being computed", which the
        extractor treats as fail-open; an empty window is a real observation."""
        if not self._up() or not payee:
            return None
        state = ReceiverState()
        try:
            members = self._redis.zrangebyscore(
                f"rcv:{payee}", now - C.RECEIVER_WINDOW_S, now)
        except Exception as exc:                       # noqa: BLE001
            self._failed("fan-in lookup", exc)
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
                self._failed("counterparty lookup", exc)
        if CAP.enabled("confirmed_cases"):
            # The cards this account dealt with over the week, either way.
            try:
                for member, score in self._redis.zrangebyscore(
                        f"cp:card:{payee}", now - C.LINK_WEEK_S, now, withscores=True):
                    state.contacts[member] = float(score)
            except Exception as exc:                   # noqa: BLE001
                self._failed("contacts lookup", exc)
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
        if not self._up():
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
            sender = F.sender_key(event)
            if CAP.enabled("confirmed_cases") and sender:
                # Each end files the other's card, as features.add_contact does.
                for account, card in ((payee, sender), (sender, payee)):
                    key = f"cp:card:{account}"
                    pipe.zadd(key, {card: now})
                    pipe.zremrangebyscore(key, "-inf", now - C.LINK_WEEK_S)
                    pipe.expire(key, int(C.LINK_WEEK_S * 2))
            pipe.execute()
        except Exception as exc:                       # noqa: BLE001
            self._failed("fan-in write", exc)

    def confirmed_among(self, event, receiver_state, now):
        """Which of the payee, the sender and the payee's cards of the week are in
        confirmed frauds, asked in one round trip; nothing when the store is down."""
        if not self._up() or not CAP.enabled("confirmed_cases"):
            return frozenset()
        cards = {F.payee_key(event), F.sender_key(event)} - {""}
        if receiver_state is not None:
            cards |= {c for c, t in receiver_state.contacts.items()
                      if now - t <= C.LINK_WEEK_S}
        cards = sorted(cards)
        try:
            hits = self._redis.smismember(C.CONFIRMED_KEY, cards) if cards else []
        except Exception as exc:                       # noqa: BLE001
            self._failed("confirmed lookup", exc)
            return frozenset()
        return frozenset(c for c, hit in zip(cards, hits) if hit)

    def last_inbound(self, account, now):
        """When this account was last paid, or None when that is not being
        computed - the other half of the transit shape: money in, money out."""
        if not self._up() or not account or not CAP.enabled("counterparty_history"):
            return None
        try:
            top = self._redis.zrevrange(f"cp:in:{account}", 0, 0, withscores=True)
        except Exception as exc:                       # noqa: BLE001
            self._failed("last-inbound lookup", exc)
            return None
        return float(top[0][1]) if top else None

    def close(self):
        if self._redis is not None:
            try:
                self._redis.close()
            except Exception:                          # noqa: BLE001
                pass
