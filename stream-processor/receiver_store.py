"""The payee's side in Redis: its transfers of the day, its payers of the month and
who is new among them, contacts and the confirmed fraud accounts. Not in Flink state:
the stream is keyed by sender, so one payee's transfers reach every copy of the job.
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
        """Decide without Redis for REDIS_RETRY_AFTER_S rather than wait on it."""
        if time.monotonic() >= self._down_until:
            log.warning("Redis %s failed, deciding without it for %.0f s: %s",
                        what, C.REDIS_RETRY_AFTER_S, exc)
        self._down_until = time.monotonic() + C.REDIS_RETRY_AFTER_S

    def load(self, payee, now):
        """The payee's side in one round trip, or None while Redis is unavailable."""
        if not self._up() or not payee:
            return None
        counters, cases = CAP.enabled("counterparty_history"), CAP.enabled("confirmed_cases")
        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.zrangebyscore(f"rcv:{payee}", now - C.LINK_DAY_S, now)
            if counters:
                pipe.zrangebyscore(f"cp:in:{payee}", now - C.LINK_MONTH_S, now, withscores=True)
                pipe.zrangebyscore(f"cp:new:{payee}", now - C.LINK_DAY_S, now, withscores=True)
            if cases:
                pipe.zrangebyscore(f"cp:card:{payee}", now - C.LINK_WEEK_S, now, withscores=True)
            replies = iter(pipe.execute())
        except Exception as exc:                       # noqa: BLE001
            self._failed("payee lookup", exc)
            return None
        state = ReceiverState()
        members = next(replies)
        if counters:
            for member, score in next(replies):
                state.payers[member] = float(score)
                state.last_inbound_ts = max(state.last_inbound_ts, float(score))
            state.new_payers = {m: float(s) for m, s in next(replies)}
        if cases:
            state.contacts = {m: float(s) for m, s in next(replies)}
        for m in members:
            try:
                parts = m.split("|", 3)
                if len(parts) < 3:
                    continue
                ts, sender, amount = parts[0], parts[1], parts[2]
                state.inbound.append((float(ts), sender, float(amount)))
            except ValueError:                         # malformed member
                continue
        return state

    def record(self, event, now, receiver_state=None):
        """Append this transfer to the payee's window and prune what expired;
        `receiver_state`, as evaluate left it, says whether the payer is new."""
        if not self._up():
            return
        payee = F.payee_key(event)
        if not payee:
            return
        key = f"rcv:{payee}"
        payer = event.get("sender_pinfl", "")
        # The id: a replay rewrites its member, and two equal transfers stay two.
        txid = event.get("transaction_id") or ""
        member = f"{now}|{payer}|{float(event['amount_uzs'])}|{txid}"
        try:
            pipe = self._redis.pipeline()
            pipe.zadd(key, {member: now})
            pipe.zremrangebyscore(key, "-inf", now - C.LINK_DAY_S)
            pipe.expire(key, int(C.LINK_DAY_S * 2))
            if CAP.enabled("counterparty_history"):
                # One member per payer, scored with their last payment.
                ckey = f"cp:in:{payee}"
                pipe.zadd(ckey, {payer: now})
                pipe.zremrangebyscore(ckey, "-inf", now - C.LINK_MONTH_S)
                pipe.expire(ckey, int(C.LINK_MONTH_S * 2))
                if receiver_state is not None and receiver_state.new_payers.get(payer) == now:
                    nkey = f"cp:new:{payee}"
                    pipe.zadd(nkey, {payer: now})
                    pipe.zremrangebyscore(nkey, "-inf", now - C.LINK_DAY_S)
                    pipe.expire(nkey, int(C.LINK_DAY_S * 2))
            sender = F.sender_key(event)
            if CAP.enabled("confirmed_cases") and sender:
                # Each end files the other, as features.add_contact does.
                for account, card in ((payee, sender), (sender, payee)):
                    key = f"cp:card:{account}"
                    pipe.zadd(key, {card: now})
                    pipe.zremrangebyscore(key, "-inf", now - C.LINK_WEEK_S)
                    pipe.expire(key, int(C.LINK_WEEK_S * 2))
            pipe.execute()
        except Exception as exc:                       # noqa: BLE001
            self._failed("fan-in write", exc)

    def confirmed_among(self, event, receiver_state, now):
        """Which of the payee, the sender and the payee's contacts of the week are
        confirmed fraud accounts, in one round trip."""
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
        """When this account was last paid, or None when that is not computed."""
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
