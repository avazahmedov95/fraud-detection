"""Live demo: one page that shows the running system deciding.

The transfers streaming through, fraud cases replayed on demand with their decision
and reasons, the analyst queue, and where the decision time goes. It adds no
detection logic: transfers are built by data-generator's
`kafka_producer._row_to_message`, decisions come back from `transactions.scored`,
reasons and queue from case-manager's `Explainer` and `CaseStore`, stage times from
the warehouse.

It runs as the stack's `demo` container (infra/demo/Dockerfile). Until Kafka,
ClickHouse, the Flink job and the generated dataset are all there, the page shows
only which of them is missing: everything it shows comes from the running system.
In the stack rather than on the host because the transfers it sends are stamped
on the containers' clock, the one the stage times are measured against.

Scenarios are real episodes from the held-out 20% of the dataset, replayed under
a fresh sender card and moved in time to end now; receivers keep their cards.
The background stream replays the same held-out part at its own pacing.

    .\\run.ps1 up; .\\run.ps1 submit-job     # then open http://localhost:8090
"""
import json
import math
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GEN = os.path.join(ROOT, "data-generator")
CM = os.path.join(ROOT, "case-manager")
MODELS = os.path.join(ROOT, "ml", "models")
# Imported from these: kafka_producer and integrity, store, case and explain. None
# of them imports `config`, the one module name both directories hold.
sys.path[:0] = [GEN, CM]
import case as CASE           # noqa: E402
import explain as EX          # noqa: E402
import integrity              # noqa: E402
import kafka_producer as KP   # noqa: E402

CSV_PATH = os.path.join(GEN, "out", "transactions.csv")
TOPIC_RAW, TOPIC_SCORED = "transactions.raw", "transactions.scored"
HELD_OUT_FROM = 0.80    # ml/train.py's cut: the model never trained past it
HISTORY_ROWS = 8        # the sender's own transfers replayed before the episode
EPISODE_HOURS = 6       # how far around the picked row the episode's rows are sought
KINDS = ("NORMAL", "APP", "ATO", "MULE", "STRUCTURING")
#: The job's decision path (stream-processor/fraud_job.py STAGES), each stored by
#: sink-writer as stage_<name>_ms.
STAGES = ("kafka", "handoff", "decode", "state", "redis", "rules", "model", "decide")
LIVE_WINDOW = 1000      # how many of the latest decisions the time figures cover
PAGE = 20               # rows the stream and the queue show before "show more"

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CLICKHOUSE = dict(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                  port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
                  username=os.getenv("CLICKHOUSE_USER", "fraud"),
                  password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                  database=os.getenv("CLICKHOUSE_DB", "fraud"))
FLINK = os.getenv("FLINK_REST", "http://jobmanager:8081")
JOB_NAME = "fraud-detection-cep-ml"     # stream-processor/fraud_job.py
PORT = 8090
# Opened by the browser, on the host: the host's port, not the service name.
GRAFANA = "http://localhost:" + os.getenv("GRAFANA_PORT", "3000")
DASHBOARD = os.path.join(ROOT, "infra", "grafana", "provisioning", "dashboards",
                         "json", "fraud-overview.json")


def _is_fraud(row):
    return str(row.get("label_is_fraud", "")).strip().lower() in ("1", "true")


def _epoch(row):
    # Naive, as the CSV writes it. Only differences between rows are used, so the
    # timezone it is read in cancels out.
    return datetime.fromisoformat(row["event_time"]).timestamp()


def _utc_iso(epoch):
    """Naive UTC, the form the job reads: its containers run in UTC, and the
    hour-of-day feature is taken from this timestamp."""
    return (datetime.fromtimestamp(epoch, timezone.utc).replace(tzinfo=None)
            .isoformat(timespec="milliseconds"))


def _as_epoch(dt):
    """A ClickHouse DateTime as epoch seconds. The client returns it naive, in the
    server's zone, which is UTC in these containers."""
    return dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()


def _digits(rng, n):
    return "".join(rng.choice("0123456789") for _ in range(n))


def _finite(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _quantile(ordered, q):
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] if ordered else None


def _limit(query):
    """?limit=N from the page, within what the server keeps."""
    try:
        return max(1, min(400, int(query.get("limit", [PAGE])[0])))
    except ValueError:
        return PAGE


def parse_filter(query):
    """The page's filter: card digits, an amount range, a time range (epoch
    seconds), a decision, an alert type. Anything empty or unreadable is left out."""
    f = {}
    for key in ("sender", "receiver"):
        digits = "".join(ch for ch in query.get(key, [""])[0] if ch.isdigit())
        if digits:
            f[key] = digits
    for key in ("min", "max", "since", "until"):
        try:
            f[key] = float(query[key][0])
        except (KeyError, IndexError, ValueError):
            pass
    for key in ("decision", "type", "held"):
        if query.get(key, [""])[0]:
            f[key] = query[key][0]
    return f


def _card_matches(card, digits):
    """Typed digits against what the page shows of a card - its first six and last
    four - or the whole number."""
    card = str(card or "")
    return (card == digits or (len(digits) <= 6 and card.startswith(digits))
            or (len(digits) <= 4 and card.endswith(digits)))


def passes(f, sender, receiver, amount, at, decisions, kind, held=()):
    """Whether one decision or case passes the filter `parse_filter` read. A record
    can match more than one decision (`_decisions`), and a case more than one cause
    of its hold (`_held_by`)."""
    amount = float(amount or 0)
    return not (
        ("sender" in f and not _card_matches(sender, f["sender"]))
        or ("receiver" in f and not _card_matches(receiver, f["receiver"]))
        or ("min" in f and amount < f["min"]) or ("max" in f and amount > f["max"])
        or ("since" in f and (at is None or at < f["since"]))
        or ("until" in f and (at is None or at > f["until"]))
        or ("decision" in f and f["decision"] not in decisions)
        or ("type" in f and (kind or "NONE") != f["type"])
        or ("held" in f and f["held"] not in held))


def mask(card):
    """8600 03** **** 2655 - enough to tell cards apart on screen, not to use one."""
    c = str(card or "")
    return f"{c[:4]} {c[4:6]}** **** {c[-4:]}" if len(c) >= 12 else c


def _dashboard_url():
    """The provisioned overview dashboard, in kiosk mode for the page's frame. Its
    panels are keyed on event_time and the background replay keeps the dataset's
    own times, so the range reaches back far enough to hold them."""
    try:
        with open(DASHBOARD, encoding="utf-8") as fh:
            uid = json.load(fh)["uid"]
    except (OSError, KeyError, ValueError):
        return ""
    return f"{GRAFANA}/d/{uid}?orgId=1&kiosk&refresh=10s&from=now-2y&to=now"


def _review_cut():
    """The alert level the job decides by: the model's cutoff, shipped beside it."""
    try:
        with open(os.path.join(MODELS, "thresholds.json"), encoding="utf-8") as fh:
            return float(json.load(fh)["review"])
    except (OSError, KeyError, TypeError, ValueError):
        return None


def _second_cut():
    """The second look's own cut-off on TabPFN's scale (ml/second_look.py), if any."""
    try:
        with open(os.path.join(MODELS, "second_look.json"), encoding="utf-8") as fh:
            return float(json.load(fh)["cut"])
    except (OSError, KeyError, TypeError, ValueError):
        return None


def _second(rec):
    """Whether the second look, not the job, took this decision."""
    return (rec.get("model_version") or "").startswith("second-look")


def _scores(rec):
    """The served model's risk, which every list on the page shows, and - when the
    second look took the decision - its own score, on TabPFN's scale."""
    if not _second(rec):
        return {"score": rec.get("final_score"), "second_score": None}
    unscored = rec["model_version"].endswith(":unscored")
    return {"score": rec.get("ml_score"),
            "second_score": None if unscored else rec.get("final_score")}


def _decisions(rec):
    """What the decision filter matches a record by: its decision, and the second
    look too once that has decided - the transfer's row has left SECOND_LOOK by then."""
    return (rec.get("decision"), "SECOND_LOOK") if _second(rec) else (rec.get("decision"),)


#: The job's MANDATORY_REVIEW_RULES (stream-processor/config.py): these hold a
#: transfer by themselves.
MANDATORY = ("STRUCTURING", "DAILY_LIMIT_BREACH")


def _held_by(case, cut):
    """What held the transfer, as fusion.decide and the second look decide: the second
    look, or the model's risk at or past its cut-off and a hard rule - both can hold
    one. A case with no model score was held by the rules' own score."""
    if _second(case):
        return ("SECOND_LOOK",)
    ml = case.get("ml_score")
    model = ml is not None and cut is not None and ml >= cut
    rule = ml is None or any(r in MANDATORY for r in case.get("rule_hits") or [])
    return tuple(k for k, on in (("MODEL", model), ("RULE", rule)) if on)


def decision_ms(rec):
    """Arrival to decision: the producer's ingested_at to the job's scored_at_job,
    both stamped on the containers' clock."""
    try:
        return (float(rec["scored_at_job"]) - float(rec["ingested_at"])) * 1000.0
    except (KeyError, TypeError, ValueError):
        return None


def job_running(overview):
    """Whether Flink's /jobs/overview lists the fraud job as RUNNING."""
    return any(j.get("name") == JOB_NAME and j.get("state") == "RUNNING"
               for j in (overview or {}).get("jobs", []))


def stage_summary(row):
    """(count, one average per stage, median, p99) -> what the page draws. The
    stage averages add up to the average time from arrival to decision."""
    n = int(row[0] or 0)
    values = [_finite(v) for v in row[1:]] if n else [None] * (len(STAGES) + 2)
    avgs, (median, p99) = values[:len(STAGES)], values[len(STAGES):]
    return {"n": n, "stages": [{"name": s, "ms": a} for s, a in zip(STAGES, avgs)],
            "total_ms": sum(a for a in avgs if a is not None) if n else None,
            "median_ms": median, "p99_ms": p99}


def retrain_status():
    """What the retrainer's latest run left (ml/retrain.py), or None before its first."""
    try:
        with open(os.path.join(MODELS, "retrain_status.json"), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _reachable(host, port):
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _get(url):
    """The body of a GET, or None when nothing answers."""
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.read()
    except OSError:
        return None


class Health:
    """What the page needs running, checked every few seconds. Until every part is
    there the page shows which one is missing, and nothing else."""

    def __init__(self, data_error):
        self.parts = {"kafka": False, "clickhouse": False, "job": False,
                      "data": not data_error}

    @property
    def ready(self):
        return all(self.parts.values())

    def check(self):
        host, _, port = KAFKA.rpartition(":")
        self.parts["kafka"] = _reachable(host, int(port))
        self.parts["clickhouse"] = _get(
            f"http://{CLICKHOUSE['host']}:{CLICKHOUSE['port']}/ping") is not None
        body = _get(f"{FLINK}/jobs/overview")
        try:
            self.parts["job"] = job_running(json.loads(body) if body else None)
        except ValueError:
            self.parts["job"] = False

    def run(self):
        while True:
            self.check()
            time.sleep(3)


class Episodes:
    """The dataset, indexed for picking one episode of a kind, held as typed
    columns (a dict per row of 500,000 cost over a gigabyte)."""

    _CATEGORIES = ("sender_pinfl", "sender_card", "sender_network", "receiver_card",
                   "receiver_network", "device_id", "sender_region", "active_call",
                   "label_fraud_type")

    def __init__(self, path=CSV_PATH, held_out_from=HELD_OUT_FROM):
        keep = set(KP.RAW_FIELDS) | {"label_is_fraud", "label_fraud_type"}
        keep.discard("transaction_id")                     # a replay gets new ids
        df = pd.read_csv(path, usecols=lambda c: c in keep,
                         dtype={c: "category" for c in self._CATEGORIES})
        # Naive, as written; ISO8601 explicitly, since the file mixes times with and
        # without fractional seconds.
        df["t"] = pd.to_datetime(df.pop("event_time"), format="ISO8601").astype("int64") / 1e9
        self.df = df.sort_values("t", kind="stable").reset_index(drop=True)
        self.t = self.df["t"].to_numpy()
        self.cut = int(len(self.df) * held_out_from)
        self.is_fraud = self.df["label_is_fraud"].to_numpy().astype(bool)
        self.kind = self.df["label_fraud_type"].astype(str).to_numpy()
        self.sender = self.df["sender_card"].astype(str).to_numpy()
        self.receiver = self.df["receiver_card"].astype(str).to_numpy()
        self.by_sender = self.df.groupby("sender_card", observed=True).indices
        held = np.arange(len(self.df)) >= self.cut
        self.fraud = {k: np.flatnonzero(held & self.is_fraud & (self.kind == k))
                      for k in KINDS if k != "NORMAL"}

    def row(self, i):
        r = {c: str(v) for c, v in self.df.iloc[i].items() if c != "t"}
        r["event_time"] = _utc_iso(float(self.t[i]))
        r["transaction_id"] = ""
        return r

    def replay_start(self, rng=random):
        """A row inside the held-out part to start the background replay at. The
        generator writes the file in time order, which the producer's --skip counts."""
        return rng.randrange(self.cut, max(self.cut + 1, len(self.df) - 1000))

    def _prior(self, i):
        idx = self.by_sender[self.sender[i]]
        return idx[idx < i]

    def _near(self, i, kind):
        span = EPISODE_HOURS * 3600
        lo = int(np.searchsorted(self.t, self.t[i] - span, side="left"))
        hi = int(np.searchsorted(self.t, self.t[i] + span, side="right"))
        j = np.arange(lo, hi)
        return j[self.is_fraud[lo:hi] & (self.kind[lo:hi] == kind)]

    def _with_history(self, idx):
        return ([(self.row(j), "history") for j in self._prior(idx[0])[-HISTORY_ROWS:]]
                + [(self.row(j), "episode") for j in idx])

    def pick(self, kind, rng=random):
        """[(row, role)] in time order; role is "history" or "episode"."""
        if kind == "NORMAL":
            for _ in range(2000):
                i = rng.randrange(self.cut, len(self.df))
                if not self.is_fraud[i] and len(self._prior(i)) >= HISTORY_ROWS:
                    return self._with_history([i])
            raise LookupError("no ordinary transfer with enough sender history")
        if not len(self.fraud.get(kind, ())):
            raise LookupError(f"no {kind} episode in the held-out slice")
        i = int(rng.choice(list(self.fraud[kind])))
        near = self._near(i, kind)
        if kind == "MULE":
            # The mule is the account the pattern turns on - paid by many, then
            # paying out - so its card is whichever side of the picked row recurs.
            rcv = self.receiver[i]
            mule = rcv if (self.receiver[near] == rcv).sum() > 1 else self.sender[i]
            return [(self.row(j), "episode") for j in near
                    if mule in (self.sender[j], self.receiver[j])]
        return self._with_history([j for j in near if self.sender[j] == self.sender[i]])


def replay_messages(items, now, rng=random):
    """The rows as kafka_producer sends them, re-identified and moved in time:
    senders get new PINFLs and new cards with the same BIN (the same issuing bank);
    receivers, and a sender also paid in the episode, keep their cards.
    Times shift so the last row lands at `now`, keeping every gap."""
    keep = {row["receiver_card"] for row, _ in items}
    cards, pinfls = {}, {}
    last = _epoch(items[-1][0])
    out = []
    for row, role in items:
        r = dict(row)
        card = row["sender_card"]
        if card not in keep:
            if card not in cards:
                cards[card] = card[:6] + _digits(rng, len(card) - 6)
                pinfls[card] = _digits(rng, 14)
            r["sender_card"], r["sender_pinfl"] = cards[card], pinfls[card]
        offset = _epoch(row) - last
        r["transaction_id"] = str(uuid.uuid4())
        r["event_time"] = _utc_iso(now + offset)
        out.append({"role": role, "offset_s": offset,
                    "kind": row["label_fraud_type"] if _is_fraud(row) else "NONE",
                    "message": KP._row_to_message(r)})
    return out


_PHRASE = re.compile(r"^(?P<label>.+?): (?P<shown>.*) \((?P<w>[+-]\d+\.\d+)\)$")


def split_phrases(phrases):
    """case-manager's explanation lines - "distinct senders paying this payee in an
    hour: 3 (+0.42)" -
    back into (feature, shown value, weight), so the page can say them in either
    language without a second explainer to drift from the first."""
    names = {label: name for name, (label, _) in EX._PHRASES.items()}
    items = []
    for p in phrases:
        m = _PHRASE.match(p)
        if m:
            items.append({"feature": names.get(m["label"], m["label"]),
                          "label_en": m["label"], "shown": m["shown"],
                          "weight": float(m["w"])})
    return items


class Decisions:
    """Every decision the job emits on transactions.scored, since the server started."""

    def __init__(self):
        self.lock = threading.Lock()
        self.recent = deque(maxlen=400)
        self.by_id = OrderedDict()
        self.counts = Counter()
        self.ms = deque(maxlen=LIVE_WINDOW)
        self.last_at, self.model = None, ""

    def add(self, rec):
        with self.lock:
            # A transfer sent for a second look is decided twice. It keeps its row and
            # counts once, as its latest decision; its time to decision is the job's.
            prev = self.by_id.get(rec.get("transaction_id"))
            if prev is not None:
                self.counts[prev.get("decision") or "?"] -= 1
            if prev is not None and prev in self.recent:
                self.recent[self.recent.index(prev)] = rec
            else:
                self.recent.append(rec)
            if rec.get("transaction_id"):
                self.by_id[rec["transaction_id"]] = rec
                while len(self.by_id) > 50_000:
                    self.by_id.popitem(last=False)
            self.counts[rec.get("decision") or "?"] += 1
            ms = decision_ms(rec) if prev is None else None
            if ms is not None:
                self.ms.append(ms)
            self.last_at = time.time()
            if not _second(rec):                           # the chip names the served model
                self.model = rec.get("model_version") or self.model

    def run(self):
        from kafka import KafkaConsumer
        while True:
            try:
                consumer = KafkaConsumer(TOPIC_SCORED, bootstrap_servers=KAFKA,
                                         group_id=None, auto_offset_reset="latest")
                for m in consumer:
                    try:
                        self.add(json.loads(m.value))
                    except ValueError:
                        continue
            except Exception:                          # noqa: BLE001 - Health reports it
                time.sleep(3)


class Background:
    """data-generator's producer run unchanged - the dataset at its own pacing,
    200 times faster - so the stream is the one every figure was measured on.
    Starts at a random row of the held-out part, which the model never trained on,
    so that two demos do not show the same minutes."""

    def __init__(self):
        self.proc = None

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, skip):
        if self.running():
            return
        self.proc = subprocess.Popen(
            [sys.executable, "kafka_producer.py", "--file", os.path.join("out", "transactions.csv"),
             "--realtime", "--speed", "200", "--skip", str(skip),
             "--bootstrap", KAFKA, "--topic", TOPIC_RAW],
            cwd=GEN, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self):
        if self.running():
            self.proc.terminate()
            self.proc.wait(timeout=5)          # so the answer says "off" when it is


class Sender:
    """One producer, opened on first use."""

    def __init__(self):
        self._producer, self._lock = None, threading.Lock()

    def send(self, messages, gap_s=0.15):
        from kafka import KafkaProducer
        with self._lock:
            if self._producer is None:
                self._producer = KafkaProducer(
                    bootstrap_servers=KAFKA, key_serializer=str.encode,
                    value_serializer=lambda v: json.dumps(v).encode())
            for m in messages:
                msg = m["message"]
                msg["ingested_at"] = time.time()
                msg["ingress_hash"] = integrity.ingress_hash(msg)
                self._producer.send(TOPIC_RAW, key=msg["sender_card"], value=msg)
                self._producer.flush()
                time.sleep(gap_s)                      # so the page fills row by row


def view(rec):
    return {"id": rec.get("transaction_id"), "at": rec.get("scored_at_job"),
            "amount": rec.get("amount_uzs"), "from": mask(rec.get("sender_card")),
            "to": mask(rec.get("receiver_card")), "decision": rec.get("decision"),
            **_scores(rec), "type": rec.get("predicted_type"),
            "rules": rec.get("rule_hits") or [], "ms": decision_ms(rec),
            "stages": rec.get("stage_ms") or {}, "second": _second(rec)}


class App:
    def __init__(self, csv_path=CSV_PATH):
        self.decisions, self.background, self.sender = Decisions(), Background(), Sender()
        self.runs, self.started = OrderedDict(), time.time()
        self.grafana, self.cut, self.second_cut = _dashboard_url(), _review_cut(), _second_cut()
        self._store, self._warehouse, self._explainer = None, None, None
        try:
            self.library, self.library_error = Episodes(csv_path), ""
        except (OSError, KeyError, ValueError) as exc:
            self.library, self.library_error = None, str(exc)
        self.health = Health(self.library_error)

    def _why(self, rec):
        """Explained once per alert and kept: the page asks every second."""
        if "_why" not in rec:
            if rec.get("decision") == "ALLOW":
                rec["_why"] = None
            elif not rec.get("features") or rec.get("ml_score") is None:
                rec["_why"] = {"status": EX.NO_FEATURES, "items": []}
            else:
                if self._explainer is None:
                    self._explainer = EX.Explainer()
                status, phrases = self._explainer.explain(rec["features"], rec["ml_score"])
                rec["_why"] = {"status": status, "items": split_phrases(phrases)}
        return rec["_why"]

    def status(self):
        d = self.decisions
        with d.lock:
            ms, counts = sorted(d.ms), dict(d.counts)
        return {"ready": self.health.ready, "parts": self.health.parts,
                "data_error": self.library_error,
                "background": {"on": self.background.running()},
                "counts": counts, "median_ms": _quantile(ms, 0.5), "p99_ms": _quantile(ms, 0.99),
                "last_decision": d.last_at, "model": d.model, "cut": self.cut,
                "second_cut": self.second_cut,
                "grafana": self.grafana}

    def stream(self, flt=None, limit=PAGE):
        """The latest `limit` decisions that pass the filter, newest first, each with
        the reasons if it is an alert and, if a scenario sent it, its role and true
        type; and how many pass, so the page can offer the rest."""
        with self.decisions.lock:
            recs = list(self.decisions.recent)
        if flt:
            recs = [r for r in recs if passes(
                flt, r.get("sender_card"), r.get("receiver_card"), r.get("amount_uzs"),
                _finite(r.get("scored_at_job")), _decisions(r), r.get("predicted_type"))]
        roles = {row["id"]: row for run in list(self.runs.values()) for row in run["rows"]}
        out = []
        for rec in reversed(recs[-limit:]):
            v = view(rec)
            v["why"] = self._why(rec)
            sent = roles.get(v["id"])
            if sent:
                v["role"], v["truth"] = sent["role"], sent["truth"]
            out.append(v)
        return {"rows": out, "total": len(recs)}

    def start_background(self):
        self.background.start(self.library.replay_start())

    def start_run(self, kind):
        if kind not in KINDS:
            raise ValueError(f"unknown scenario {kind!r}")
        msgs = replay_messages(self.library.pick(kind), time.time())
        run = {"id": uuid.uuid4().hex[:10], "kind": kind, "started": time.time(), "error": "",
               "rows": [{"id": m["message"]["transaction_id"], "role": m["role"],
                         "truth": m["kind"], "offset_s": m["offset_s"],
                         "amount": m["message"]["amount_uzs"],
                         "from": mask(m["message"]["sender_card"]),
                         "to": mask(m["message"]["receiver_card"])} for m in msgs]}
        self.runs[run["id"]] = run
        while len(self.runs) > 30:
            self.runs.popitem(last=False)

        def go():
            try:
                self.sender.send(msgs)
            except Exception as exc:                   # noqa: BLE001 - shown on the page
                run["error"] = str(exc)[:300]
        threading.Thread(target=go, daemon=True).start()
        return {"id": run["id"]}

    def run(self, run_id):
        run = self.runs.get(run_id)
        if run is None:
            return None
        rows = []
        for row in run["rows"]:
            r, rec = dict(row), self.decisions.by_id.get(row["id"])
            if rec is not None:
                r.update(decision=rec.get("decision"), score=rec.get("final_score"))
            rows.append(r)
        return {**run, "rows": rows}

    def results(self):
        """The model's test figures as ml/train.py wrote them to metrics.json, when
        it was trained, what the data it was tested on holds, and the public
        datasets from results.json."""
        own = {}
        try:
            with open(os.path.join(MODELS, "metrics.json"), encoding="utf-8") as fh:
                m = json.load(fh)
            own = {"precision": m["at_review"]["precision"], "recall": m["at_review"]["recall"],
                   "f1": m["at_review"]["f1"], "types": len(m["by_fraud_type"])}
            with open(os.path.join(MODELS, "manifest.json"), encoding="utf-8") as fh:
                own["trained_at"] = json.load(fh)["exported_at"]
        except (OSError, KeyError, ValueError) as exc:
            own = {"error": str(exc)[:200]}
        lib = self.library
        fit = int(lib.cut * 0.80)              # ml/train.py's FIT_SHARE
        own.update(rows=len(lib.df), fit=fit, val=lib.cut - fit, test=len(lib.df) - lib.cut,
                   fraud_share=float(lib.is_fraud.mean()),
                   test_fraud=int(lib.is_fraud[lib.cut:].sum()))
        with open(os.path.join(HERE, "results.json"), encoding="utf-8") as fh:
            quoted = json.load(fh)
        own["second"] = quoted["own"]["second"]
        return {"own": own, "public": quoted["datasets"]}

    def store(self):
        if self._store is None:
            from store import CaseStore
            c = CLICKHOUSE
            s = CaseStore(c["host"], c["port"], c["username"], c["password"], c["database"])
            s.open()
            self._store = s
        return self._store

    def warehouse(self):
        if self._warehouse is None:
            import clickhouse_connect
            self._warehouse = clickhouse_connect.get_client(**CLICKHOUSE)
        return self._warehouse

    def live(self):
        """What the warehouse holds now: each stage's average over the latest
        decisions that carry stage times, arrival to decision over the same rows,
        and the analyst's verdicts."""
        cols = [f"stage_{s}_ms" for s in STAGES]
        q = (f"SELECT count(), {', '.join(f'avg({c})' for c in cols)}, "
             f"quantileExact(0.5)(total), quantileExact(0.99)(total) FROM ("
             f"SELECT {', '.join(cols)}, toUnixTimestamp64Milli(scored_at_job)"
             f" - toUnixTimestamp64Milli(ingested_at) AS total"
             f" FROM transactions_scored WHERE stage_decide_ms IS NOT NULL"
             f" AND toUnixTimestamp64Milli(ingested_at) > 0"
             f" ORDER BY scored_at_job DESC LIMIT {LIVE_WINDOW})")
        try:
            timing = stage_summary(self.warehouse().query(q).result_rows[0])
        except Exception as exc:                       # noqa: BLE001 - shown on the page
            self._warehouse = None
            timing = {"error": str(exc)[:300]}
        queue = self.cases()
        return {"timing": timing, "verdicts": queue["stats"], "holds": queue["holds"],
                "retrain": retrain_status()}

    def cases(self, new_only=False, limit=PAGE, flt=None):
        """case-manager's queue, in its own order: the first `limit` cases that pass
        the filter and how many do. `new_only` keeps the cases opened since this
        server started: the warehouse also holds every alert of every earlier
        measurement run, and the case a scenario just opened drowns in them - the
        store is asked for them, since its first 500 by amount can all be older."""
        try:
            s = self.store()
            # A store that cannot connect answers with an empty queue, which the page
            # would show as "no new alerts".
            if not s._ensure():
                raise RuntimeError("the case store cannot reach ClickHouse; see docker logs demo")
            items = s.open_cases(limit=500, since=self.started if new_only else None)
            # The verdicts given during this showing, each of which can be changed.
            done = s.resolved_cases(since=self.started, limit=PAGE) if new_only else []
            stats, holds = s.stats(), s.holds()
        except Exception as exc:                       # noqa: BLE001 - shown on the page
            return {"error": str(exc)[:300], "cases": [], "done": [], "total": 0,
                    "stats": {}, "holds": {}}
        now = time.time()
        if flt:
            items = [c for c in items if passes(
                flt, c["sender_card"], c["receiver_card"], c["amount_uzs"],
                _as_epoch(c["opened_at"]), _decisions(c), c["predicted_type"],
                _held_by(c, self.cut))]
        total, items = len(items), items[:limit]
        return {"total": total,
                "cases": [{"id": c["case_id"], "at": c["opened_at"], "amount": c["amount_uzs"],
                           "from": mask(c["sender_card"]), "to": mask(c["receiver_card"]),
                           "decision": c["decision"], "type": c["predicted_type"],
                           **_scores(c), "rules": list(c["rule_hits"] or []),
                           "held_s": round(CASE.held_seconds(c, now)), "second": _second(c),
                           "why": {"status": c.get("explanation_status") or "",
                                   "items": split_phrases(c.get("explanation") or [])}}
                          for c in items],
                "done": [{"id": c["case_id"], "amount": c["amount_uzs"],
                          "to": mask(c["receiver_card"]), "type": c["predicted_type"],
                          "verdict": c["disposition"], "by": c["resolved_by"],
                          "held": c["decision"] == "REVIEW"}
                         for c in done],
                "stats": {k: v for k, v in stats.items()
                          if k in ("NEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "_precision")},
                "holds": holds}

    def resolve(self, case_id, disposition):
        """Block the held transfer (CONFIRMED_FRAUD) or release it (FALSE_POSITIVE);
        on a resolved case, change the verdict."""
        if disposition not in ("CONFIRMED_FRAUD", "FALSE_POSITIVE"):
            raise ValueError(f"unknown disposition {disposition!r}")
        return {"ok": bool(self.store().resolve(case_id, disposition, "demo"))}

    def report(self, transaction_id):
        """The client says a transfer the system let go was fraud."""
        return {"ok": bool(self.store().report(transaction_id, "demo"))}


APP = None


class Handler(BaseHTTPRequestHandler):
    server_version = "fraud-demo"

    def log_message(self, fmt, *args):                 # the page polls every second
        pass

    def _send(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(json.dumps(obj, default=str, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8", code)

    def do_GET(self):
        path, _, query = self.path.partition("?")
        q = parse_qs(query)
        if path in ("/", "/index.html", "/about.js"):
            name = path.strip("/") or "index.html"
            with open(os.path.join(HERE, name), "rb") as fh:
                self._send(fh.read(), "text/javascript; charset=utf-8" if name.endswith(".js")
                           else "text/html; charset=utf-8")
        elif path == "/api/status":
            self._json(APP.status())
        elif not APP.health.ready:
            self._json({"error": "the system is not running"}, 503)
        elif path == "/api/stream":
            self._json(APP.stream(flt=parse_filter(q), limit=_limit(q)))
        elif path.startswith("/api/episode/"):
            run = APP.run(path.rsplit("/", 1)[1])
            self._json(run if run else {"error": "unknown run"}, 200 if run else 404)
        elif path == "/api/live":
            self._json(APP.live())
        elif path == "/api/results":
            self._json(APP.results())
        elif path == "/api/cases":
            self._json(APP.cases(new_only=q.get("new") == ["1"], limit=_limit(q),
                                 flt=parse_filter(q)))
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        if not APP.health.ready:
            self._json({"error": "the system is not running"}, 503)
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(n) or b"{}")
            if self.path == "/api/background":
                (APP.start_background() if data.get("on")
                 else APP.background.stop())
                self._json({"on": APP.background.running()})
            elif self.path == "/api/episode":
                self._json(APP.start_run(data.get("kind", "")))
            elif self.path.startswith("/api/cases/") and self.path.endswith("/resolve"):
                self._json(APP.resolve(self.path.split("/")[3], data.get("disposition", "")))
            elif self.path == "/api/report":
                self._json(APP.report(data.get("transaction_id", "")))
            else:
                self._json({"error": "not found"}, 404)
        except Exception as exc:                       # noqa: BLE001 - shown on the page
            self._json({"error": str(exc)[:300]}, 400)


def main():
    global APP
    APP = App()
    APP.health.check()
    threading.Thread(target=APP.health.run, daemon=True).start()
    threading.Thread(target=APP.decisions.run, daemon=True).start()
    # Every interface inside the container; compose publishes the port on the
    # host's loopback only, since the page can send transfers and close cases.
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"demo: http://localhost:{PORT}   (Kafka {KAFKA}, ClickHouse "
          f"{CLICKHOUSE['host']}:{CLICKHOUSE['port']}, Flink {FLINK})")
    if APP.library_error:
        print(f"no dataset: {APP.library_error}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        APP.background.stop()


if __name__ == "__main__":
    main()
