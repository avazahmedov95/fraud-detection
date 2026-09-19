"""Live demo: one page that shows the pipeline deciding.

Three views over the running stack - the transfers streaming through, a fraud
episode replayed on demand with its decision and reasons, and the analyst queue.
It adds no detection logic: transfers are built by data-generator's
`kafka_producer._row_to_message`, decisions come back from `transactions.scored`,
reasons and queue from case-manager's `Explainer` and `CaseStore`.

Scenarios are real episodes from the held-out 20% of the dataset, replayed under
a fresh sender card and moved in time to end now; receivers keep their cards.

    python demo/server.py        # then open http://localhost:8090
"""
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GEN = os.path.join(ROOT, "data-generator")
CM = os.path.join(ROOT, "case-manager")
# Imported from these: kafka_producer and integrity, store, case and explain. None
# of them imports `config`, the one module name both directories hold.
sys.path[:0] = [GEN, CM]
import explain as EX          # noqa: E402
import integrity              # noqa: E402
import kafka_producer as KP   # noqa: E402

CSV_PATH = os.path.join(GEN, "out", "transactions.csv")
TOPIC_RAW, TOPIC_SCORED = "transactions.raw", "transactions.scored"
HELD_OUT_FROM = 0.80    # ml/train.py's cut: the model never trained past it
HISTORY_ROWS = 8        # the sender's own transfers replayed before the episode
EPISODE_HOURS = 6       # how far around the picked row the episode's rows are sought
KINDS = ("NORMAL", "APP", "ATO", "MULE", "STRUCTURING")


def _dotenv(path):
    """The stack's .env, so the demo reaches the ports and credentials the
    containers were started with. Values stay in memory; nothing is printed."""
    vals = {}
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip().strip("\"'")
    except OSError:
        pass
    return vals


ENV = {**_dotenv(os.path.join(ROOT, ".env")), **os.environ}
KAFKA = ENV.get("DEMO_KAFKA", "localhost:" + ENV.get("KAFKA_HOST_PORT", "29092"))
CLICKHOUSE = dict(host=ENV.get("DEMO_CLICKHOUSE_HOST", "localhost"),
                  port=int(ENV.get("CLICKHOUSE_HTTP_PORT", "8123")),
                  user=ENV.get("CLICKHOUSE_USER", "fraud"),
                  password=ENV.get("CLICKHOUSE_PASSWORD", ""),
                  database=ENV.get("CLICKHOUSE_DB", "fraud"))
PORT = int(ENV.get("DEMO_PORT", "8090"))
GRAFANA = "http://localhost:" + ENV.get("GRAFANA_PORT", "3000")
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
                    "message": KP._row_to_message(r, include_labels=False)})
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
        self.ms = deque(maxlen=1000)
        self.connected, self.error, self.last_at, self.model = False, "", None, ""

    def add(self, rec):
        with self.lock:
            self.recent.append(rec)
            if rec.get("transaction_id"):
                self.by_id[rec["transaction_id"]] = rec
                while len(self.by_id) > 50_000:
                    self.by_id.popitem(last=False)
            self.counts[rec.get("decision") or "?"] += 1
            if rec.get("scoring_ms") is not None:
                self.ms.append(float(rec["scoring_ms"]))
            self.last_at = time.time()
            self.model = rec.get("model_version") or self.model

    def run(self):
        from kafka import KafkaConsumer
        while True:
            try:
                consumer = KafkaConsumer(TOPIC_SCORED, bootstrap_servers=KAFKA,
                                         group_id=None, auto_offset_reset="latest")
                self.connected, self.error = True, ""
                for m in consumer:
                    try:
                        self.add(json.loads(m.value))
                    except ValueError:
                        continue
            except Exception as exc:                   # noqa: BLE001 - shown on the page
                self.connected, self.error = False, str(exc)[:200]
                time.sleep(3)


class Background:
    """data-generator's producer run unchanged - the dataset at its own pacing,
    200 times faster - so the stream is the one every figure was measured on.
    Starts at a random row so that two demos do not show the same minutes."""

    def __init__(self):
        self.proc, self.speed = None, 200

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, speed):
        if self.running():
            return
        self.speed = int(speed)
        self.proc = subprocess.Popen(
            [sys.executable, "kafka_producer.py", "--file", os.path.join("out", "transactions.csv"),
             "--realtime", "--speed", str(self.speed), "--skip", str(random.randrange(40_000)),
             "--bootstrap", KAFKA, "--topic", TOPIC_RAW],
            cwd=GEN, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self):
        if not self.running():
            return
        if os.name == "nt":
            # The venv's python.exe is a launcher that starts the real interpreter as
            # a child; terminating the launcher alone leaves the replay running.
            subprocess.run(["taskkill", "/PID", str(self.proc.pid), "/T", "/F"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            self.proc.terminate()


class Sender:
    """One producer, opened on first use, so the page loads with the stack down."""

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
            "score": rec.get("final_score"), "type": rec.get("predicted_type"),
            "rules": rec.get("rule_hits") or [], "ms": rec.get("scoring_ms")}


class App:
    def __init__(self, csv_path=CSV_PATH):
        self.decisions, self.background, self.sender = Decisions(), Background(), Sender()
        self.runs, self.started = OrderedDict(), time.time()
        self.grafana = _dashboard_url()
        self._store, self._explainer = None, None
        try:
            self.library, self.library_error = Episodes(csv_path), ""
        except (OSError, KeyError, ValueError) as exc:
            self.library, self.library_error = None, str(exc)

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
        return {"kafka": {"connected": d.connected, "error": d.error, "last": d.last_at},
                "background": {"on": self.background.running(), "speed": self.background.speed},
                "counts": counts, "median_ms": ms[len(ms) // 2] if ms else None,
                "model": d.model, "scenarios_error": self.library_error,
                "grafana": self.grafana}

    def stream(self, alerts_only=False, limit=60):
        with self.decisions.lock:
            recs = list(self.decisions.recent)
        if alerts_only:
            recs = [r for r in recs if r.get("decision") != "ALLOW"]
        return [view(r) for r in reversed(recs[-limit:])]

    def start_run(self, kind):
        if kind not in KINDS:
            raise ValueError(f"unknown scenario {kind!r}")
        if self.library is None:
            raise RuntimeError(self.library_error or "the dataset is not loaded")
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
                r.update(decision=rec.get("decision"), score=rec.get("final_score"),
                         rules=rec.get("rule_hits") or [], ms=rec.get("scoring_ms"),
                         type=rec.get("predicted_type"), why=self._why(rec))
            rows.append(r)
        return {**run, "rows": rows}

    def results(self):
        """The model's test figures as ml/train.py wrote them to metrics.json, the
        split they were measured on, and the public datasets from results.json."""
        own = {}
        try:
            with open(os.path.join(ROOT, "ml", "models", "metrics.json"),
                      encoding="utf-8") as fh:
                m = json.load(fh)
            own = {"roc_auc": m["roc_auc"], "pr_auc": m["pr_auc"],
                   "precision": m["at_review"]["precision"],
                   "recall": m["at_review"]["recall"],
                   "review": m["thresholds"]["review"],
                   "rules_precision": m["cep_only"]["precision"],
                   "rules_recall": m["cep_only"]["recall"],
                   "by_type": {k: v["recall"] for k, v in m["by_fraud_type"].items()}}
        except (OSError, KeyError, ValueError) as exc:
            own = {"error": str(exc)[:200]}
        if self.library is not None:
            lib = self.library
            n, cut = len(lib.df), lib.cut
            fit = int(cut * 0.80)                  # ml/train.py's FIT_SHARE
            own.update(rows=n, fit=fit, val=cut - fit, test=n - cut,
                       fraud_share=float(lib.is_fraud.mean()),
                       test_fraud=int(lib.is_fraud[cut:].sum()))
        with open(os.path.join(HERE, "results.json"), encoding="utf-8") as fh:
            return {"own": own, "public": json.load(fh)["datasets"]}

    def store(self):
        if self._store is None:
            from store import CaseStore
            s = CaseStore(**CLICKHOUSE)
            s.open()
            self._store = s
        return self._store

    def cases(self, new_only=False):
        """case-manager's queue, in its own order. `new_only` keeps the cases opened
        since this server started: the warehouse also holds every alert of every
        earlier measurement run, and the case a scenario just opened drowns in them."""
        try:
            s = self.store()
            items, stats = s.open_cases(limit=500), s.stats()
        except Exception as exc:                       # noqa: BLE001 - shown on the page
            return {"error": str(exc)[:300], "cases": [], "stats": {}}
        if new_only:
            items = [c for c in items if _as_epoch(c["opened_at"]) >= self.started]
        items = items[:40]
        return {"cases": [{"id": c["case_id"], "at": c["opened_at"], "amount": c["amount_uzs"],
                           "from": mask(c["sender_card"]), "to": mask(c["receiver_card"]),
                           "decision": c["decision"], "type": c["predicted_type"],
                           "score": c["final_score"], "rules": list(c["rule_hits"] or []),
                           "priority": c["priority"],
                           "why": {"status": c.get("explanation_status") or "",
                                   "items": split_phrases(c.get("explanation") or [])}}
                          for c in items],
                "stats": {k: v for k, v in stats.items()
                          if k in ("NEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "_precision")}}

    def resolve(self, case_id, disposition):
        if disposition not in ("CONFIRMED_FRAUD", "FALSE_POSITIVE"):
            raise ValueError(f"unknown disposition {disposition!r}")
        return {"ok": bool(self.store().resolve(case_id, disposition, "demo"))}


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
        if path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as fh:
                self._send(fh.read(), "text/html; charset=utf-8")
        elif path == "/api/status":
            self._json(APP.status())
        elif path == "/api/stream":
            self._json(APP.stream(alerts_only="alerts=1" in query))
        elif path.startswith("/api/episode/"):
            run = APP.run(path.rsplit("/", 1)[1])
            self._json(run if run else {"error": "unknown run"}, 200 if run else 404)
        elif path == "/api/results":
            self._json(APP.results())
        elif path == "/api/cases":
            self._json(APP.cases(new_only="new=1" in query))
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(n) or b"{}")
            if self.path == "/api/background":
                (APP.background.start(data.get("speed", 200)) if data.get("on")
                 else APP.background.stop())
                self._json({"on": APP.background.running()})
            elif self.path == "/api/episode":
                self._json(APP.start_run(data.get("kind", "")))
            elif self.path.startswith("/api/cases/") and self.path.endswith("/resolve"):
                self._json(APP.resolve(self.path.split("/")[3], data.get("disposition", "")))
            else:
                self._json({"error": "not found"}, 404)
        except Exception as exc:                       # noqa: BLE001 - shown on the page
            self._json({"error": str(exc)[:300]}, 400)


def main():
    global APP
    APP = App()
    threading.Thread(target=APP.decisions.run, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"demo: http://localhost:{PORT}   (Kafka {KAFKA}, ClickHouse "
          f"{CLICKHOUSE['host']}:{CLICKHOUSE['port']})")
    if APP.library_error:
        print(f"scenarios disabled: {APP.library_error}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        APP.background.stop()


if __name__ == "__main__":
    main()
