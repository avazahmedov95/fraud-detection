"""How the services talk to each other: every connection numbered on the picture,
and a table under it saying what passes along each one - the Kafka topics, the HTTP
endpoints, the SQL and the Redis commands. Writes docs/integration as PNG and SVG."""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Ellipse, FancyArrowPatch, FancyBboxPatch, Rectangle

INK, MUTED, ACCENT = "#14202e", "#5b6b7f", "#1c63c4"
FAMILY = ["Calibri", "DejaVu Sans"]
#: One colour per way of talking; the table's numbers carry the same colour.
WAY = {"kafka": "#2b2b2b", "redis": "#d82c20", "sql": "#b07d00", "http": ACCENT,
       "file": "#7b4bb7"}

fig, ax = plt.subplots(figsize=(20, 14.6))
ax.set_xlim(0, 200)
ax.set_ylim(0, 146)
ax.set_aspect("equal")
ax.axis("off")
fig.patch.set_facecolor("white")

ax.text(0, 145, "How the services talk to each other", fontsize=19, fontweight="bold",
        color=INK, family=FAMILY, va="top")
ax.text(0, 141.2, "Every connection is numbered; the table under the picture says what "
                  "passes along it.", fontsize=10.5, color=MUTED, family=FAMILY, va="top")


def node(x, y, title, caption, w=24, h=10, edge=INK):
    ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h,
                                boxstyle="round,pad=0,rounding_size=1.2", facecolor="white",
                                edgecolor=edge, lw=1.4, zorder=3))
    ax.text(x, y + h / 2 - 1.5, title, ha="center", va="top", fontsize=10,
            fontweight="bold", color=INK, family=FAMILY, zorder=6)
    ax.text(x, y + h / 2 - 4.4, caption, ha="center", va="top", fontsize=8,
            color=MUTED, family=FAMILY, linespacing=1.15, zorder=6)


def topic(x, y, name, caption, w=17, h=3.6, above=False):
    c = WAY["kafka"]
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, color=c, zorder=3))
    ax.add_patch(Ellipse((x - w / 2, y), h * 0.45, h, color=c, zorder=3))
    ax.add_patch(Ellipse((x + w / 2, y), h * 0.45, h, facecolor="#8a8a8a", edgecolor=c,
                         lw=1.2, zorder=4))
    ax.text(x, y, name, ha="center", va="center", fontsize=8.6, fontweight="bold",
            color="white", family=FAMILY, zorder=5)
    ax.text(x, y + h / 2 + 0.7 if above else y - h / 2 - 0.8, caption, ha="center",
            va="bottom" if above else "top", fontsize=7.6, color=MUTED, family=FAMILY,
            zorder=6)


def store(x, y, title, caption, colour, w=20, h=13):
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, facecolor="white",
                           edgecolor=colour, lw=1.6, zorder=3))
    ax.add_patch(Ellipse((x, y - h / 2), w, 3, facecolor="white", edgecolor=colour, lw=1.6,
                         zorder=3))
    ax.add_patch(Rectangle((x - w / 2 + 0.1, y - h / 2), w - 0.2, 1.2, color="white",
                           zorder=3))
    ax.add_patch(Ellipse((x, y + h / 2), w, 3, facecolor=colour, edgecolor=colour, lw=1.6,
                         zorder=4))
    ax.text(x, y + h / 2 - 2.6, title, ha="center", va="top", fontsize=10,
            fontweight="bold", color=INK, family=FAMILY, zorder=6)
    ax.text(x, y + h / 2 - 5.4, caption, ha="center", va="top", fontsize=7.8,
            color=MUTED, family=FAMILY, linespacing=1.15, zorder=6)


def link(n, way, points, at=None, both=False):
    """A connection: straight segments through the points, an arrowhead at the end
    (both ends when `both`), and its number in a circle."""
    c = WAY[way]
    xs, ys = zip(*points)
    if len(points) > 2:
        ax.plot(xs[:-1], ys[:-1], color=c, lw=1.5, zorder=2, solid_capstyle="butt",
                linestyle=(0, (4, 2.5)) if way == "file" else "-")
    ax.add_patch(FancyArrowPatch(points[-2], points[-1],
                                 arrowstyle="<|-|>" if both else "-|>", mutation_scale=11,
                                 lw=1.5, color=c, zorder=2, shrinkA=0, shrinkB=0,
                                 linestyle=(0, (4, 2.5)) if way == "file" else "-"))
    if at is None:
        (x0, y0), (x1, y1) = points[0], points[1]
        at = ((x0 + x1) / 2, (y0 + y1) / 2)
    ax.add_patch(Circle(at, 1.55, facecolor="white", edgecolor=c, lw=1.4, zorder=7))
    ax.text(at[0], at[1] - 0.05, str(n), ha="center", va="center", fontsize=8.2,
            fontweight="bold", color=c, family=FAMILY, zorder=8)


# ------------------------------------------------------------------ the parts
node(13, 122, "Payment switch", "creates each transfer;\nhere kafka_producer.py")
topic(40, 122, "transactions.raw", "6 parts, in order per sender card", above=True)
node(70, 122, "Flink job", "fraud_job.py: features,\nrules, model, decision", w=26, h=12,
     edge=ACCENT)
store(70, 96, "Redis", "who paid each card, contacts,\nconfirmed fraud cards", WAY["redis"])
topic(101, 131, "transactions.scored", "6 parts: every decision")
topic(101, 103, "fraud.second_look", "3 parts: just under the cut-off")
node(136, 131, "sink-writer", "decisions, audit,\na case per hold")
node(136, 103, "second-look", "TabPFN on the\ntransfers just under")
store(172, 117, "ClickHouse", "transactions_scored,\naudit_log, cases", WAY["sql"], w=22)
node(192, 96, "Grafana", "charts", w=14, h=8)
node(110, 83, "demo server", "the page's API, the live stream,\nthe analyst's queue (store.py)", w=26)
node(150, 83, "Analyst's browser", "the demo page,\nGrafana", w=22)
node(22, 96, "ml/ and the retrainer", "trains the model; the retrainer\nmakes a new one every day", w=28)
node(70, 136, "Operator: run.ps1, make", "", w=30, h=5)

# ------------------------------------------------------------------ Kafka
link(1, "kafka", [(25, 122), (31.5, 122)], at=(28.2, 124.5))
link(2, "kafka", [(48.5, 122), (57, 122)], at=(52.8, 124.5))
link(4, "kafka", [(83, 125), (88, 125), (88, 131), (92.5, 131)], at=(88, 128))
link(5, "kafka", [(83, 117.5), (86, 117.5), (86, 103), (92.5, 103)], at=(86, 108))
link(9, "kafka", [(109.5, 131), (124, 131)], at=(117, 133.4))
link(6, "kafka", [(109.5, 103), (124, 103)], at=(116, 105.4))
link(7, "kafka", [(124, 100.5), (120.5, 100.5), (120.5, 128.8), (109.9, 128.8)],
     at=(120.5, 114))
link(11, "kafka", [(113.5, 131), (113.5, 88.1)], at=(113.5, 95))
link(12, "kafka", [(97, 83), (40, 83), (40, 120.1)], at=(70, 83))

# ------------------------------------------------------------------ Redis
link(3, "redis", [(70, 116), (70, 103.2)], at=(70, 109.5), both=True)
link(8, "redis", [(130, 98), (130, 93.5), (80.2, 93.5)], at=(127, 93.5))
link(14, "redis", [(103, 88), (103, 90.8), (80.2, 90.8)], at=(95, 90.8))
link(20, "redis", [(36, 96), (59.8, 96)], at=(48, 96))

# ------------------------------------------------------------------ SQL
link(10, "sql", [(148, 131), (172, 131), (172, 124.2)], at=(160, 131))
link(13, "sql", [(116, 78), (116, 73), (176, 73), (176, 110.3)], at=(146, 73))
link(18, "sql", [(183.2, 119), (192, 119), (192, 100.2)], at=(192, 109))
link(21, "sql", [(16, 91), (16, 75.5), (180, 75.5), (180, 110.6)], at=(60, 75.5))

# ------------------------------------------------------------------ HTTP
link(15, "http", [(97, 85.5), (89.5, 85.5), (89.5, 111), (80, 111), (80, 115.9)], at=(89.5, 98))
link(16, "http", [(139, 83), (123.1, 83)], at=(131, 85.4), both=True)
link(17, "http", [(161, 83), (192, 83), (192, 91.8)], at=(186, 83))
link(22, "http", [(70, 133.5), (70, 128.1)], at=(73.5, 131))

# ------------------------------------------------------------------ files
link(19, "file", [(30, 101), (30, 108.5), (60, 108.5), (60, 115.9)], at=(45, 108.5))

# The ways of talking, as the arrows draw them.
for x0, way, name in ((118, "kafka", "Kafka"), (132, "redis", "Redis"),
                      (146, "sql", "SQL to ClickHouse"), (169, "http", "HTTP"),
                      (182, "file", "files")):
    ax.plot([x0, x0 + 4.5], [139.6, 139.6], color=WAY[way], lw=2,
            linestyle=(0, (4, 2.5)) if way == "file" else "-")
    ax.text(x0 + 5.5, 139.6, name, ha="left", va="center", fontsize=9, color=INK,
            family=FAMILY)

# ------------------------------------------------------------------ the table
ROWS = [
    (1, "kafka", "Payment switch -> Kafka", "sends each transfer to transactions.raw as JSON "
     "(encrypted, if switched on), keyed by sender card, with its arrival time "
     "(ingested_at) and hash (ingress_hash)"),
    (2, "kafka", "Kafka -> Flink job", "the job reads transactions.raw (group fraud-cep); "
     "its read position is saved every 2 s"),
    (3, "redis", "Flink job <-> Redis", "reads the payee's side: ZRANGEBYSCORE rcv:{payee}, "
     "cp:in:{payee}, cp:card:{payee}; ZREVRANGE cp:in:{sender}; SMISMEMBER confirmed:accounts; "
     "EXISTS second-look:alive. Writes back in one go: ZADD, ZREMRANGEBYSCORE, EXPIRE"),
    (4, "kafka", "Flink job -> Kafka", "every decision to transactions.scored, with "
     "its feature values and the time of each step"),
    (5, "kafka", "Flink job -> Kafka", "transfers just under the cut-off to "
     "fraud.second_look"),
    (6, "kafka", "Kafka -> second-look", "reads fraud.second_look (group fraud-second-look)"),
    (7, "kafka", "second-look -> Kafka", "its final decision to transactions.scored"),
    (8, "redis", "second-look -> Redis", "SET second-look:alive PX 5000 while it is working; "
     "without this flag the job holds these transfers itself"),
    (9, "kafka", "Kafka -> sink-writer", "reads transactions.scored (group "
     "fraud-sink-writer)"),
    (10, "sql", "sink-writer -> ClickHouse", "HTTP 8123: INSERT every decision into "
     "transactions_scored and audit_log (each record linked to the one before by a hash); "
     "INSERT a case with its reasons into cases for every hold; SELECT seq, record_hash to "
     "continue the chain"),
    (11, "kafka", "Kafka -> demo server", "reads transactions.scored from the newest "
     "message: the live stream"),
    (12, "kafka", "demo server -> Kafka", "sends the fraud scenarios and the background "
     "stream of generated transfers to transactions.raw"),
    (13, "sql", "demo server -> ClickHouse", "SELECT the step times FROM "
     "transactions_scored; the analyst's queue (demo/store.py, also the command line): "
     "SELECT ... FROM cases FINAL; a verdict or a client's report is INSERTed as a newer "
     "version of the case"),
    (14, "redis", "demo server -> Redis", "a confirmed fraud: SADD confirmed:accounts; "
     "cancelled: SREM, unless another case or confirmed:history (SISMEMBER) still has the card"),
    (15, "http", "demo server -> Flink", "GET :8081/jobs/overview: the page opens only "
     "while the job runs"),
    (16, "http", "browser <-> demo server", ":8090 GET /api/status, /api/stream, /api/live, "
     "/api/cases, /api/results, /api/episode/{id}; POST /api/episode, /api/background, "
     "/api/cases/{id}/resolve, /api/report"),
    (17, "http", "browser -> Grafana", ":3000, also shown inside the demo page; its own login"),
    (18, "sql", "Grafana -> ClickHouse", "port 9000: SELECT ... FROM transactions_scored "
     "for eight charts"),
    (19, "file", "ml/ -> the services", "model.onnx, thresholds.json, second_look.json "
     "copied next to the job (run.ps1 serve-prep); ml/models shared with the sink writer "
     "(model.txt explains the cases), second-look and the demo"),
    (20, "redis", "ml/seed_confirmed.py -> Redis", "SADD confirmed:accounts and "
     "confirmed:history: the confirmed fraud cards from the history"),
    (21, "sql", "retrainer -> ClickHouse", "every day: SELECT transaction_id, features FROM "
     "transactions_scored; SELECT transaction_id FROM cases FINAL WHERE disposition = "
     "'CONFIRMED_FRAUD'"),
    (22, "http", "Operator -> Flink", "GET /jobs/overview, GET /taskmanagers, PATCH "
     "/jobs/{id}?mode=cancel on :8081; flink run -s <checkpoint> in the jobmanager"),
]


def wrap(s, width):
    out, line = [], ""
    for word in s.split():
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    return out + [line]


ax.plot([0, 200], [70.5, 70.5], color="#c7d2e0", lw=1.2)
ROW_H = 1.95
y = {0: 68.5, 1: 68.5}
for i, (n, way, who, what) in enumerate(ROWS):
    col = 0 if i < 11 else 1
    x = 0.5 + col * 100.5
    lines = wrap(what, 118)
    ax.add_patch(Circle((x + 1.4, y[col] - 0.9), 1.3, facecolor="white", edgecolor=WAY[way],
                        lw=1.3, zorder=7))
    ax.text(x + 1.4, y[col] - 0.95, str(n), ha="center", va="center", fontsize=7.8,
            fontweight="bold", color=WAY[way], family=FAMILY, zorder=8)
    ax.text(x + 3.8, y[col], who, ha="left", va="top", fontsize=8.6, fontweight="bold",
            color=INK, family=FAMILY)
    for k, ln in enumerate(lines):
        ax.text(x + 3.8, y[col] - ROW_H * (k + 1), ln, ha="left", va="top", fontsize=8.2,
                color=MUTED, family=FAMILY)
    y[col] -= ROW_H * (len(lines) + 1) + 0.45

for ext in ("png", "svg"):
    out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docs", f"integration.{ext}")
    fig.savefig(out, dpi=170 if ext == "png" else None, bbox_inches="tight",
                facecolor="white")
    print("wrote", out)
