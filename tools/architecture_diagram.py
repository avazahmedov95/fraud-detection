"""The architecture at a glance, laid out like the cloud reference diagrams the
mentor sent: stages left to right, each part with the technology that runs it,
and the offline training path underneath. Writes docs/architecture as PNG and SVG.
No measured figures here: tools/pipeline_diagram.py carries those."""
import os

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import (Circle, Ellipse, FancyArrowPatch, FancyBboxPatch,
                                Polygon, Rectangle, Wedge)

INK, MUTED, ACCENT, ALERT = "#14202e", "#5b6b7f", "#1c63c4", "#b34310"
FAMILY = ["Calibri", "DejaVu Sans"]
COLOUR = dict(source="#6b7a8f", kafka="#2b2b2b", redis="#d82c20", clickhouse="#e0a800",
              python="#3776ab", grafana="#f46800")

fig, ax = plt.subplots(figsize=(19, 10))
ax.set_xlim(0, 190)
ax.set_ylim(0, 100)
ax.set_aspect("equal")
ax.axis("off")
fig.patch.set_facecolor("white")


def text(x, y, title, caption="", ha="center", size=10.5, colour=INK):
    ax.text(x, y, title, ha=ha, va="top", fontsize=size, fontweight="bold", color=colour,
            family=FAMILY, zorder=6)
    if caption:
        ax.text(x, y - 2.1, caption, ha=ha, va="top", fontsize=8.8, color=MUTED,
                family=FAMILY, linespacing=1.2, zorder=6)


def arrow(p, q, colour=INK, dashed=False, both=False, label="", at=None, ha="center"):
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="<|-|>" if both else "-|>",
                                 mutation_scale=12, lw=1.5, color=colour, zorder=2,
                                 linestyle=(0, (4, 3)) if dashed else "-",
                                 shrinkA=0, shrinkB=0))
    if label:
        x, y = at or ((p[0] + q[0]) / 2, (p[1] + q[1]) / 2)
        ax.text(x, y, label, ha=ha, va="center", fontsize=8.5, color=MUTED, family=FAMILY,
                zorder=6, bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                                    edgecolor="none"))


def elbow(points, colour=INK):
    """A right-angled connector: lines through the points, an arrowhead at the end."""
    xs, ys = zip(*points)
    ax.plot(xs[:-1], ys[:-1], color=colour, lw=1.5, zorder=2, solid_capstyle="butt")
    arrow(points[-2], points[-1], colour)


# ---------------------------------------------------------------- the icons

def person(x, y, s, colour):
    ax.add_patch(Circle((x, y + 0.45 * s), 0.22 * s, color=colour, zorder=3))
    ax.add_patch(Wedge((x, y - 0.32 * s), 0.45 * s, 0, 180, color=colour, zorder=3))


def server(x, y, s, colour):
    for i in range(3):
        ax.add_patch(FancyBboxPatch((x - 0.6 * s, y - 0.5 * s + i * 0.36 * s), 1.2 * s,
                                    0.28 * s, boxstyle="round,pad=0,rounding_size=0.25",
                                    color=colour, zorder=3))
        ax.add_patch(Circle((x + 0.42 * s, y - 0.36 * s + i * 0.36 * s), 0.05 * s,
                            color="white", zorder=4))


def topic(x, y, w, h, colour):
    """A Kafka topic: an append-only log, drawn as a pipe in segments."""
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, color=colour, zorder=3))
    ax.add_patch(Ellipse((x - w / 2, y), h * 0.45, h, color=colour, zorder=3))
    ax.add_patch(Ellipse((x + w / 2, y), h * 0.45, h, facecolor="#8a8a8a",
                         edgecolor=colour, lw=1.2, zorder=4))
    for i in range(1, 5):
        ax.plot([x - w / 2 + i * w / 5] * 2, [y - h / 2, y + h / 2], color="white",
                lw=1.1, zorder=4)


def database(x, y, w, h, colour):
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, color=colour, zorder=3))
    ax.add_patch(Ellipse((x, y - h / 2), w, w * 0.4, color=colour, zorder=3))
    ax.add_patch(Ellipse((x, y + h / 2), w, w * 0.4, facecolor="white", edgecolor=colour,
                         lw=1.6, zorder=4))


def gear(x, y, r, colour):
    for k in range(8):
        a = np.pi * k / 4
        ax.add_patch(Rectangle((x + np.cos(a) * r - 0.2 * r, y + np.sin(a) * r - 0.2 * r),
                               0.4 * r, 0.4 * r, angle=np.degrees(a),
                               rotation_point="center", color=colour, zorder=3))
    ax.add_patch(Circle((x, y), 0.85 * r, color=colour, zorder=3))
    ax.add_patch(Circle((x, y), 0.35 * r, color="white", zorder=4))


def screen(x, y, s, colour):
    ax.add_patch(FancyBboxPatch((x - 0.6 * s, y - 0.25 * s), 1.2 * s, 0.8 * s,
                                boxstyle="round,pad=0,rounding_size=0.3", facecolor="white",
                                edgecolor=colour, lw=2, zorder=3))
    for i, h in enumerate((0.25, 0.5, 0.35)):
        ax.add_patch(Rectangle((x - 0.38 * s + i * 0.28 * s, y - 0.12 * s), 0.18 * s, h * s,
                               color=colour, zorder=4))
    ax.add_patch(Polygon([(x - 0.15 * s, y - 0.25 * s), (x + 0.15 * s, y - 0.25 * s),
                          (x + 0.3 * s, y - 0.45 * s), (x - 0.3 * s, y - 0.45 * s)],
                         color=colour, zorder=3))


def page(x, y, s, colour):
    w, h, f = 0.8 * s, 1.0 * s, 0.25 * s
    ax.add_patch(Polygon([(x - w / 2, y - h / 2), (x + w / 2, y - h / 2),
                          (x + w / 2, y + h / 2 - f), (x + w / 2 - f, y + h / 2),
                          (x - w / 2, y + h / 2)], facecolor="white", edgecolor=colour,
                         lw=1.8, zorder=3))
    for i in range(3):
        ax.plot([x - w / 2 + 0.12 * s, x + w / 2 - 0.12 * s],
                [y + 0.12 * s - i * 0.2 * s] * 2, color=colour, lw=1.4, zorder=4)


# ---------------------------------------------------------------- the frame

ax.text(0, 98, "Real-time fraud detection: architecture", fontsize=19, fontweight="bold",
        color=INK, family=FAMILY, va="top")
ax.text(0, 94.4, "What runs where, from the payer's confirmation to the analyst's verdict. "
                 "The model is trained offline, retrained on people's decisions, and "
                 "loaded into the stream job.",
        fontsize=10.5, color=MUTED, family=FAMILY, va="top")

STAGES = [("Event source", 0, 24), ("Ingestion", 25, 44), ("Stream processing", 45, 110),
          ("Delivery", 111, 131), ("Storage and cases", 132, 160), ("Outcome", 161, 190)]
for i, (name, x0, x1) in enumerate(STAGES):
    tip = 1.6
    points = [(x0, 86), (x1 - tip, 86), (x1, 88.5), (x1 - tip, 91), (x0, 91)]
    if i:
        points.append((x0 + tip, 88.5))
    ax.add_patch(Polygon(points, color=ACCENT, zorder=2))
    ax.text((x0 + x1) / 2 + (tip / 2 if i else 0), 88.5, name, ha="center", va="center",
            fontsize=11.5, fontweight="bold", color="white", family=FAMILY, zorder=3)
    ax.add_patch(FancyBboxPatch((x0 + 0.3, 27), x1 - x0 - 0.6, 57,
                                boxstyle="round,pad=0,rounding_size=1.5", facecolor="none",
                                edgecolor="#9db8e0", lw=1.3, linestyle=(0, (5, 3)), zorder=1))

# ---------------------------------------------------------------- event source
person(12, 74, 6, COLOUR["source"])
text(12, 69.5, "Payer", "confirms the transfer\nin the bank app")
arrow((12, 62.6), (12, 59.2))
server(12, 56, 5, COLOUR["source"])
text(12, 51.8, "Payment switch", "creates the transfer event;\nhere the data generator,\n"
                                  "Python")

# ---------------------------------------------------------------- ingestion
topic(34.5, 56, 11, 4, COLOUR["kafka"])
text(34.5, 51.8, "Apache Kafka", "topic transactions.raw,\nkeyed by sender")
arrow((15.6, 56), (28.2, 56))

# ---------------------------------------------------------------- the Flink job
ax.add_patch(FancyBboxPatch((46.5, 41), 62.3, 34, boxstyle="round,pad=0,rounding_size=1.4",
                            facecolor="#f3f7fd", edgecolor=ACCENT, lw=1.5, zorder=1))
ax.text(48.5, 73.6, "Apache Flink job (PyFlink): three copies side by side, keyed by sender",
        fontsize=10.5, fontweight="bold", color=ACCENT, family=FAMILY, va="top", zorder=6)


def step(cx, cy, title, caption="", w=8.4, fill="white"):
    ax.add_patch(FancyBboxPatch((cx - w / 2, cy - 4), w, 8,
                                boxstyle="round,pad=0,rounding_size=0.8", facecolor=fill,
                                edgecolor=ACCENT, lw=1.2, zorder=3))
    ax.text(cx, cy + (1.4 if caption else 0), title, ha="center", va="center", fontsize=8.8,
            fontweight="bold", color=INK, family=FAMILY, linespacing=1.1, zorder=6)
    if caption:
        ax.text(cx, cy - 2.2, caption, ha="center", va="center", fontsize=7.8, color=MUTED,
                family=FAMILY, linespacing=1.1, zorder=6)


step(52.8, 56, "Decode")
step(62.5, 56, "Sender's\nhistory", "Flink state")
step(72.2, 56, "Receiver's\npayers", "Redis")
step(81.9, 56, "Features")
step(93.1, 65, "Hard rules", "Python", w=10.4)
step(93.1, 47, "Model", "LightGBM,\nONNX Runtime", w=10.4, fill="#e6eefb")
step(103.8, 56, "Decision", "allow, hold,\nsecond look", w=7.6)
for a, b in ((57.0, 58.3), (66.7, 68.0), (76.4, 77.7)):
    arrow((a, 56), (b, 56))
elbow([(86.1, 56), (87.0, 56), (87.0, 65), (87.9, 65)])
elbow([(86.1, 56), (87.0, 56), (87.0, 47), (87.9, 47)])
elbow([(98.3, 65), (99.15, 65), (99.15, 56), (100.0, 56)])
elbow([(98.3, 47), (99.15, 47), (99.15, 56), (100.0, 56)])
arrow((40.6, 56), (48.6, 56))

database(72.2, 33, 5.5, 4, COLOUR["redis"])
arrow((72.2, 52), (72.2, 36.2), both=True)
text(66.5, 35.6, "Redis", "recent payers;\nconfirmed fraud\naccounts", ha="right")

# The second look: a score just under the cut-off waits while TabPFN decides it.
gear(103.8, 35.5, 2.4, "#7b4bb7")
text(103.8, 32.4, "Second look", "TabPFN, Python")
arrow((103.8, 52), (103.8, 38.4))
arrow((106.4, 36.5), (114.6, 46.4), label="its decision", at=(110.5, 41.4))

# ---------------------------------------------------------------- delivery
text(121, 74, "Apache Kafka")
topic(121, 66, 11, 4, COLOUR["kafka"])
text(121, 62.6, "", "transactions.scored:\nevery decision")
topic(121, 47, 11, 4, COLOUR["kafka"])
text(121, 43.6, "", "fraud.alerts:\nheld only")
elbow([(107.6, 56), (110.5, 56), (110.5, 66), (114.6, 66)])
elbow([(110.5, 56), (110.5, 47), (114.6, 47)], ALERT)

# ---------------------------------------------------------------- storage and cases
gear(138, 66, 2.6, COLOUR["python"])
text(138, 62.4, "Sink writer", "Python")
database(153.5, 70, 5.5, 4, COLOUR["clickhouse"])
text(153.5, 66.2, "ClickHouse", "decisions,\naudit log")
gear(138, 40, 2.6, COLOUR["python"])
text(138, 36.4, "Case manager", "Python;\nverdicts, reports")
database(153.5, 40, 5.5, 4, COLOUR["clickhouse"])
text(153.5, 36.2, "ClickHouse", "cases,\nverdicts")
arrow((127.4, 66), (134.6, 66))
arrow((141.4, 67), (150.4, 69.4))
arrow((127.4, 47), (135, 41.6), ALERT)
arrow((141.4, 40), (150.4, 40), ALERT)

# ---------------------------------------------------------------- outcome
screen(166, 70, 5, COLOUR["grafana"])
text(170.5, 72.4, "Dashboards", "Grafana, over\nClickHouse", ha="left")
person(166, 40, 5, ALERT)
text(170.5, 42.4, "Analyst queue", "demo page or CLI:\nblock or release,\na client's "
     "report,\ncorrect a verdict", ha="left", colour=ALERT)
arrow((156.6, 70), (162.6, 70))
arrow((156.6, 40), (162.8, 40), ALERT, both=True)
# A confirmation: the payee's card joins the confirmed fraud accounts the job reads;
# a withdrawn one takes it out.
elbow([(135.4, 39), (131.5, 39), (131.5, 26.5), (72.2, 26.5), (72.2, 30.4)], ALERT)
ax.text(110, 25.1, "a confirmation adds the payee to the confirmed fraud accounts; "
                   "a withdrawn one takes it out", ha="center",
        va="center", fontsize=8.5, color=MUTED, family=FAMILY, zorder=6,
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none"))

# ---------------------------------------------------------------- offline
ax.add_patch(FancyBboxPatch((0.3, 3), 189.4, 20, boxstyle="round,pad=0,rounding_size=1.5",
                            facecolor="#f7f9fc", edgecolor="#c7d2e0", lw=1.3,
                            linestyle=(0, (5, 3)), zorder=1))
ax.text(2, 21.2, "Offline: training and retraining, not in the live path", fontsize=11,
        fontweight="bold", color=MUTED, family=FAMILY, va="top")
gear(8, 11, 2.6, COLOUR["python"])
text(12.5, 13.6, "Data generator", "Python: people,\ntransfers, fraud", ha="left")
arrow((27, 11), (33.4, 11), MUTED)
page(37, 11, 5, MUTED)
text(40.5, 13.6, "Dataset", "CSV, labelled", ha="left")
arrow((53, 11), (59.6, 11), MUTED)
gear(63, 11, 2.6, COLOUR["python"])
text(67.5, 13.6, "Training", "Python, LightGBM\ncommittee", ha="left")
arrow((80, 11), (85.4, 11), MUTED)
page(89, 11, 5, MUTED)
text(92.5, 13.6, "model.onnx", "and the alert\ncut-off", ha="left")
arrow((93.1, 16.6), (93.1, 43), MUTED, dashed=True, label="loaded by the job",
      at=(93.1, 33))

# Retraining: the logged decisions and people's verdicts make a candidate, which a
# person promotes (ml/retrain.py).
gear(136, 11, 2.6, COLOUR["python"])
text(140.5, 13.6, "Retraining", "Python: the same committee on the\nlogged decisions, "
                                "labelled by people;\ndrift reported", ha="left")
arrow((152.5, 30.2), (138.6, 13.8), MUTED, dashed=True)
ax.text(149, 19.8, "from ClickHouse: decisions with\ntheir features, verdicts, reports",
        ha="left",
        va="center", fontsize=8.3, color=MUTED, family=FAMILY, zorder=6,
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none"))
arrow((132.6, 9), (107, 9), MUTED, label="a person promotes it", at=(119.8, 9))

for ext in ("png", "svg"):
    out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docs", f"architecture.{ext}")
    fig.savefig(out, dpi=200 if ext == "png" else None, bbox_inches="tight",
                facecolor="white")
    print("wrote", out)
