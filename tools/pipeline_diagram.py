"""The end-to-end diagram: every component, what it does, and the measured time of
each stage inside the engine (thesis/results.md, Speed). Writes docs/pipeline_diagram
as PNG, for a document, and SVG, which stays sharp at any size."""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

INK, MUTED, LINE = "#14202e", "#5b6b7f", "#c7d2e0"
ACCENT, ACCENT_SOFT = "#1c63c4", "#e6eefb"
ALERT, ALERT_SOFT = "#b34310", "#fbece3"
STORE_FILL, GROUP_FILL = "#f0f3f8", "#f7f9fc"
FAMILY = ["Calibri", "DejaVu Sans"]

fig, ax = plt.subplots(figsize=(16, 9))
ax.set_xlim(0, 160)
ax.set_ylim(0, 90)
ax.axis("off")
fig.patch.set_facecolor("white")


def box(x, y, w, h, title, sub="", time="", fill="white", edge=LINE, bold=True,
        title_size=11, sub_size=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=1.2",
                                linewidth=1.2, edgecolor=edge, facecolor=fill, zorder=2))
    ty = y + h - (3.0 if sub or time else h / 2 + 1.2)
    ax.text(x + w / 2, ty, title, ha="center", va="top", fontsize=title_size,
            fontweight="bold" if bold else "normal", color=INK, family=FAMILY, zorder=3)
    if sub:
        ax.text(x + w / 2, ty - 3.4, sub, ha="center", va="top", fontsize=sub_size,
                color=MUTED, family=FAMILY, zorder=3)
    if time:
        ax.text(x + w / 2, y + 1.4, time, ha="center", va="bottom", fontsize=9,
                color=ACCENT, fontweight="bold", family=FAMILY, zorder=3)
    return (x, y, w, h)


def arrow(p1, p2, style="-|>", dashed=False, color=ACCENT, rad=0.0, label="",
          label_dy=1.6, lw=1.6):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle=style, mutation_scale=14,
                                 linewidth=lw, color=color, zorder=1,
                                 linestyle="--" if dashed else "-",
                                 connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2 + label_dy, label,
                ha="center", va="bottom", fontsize=8.5, color=MUTED, family=FAMILY)


def right(b):
    return (b[0] + b[2], b[1] + b[3] / 2)


def left(b):
    return (b[0], b[1] + b[3] / 2)


def top(b):
    return (b[0] + b[2] / 2, b[1] + b[3])


def bottom(b):
    return (b[0] + b[2] / 2, b[1])


ax.text(0, 87, "Real-time fraud detection: one transfer, end to end",
        fontsize=19, fontweight="bold", color=INK, family=FAMILY, va="bottom")
ax.text(0, 83.6, "Every component the system runs, what it does, and how long each stage "
                 "of the decision takes. Measured 5 October 2026 on the running stack, on battery.",
        fontsize=10.5, color=MUTED, family=FAMILY, va="bottom")

# --- row 1: into the engine ------------------------------------------------
app = box(0, 64, 22, 12, "Bank app", "a customer confirms\na P2P transfer")
raw = box(27, 64, 24, 12, "Kafka", "topic transactions.raw,\nkeyed by sender")

ax.add_patch(FancyBboxPatch((56, 57.5), 104, 23, boxstyle="round,pad=0,rounding_size=1.6",
                            linewidth=1.4, edgecolor=ACCENT, facecolor=GROUP_FILL, zorder=0))
ax.text(58, 78.6, "Apache Flink (PyFlink)  |  three copies, senders hashed across them  |  4 slots",
        fontsize=10, fontweight="bold", color=ACCENT, family=FAMILY, va="top")

dec = box(58, 61, 18, 12, "Decode", "parse, decrypt\nif encrypted", "0.05 ms")
fea = box(79, 61, 18, 12, "Sender's history", "this sender's state,\nkept in Flink", "1.9 ms")
rul = box(100, 61, 18, 12, "Features, rules", "24 features,\n10 hard rules", "0.65 ms")
mod = box(121, 61, 18, 12, "Model", "gradient boosting,\nserved as ONNX", "0.40 ms",
          fill=ACCENT_SOFT, edge=ACCENT)
dcd = box(142, 61, 16, 12, "Decision", "allow, or hold\nfor the analyst", "0.03 ms")

arrow(right(app), left(raw), label="1.1 ms")
arrow(right(raw), (58, 67), label="25 ms")
for a, b in ((dec, fea), (fea, rul), (rul, mod), (mod, dcd)):
    arrow(right(a), left(b))

red = box(79, 40, 18, 12, "Redis", "recent payers,\nconfirmed fraud accounts", "1.6 ms",
          fill=STORE_FILL)
arrow(top(red), bottom(fea), style="<|-|>", color=MUTED, lw=1.3)

ax.text(36, 45, "The engine's own work is 4.6 ms.\nThe other 26 ms is the transfer waiting "
                "between\nKafka and the engine, in batches.",
        fontsize=9.5, color=MUTED, family=FAMILY, va="center",
        bbox=dict(boxstyle="round,pad=0.6", facecolor="white", edgecolor=LINE))

# --- row 2: out of the engine ----------------------------------------------
sco = box(136, 24, 24, 12, "Kafka", "transactions.scored:\nevery decision")
arrow(bottom(dcd), top(sco), rad=-0.15)

# A transfer just under the cut-off waits for TabPFN (second-look/README.md).
sec = box(110, 40, 24, 12, "Second look", "TabPFN, for transfers\njust under the cut-off",
          "about 0.3 s", fill=ACCENT_SOFT, edge=ACCENT)
arrow((146, 61), (130, 52))
arrow((131, 40), (139, 36))
for x, y, label in ((139.5, 53.8, "just under\nthe cut-off"), (128.5, 36.6, "its decision")):
    ax.text(x, y, label, ha="left" if x > 130 else "right", va="bottom", fontsize=8.5,
            color=MUTED, family=FAMILY)

snk = box(104, 24, 24, 12, "Sink writer", "every decision, its audit\nrecord, and a case per\nhold with its reasons",
          sub_size=8.5)
arrow(left(sco), right(snk))

chs = box(68, 24, 28, 12, "ClickHouse", "every scored transfer,\naudit chain, cases",
          fill=STORE_FILL)
arrow(left(snk), right(chs))

dash = box(20, 24, 40, 12, "Grafana and the demo page",
           "volumes, alert rate, types over time;\nthe same decisions live, in two languages",
           fill=STORE_FILL, sub_size=8.5)
arrow(left(chs), right(dash), dashed=True, color=MUTED, label="read only")

# --- row 3: the people --------------------------------------------------------
ana = box(104, 4, 24, 12, "Analyst", "blocks or releases, corrects a\nverdict, records a client's report;\nverdicts are the retraining labels",
          fill=ALERT_SOFT, edge=ALERT, sub_size=8.5)
arrow(bottom(chs), left(ana), color=ALERT, label="the queue: demo or CLI", rad=0.25)


ax.text(0, 61.4, "Offline, not in the live path", fontsize=10, fontweight="bold",
        color=INK, family=FAMILY, va="center")
trn = box(0, 42, 32, 18, "Training (ml/)", "replays the same feature code,\nfits five models into one,\n"
                                           "exports ONNX and the cut-off;\nretrains every day on the decisions",
          sub_size=8.5)
arrow(right(trn), (56, 62), dashed=True, color=MUTED, label="model.onnx", label_dy=0.8)

ax.text(0, 1.2, "Times are averages over 1,000 transfers at 10 a second and add up to 31 ms; 99% of decisions "
                "are inside 0.05 s against a 0.3 s target, and inside 0.3 s up to 200 a second; a transfer just "
                "under the cut-off waits about 0.3 s more for the second look.",
        fontsize=9, color=MUTED, family=FAMILY, va="bottom")

for ext in ("png", "svg"):
    out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docs", f"pipeline_diagram.{ext}")
    fig.savefig(out, dpi=200 if ext == "png" else None, bbox_inches="tight",
                facecolor="white")
    print("wrote", out)
