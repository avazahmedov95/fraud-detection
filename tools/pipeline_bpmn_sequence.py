"""Two more views of the same pipeline for docs/: an editable BPMN 2.0 file, and a
sequence diagram as Mermaid source plus a drawn PNG and SVG. Same components and
same measured times as tools/pipeline_diagram.py."""
import os
import xml.dom.minidom as minidom

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")

# --------------------------------------------------------------- BPMN 2.0
# Every task names what runs it, in brackets; the databases have a lane of their
# own, read and written along dotted arrows. No counts: they change.

POOL = dict(x=160, y=60, w=2880)
LANES = [("Lane_channel", "Payment channel", 160),
         ("Lane_flink", "Apache Flink job", 360),
         ("Lane_sink", "Sink writer", 260),
         ("Lane_data", "Data stores", 140),
         ("Lane_case", "Case manager", 150),
         ("Lane_analyst", "Fraud analyst", 150)]

NODES = [
    # id, kind, name, lane, (x, y, w, h) of the shape, its label's box or None
    ("Start", "startEvent", "Transfer confirmed by the payer", 0,
     (232, 122, 36, 36), (195, 162, 110, 28)),
    ("Create", "serviceTask", "Create the transfer event\n[payment switch, simulated in Python]",
     0, (310, 90, 180, 100), None),
    ("Publish", "sendTask", "Publish the event\n[Kafka: transactions.raw]",
     0, (530, 90, 180, 100), None),
    ("Read", "serviceTask", "Read and decode the event\n[Flink Kafka source]",
     1, (530, 350, 180, 100), None),
    ("Sender", "serviceTask", "Load the sender's history\n[Flink keyed state]",
     1, (750, 350, 180, 100), None),
    ("Receiver", "serviceTask", "Read and update the receiver's recent payers\n[Redis]",
     1, (970, 350, 180, 100), None),
    ("Features", "serviceTask", "Compute the features\n[Python in Flink]",
     1, (1190, 350, 180, 100), None),
    ("Split", "parallelGateway", "", 1, (1410, 375, 50, 50), None),
    ("Rules", "businessRuleTask", "Check the hard rules, name the alert type\n[Python in Flink]",
     1, (1500, 235, 180, 100), None),
    ("Model", "serviceTask", "Score the risk\n[LightGBM via ONNX Runtime]",
     1, (1500, 465, 180, 100), None),
    ("Join", "parallelGateway", "", 1, (1720, 375, 50, 50), None),
    ("Decision", "businessRuleTask",
     "Decide from the model score and the rule results\n[Python in Flink]",
     1, (1810, 350, 180, 100), None),
    ("Decide", "exclusiveGateway", "Send to review?",
     1, (2030, 375, 50, 50), (1995, 347, 120, 20)),
    ("AllowOut", "sendTask", "Publish the decision ALLOW\n[Kafka: transactions.scored]",
     1, (2130, 350, 180, 100), None),
    ("AlertOut", "sendTask", "Publish REVIEW and the alert\n[Kafka: transactions.scored, fraud.alerts]",
     1, (2130, 465, 180, 100), None),
    ("Fork", "parallelGateway", "", 2, (2195, 635, 50, 50), None),
    ("Merge", "exclusiveGateway", "", 2, (2335, 635, 50, 50), None),
    ("Store", "serviceTask", "Store the decision and its audit record\n[Python → ClickHouse]",
     2, (2425, 610, 180, 100), None),
    ("IsReview", "exclusiveGateway", "Is the decision REVIEW?",
     2, (2645, 635, 50, 50), (2605, 598, 130, 28)),
    ("Allowed", "endEvent", "Transfer allowed and recorded",
     2, (2745, 642, 36, 36), (2789, 646, 130, 28)),
    ("Graph", "serviceTask", "Add the alert to the graph\n[Python → Neo4j]",
     2, (2725, 720, 180, 100), None),
    ("AlertDone", "endEvent", "Alert recorded", 2, (2945, 752, 36, 36), (2920, 792, 90, 20)),
    ("Case", "serviceTask", "Open a case with its reasons\n[Python service]",
     4, (2130, 1005, 180, 100), None),
    ("Opened", "intermediateThrowEvent", "Case opened",
     4, (2350, 1037, 36, 36), (2325, 1077, 86, 20)),
    ("Review", "userTask", "Review the case\n[analyst queue: demo page or CLI]",
     5, (2415, 1155, 180, 100), None),
    ("Verdict", "userTask", "Mark the case fraud or false alarm\n[saved to ClickHouse]",
     5, (2635, 1155, 180, 100), None),
    ("Closed", "endEvent", "Case closed", 5, (2855, 1187, 36, 36), (2830, 1227, 86, 20)),
]

FLOWS = [
    # id, from, to, label, waypoints, label's box
    ("F_start", "Start", "Create", "", [(268, 140), (310, 140)], None),
    ("F_create", "Create", "Publish", "", [(490, 140), (530, 140)], None),
    ("F_publish", "Publish", "Read", "", [(620, 190), (620, 350)], None),
    ("F_read", "Read", "Sender", "", [(710, 400), (750, 400)], None),
    ("F_sender", "Sender", "Receiver", "", [(930, 400), (970, 400)], None),
    ("F_receiver", "Receiver", "Features", "", [(1150, 400), (1190, 400)], None),
    ("F_features", "Features", "Split", "", [(1370, 400), (1410, 400)], None),
    ("F_to_rules", "Split", "Rules", "", [(1435, 375), (1435, 285), (1500, 285)], None),
    ("F_to_model", "Split", "Model", "", [(1435, 425), (1435, 515), (1500, 515)], None),
    ("F_rules", "Rules", "Join", "", [(1680, 285), (1745, 285), (1745, 375)], None),
    ("F_model", "Model", "Join", "", [(1680, 515), (1745, 515), (1745, 425)], None),
    ("F_join", "Join", "Decision", "", [(1770, 400), (1810, 400)], None),
    ("F_decided", "Decision", "Decide", "", [(1990, 400), (2030, 400)], None),
    ("F_allow", "Decide", "AllowOut", "No", [(2080, 400), (2130, 400)], (2086, 381, 18, 14)),
    ("F_alert", "Decide", "AlertOut", "Yes", [(2055, 425), (2055, 515), (2130, 515)],
     (2062, 452, 20, 14)),
    ("F_allow_out", "AllowOut", "Merge", "", [(2310, 400), (2360, 400), (2360, 635)], None),
    ("F_alert_out", "AlertOut", "Fork", "", [(2220, 565), (2220, 635)], None),
    ("F_to_merge", "Fork", "Merge", "", [(2245, 660), (2335, 660)], None),
    ("F_to_case", "Fork", "Case", "", [(2220, 685), (2220, 1005)], None),
    ("F_merge", "Merge", "Store", "", [(2385, 660), (2425, 660)], None),
    ("F_store", "Store", "IsReview", "", [(2605, 660), (2645, 660)], None),
    ("F_allowed", "IsReview", "Allowed", "No", [(2695, 660), (2745, 660)], (2701, 641, 18, 14)),
    ("F_to_graph", "IsReview", "Graph", "Yes", [(2670, 685), (2670, 770), (2725, 770)],
     (2677, 712, 20, 14)),
    ("F_graph", "Graph", "AlertDone", "", [(2905, 770), (2945, 770)], None),
    ("F_case", "Case", "Opened", "", [(2310, 1055), (2350, 1055)], None),
    ("F_opened", "Opened", "Review", "", [(2386, 1055), (2505, 1055), (2505, 1155)], None),
    ("F_review", "Review", "Verdict", "", [(2595, 1205), (2635, 1205)], None),
    ("F_verdict", "Verdict", "Closed", "", [(2815, 1205), (2855, 1205)], None),
]
#: The "No" of each question is its default flow.
DEFAULTS = {"Decide": "F_allow", "IsReview": "F_allowed"}

STORES = [
    # id, name, (x, y, w, h), label's box
    ("Redis", "Redis: receivers' recent payers", (1035, 875, 50, 50), (985, 930, 150, 28)),
    ("Cases", "ClickHouse: cases and verdicts", (2260, 875, 50, 50), (2316, 886, 120, 28)),
    ("Warehouse", "ClickHouse: decisions, audit log", (2490, 875, 50, 50),
     (2455, 930, 120, 28)),
    ("GraphDb", "Neo4j: alert graph", (2790, 875, 50, 50), (2760, 930, 110, 20)),
]

DATA = [
    # id, task, store, "in" (the task reads) or "out" (the task writes), waypoints
    ("D_redis_read", "Receiver", "Redis", "in", [(1050, 875), (1050, 450)]),
    ("D_redis_write", "Receiver", "Redis", "out", [(1070, 450), (1070, 875)]),
    ("D_store", "Store", "Warehouse", "out", [(2515, 710), (2515, 875)]),
    ("D_graph", "Graph", "GraphDb", "out", [(2815, 820), (2815, 875)]),
    ("D_case", "Case", "Cases", "out", [(2285, 1005), (2285, 925)]),
    ("D_verdict", "Verdict", "Cases", "out", [(2725, 1155), (2305, 925)]),
]

NOTES = [
    # id, text, (x, y, w, h), the task it explains, waypoints
    ("Note_decision", "REVIEW when the model score is at or above the alert cut-off, "
                      "or a mandatory rule fired; otherwise ALLOW",
     (1785, 232, 230, 58), "Decision", [(1900, 350), (1900, 290)]),
]

BOX = ({n[0]: n[4] for n in NODES} | {s[0]: s[2] for s in STORES}
       | {n[0]: n[2] for n in NOTES})


def on_border(box, point):
    x, y, w, h = box
    px, py = point
    return (x - 1 <= px <= x + w + 1 and y - 1 <= py <= y + h + 1
            and min(abs(px - x), abs(px - x - w), abs(py - y), abs(py - y - h)) <= 1)


# A line that starts or ends off its shape opens fine and looks broken.
for fid, src, tgt, _, points, _ in FLOWS:
    assert on_border(BOX[src], points[0]) and on_border(BOX[tgt], points[-1]), fid
for did, task, store, way, points in DATA:
    src, tgt = (store, task) if way == "in" else (task, store)
    assert on_border(BOX[src], points[0]) and on_border(BOX[tgt], points[-1]), did
for nid, _, _, task, points in NOTES:
    assert on_border(BOX[task], points[0]) and on_border(BOX[nid], points[-1]), nid


def esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("\n", "&#10;"))


def bounds(x, y, w, h):
    return f'<dc:Bounds x="{x}" y="{y}" width="{w}" height="{h}" />'


parts = ['<?xml version="1.0" encoding="UTF-8"?>',
         '<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"'
         ' xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"'
         ' xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"'
         ' xmlns:di="http://www.omg.org/spec/DD/20100524/DI"'
         ' id="Definitions_fraud" targetNamespace="http://bpmn.io/schema/bpmn">',
         '  <bpmn:collaboration id="Collaboration_1">',
         '    <bpmn:participant id="Participant_1" name="Real-time fraud detection"'
         ' processRef="Process_1" />',
         '  </bpmn:collaboration>',
         '  <bpmn:process id="Process_1" isExecutable="false">',
         '    <bpmn:laneSet id="LaneSet_1">']
for i, (lid, lname, _) in enumerate(LANES):
    parts.append(f'      <bpmn:lane id="{lid}" name="{esc(lname)}">')
    parts += [f'        <bpmn:flowNodeRef>{n[0]}</bpmn:flowNodeRef>' for n in NODES if n[3] == i]
    parts.append('      </bpmn:lane>')
parts.append('    </bpmn:laneSet>')

for nid, kind, name, *_ in NODES:
    attrs = f' id="{nid}"' + (f' name="{esc(name)}"' if name else "")
    if nid in DEFAULTS:
        attrs += f' default="{DEFAULTS[nid]}"'
    parts.append(f'    <bpmn:{kind}{attrs}>')
    parts += [f'      <bpmn:incoming>{f[0]}</bpmn:incoming>' for f in FLOWS if f[2] == nid]
    parts += [f'      <bpmn:outgoing>{f[0]}</bpmn:outgoing>' for f in FLOWS if f[1] == nid]
    reads = [d for d in DATA if d[1] == nid and d[3] == "in"]
    if reads:
        parts.append(f'      <bpmn:property id="Property_{nid}" name="__targetRef_placeholder" />')
    for did, _, store, _, _ in reads:
        parts += [f'      <bpmn:dataInputAssociation id="{did}">',
                  f'        <bpmn:sourceRef>{store}</bpmn:sourceRef>',
                  f'        <bpmn:targetRef>Property_{nid}</bpmn:targetRef>',
                  '      </bpmn:dataInputAssociation>']
    for did, _, store, _, _ in (d for d in DATA if d[1] == nid and d[3] == "out"):
        parts += [f'      <bpmn:dataOutputAssociation id="{did}">',
                  f'        <bpmn:targetRef>{store}</bpmn:targetRef>',
                  '      </bpmn:dataOutputAssociation>']
    parts.append(f'    </bpmn:{kind}>')
for sid, sname, *_ in STORES:
    parts.append(f'    <bpmn:dataStoreReference id="{sid}" name="{esc(sname)}" />')
for fid, src, tgt, label, *_ in FLOWS:
    nm = f' name="{esc(label)}"' if label else ""
    parts.append(f'    <bpmn:sequenceFlow id="{fid}"{nm} sourceRef="{src}" targetRef="{tgt}" />')
for nid, note, _, task, _ in NOTES:
    parts += [f'    <bpmn:textAnnotation id="{nid}">',
              f'      <bpmn:text>{esc(note)}</bpmn:text>',
              '    </bpmn:textAnnotation>',
              f'    <bpmn:association id="{nid}_link" sourceRef="{task}" targetRef="{nid}" />']
parts.append('  </bpmn:process>')

parts += ['  <bpmndi:BPMNDiagram id="Diagram_1">',
          '    <bpmndi:BPMNPlane id="Plane_1" bpmnElement="Collaboration_1">',
          '      <bpmndi:BPMNShape id="Participant_1_di" bpmnElement="Participant_1"'
          ' isHorizontal="true">',
          f'        {bounds(POOL["x"], POOL["y"], POOL["w"], sum(h for *_, h in LANES))}',
          '      </bpmndi:BPMNShape>']
top = POOL["y"]
for lid, _, h in LANES:
    parts += [f'      <bpmndi:BPMNShape id="{lid}_di" bpmnElement="{lid}" isHorizontal="true">',
              f'        {bounds(POOL["x"] + 30, top, POOL["w"] - 30, h)}',
              '      </bpmndi:BPMNShape>']
    top += h
shapes = ([(n[0], n[1], n[4], n[5]) for n in NODES] + [(s[0], "", s[2], s[3]) for s in STORES]
          + [(n[0], "", n[2], None) for n in NOTES])
for sid, kind, box, label in shapes:
    marker = ' isMarkerVisible="true"' if kind == "exclusiveGateway" else ""
    parts += [f'      <bpmndi:BPMNShape id="{sid}_di" bpmnElement="{sid}"{marker}>',
              f'        {bounds(*box)}']
    if label:
        parts.append(f'        <bpmndi:BPMNLabel>{bounds(*label)}</bpmndi:BPMNLabel>')
    parts.append('      </bpmndi:BPMNShape>')
edges = ([(f[0], f[4], f[5]) for f in FLOWS] + [(d[0], d[4], None) for d in DATA]
         + [(f"{n[0]}_link", n[4], None) for n in NOTES])
for eid, points, label in edges:
    parts.append(f'      <bpmndi:BPMNEdge id="{eid}_di" bpmnElement="{eid}">')
    parts += [f'        <di:waypoint x="{x}" y="{y}" />' for x, y in points]
    if label:
        parts.append(f'        <bpmndi:BPMNLabel>{bounds(*label)}</bpmndi:BPMNLabel>')
    parts.append('      </bpmndi:BPMNEdge>')
parts += ['    </bpmndi:BPMNPlane>', '  </bpmndi:BPMNDiagram>', '</bpmn:definitions>']

xml = "\n".join(parts)
minidom.parseString(xml)                       # refuse to write something unopenable
path = os.path.join(OUT, "pipeline.bpmn")
with open(path, "w", encoding="utf-8", newline="\n") as fh:
    fh.write(xml + "\n")
print("wrote", path)

# --------------------------------------------------------------- sequence

MERMAID = """sequenceDiagram
    autonumber
    actor Customer
    participant App as Bank app
    participant Kafka as Kafka
    participant Flink as Flink engine
    participant Redis as Redis
    participant Sink as Sink writer
    participant DB as ClickHouse / Neo4j
    participant Case as Case manager
    actor Analyst

    Customer->>App: confirms a transfer
    App->>Kafka: transactions.raw, keyed by sender (1.7 ms)
    Kafka->>Flink: the record, fetched and batched (92 ms)
    Flink->>Flink: decode (0.05 ms)
    Flink->>Flink: read this sender's state (0.12 ms)
    Flink->>Redis: how many people paid this receiver?
    Redis-->>Flink: counts for the hour, day and week (2.07 ms)
    Flink->>Flink: build 21 features, run the 10 hard rules (0.90 ms)
    Flink->>Flink: score with the model (0.46 ms)
    Flink->>Flink: decide: allow or review (0.03 ms)
    Flink->>Kafka: transactions.scored, and fraud.alerts when it is an alert (97 ms in all)
    Kafka->>Sink: every decision
    Sink->>DB: the decision, its audit record, and alerts into the graph
    Kafka->>Case: fraud.alerts
    Case->>DB: open a case, with its reason in words
    Case->>Analyst: the case appears in the queue
    Analyst->>DB: fraud, or false alarm
"""
path = os.path.join(OUT, "pipeline_sequence.mmd")
with open(path, "w", encoding="utf-8", newline="\n") as fh:
    fh.write(MERMAID)
print("wrote", path)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

INK, MUTED, LINE = "#14202e", "#5b6b7f", "#c7d2e0"
ACCENT, ALERT = "#1c63c4", "#b34310"
FAMILY = ["Calibri", "DejaVu Sans"]

ACTORS = ["Bank app", "Kafka", "Flink engine", "Redis", "Sink writer",
          "ClickHouse\nNeo4j", "Case manager", "Analyst"]
X = {name: 11 + i * 19.4 for i, name in enumerate(ACTORS)}

MESSAGES = [
    ("Bank app", "Kafka", "the transfer, keyed by sender", "1.7 ms", False),
    ("Kafka", "Flink engine", "the record, fetched and batched", "92 ms", False),
    ("Flink engine", "Flink engine", "decode the record", "0.05 ms", False),
    ("Flink engine", "Flink engine", "read the sender's state", "0.12 ms", False),
    ("Flink engine", "Redis", "how many people paid this receiver?", "", False),
    ("Redis", "Flink engine", "counts for the hour, day and week", "2.07 ms", True),
    ("Flink engine", "Flink engine", "build 21 features, run the 10 hard rules", "0.90 ms", False),
    ("Flink engine", "Flink engine", "score with the model", "0.46 ms", False),
    ("Flink engine", "Flink engine", "decide: allow, or send to review", "0.03 ms", False),
    ("Flink engine", "Kafka", "scored, and an alert when it is one", "97 ms in all", False),
    ("Kafka", "Sink writer", "every decision", "", False),
    ("Sink writer", "ClickHouse\nNeo4j", "the decision, its audit record, the graph", "", False),
    ("Kafka", "Case manager", "fraud.alerts", "", False),
    ("Case manager", "ClickHouse\nNeo4j", "a case, with its reason in words", "", False),
    ("Case manager", "Analyst", "the case appears in the queue", "", False),
    ("Analyst", "ClickHouse\nNeo4j", "fraud, or false alarm", "", True),
]

fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 160)
ax.set_ylim(0, 100)
ax.axis("off")
fig.patch.set_facecolor("white")

ax.text(0, 96, "One transfer through the system, step by step",
        fontsize=18, fontweight="bold", color=INK, family=FAMILY, va="bottom")
ax.text(0, 92.8, "The same path as the diagram, read top to bottom. Times are averages over "
                 "1,000 transfers at 10 a second, measured on the running stack on 30 September 2026.",
        fontsize=10.5, color=MUTED, family=FAMILY, va="bottom")

top_y, step = 86, 4.6
for name in ACTORS:
    x = X[name]
    ax.add_patch(FancyBboxPatch((x - 9, top_y), 18, 5.4,
                                boxstyle="round,pad=0,rounding_size=1.0",
                                linewidth=1.2, edgecolor=LINE, facecolor="white", zorder=3))
    ax.text(x, top_y + 2.7, name, ha="center", va="center", fontsize=9.5,
            fontweight="bold", color=INK, family=FAMILY, zorder=4)
    ax.plot([x, x], [6, top_y], color=LINE, linewidth=1.1, linestyle=(0, (4, 4)), zorder=1)

y = top_y - 4
for src, dst, text, when, dashed in MESSAGES:
    y -= step
    colour = ALERT if dst == "Analyst" or src == "Analyst" else ACCENT
    if src == dst:
        x = X[src]
        ax.add_patch(FancyArrowPatch((x, y + 1.2), (x, y - 1.2), arrowstyle="-|>",
                                     mutation_scale=12, linewidth=1.5, color=colour,
                                     connectionstyle="arc3,rad=-2.2", zorder=2))
        ax.text(x + 4.2, y + 0.4, text, ha="left", va="center", fontsize=9,
                color=INK, family=FAMILY, zorder=4)
        if when:
            ax.text(x + 4.2, y - 1.6, when, ha="left", va="center", fontsize=8.5,
                    color=ACCENT, fontweight="bold", family=FAMILY, zorder=4)
    else:
        x1, x2 = X[src], X[dst]
        ax.add_patch(FancyArrowPatch((x1, y), (x2, y), arrowstyle="-|>", mutation_scale=13,
                                     linewidth=1.5, color=colour, zorder=2,
                                     linestyle="--" if dashed else "-"))
        mid = (x1 + x2) / 2
        ax.text(mid, y + 0.9, text, ha="center", va="bottom", fontsize=9, color=INK,
                family=FAMILY, zorder=4)
        if when:
            ax.text(mid, y - 2.4, when, ha="center", va="bottom", fontsize=8.5,
                    color=ACCENT, fontweight="bold", family=FAMILY, zorder=4)

ax.text(0, 1.5, "Nothing is blocked automatically: an alert only moves the transfer into a "
                "person's queue. 999 of 1000 transfers stop at the warehouse.",
        fontsize=9, color=MUTED, family=FAMILY, va="bottom")

for ext in ("png", "svg"):
    out = os.path.join(OUT, f"pipeline_sequence.{ext}")
    fig.savefig(out, dpi=200 if ext == "png" else None, bbox_inches="tight", facecolor="white")
    print("wrote", out)
