"""Two more views of the same pipeline for docs/: an editable BPMN 2.0 file, and a
sequence diagram as Mermaid source plus a drawn PNG and SVG. Same components and
same measured times as tools/pipeline_diagram.py."""
import os
import xml.dom.minidom as minidom

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")

# --------------------------------------------------------------- BPMN 2.0

POOL = dict(x=160, y=80, w=1700, h=660)
LANES = [("Lane_channel", "Channel", 140),
         ("Lane_engine", "Streaming engine (Apache Flink)", 160),
         ("Lane_services", "Sink writer and case manager", 220),
         ("Lane_analyst", "Analyst", 140)]

NODES = [
    # id, kind, name, lane, x, y (top-left of the shape), w, h
    ("Start_1", "startEvent", "Transfer confirmed", 0, 240, 132, 36, 36),
    ("Task_publish", "task", "Publish to transactions.raw", 0, 330, 110, 150, 80),
    ("Task_decode", "task", "Decode the record", 1, 530, 260, 150, 80),
    ("Task_features", "task", "Build the 21 features", 1, 710, 260, 150, 80),
    ("Task_rules", "task", "Run the 10 hard rules", 1, 890, 260, 150, 80),
    ("Task_model", "task", "Score with the model", 1, 1070, 260, 150, 80),
    ("Gateway_1", "exclusiveGateway", "Above the cut-off, or a mandatory rule?",
     1, 1270, 278, 50, 50),
    ("Task_record", "task", "Write the decision and the audit record", 2, 1380, 400, 160, 80),
    ("End_allowed", "endEvent", "Allowed and recorded", 2, 1600, 422, 36, 36),
    ("Task_graph", "task", "Write the alert to the graph", 2, 1380, 520, 160, 80),
    ("Task_case", "task", "Open a case with its reason", 2, 1180, 520, 160, 80),
    ("Task_verdict", "task", "Mark fraud or false alarm", 3, 1380, 760, 160, 80),
    ("End_case", "endEvent", "Case closed", 3, 1600, 782, 36, 36),
]

FLOWS = [
    ("Flow_start", "Start_1", "Task_publish", ""),
    ("Flow_publish", "Task_publish", "Task_decode", "transactions.raw"),
    ("Flow_decode", "Task_decode", "Task_features", ""),
    ("Flow_features", "Task_features", "Task_rules", ""),
    ("Flow_rules", "Task_rules", "Task_model", ""),
    ("Flow_model", "Task_model", "Gateway_1", ""),
    ("Flow_allow", "Gateway_1", "Task_record", "allow"),
    ("Flow_recorded", "Task_record", "End_allowed", ""),
    ("Flow_review", "Gateway_1", "Task_graph", "send to review"),
    ("Flow_graph", "Task_graph", "Task_case", ""),
    ("Flow_case", "Task_case", "Task_verdict", "fraud.alerts"),
    ("Flow_verdict", "Task_verdict", "End_case", ""),
]

STORES = [("Store_redis", "Redis: who paid this account", 710, 400, 50, 50),
          ("Store_ch", "ClickHouse: decisions, audit, cases", 1600, 520, 50, 50),
          ("Store_neo", "Neo4j: the alert graph", 1380, 640, 50, 50)]
ASSOCIATIONS = [("Assoc_redis", "Task_features", "Store_redis"),
                ("Assoc_ch", "Task_record", "Store_ch"),
                ("Assoc_neo", "Task_graph", "Store_neo")]


def centre(node):
    return node[4] + node[6] / 2, node[5] + node[7] / 2


by_id = {n[0]: n for n in NODES}
store_by_id = {s[0]: s for s in STORES}


def lane_y(i):
    y = POOL["y"]
    for k in range(i):
        y += LANES[k][2]
    return y


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


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
    for n in NODES:
        if n[3] == i:
            parts.append(f'        <bpmn:flowNodeRef>{n[0]}</bpmn:flowNodeRef>')
    parts.append('      </bpmn:lane>')
parts.append('    </bpmn:laneSet>')

for nid, kind, name, _, *_rest in NODES:
    inc = [f[0] for f in FLOWS if f[2] == nid]
    out = [f[0] for f in FLOWS if f[1] == nid]
    parts.append(f'    <bpmn:{kind} id="{nid}" name="{esc(name)}">')
    parts += [f'      <bpmn:incoming>{f}</bpmn:incoming>' for f in inc]
    parts += [f'      <bpmn:outgoing>{f}</bpmn:outgoing>' for f in out]
    parts.append(f'    </bpmn:{kind}>')

for sid, sname, *_ in STORES:
    parts.append(f'    <bpmn:dataStoreReference id="{sid}" name="{esc(sname)}" />')
for fid, src, tgt, label in FLOWS:
    nm = f' name="{esc(label)}"' if label else ""
    parts.append(f'    <bpmn:sequenceFlow id="{fid}"{nm} sourceRef="{src}" targetRef="{tgt}" />')
for aid, src, tgt in ASSOCIATIONS:
    parts.append(f'    <bpmn:association id="{aid}" sourceRef="{src}" targetRef="{tgt}" />')
parts.append('  </bpmn:process>')

parts += ['  <bpmndi:BPMNDiagram id="Diagram_1">',
          '    <bpmndi:BPMNPlane id="Plane_1" bpmnElement="Collaboration_1">',
          f'      <bpmndi:BPMNShape id="Participant_1_di" bpmnElement="Participant_1"'
          f' isHorizontal="true">',
          f'        <dc:Bounds x="{POOL["x"]}" y="{POOL["y"]}" width="{POOL["w"]}"'
          f' height="{POOL["h"]}" />',
          '      </bpmndi:BPMNShape>']
for i, (lid, _, h) in enumerate(LANES):
    parts += [f'      <bpmndi:BPMNShape id="{lid}_di" bpmnElement="{lid}" isHorizontal="true">',
              f'        <dc:Bounds x="{POOL["x"] + 30}" y="{lane_y(i)}"'
              f' width="{POOL["w"] - 30}" height="{h}" />',
              '      </bpmndi:BPMNShape>']
for nid, kind, name, _, x, y, w, h in NODES:
    label = ""
    if kind in ("startEvent", "endEvent", "exclusiveGateway"):
        label = (f'        <bpmndi:BPMNLabel>'
                 f'<dc:Bounds x="{x - 40}" y="{y + h + 6}" width="{w + 80}" height="30" />'
                 f'</bpmndi:BPMNLabel>')
    parts.append(f'      <bpmndi:BPMNShape id="{nid}_di" bpmnElement="{nid}">')
    parts.append(f'        <dc:Bounds x="{x}" y="{y}" width="{w}" height="{h}" />')
    if label:
        parts.append(label)
    parts.append('      </bpmndi:BPMNShape>')
for sid, _, x, y, w, h in STORES:
    parts += [f'      <bpmndi:BPMNShape id="{sid}_di" bpmnElement="{sid}">',
              f'        <dc:Bounds x="{x}" y="{y}" width="{w}" height="{h}" />',
              f'        <bpmndi:BPMNLabel><dc:Bounds x="{x - 60}" y="{y + h + 5}"'
              f' width="{w + 120}" height="40" /></bpmndi:BPMNLabel>',
              '      </bpmndi:BPMNShape>']
for fid, src, tgt, _ in FLOWS:
    a, b = by_id[src], by_id[tgt]
    ax, ay = centre(a)
    bx, by = centre(b)
    points = [(a[4] + a[6], ay), (b[4], by)] if abs(ay - by) < 1 else [
        (ax, a[5] + a[7]), (ax, by), (b[4], by)]
    if a[1] == "exclusiveGateway":
        points = [(a[4] + a[6] / 2, a[5] + a[7]), (ax, by), (b[4], by)]
    parts.append(f'      <bpmndi:BPMNEdge id="{fid}_di" bpmnElement="{fid}">')
    for px, py in points:
        parts.append(f'        <di:waypoint x="{int(px)}" y="{int(py)}" />')
    parts.append('      </bpmndi:BPMNEdge>')
for aid, src, tgt in ASSOCIATIONS:
    a, s = by_id[src], store_by_id[tgt]
    parts += [f'      <bpmndi:BPMNEdge id="{aid}_di" bpmnElement="{aid}">',
              f'        <di:waypoint x="{int(a[4] + a[6] / 2)}" y="{int(a[5] + a[7])}" />',
              f'        <di:waypoint x="{int(s[2] + s[4] / 2)}" y="{int(s[3])}" />',
              '      </bpmndi:BPMNEdge>']
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
