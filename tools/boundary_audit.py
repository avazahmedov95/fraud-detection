"""Checks every place one component hands something to another.
Reading a file says a component is right, not that what it PRODUCES is what the
next one EXPECTS. Run before a walkthrough and after touching any wire format.
"""

import argparse
import ast
import csv
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# config.py, integrity.py and payload_crypto.py exist in more than one package, so
# each package is imported in isolation rather than from one flat sys.path.
_PKG_CACHE = {}


def pkg(name, module):
    """Import `module` from package directory `name`, isolated from the others."""
    key = (name, module)
    if key in _PKG_CACHE:
        return _PKG_CACHE[key]
    import importlib
    path = os.path.join(ROOT, name)
    saved_path, saved_mods = list(sys.path), dict(sys.modules)
    sys.path.insert(0, path)
    try:
        for m in list(sys.modules):
            f = getattr(sys.modules[m], "__file__", None) or ""
            if f.startswith(ROOT) and not f.startswith(path):
                del sys.modules[m]
        mod = importlib.import_module(module)
        importlib.reload(mod)
        _PKG_CACHE[key] = mod
        return mod
    finally:
        sys.path[:] = saved_path
        for m in list(sys.modules):
            f = getattr(sys.modules[m], "__file__", None) or ""
            if f.startswith(ROOT):
                del sys.modules[m]
        sys.modules.update({k: v for k, v in saved_mods.items()
                            if k not in sys.modules})

RESULTS = []


def check(name, boundary):
    """Register one boundary check. The function returns None (pass) or a string."""
    def run():
        try:
            problem = boundary()
        except Exception as exc:                          # noqa: BLE001
            problem = f"check itself failed: {type(exc).__name__}: {exc}"
        RESULTS.append((name, problem))
    return run


def _read(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as fh:
        return fh.read()


def _sample_row():
    path = os.path.join(ROOT, "data-generator", "out", "transactions.csv")
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as fh:
        return next(csv.DictReader(fh))


def b_producer_types():
    P = pkg("data-generator", "kafka_producer")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    msg = P._row_to_message(row, include_labels=False)
    bad = {k: v for k, v in msg.items()
           if isinstance(v, str) and v.strip() in ("True", "False")}
    if bad:
        return (f"boolean fields travelling as text: {sorted(bad)} - every "
                f"consumer testing them for truthiness reads them as TRUE")
    return None


def b_wire_extracts_like_typed():
    P = pkg("data-generator", "kafka_producer")
    F = pkg("stream-processor", "features")
    R = pkg("stream-processor", "rules")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    wire = P._row_to_message(row, include_labels=False)
    typed = dict(wire)
    for k, v in list(typed.items()):                      # re-type as a caller would
        if isinstance(v, str) and v.lower() in ("true", "false"):
            typed[k] = v.lower() == "true"
    a = F.to_vector(F.extract(wire, R.SenderState(), now=1000.0))
    b = F.to_vector(F.extract(typed, R.SenderState(), now=1000.0))
    if a != b:
        diff = [n for n, x, y in zip(F.FEATURE_NAMES, a, b) if x != y]
        return f"wire and typed events disagree on: {diff}"
    return None


def b_extractor_needs_nothing_absent():
    """Every field features.extract reads must be one the producer sends."""
    P = pkg("data-generator", "kafka_producer")
    src = _read("stream-processor", "features.py")
    read = set(re.findall(r'event\.get\(\s*"([a-z_]+)"', src))
    read |= set(re.findall(r'event\[\s*"([a-z_]+)"\s*\]', src))
    sent = set(P.RAW_FIELDS) | {"ingested_at", "ingress_hash"}
    # documented optional inputs: a bank-side lookup supplies them, not the wire
    optional = {"is_family_transfer", "receiver_pinfl"}
    missing = sorted(read - sent - optional)
    if missing:
        return f"features.extract reads fields the producer never sends: {missing}"
    return None


def b_hash_covers_only_sent_fields():
    P = pkg("data-generator", "kafka_producer")
    integrity = pkg("data-generator", "integrity")
    msg_keys = set(P.RAW_FIELDS)
    missing = sorted(f for f in integrity.INGRESS_FIELDS if f not in msg_keys)
    if missing:
        return (f"hashed but never sent: {missing} - each contributes an empty "
                f"string, weakening the binding silently")
    return None


def b_duplicated_modules_are_identical():
    """Modules that DECLARE themselves duplicates must be byte-identical. The set is
    whatever says "byte-identical" in its own docstring, not a hard-coded pair."""
    declared = {}
    for pkg in ("stream-processor", "data-generator", "sink-writer",
                "case-manager", "validation", "ml"):
        d = os.path.join(ROOT, pkg)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".py") or fn.startswith("test_"):
                continue
            body = _read(pkg, fn)
            if "byte-identical" in body.split('"""')[1 if body.startswith('"""') else 0]:
                declared.setdefault(fn, []).append((f"{pkg}/{fn}", body))
    problems = []
    for fn, copies in sorted(declared.items()):
        if len(copies) < 2:
            problems.append(f"{copies[0][0]} claims to be byte-identical to a "
                            f"copy that does not exist")
        elif len({b for _, b in copies}) > 1:
            problems.append(f"{fn} differs between "
                            f"{', '.join(p for p, _ in copies)}")
    if not declared:
        return "no module declares itself a duplicate - has the convention changed?"
    return "; ".join(problems) or None


def b_routing_key_survives_the_wire():
    P = pkg("data-generator", "kafka_producer")
    PC = pkg("data-generator", "payload_crypto")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    msg = P._row_to_message(row, include_labels=False)
    plain = json.dumps(msg)
    got = PC.routing_key(plain)
    if got != msg["sender_card"]:
        return f"routing_key read {got!r} from a plaintext record, expected the sender card"
    key = bytes(range(32))
    enc = PC.encrypt(msg, key)
    got = PC.routing_key(enc)
    if got != msg["sender_card"]:
        return f"routing_key read {got!r} from an encrypted envelope"
    if PC.decrypt(enc, key) != msg:
        return "encrypt/decrypt did not round-trip the message"
    return None


def _job_output_keys():
    src = _read("stream-processor", "fraud_job.py")
    block = src.split("out = {", 1)[1].split("\n        }", 1)[0]
    keys = set(re.findall(r'^\s*"([a-z_]+)":', block, re.M))
    keys |= {"label_is_fraud", "label_fraud_type", "features"}   # conditional
    return keys


def b_sink_reads_only_emitted_keys():
    src = _read("sink-writer", "record.py")
    read = set(re.findall(r'e\.get\(\s*"([a-z_]+)"', src))
    missing = sorted(read - _job_output_keys())
    if missing:
        return f"sink-writer reads keys the job does not emit: {missing}"
    return None


def b_case_manager_reads_only_emitted_keys():
    src = _read("case-manager", "case.py") + _read("case-manager", "store.py")
    read = set(re.findall(r'alert\.get\(\s*"([a-z_]+)"', src))
    missing = sorted(read - _job_output_keys())
    if missing:
        return f"case-manager reads keys the job does not emit: {missing}"
    return None


def b_neo4j_params_match_the_cypher():
    R = pkg("sink-writer", "record")
    src = _read("sink-writer", "neo4j_writer.py")
    cypher = src.split("_MERGE = \"\"\"", 1)[1].split("\"\"\"", 1)[0]
    used = set(re.findall(r"row\.([a-z_]+)", cypher))
    supplied = set(R.alert_params({}))
    missing = sorted(used - supplied)
    extra = sorted(supplied - used)
    if missing:
        return f"Cypher references row.{{{','.join(missing)}}} which alert_params does not supply"
    if extra:
        return f"alert_params supplies unused keys: {extra} (harmless, but a sign of drift)"
    return None


def _ddl_columns(sql, table):
    body = sql.split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1].split("ENGINE", 1)[0]
    cols = []
    for line in body.splitlines():
        line = line.split("--", 1)[0].strip()
        m = re.match(r"^([a-z_]+)\s+[A-Za-z]", line)
        if m:
            cols.append(m.group(1))
    return cols


def b_scored_row_matches_the_schema():
    R = pkg("sink-writer", "record")
    sql = _read("infra", "clickhouse", "init", "01-schema.sql")
    declared = _ddl_columns(sql, "fraud.transactions_scored")
    extra = [c for c in R.SCORED_COLUMNS if c not in declared]
    if extra:
        return f"scored_row writes columns the table does not have: {extra}"
    if len(R.SCORED_COLUMNS) != len(R.scored_row({})):
        return "SCORED_COLUMNS and scored_row() have different lengths"
    return None


def b_case_row_matches_the_schema():
    C = pkg("case-manager", "case")
    sql = _read("infra", "clickhouse", "init", "02-cases.sql")
    declared = _ddl_columns(sql, "fraud.cases")
    declared += re.findall(r"ADD COLUMN IF NOT EXISTS\s+([a-z_]+)\s", sql)
    if declared != C.CASE_COLUMNS:
        return f"fraud.cases DDL {declared} != CASE_COLUMNS {C.CASE_COLUMNS}"
    return None


def _job_import_closure():
    """Every stream-processor module fraud_job.py reaches, transitively (to a fixed
    point, so a module three hops down is not missed)."""
    here = os.path.join(ROOT, "stream-processor")

    def local_imports(mod):
        found = set()
        for node in ast.walk(ast.parse(_read("stream-processor", mod))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            for n in names:
                f = n.split(".")[0] + ".py"
                if os.path.exists(os.path.join(here, f)):
                    found.add(f)
        return found

    closure, frontier = set(), {"fraud_job.py"}
    while frontier:
        mod = frontier.pop()
        for dep in local_imports(mod):
            if dep not in closure:
                closure.add(dep)
                frontier.add(dep)
    return closure - {"fraud_job.py"}


def b_job_modules_cover_every_import():
    """Both submitters (Makefile PYFILES and run.ps1) must ship the whole closure -
    the mounted directory can hide a module missing from one of them."""
    needed = _job_import_closure()

    ps1 = _read("run.ps1")
    listed_ps1 = set(re.findall(r'"([a-z_]+\.py)"',
                                ps1.split("$JobModules = @(", 1)[1].split(")", 1)[0]))

    mk = _read("Makefile")
    pyfiles_line = [l for l in mk.splitlines() if l.startswith("PYFILES")][0]
    listed_mk = set(re.findall(r'([a-z_]+\.py)', pyfiles_line))

    problems = []
    for where, listed in (("run.ps1 $JobModules", listed_ps1),
                          ("Makefile PYFILES", listed_mk)):
        missing = sorted(needed - listed)
        if missing:
            problems.append(f"{where} does not ship {missing}")
    if problems:
        return ("; ".join(problems) + " - the job then depends on the usrjobs "
                "mount rather than on what it declares")
    return None


def b_serve_prep_ships_every_artefact():
    ps1 = _read("run.ps1")
    prep = ps1.split('"serve-prep" {', 1)[1].split("}", 1)[0]
    copied = set(re.findall(r'Copy-Item "[^"]*/([A-Za-z0-9_.]+)"', prep))
    cfg = _read("stream-processor", "config.py")
    needed = set(re.findall(r'_resolve_artefact\(\s*"[A-Z_]+",\s*"([^"]+)"', cfg))
    missing = sorted(needed - copied)
    if missing:
        return (f"config resolves {missing} but serve-prep does not copy them "
                f"into the mounted job directory")
    return None


def b_no_artefact_path_derived_from_file():
    """__file__-relative data paths are the trap that killed the job twice."""
    offenders = []
    here = os.path.join(ROOT, "stream-processor")
    for fn in sorted(os.listdir(here)):
        if not fn.endswith(".py") or fn.startswith("test_"):
            continue
        src = _read("stream-processor", fn)
        for m in re.finditer(r"os\.path\.join\([^)]*__file__[^)]*\)", src):
            frag = m.group(0)
            if re.search(r"\.(csv|onnx|json|txt|joblib)", frag):
                offenders.append(f"{fn}: {frag[:70]}")
    if offenders:
        return ("data artefact resolved against __file__ (Flink ships modules to "
                "a temp dir; use config._resolve_artefact): " + "; ".join(offenders))
    return None


def b_manifest_matches_the_deployment():
    """The served artefacts, dataset and feature contract must be the manifest's - a
    drift in any of them leaves a system that runs and reports itself healthy."""
    m = pkg("ml", "manifest")
    if not os.path.exists(m.PATH):
        return "SKIP: no manifest - run ml/export_onnx.py"
    problems = m.check()
    return "; ".join(problems) or None


def b_receiver_store_round_trips():
    F = pkg("stream-processor", "features")
    RS = pkg("stream-processor", "receiver_store")

    written = {}

    class Pipe:
        def zadd(self, key, mapping): written.setdefault(key, {}).update(mapping)
        def zremrangebyscore(self, *a): pass
        def expire(self, *a): pass
        def execute(self): pass

    class Fake:
        def pipeline(self): return Pipe()

        def zrangebyscore(self, key, lo, hi, withscores=False):
            rows = sorted(written.get(key, {}).items(), key=lambda kv: kv[1])
            return rows if withscores else [m for m, _ in rows]

        def zrevrange(self, key, start, stop, withscores=False):
            rows = sorted(written.get(key, {}).items(), key=lambda kv: -kv[1])
            return rows if withscores else [m for m, _ in rows]

    store = RS.ReceiverStore("h", 1)
    store._redis = Fake()
    ev = {"transaction_id": "t1", "sender_pinfl": "S1", "amount_uzs": 500_000,
          "receiver_card": "8600330000000002", "receiver_pinfl": "R1"}
    store.record(ev, now=1000.0)
    state = store.load(F.payee_key(ev), now=1000.0)
    if state is None or len(state.inbound) != 1:
        return f"a member written by record() was not read back by load(): {written}"
    ts, sender, amount = state.inbound[0]
    if sender != "S1" or amount != 500_000.0:
        return f"member parsed as {(ts, sender, amount)}, expected S1 / 500000"
    # The counterparty map travels the same way, under its own key.
    CAP = pkg("stream-processor", "capabilities")
    if CAP.enabled("counterparty_history"):
        if state.payers != {"S1": 1000.0}:
            return f"counterparty map read back as {state.payers}, expected S1 at 1000"
        if store.last_inbound(F.payee_key(ev), now=1000.0) != 1000.0:
            return "last_inbound did not read back the time the payee was paid"
    return None


def b_latency_query_matches_its_parser():
    """The SELECT list and the row parser must agree on the column count, and every
    index the report reads must exist - otherwise rows drop silently or columns swap."""
    L = pkg(os.path.join("stream-processor", "experiments"), "latency")
    body = L.QUERY.split("SELECT", 1)[1].split("FROM", 1)[0]
    depth, cols = 0, 1
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            cols += 1
    src = _read("stream-processor", "experiments", "latency.py")
    want = int(re.search(r"len\(parts\) == (\d+)", src).group(1))
    if cols != want:
        return (f"the query selects {cols} columns and the parser accepts rows "
                f"of {want} - every row would be discarded and the report would "
                f"claim there is no instrumented data")
    used = {int(m) for m in re.findall(r"r\[(\d)\]", src)}
    if used and max(used) >= cols:
        return f"the report reads r[{max(used)}] but the query selects {cols} columns"
    return None


def b_event_mapping_matches_the_csv():
    """`features.event_from` must be satisfiable by a real generated row - exercised
    on one, rather than checked against a list of names."""
    F = pkg("stream-processor", "features")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    try:
        event = F.event_from(row)
    except KeyError as exc:
        return (f"features.event_from requires CSV column {exc} and the "
                f"generator does not produce it")
    empty = sorted(k for k, v in event.items() if v == "")
    if empty:
        return (f"features.event_from defaulted {empty} to empty on a real row - "
                f"the column was renamed or dropped, and the default hid it")
    return None


def b_compose_env_names_are_read():
    compose = _read("docker-compose.yml")
    svc = compose.split("  case-manager:", 1)[1].split("networks:", 1)[0]
    declared = set(re.findall(r"^\s{6}([A-Z_]+):", svc, re.M))
    cfg = _read("case-manager", "config.py")
    read = set(re.findall(r'os\.getenv\(\s*"([A-Z_]+)"', cfg))
    unused = sorted(declared - read)
    if unused:
        return (f"docker-compose sets {unused} for case-manager but its config "
                f"never reads them - the setting has no effect")
    return None


def b_every_module_is_documented():
    """Each package's README must mention every .py file beside it."""
    problems = []
    for pkg in ("stream-processor", "data-generator", "ml", "sink-writer",
                "case-manager", "validation", "tools", "demo"):
        d = os.path.join(ROOT, pkg)
        readme = os.path.join(d, "README.md")
        if not os.path.isdir(d) or not os.path.exists(readme):
            continue
        text = _read(pkg, "README.md")
        # One level down as well, so experiments/ stays covered.
        names = [fn for fn in os.listdir(d) if fn.endswith(".py")]
        for sub in sorted(os.listdir(d)):
            subdir = os.path.join(d, sub)
            if os.path.isdir(subdir) and sub not in ("tests", "__pycache__"):
                names += [fn for fn in os.listdir(subdir) if fn.endswith(".py")]
        missing = sorted(fn for fn in names if fn not in text)
        if missing:
            problems.append(f"{pkg}/README.md does not mention {missing}")
    return "; ".join(problems) or None


#: Columns known to be constant in the dataset of record, each named here so a NEW
#: constant column still fails; an entry that stops being constant fails too.
KNOWN_CONSTANT = {}


def b_no_new_constant_columns():
    """A column with one value everywhere is a feature that was never wired."""
    out = os.path.join(ROOT, "data-generator", "out")
    if not os.path.isdir(out):
        return "SKIP: dataset not generated"
    problems = []
    for name in ("transactions.csv", "persons.csv"):
        path = os.path.join(out, name)
        if not os.path.exists(path):
            continue
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            seen = {c: set() for c in (reader.fieldnames or [])}
            for row in reader:
                for c, acc in seen.items():
                    if len(acc) < 2:
                        acc.add(row.get(c))
        constant = {c for c, acc in seen.items() if len(acc) <= 1}
        unexpected = sorted(constant - KNOWN_CONSTANT.get(name, set()))
        vanished = sorted(KNOWN_CONSTANT.get(name, set()) - constant)
        if unexpected:
            problems.append(f"{name}: {unexpected} carry one value for every row")
        if vanished:
            problems.append(f"{name}: {vanished} are no longer constant - drop "
                            f"them from KNOWN_CONSTANT")
    return "; ".join(problems) or None


#: Every figure a DOCUMENT quotes from a GENERATED artefact: (document, pattern with
#: ONE capture group, source, how the raw value renders). `source` is ("metrics",
#: *keys) into metrics.json, or ("seeds", "mean"|"sd"). A pattern that stops
#: matching fails as loudly as a wrong number.
_f3 = lambda v: f"{v:.3f}"
_pct1 = lambda v: f"{v*100:.1f}"

_TRACKED_FIGURES = [
    # --- ml/README.md: the metrics table and the calibration block -----------
    ("ml/README.md", r"^\| ROC-AUC\s+\|\s+([\d.]+)", ("metrics", "roc_auc"), _f3),
    ("ml/README.md", r"^\| PR-AUC\s+\|\s+([\d.]+)", ("metrics", "pr_auc"), _f3),
    ("ml/README.md", r"^\| precision at REVIEW\s+\|\s+([\d.]+)", ("metrics", "at_review", "precision"), _f3),
    ("ml/README.md", r"^\| precision at REVIEW\s+\|\s+[\d.]+\s+\|\s+([\d.]+)", ("metrics", "cep_only", "precision"), _f3),
    ("ml/README.md", r"^\| recall at REVIEW\s+\|\s+([\d.]+)", ("metrics", "at_review", "recall"), _f3),
    ("ml/README.md", r"^\| recall at REVIEW\s+\|\s+[\d.]+\s+\|\s+([\d.]+)", ("metrics", "cep_only", "recall"), _f3),
    ("ml/README.md", r"STRUCTURING ([\d.]+)%", ("metrics", "by_fraud_type", "STRUCTURING", "recall"), _pct1),
    ("ml/README.md", r"fraud type \(ML at REVIEW\):.*?APP ([\d.]+)%", ("metrics", "by_fraud_type", "APP", "recall"), _pct1),
    ("ml/README.md", r"fraud type \(ML at REVIEW\):.*?ATO ([\d.]+)%", ("metrics", "by_fraud_type", "ATO", "recall"), _pct1),
    ("ml/README.md", r"fraud type \(ML at REVIEW\):.*?MULE ([\d.]+)%", ("metrics", "by_fraud_type", "MULE", "recall"), _pct1),
    ("ml/README.md", r"cut at REVIEW = ([\d.]+)", ("metrics", "thresholds", "review"), lambda v: f"{v:.4f}"),
    ("ml/README.md", r"^brier\s+([\d.]+)", ("metrics", "calibration", "brier"), lambda v: f"{v:.5f}"),
    ("ml/README.md", r"^n_alerts\s+(\d+)", ("metrics", "calibration", "n_alerts"), str),
    ("ml/README.md", r"^saturated_share\s+([\d.]+)%", ("metrics", "calibration", "saturated_share"), _pct1),
    ("ml/README.md", r"^distinct_scores\s+(\d+)", ("metrics", "calibration", "distinct_scores"), str),
    ("ml/README.md", r"^median_alert_score\s+([\d.]+)", ("metrics", "calibration", "median_alert_score"), lambda v: f"{v:.6f}"),
    ("ml/README.md", r"baseline PR-AUC \*\*([\d.]+) ±", ("seeds", "mean"), _f3),
    ("ml/README.md", r"baseline PR-AUC \*\*[\d.]+ ± ([\d.]+)\*\*", ("seeds", "sd"), _f3),
    ("ml/README.md", r"5 seeds, baseline ([\d.]+) ±", ("seeds", "mean"), _f3),
    # --- the same numbers, in the documents written for other readers --------
    ("docs/generator-spec.md", r"\(([\d.]+) ± [\d.]+ across seeds\)", ("seeds", "mean"), _f3),
    ("docs/generator-spec.md", r"\([\d.]+ ± ([\d.]+) across seeds\)", ("seeds", "sd"), _f3),
    ("docs/irp-framing.md", r"Baseline ([\d.]+) ±", ("seeds", "mean"), _f3),
    ("docs/irp-framing.md", r"Baseline [\d.]+ ± ([\d.]+)\.", ("seeds", "sd"), _f3),
    ("docs/related-work.md", r"\| PR-AUC \| [\d.]+ \(([\d.]+) ±", ("seeds", "mean"), _f3),
    ("docs/related-work.md", r"\| PR-AUC \| [\d.]+ \([\d.]+ ± ([\d.]+) across", ("seeds", "sd"), _f3),
    # Only the realistic profile's figures are current here; baseline rows are history.
    ("docs/related-work.md", r"the committee scores ([\d.]+) PR-AUC", ("metrics", "pr_auc"), _f3),
    ("validation/README.md", r"On the realistic profile the full system scores PR-AUC ([\d.]+)", ("metrics", "pr_auc"), _f3),
]


def b_documents_match_the_generated_figures():
    """Every figure a document quotes must still be the one that was measured - not
    that the numbers are good, but that the text describes the system that exists."""
    mpath = os.path.join(ROOT, "ml", "models", "metrics.json")
    spath = os.path.join(ROOT, "ml", "models", "ablation", "seeds.json")
    if not os.path.exists(mpath):
        return "SKIP: metrics.json not present"
    with open(mpath, encoding="utf-8") as fh:
        metrics = json.load(fh)

    seeds = None
    if os.path.exists(spath):
        import statistics
        with open(spath, encoding="utf-8") as fh:
            data = json.load(fh)
        vals = [v["baseline"]["pr_auc"] for k, v in data.items()
                if k != "_contract" and "baseline" in v]
        if len(vals) > 1:
            seeds = {"mean": statistics.mean(vals), "sd": statistics.stdev(vals)}

    cache, problems = {}, []
    for doc, pattern, source, render in _TRACKED_FIGURES:
        if source[0] == "seeds":
            if seeds is None:
                continue                      # no ablation recorded; nothing to compare
            value = seeds[source[1]]
        else:
            value = metrics
            for k in source[1:]:
                value = value[k]
        if doc not in cache:
            cache[doc] = _read(*doc.split("/"))
        m = re.search(pattern, cache[doc], re.M | re.S)
        label = f"{doc}:{'.'.join(source[1:])}"
        if not m:
            problems.append(f"{label}: no longer stated where this check looks")
            continue
        expected = render(value)
        if m.group(1) != expected:
            problems.append(f"{label}: document says {m.group(1)}, "
                            f"the artefact gives {expected}")
    return "; ".join(problems) or None


def b_external_baseline_matches_the_docs():
    """related-work.md 6 and paysim_adapter.BASELINE quote the same published PaySim
    baseline; only the REPORTED column is pinned (the reproduced one moves with
    library versions)."""
    sys.path.insert(0, os.path.join(ROOT, "validation"))
    try:
        import paysim_adapter as PA
    except Exception as exc:                       # pragma: no cover
        return f"SKIP: cannot import paysim_adapter ({exc})"
    text = _read("docs", "related-work.md")

    problems = []
    for label, key, pattern in (
            ("PR-AUC", "pr_auc", r"\| AUPRC \(= PR-AUC\) \| \*\*([\d.]+)\*\*"),
            ("ROC-AUC", "roc_auc", r"\| AUC-ROC \| ([\d.]+) \|"),
            ("recall @2%", "recall_at_2pct", r"\| recall \| ([\d.]+)% \(916 of"),
            ("top-decile lift", "lift_at_decile", r"\| top-decile lift \| ([\d.]+)"),
            ("PR-AUC with leakage", "pr_auc_with_leakage",
             r"before\* removing balance leakage \| ([\d.]+) \|")):
        m = re.search(pattern, text)
        if not m:
            problems.append(f"{label}: related-work.md 6 no longer states it "
                            f"where this check looks")
            continue
        want = PA.BASELINE[key]
        if key == "recall_at_2pct":
            want *= 100
        if abs(float(m.group(1)) - want) > 1e-9:
            problems.append(f"{label}: doc says {m.group(1)}, "
                            f"paysim_adapter.BASELINE says {want}")
    return "; ".join(problems) or None


def b_demo_results_quote_their_sources():
    """demo/results.json against the documents it summarises: each figure carries
    the exact line it came from, so a changed document fails here."""
    import json
    path = os.path.join(ROOT, "demo", "results.json")
    if not os.path.exists(path):
        return "SKIP demo/results.json absent"
    with open(path, encoding="utf-8") as fh:
        datasets = json.load(fh)["datasets"]
    problems = []
    for ds in datasets:
        if not ds.get("quotes"):
            problems.append(f"{ds['key']} quotes nothing")
        for q in ds.get("quotes", []):
            if q["text"] not in _read(*q["file"].split("/")):
                problems.append(f"{ds['key']}: {q['file']} no longer says {q['text'][:60]!r}")
    return "; ".join(problems) or None


CHECKS = [
    ("generator CSV -> producer message (types)", b_producer_types),
    ("producer message -> feature extractor (equivalence)", b_wire_extracts_like_typed),
    ("feature extractor -> producer (nothing absent)", b_extractor_needs_nothing_absent),
    ("producer message -> ingress hash (fields sent)", b_hash_covers_only_sent_fields),
    ("duplicated modules identical", b_duplicated_modules_are_identical),
    ("wire -> routing key, plaintext and encrypted", b_routing_key_survives_the_wire),
    ("job record -> sink-writer", b_sink_reads_only_emitted_keys),
    ("job record -> case-manager", b_case_manager_reads_only_emitted_keys),
    ("alert_params -> Neo4j Cypher", b_neo4j_params_match_the_cypher),
    ("scored_row -> ClickHouse 01-schema", b_scored_row_matches_the_schema),
    ("case_row -> ClickHouse 02-cases", b_case_row_matches_the_schema),
    ("fraud_job imports -> run.ps1 AND Makefile", b_job_modules_cover_every_import),
    ("config artefacts -> run.ps1 serve-prep", b_serve_prep_ships_every_artefact),
    ("no data path derived from __file__", b_no_artefact_path_derived_from_file),
    ("ReceiverStore write -> read (Redis member)", b_receiver_store_round_trips),
    ("features.event_from -> generated CSV columns", b_event_mapping_matches_the_csv),
    ("model manifest -> deployed artefacts", b_manifest_matches_the_deployment),
    ("latency query -> its row parser", b_latency_query_matches_its_parser),
    ("docker-compose env -> case-manager config", b_compose_env_names_are_read),
    ("modules -> their package README", b_every_module_is_documented),
    ("generated CSV -> no new constant columns", b_no_new_constant_columns),
    ("generated artefacts -> every document that quotes them", b_documents_match_the_generated_figures),
    ("paysim_adapter.BASELINE -> related-work 6", b_external_baseline_matches_the_docs),
    ("demo/results.json -> the documents it quotes", b_demo_results_quote_their_sources),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    for name, fn in CHECKS:
        check(name, fn)()

    failed = [(n, p) for n, p in RESULTS if p and not p.startswith("SKIP")]
    skipped = [(n, p) for n, p in RESULTS if p and p.startswith("SKIP")]
    passed = [n for n, p in RESULTS if p is None]

    if args.verbose:
        for n in passed:
            print(f"  ok    {n}")
    for n, p in skipped:
        print(f"  skip  {n}  ({p[6:]})")
    for n, p in failed:
        print(f"\nFAIL  {n}\n      {p}")

    print(f"\n{len(passed)} ok, {len(skipped)} skipped, {len(failed)} FAILED "
          f"across {len(RESULTS)} boundaries")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
