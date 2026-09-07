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

# Three module names occur twice across the deploying packages (config.py,
# integrity.py, payload_crypto.py), so a flat sys.path resolves `import config` to
# whichever directory comes first - which is how this audit's first run reported
# three false failures. Each package is imported in isolation instead. (The same
# collision is why pytest cannot collect all five packages in one invocation; run
# them one directory at a time.)
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
    a = F.to_vector(F.extract(wire, 800, R.SenderState(), now=1000.0))
    b = F.to_vector(F.extract(typed, 800, R.SenderState(), now=1000.0))
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
    # documented optional inputs: enrichment supplies them, not the wire
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
    """Modules that DECLARE themselves duplicates must be byte-identical.

    Duplicated because the packages deploy as separate units with no shared library,
    and drift is silent. The set is not hard-coded - it is whatever says
    "byte-identical" in its own docstring, because a hard-coded pair went stale once:
    the check covered integrity.py, payload_crypto.py drifted, nothing complained.
    """
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
    """Every stream-processor module `fraud_job.py` reaches, transitively.

    Fixed-point rather than two passes: a module added three hops down is exactly
    the one nobody remembers to list.
    """
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
    """Both submitters must ship the whole closure.

    This check once read run.ps1 only, and the Makefile silently fell a module
    behind it: bins.py was in $JobModules and not in PYFILES, and the job kept
    working because ./stream-processor is ALSO mounted at /opt/flink/usrjobs, so
    sys.path found it without --pyFiles. One submitter was correct, the other was
    relying on the mount, and nothing could tell them apart.
    """
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
    """The served artefacts, dataset and feature contract must be the manifest's.

    Nothing else notices when they drift: each leaves a system that runs, reports
    itself healthy, and describes something other than what it is doing.
    """
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
        def zadd(self, key, mapping): written.update(mapping)
        def zremrangebyscore(self, *a): pass
        def expire(self, *a): pass
        def execute(self): pass

    class Fake:
        def pipeline(self): return Pipe()
        def zrangebyscore(self, key, lo, hi): return list(written)

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
    return None


def b_latency_query_matches_its_parser():
    """The SELECT list and the row parser must agree on the column count, and every
    index the report reads must exist.

    Silent: add a column, forget the parser, and `fetch()` drops every row on a length
    check - the report says "no instrumented rows found" and looks like a data problem
    rather than a code one. Get the ORDER wrong and it prints scoring time as end-to-end.
    """
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
    """`features.event_from` must be satisfiable by a real generated row.

    Exercised rather than name-matched: the mapping subscripts the columns it
    requires and `.get`s the ones it can default, so calling it on an actual row
    tests the real contract instead of a list that has to be kept in step with it.
    This check used to read `ml/dataset._EVENT_KEYS`, a fourth copy of that
    mapping, and it broke the moment the copy was removed - which is the check
    working, not the removal failing.
    """
    F = pkg("stream-processor", "features")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    try:
        event = F.event_from(row)
    except KeyError as exc:
        return (f"features.event_from requires CSV column {exc} and the "
                f"generator does not produce it")
    empty = sorted(k for k, v in event.items()
                   if v == "" and k not in ("sender_network", "receiver_network"))
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
    """Each package's README must mention every .py file beside it.

    A documentation boundary rather than a data one, and it earns its place the
    way the others did - by having failed. `ml/README.md` listed four modules of
    ten, `data-generator/README.md` omitted `integrity.py` and
    `payload_crypto.py` - the two modules that exist as the answer to a reviewer's
    point about cryptographic guarantees - and `stream-processor/README.md`
    described a test layout the repository had moved away from.

    None of that breaks anything, which is exactly the failure mode: a reader
    forms a picture of the package from a list that is quietly short, and the
    modules missing from it are not the unimportant ones. They are the ones added
    last, which is to say the ones answering the most recent question.
    """
    problems = []
    for pkg in ("stream-processor", "data-generator", "ml", "sink-writer",
                "case-manager", "validation", "tools"):
        d = os.path.join(ROOT, pkg)
        readme = os.path.join(d, "README.md")
        if not os.path.isdir(d) or not os.path.exists(readme):
            continue
        text = _read(pkg, "README.md")
        # One level down as well: the harnesses moved into experiments/ and would
        # otherwise have fallen out of this check by being moved, which is the
        # opposite of what a completeness check is for.
        names = [fn for fn in os.listdir(d) if fn.endswith(".py")]
        for sub in sorted(os.listdir(d)):
            subdir = os.path.join(d, sub)
            if os.path.isdir(subdir) and sub not in ("tests", "__pycache__"):
                names += [fn for fn in os.listdir(subdir) if fn.endswith(".py")]
        missing = sorted(fn for fn in names if fn not in text)
        if missing:
            problems.append(f"{pkg}/README.md does not mention {missing}")
    return "; ".join(problems) or None


#: Columns that are constant in the dataset of record and known to be so.
#:
#: `bank_code` is "00000" everywhere because banks.csv shipped that column as a
#: placeholder - every row the same - and the dataset of record was generated
#: (2026-07-19) while it was. Commit 538dc22 filled in the real codes and added
#: bins.py the same day, so no measurement ever ran against the placeholder: the
#: pipeline reads banks.csv directly, and bins._bank_identity REFUSES to load a
#: `code` column with fewer than two distinct values, precisely because that
#: would make is_on_us() true for every transfer.
#:
#: It is not fixed by regenerating, and the reason is not the SHA-256 pins - it
#: is that the dataset cannot be regenerated at all. The determinism fix in
#: generator._assign_payees changed the RNG stream: same seed, same versions,
#: 36,072 of 50,000 transaction rows different (generator-spec.md, and the
#: comment at the fix). Every measurement in the thesis was taken on THIS file.
#: So the column is stale because the dataset is deliberately frozen, which is
#: the right decision, and this is its cost. Nothing reads it - the producer
#: does not put it on the wire, the pipeline resolves the issuer from the BIN,
#: and no Cypher query reads the property it lands in on (:Person).
#:
#: Named rather than ignored, the way bins.RETIRED_BINS is, so a NEW constant
#: column still fails.
KNOWN_CONSTANT = {
    "transactions.csv": {"sender_bank_code", "receiver_bank_code"},
    "persons.csv": {"bank_code"},
}


def b_no_new_constant_columns():
    """A column with one value everywhere is a feature that was never wired.

    This is how three warehouse columns were found writing constant zero, and the
    same shape exists on the generator's side. Cheap to check and invisible
    otherwise: nothing fails, the column is simply always the same.
    """
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


#: Every figure ml/README.md quotes from metrics.json, with the pattern that
#: finds it and how metrics.json's raw value is rendered to match. A missing
#: pattern fails as loudly as a wrong number: a check that quietly stops finding
#: what it verifies is the seventeenth entry in irp-framing.md 8.
_README_FIGURES = [
    (r"^\| ROC-AUC\s+\|\s+([\d.]+)", ("roc_auc",), lambda v: f"{v:.3f}"),
    (r"^\| PR-AUC\s+\|\s+([\d.]+)", ("pr_auc",), lambda v: f"{v:.3f}"),
    (r"^\| precision @0\.50\s+\|\s+([\d.]+)", ("at_0_50", "precision"), lambda v: f"{v:.3f}"),
    (r"^\| precision @0\.50\s+\|\s+[\d.]+\s+\|\s+([\d.]+)", ("cep_only", "precision"), lambda v: f"{v:.3f}"),
    (r"^\| recall @0\.50\s+\|\s+([\d.]+)", ("at_0_50", "recall"), lambda v: f"{v:.3f}"),
    (r"^\| recall @0\.50\s+\|\s+[\d.]+\s+\|\s+([\d.]+)", ("cep_only", "recall"), lambda v: f"{v:.3f}"),
    (r"STRUCTURING ([\d.]+)%", ("by_fraud_type", "STRUCTURING", "recall"), lambda v: f"{v*100:.1f}"),
    (r"fraud type \(ML @0\.50\):.*?APP ([\d.]+)%", ("by_fraud_type", "APP", "recall"), lambda v: f"{v*100:.1f}"),
    (r"fraud type \(ML @0\.50\):.*?ATO ([\d.]+)%", ("by_fraud_type", "ATO", "recall"), lambda v: f"{v*100:.1f}"),
    (r"fraud type \(ML @0\.50\):.*?MULE ([\d.]+)%", ("by_fraud_type", "MULE", "recall"), lambda v: f"{v*100:.1f}"),
    (r"^brier\s+([\d.]+)", ("calibration", "brier"), lambda v: f"{v:.5f}"),
    (r"^n_alerts\s+(\d+)", ("calibration", "n_alerts"), str),
    (r"^saturated_share\s+([\d.]+)%", ("calibration", "saturated_share"), lambda v: f"{v*100:.1f}"),
    (r"^distinct_scores\s+(\d+)", ("calibration", "distinct_scores"), str),
    (r"^review_band\s+(\d+)", ("calibration", "review_band"), str),
    (r"^median_alert_score\s+([\d.]+)", ("calibration", "median_alert_score"), lambda v: f"{v:.6f}"),
]


def b_readme_figures_match_metrics_json():
    """ml/README.md quotes metrics.json; the quotes must still be true.

    The identical construction in stream-processor/README.md was removed rather
    than checked, because that file is not about the model. This one is - a ml
    README with no figures in it is worse than one that can drift - so the table
    stays and drifting is what fails. It has drifted once already, reporting MULE
    recall at 68% while the pipeline achieved 85.7%, across a whole capability,
    under a note saying metrics.json was authoritative. A disclaimer is not a
    refresh; this is.
    """
    path = os.path.join(ROOT, "ml", "models", "metrics.json")
    if not os.path.exists(path):
        return "SKIP: metrics.json not present"
    with open(path) as fh:
        metrics = json.load(fh)
    text = _read("ml", "README.md")

    problems = []
    for pattern, keys, render in _README_FIGURES:
        m = re.search(pattern, text, re.M | re.S)
        label = ".".join(keys)
        if not m:
            problems.append(f"{label}: ml/README.md no longer states it where "
                            f"this check looks")
            continue
        value = metrics
        for k in keys:
            value = value[k]
        expected = render(value)
        if m.group(1) != expected:
            problems.append(f"{label}: README says {m.group(1)}, "
                            f"metrics.json gives {expected}")
    return "; ".join(problems) or None


def b_external_baseline_matches_the_docs():
    """related-work.md 6 calibrates this project against a published PaySim
    baseline; paysim_adapter.BASELINE holds the same figures for the code that
    reproduces them. Two copies of an external number drift the way ml/README
    drifted from metrics.json.

    Only the REPORTED column is pinned. The reproduced column moves with the
    library versions and belongs to whoever last ran `--baseline`, whereas what
    the source says it measured is fixed and citable.
    """
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
    ("metrics.json -> ml/README figures", b_readme_figures_match_metrics_json),
    ("paysim_adapter.BASELINE -> related-work 6", b_external_baseline_matches_the_docs),
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
