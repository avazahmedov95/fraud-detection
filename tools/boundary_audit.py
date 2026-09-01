"""Check every place one component hands something to another.

Reading a file tells you a component is right. It does not tell you that what it
PRODUCES is what the next component EXPECTS - and three defects found in one day
lived in exactly that gap: a boolean that travelled as the string "False", a
feature vector the consumer needed and the producer never sent, and a data file
resolved against a path that does not exist inside a Flink worker. Each
component was correct. The joins were not.

So this walks the joins. Where it can, it builds the real artefact on one side
and asks the other side to accept it; where it cannot (a Cypher string, a
PowerShell list) it compares the two declarations directly.

  python tools/boundary_audit.py
  python tools/boundary_audit.py -v      # list every check, not only failures
"""

import argparse
import ast
import csv
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The packages deploy as separate units and three module names occur twice
# (config.py, integrity.py, payload_crypto.py). A flat sys.path therefore
# resolves `import config` to whichever directory happens to come first, and the
# audit would quietly measure the wrong module - which is how its first run
# reported three false failures. Each package is imported in isolation instead.
# (The same collision is why pytest cannot collect all five packages in one
# invocation; run them one directory at a time.)
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


# --- 1. generator CSV -> producer message ----------------------------------

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


# --- 2. producer message -> the feature extractor ---------------------------

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


# --- 3. producer message -> the integrity hash ------------------------------

def b_hash_covers_only_sent_fields():
    P = pkg("data-generator", "kafka_producer")
    integrity = pkg("data-generator", "integrity")
    msg_keys = set(P.RAW_FIELDS)
    missing = sorted(f for f in integrity.INGRESS_FIELDS if f not in msg_keys)
    if missing:
        return (f"hashed but never sent: {missing} - each contributes an empty "
                f"string, weakening the binding silently")
    return None


def b_both_integrity_copies_identical():
    a = _read("data-generator", "integrity.py")
    b = _read("sink-writer", "integrity.py")
    if a != b:
        return ("data-generator/integrity.py and sink-writer/integrity.py have "
                "drifted; the audit chain would be unverifiable across them")
    return None


# --- 4. the partitioning key is readable without decrypting -----------------

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


# --- 5. the job's output -> its three consumers -----------------------------

def _job_output_keys():
    """Keys the job puts in its emitted record, read from the source."""
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


# --- 6. row builders -> the ClickHouse schemas ------------------------------

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


# --- 7. deployment: what ships with the job ---------------------------------

def b_job_modules_cover_every_import():
    ps1 = _read("run.ps1")
    listed = set(re.findall(r'"([a-z_]+\.py)"', ps1.split("$JobModules = @(", 1)[1]
                            .split(")", 1)[0]))
    tree = ast.parse(_read("stream-processor", "fraud_job.py"))
    local = set()
    here = os.path.join(ROOT, "stream-processor")
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        for n in names:
            if os.path.exists(os.path.join(here, n.split(".")[0] + ".py")):
                local.add(n.split(".")[0] + ".py")
    # modules those modules import, one level down
    for mod in list(local):
        for node in ast.walk(ast.parse(_read("stream-processor", mod))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            for n in names:
                f = n.split(".")[0] + ".py"
                if os.path.exists(os.path.join(here, f)):
                    local.add(f)
    missing = sorted(local - listed - {"fraud_job.py"})
    if missing:
        return (f"$JobModules in run.ps1 does not ship {missing} - the job fails "
                f"at submission with an ImportError inside the cluster")
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


# --- 8. Redis: what is written is what is read ------------------------------

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


# --- 9. training matrix -> the generated CSV --------------------------------

def b_dataset_keys_exist_in_the_csv():
    D = pkg("ml", "dataset")
    row = _sample_row()
    if row is None:
        return "SKIP: dataset not generated"
    missing = sorted(k for k in D._EVENT_KEYS if k not in row)
    if missing:
        return f"ml/dataset expects CSV columns that do not exist: {missing}"
    return None


# --- 10. compose environment -> config defaults -----------------------------

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


CHECKS = [
    ("generator CSV -> producer message (types)", b_producer_types),
    ("producer message -> feature extractor (equivalence)", b_wire_extracts_like_typed),
    ("feature extractor -> producer (nothing absent)", b_extractor_needs_nothing_absent),
    ("producer message -> ingress hash (fields sent)", b_hash_covers_only_sent_fields),
    ("integrity.py copies identical", b_both_integrity_copies_identical),
    ("wire -> routing key, plaintext and encrypted", b_routing_key_survives_the_wire),
    ("job record -> sink-writer", b_sink_reads_only_emitted_keys),
    ("job record -> case-manager", b_case_manager_reads_only_emitted_keys),
    ("alert_params -> Neo4j Cypher", b_neo4j_params_match_the_cypher),
    ("scored_row -> ClickHouse 01-schema", b_scored_row_matches_the_schema),
    ("case_row -> ClickHouse 02-cases", b_case_row_matches_the_schema),
    ("fraud_job imports -> run.ps1 $JobModules", b_job_modules_cover_every_import),
    ("config artefacts -> run.ps1 serve-prep", b_serve_prep_ships_every_artefact),
    ("no data path derived from __file__", b_no_artefact_path_derived_from_file),
    ("ReceiverStore write -> read (Redis member)", b_receiver_store_round_trips),
    ("ml/dataset -> generated CSV columns", b_dataset_keys_exist_in_the_csv),
    ("docker-compose env -> case-manager config", b_compose_env_names_are_read),
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
