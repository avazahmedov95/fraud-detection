"""
PyFlink job (phase 4 + phase 6): streaming CEP + ML score fusion.

  transactions.raw --(key by sender)--> enrich (Neo4j/Redis) + CEP rules
                                      --> ONNX model on the feature vector
                                      --> final_score = model prob; CEP adds
                                          deterministic must-flags + reason codes
                   --> transactions.scored        (every event)
                   --> fraud.alerts               (decision != ALLOW)

Per-sender behavioural state lives in Flink keyed state; the receiver-account
lookup is Redis-cached Neo4j; the LightGBM model is served via ONNX Runtime
(bundled in the Flink image) on the SAME feature vector used in training. If the
ONNX model is absent the job degrades gracefully to CEP-only scoring.

Submit (dir mounted at /opt/flink/usrjobs, with model.onnx copied alongside):

  flink run -py /opt/flink/usrjobs/fraud_job.py \
      --pyFiles /opt/flink/usrjobs/config.py,/opt/flink/usrjobs/features.py,\
/opt/flink/usrjobs/rules.py,/opt/flink/usrjobs/enrichment.py,/opt/flink/usrjobs/fusion.py
"""

import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyflink.common import Configuration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
    DeliveryGuarantee, KafkaOffsetResetStrategy,
)

import config as C
from rules import SenderState, evaluate
import features as F
from enrichment import EnrichmentClient
from receiver_store import ReceiverStore, PopulationStore
import fusion
import payload_crypto


def _event_epoch(event: dict) -> float:
    ts = event.get("event_time")
    if not ts:
        return time.time()
    try:
        return datetime.fromisoformat(ts).timestamp()
    except ValueError:
        return time.time()


def _warn_cep_only(reason: str) -> None:
    """Announce a degraded run loudly enough to be noticed in the logs.

    Falling back to CEP-only is a designed behaviour — the rule layer is the
    model-down fallback — but it changes what every downstream number means, and
    a single INFO line among thousands of Kafka config dumps is not enough to
    stop someone reporting a rules-only run as a fused one. Records from such a
    run are additionally stamped with MODEL_VERSION_CEP_ONLY, so the warehouse
    can tell them apart even if nobody read the log.
    """
    # NOTE: no `flush=True` on any print in this file. PyFlink replaces
    # sys.stdout inside the Beam worker with a logging shim that does not accept
    # the keyword, and the resulting TypeError is raised from open() - which
    # fails the whole job with a Beam "Failed to close remote bundle" stack that
    # names none of this. Plain print() is the working pattern here.
    bar = "!" * 72
    print(f"\n{bar}\n[fraud_job] RUNNING CEP-ONLY, NO ML SCORE - {reason}\n"
          f"[fraud_job] Rule scores only. Run `serve-prep` to place model.onnx\n"
          f"[fraud_job] in the mounted job directory, then resubmit.\n{bar}\n")


def _positive_proba(outputs) -> float:
    """Extract the fraud-class probability from a single-row ONNX output."""
    import numpy as np
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:           # [1, 2] probability tensor
            return float(arr[0, 1])
    for out in outputs:                                   # ZipMap fallback
        if isinstance(out, list) and out and isinstance(out[0], dict):
            row = out[0]
            return float(row.get(1, row.get("1", 0.0)))
    return 0.0


class FraudDetector(KeyedProcessFunction):
    """Stateful per-sender enrichment + CEP, with ONNX model fusion."""

    def open(self, ctx: RuntimeContext):
        self._state = ctx.get_state(
            ValueStateDescriptor("sender_state", Types.PICKLED_BYTE_ARRAY()))
        self._enrich = EnrichmentClient(
            C.NEO4J_URI, C.NEO4J_USER, C.NEO4J_PASSWORD,
            C.REDIS_HOST, C.REDIS_PORT, C.ENRICH_CACHE_TTL_S)
        # Receiver-side window lives outside Flink state: this stream is keyed
        # by sender, so one payee's inbound transfers are spread across every
        # partition. See receiver_store.py.
        self._receivers = ReceiverStore(C.REDIS_HOST, C.REDIS_PORT)
        # The threshold MULE_FAN_IN compares against is a property of the whole
        # population, so it cannot be derived inside a partition either. Opened
        # unconditionally: it is inert while MULE_FAN_IN_MODE is "absolute", and
        # constructing it only in relative mode would mean the mode could be
        # switched on against a job that has nothing to switch on.
        self._population = PopulationStore(C.REDIS_HOST, C.REDIS_PORT)
        self._enrich.open()
        self._receivers.open()
        self._population.open()

        # Payload decryption key. Absent is legitimate - it is the plaintext arm
        # of the experiment, and the deserialiser decides per record - but a key
        # that is present and unusable is not, so that fails loudly here rather
        # than as a stream of undecodable records later.
        self._undecodable = 0
        self._crypto_key = None
        if os.getenv("PAYLOAD_KEY_HEX"):
            self._crypto_key = payload_crypto.key_from_env()
            print("[fraud_job] payload decryption enabled (AES-256-GCM)")

        # Load the ONNX model; degrade to CEP-only if unavailable.
        self._sess = None
        self._in_name = None
        try:
            import onnxruntime as ort
            if os.path.exists(C.MODEL_ONNX_PATH):
                self._sess = ort.InferenceSession(
                    C.MODEL_ONNX_PATH, providers=["CPUExecutionProvider"])
                self._in_name = self._sess.get_inputs()[0].name
                print(f"[fraud_job] ONNX model loaded from {C.MODEL_ONNX_PATH}")
            else:
                _warn_cep_only(f"model file not found at {C.MODEL_ONNX_PATH}")
        except Exception as exc:                          # noqa: BLE001
            _warn_cep_only(f"ONNX init failed ({exc})")

    def _ml_score(self, feature_vector):
        if self._sess is None:
            return None
        import numpy as np
        x = np.asarray([feature_vector], dtype=np.float32)
        return _positive_proba(self._sess.run(None, {self._in_name: x}))

    def process_element(self, value, ctx):
        scoring_started = time.time()
        try:
            # Handles both arms of the security-overhead experiment: plaintext
            # JSON and the AES-256-GCM envelope, discriminated by prefix without
            # needing the key. Decryption is INSIDE the scoring_ms bracket on
            # purpose - its cost is exactly what reviewer point 3 asks for.
            event = payload_crypto.loads_maybe_encrypted(value, self._crypto_key)
        except Exception as exc:                          # noqa: BLE001
            # Undecodable records used to vanish here with `return`. With
            # encryption in play that is a whole-stream outage rendered as
            # silence: a missing or wrong key makes every record undecodable and
            # the job goes on reporting itself healthy while scoring nothing.
            self._undecodable += 1
            if self._undecodable in (1, 10, 100) or self._undecodable % 1000 == 0:
                print(f"[fraud_job] UNDECODABLE RECORD "
                      f"({self._undecodable} so far): {type(exc).__name__}: {exc}")
            return

        state = self._state.value() or SenderState()
        # Looked up by the identity this deployment can actually pin the payee
        # to - the destination PAN by default. See features.payee_key.
        receiver_age = self._enrich.lookup(F.payee_key(event))

        # The fan-in window is read on the SIMULATED clock, because the windows
        # it is compared against (velocity, structuring) are simulated too. The
        # wall-clock stamps below exist only to measure the pipeline itself and
        # never enter a feature.
        event_epoch = _event_epoch(event)
        receiver_state = self._receivers.load(F.payee_key(event), event_epoch)

        result = evaluate(event, receiver_age, state, event_epoch, receiver_state,
                          population=self._population)
        self._state.update(state)
        self._receivers.record(event, event_epoch)

        cep_score = result["cep_score"]
        ml_score = self._ml_score(result["features"])
        final = fusion.final_score(cep_score, ml_score)
        decision = fusion.decide(final, result["rule_hits"])
        predicted_type = fusion.classify_type(result["rule_hits"]) if decision != "ALLOW" else None

        out = {
            "transaction_id": event.get("transaction_id"),
            "event_time": event.get("event_time"),
            "sender_card": event.get("sender_card"),
            "receiver_card": event.get("receiver_card"),
            "sender_pinfl": event.get("sender_pinfl"),
            # No receiver_pinfl: it is not on the wire, because a sending bank
            # cannot resolve the destination PAN to a person. Everything
            # downstream keys the payee by card.
            "amount_uzs": event.get("amount_uzs"),
            "channel": event.get("channel"),
            "sender_region": event.get("sender_region"),
            "receiver_region": event.get("receiver_region"),
            "is_new_payee": result["is_new_payee"],
            "receiver_account_age_days": result["receiver_account_age_days"],
            "cep_score": cep_score,
            "ml_score": round(ml_score, 4) if ml_score is not None else None,
            "final_score": round(final, 4),
            "decision": decision,
            "predicted_type": predicted_type,
            "rule_hits": result["rule_hits"],
            # Session telemetry, carried into the record.
            #
            # `evaluate` has always RETURNED these two - it computes them for
            # exactly this purpose - and this dict never forwarded them. The
            # ClickHouse schema declares all three columns and record.py reads
            # them, so every row ever written carried a constant zero for the
            # capability measured as the second most valuable one. Nothing
            # failed: the columns exist, the writes succeed, and the warehouse
            # answers questions about session telemetry with zeros.
            "active_call": result["active_call"],
            "secs_login_z": result["secs_login_z"],
            # Raw, from the event: the job does not recompute what the app sent.
            "secs_login_to_confirm": event.get("secs_login_to_confirm"),
            # Stamped from what actually ran, not from configuration. A run
            # that degraded to CEP-only used to be recorded as a fused run,
            # which made the two indistinguishable in the warehouse afterwards.
            "model_version": (C.MODEL_VERSION if self._sess is not None
                              else C.MODEL_VERSION_CEP_ONLY),
            # --- latency instrumentation (wall clock, never a feature) -------
            # t0 from the producer, t1 here. The sink adds t2. Splitting them
            # this way separates transport and queueing from the scoring work
            # itself, so a breach of the target points at a stage.
            "ingested_at": event.get("ingested_at"),
            "scored_at_job": time.time(),
            "scoring_ms": round((time.time() - scoring_started) * 1000.0, 3),
            # Carried through untouched from ingress. The job does not recompute
            # it - recomputing would defeat the point, since the job could then
            # mint a hash for a substituted event. It only forwards what the
            # producer sealed.
            "ingress_hash": event.get("ingress_hash"),
        }
        # The vector the decision was made on, republished on ALERTS ONLY.
        #
        # Why publish it at all: the case-manager explains the model's verdict
        # with exact tree contributions, and it cannot recompute these features
        # - they come from the sender's streaming state, which exists only in
        # this operator. Recomputing downstream would explain a different event
        # than the one that alerted.
        #
        # Why alerts only: this rides in every record on transactions.scored
        # otherwise, which is ~24 numbers on 98.5% of traffic that nothing reads
        # - and it would grow the audit payload and the warehouse write for
        # every measured run. The rows an auditor or an analyst reopens are the
        # ones that alerted.
        #
        # NaN -> None: a NaN receiver_age is meaningful (see features.extract),
        # but json.dumps writes a bare `NaN`, which is not valid JSON. Both ends
        # here are Python and would round-trip it, which is exactly why it would
        # go unnoticed until something else consumed the topic.
        if decision != "ALLOW":
            out["features"] = [None if v != v else round(float(v), 6)
                               for v in result["features"]]

        if "label_is_fraud" in event:
            out["label_is_fraud"] = event["label_is_fraud"]
            out["label_fraud_type"] = event.get("label_fraud_type")
        yield json.dumps(out)

    def close(self):
        if hasattr(self, "_enrich"):
            self._enrich.close()
        if hasattr(self, "_receivers"):
            self._receivers.close()
            self._population.close()


def _apply_security(builder):
    """Add the transport-security properties, if any, to a Kafka builder.

    Shared by the source and both sinks so an arm cannot be half-applied - a job
    reading over TLS and writing in clear would produce a figure belonging to
    neither arm.
    """
    props = C.kafka_security_properties()
    for k, v in props.items():
        builder = builder.set_property(k, v)
    if props:
        print(f"[fraud_job] Kafka transport: {C.KAFKA_SECURITY_PROTOCOL} "
              f"(mutual TLS, keystore {C.KAFKA_SSL_KEYSTORE})")
    return builder


def _kafka_source():
    return (_apply_security(KafkaSource.builder())
            .set_bootstrap_servers(C.KAFKA_BOOTSTRAP)
            .set_topics(C.TOPIC_RAW)
            .set_group_id(C.CONSUMER_GROUP)
            # Resume from the group's committed offsets, falling back to the
            # start of the topic only the first time it runs.
            #
            # `earliest()` was wrong in both directions. Operationally, every
            # restart without a savepoint replayed the entire topic and re-scored
            # months of history, raising duplicate alerts on transactions long
            # since settled. For measurement, it silently poisoned the numbers:
            # replayed events carry their original ingest stamps, so a restarted
            # job reports latencies in the hundreds of seconds that are really
            # the age of the data.
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.EARLIEST))
            .set_property("commit.offsets.on.checkpoint", "true")
            .set_value_only_deserializer(SimpleStringSchema())
            # A consumer whose fetch finds an empty topic parks the request for
            # up to fetch.max.wait.ms before returning. At the default 500 ms and
            # a P2P arrival rate, a transaction that lands just after one fetch
            # returns waits out the whole interval before the next one sees it -
            # which is what put p95 at 622 ms while the median was 81 ms.
            .set_property("fetch.max.wait.ms", str(C.KAFKA_FETCH_MAX_WAIT_MS))
            .set_property("fetch.min.bytes", "1")
            .build())


def _kafka_sink(topic):
    return (_apply_security(KafkaSink.builder())
            .set_bootstrap_servers(C.KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())


def _tune_for_latency(env):
    """Trade throughput for latency, which is what a pre-settlement decision needs.

    PyFlink does not call the Python function per record. It accumulates records
    into a bundle and ships the bundle across the Java-Python boundary, flushing
    when the bundle is full OR when its timer expires. The defaults — 100000
    records, 1000 ms — are throughput settings: below ~100k events/s the bundle
    never fills, so every record waits out the full second regardless of how
    fast the scoring itself is.

    Measured here: with the defaults, scoring took 7.6 ms while reaching the
    scorer took 1923 ms. The work was never the constraint; the batching in
    front of it was.

    The same reasoning applies to the network buffer timeout, which batches
    records between operators.

    These are the knobs a real-time deployment turns down. The cost is more,
    smaller round trips to the Python process — throughput falls, which is
    acceptable when the requirement is to answer before a transfer settles.
    """
    # These are job-level Flink options, so they go through Configuration and
    # env.configure(). `env.get_config()` returns an ExecutionConfig, which
    # holds a different set of knobs and has no string interface at all.
    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.time", str(C.PY_BUNDLE_TIME_MS))
    conf.set_string("python.fn-execution.bundle.size", str(C.PY_BUNDLE_SIZE))

    # Explicit restart strategy. Fault injection showed the job did not come
    # back after a single taskmanager kill, which for a continuously-running
    # detector is a worse failure than a crash: nothing alerts on silence.
    conf.set_string("restart-strategy.type", "failure-rate")
    conf.set_string("restart-strategy.failure-rate.max-failures-per-interval",
                    str(C.RESTART_ATTEMPTS))
    conf.set_string("restart-strategy.failure-rate.failure-rate-interval",
                    f"{C.RESTART_WINDOW_MS} ms")
    conf.set_string("restart-strategy.failure-rate.delay",
                    f"{C.RESTART_DELAY_MS} ms")

    env.configure(conf)
    env.set_buffer_timeout(C.BUFFER_TIMEOUT_MS)
    return env


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(C.CHECKPOINT_INTERVAL_MS)

    # Retain checkpoints beyond the job's lifetime, and store them somewhere
    # that survives the container. Without this a resubmitted job has nothing to
    # restore from and re-reads the topic from the beginning — measured as every
    # transaction being scored four times over four submissions.
    chk = env.get_checkpoint_config()
    chk.set_checkpoint_storage_dir(C.CHECKPOINT_DIR)
    try:
        from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
        chk.set_externalized_checkpoint_cleanup(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    except ImportError:                                   # older PyFlink layout
        chk.enable_externalized_checkpoints(
            __import__("pyflink.datastream",
                       fromlist=["ExternalizedCheckpointCleanup"])
            .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    _tune_for_latency(env)

    raw = env.from_source(
        _kafka_source(), WatermarkStrategy.no_watermarks(), "transactions.raw")

    scored = (raw
              # Partitioning reads the record BEFORE scoring does, so this is
              # the second place a raw event is parsed. It must not decrypt:
              # doing so would run AES twice per event and inflate the very
              # figure the security-overhead measurement reports. The routing
              # field travels in clear inside the envelope - authenticated as
              # GCM associated data, so it cannot be re-pointed at another
              # sender - and this extracts it with a string split. On plaintext
              # records it parses JSON exactly as before.
              .key_by(lambda v: payload_crypto.routing_key(v), key_type=Types.STRING())
              .process(FraudDetector(), output_type=Types.STRING()))

    scored.sink_to(_kafka_sink(C.TOPIC_SCORED)).name("scored-sink")

    (scored
     .filter(lambda v: json.loads(v)["decision"] != "ALLOW")
     .sink_to(_kafka_sink(C.TOPIC_ALERTS))
     .name("alerts-sink"))

    env.execute("fraud-detection-cep-ml")


if __name__ == "__main__":
    main()
