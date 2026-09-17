"""The PyFlink job: enrich, apply the CEP rules, score with ONNX, decide.

    transactions.raw --(key by sender)--> transactions.scored + fraud.alerts

Degrades to CEP-only if the model is absent, stamping what actually ran.
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

import capabilities as CAP
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
    """Announce a rules-only run loudly; its records carry a distinct version."""
    # No flush=True: PyFlink's stdout shim rejects the keyword.
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
        # Outside Flink state: keyed by sender, so a payee's inbound transfers
        # are spread across every partition (receiver_store.py).
        self._receivers = ReceiverStore(C.REDIS_HOST, C.REDIS_PORT)
        # link_history: every card's outbound transfers, the other half of who paid whom.
        self._outbound = (ReceiverStore(C.REDIS_HOST, C.REDIS_PORT, prefix="out")
                          if CAP.enabled("link_history") else None)
        # Opened in every mode: inert in "absolute", never missing in "relative".
        self._population = PopulationStore(C.REDIS_HOST, C.REDIS_PORT)
        self._enrich.open()
        self._receivers.open()
        if self._outbound is not None:
            self._outbound.open()
        self._population.open()

        # Absent is legitimate (the plaintext arm); present-and-unusable is not,
        # so it fails here rather than as undecodable records later.
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
            # Both arms, discriminated by prefix. Decryption sits INSIDE the
            # scoring_ms bracket on purpose - its cost is what is measured.
            event = payload_crypto.loads_maybe_encrypted(value, self._crypto_key)
        except Exception as exc:                          # noqa: BLE001
            # These used to vanish with `return`. A wrong key makes every record
            # undecodable, so that was a whole-stream outage rendered as silence.
            self._undecodable += 1
            if self._undecodable in (1, 10, 100) or self._undecodable % 1000 == 0:
                print(f"[fraud_job] UNDECODABLE RECORD "
                      f"({self._undecodable} so far): {type(exc).__name__}: {exc}")
            return

        state = self._state.value() or SenderState()
        # Looked up by the identity this deployment can actually pin the payee
        # to - the destination PAN by default. See features.payee_key.
        receiver_age = self._enrich.lookup(F.payee_key(event))

        # SIMULATED clock, like the windows it is compared against. The
        # wall-clock stamps below measure the pipeline and never enter a feature.
        event_epoch = _event_epoch(event)
        receiver_state = self._receivers.load(F.payee_key(event), event_epoch)
        # link_history: what reached the sender, whom the sender paid, whom the payee
        # paid - three more reads of the shared stores, each failing open.
        links = {}
        if self._outbound is not None:
            payer = F.payer_key(event)
            links = dict(sender_inbound=self._receivers.load(payer, event_epoch),
                         sender_outbound=self._outbound.load(payer, event_epoch),
                         payee_outbound=self._outbound.load(F.payee_key(event), event_epoch))

        result = evaluate(event, receiver_age, state, event_epoch, receiver_state,
                          population=self._population, **links)
        self._state.update(state)
        self._receivers.record(event, event_epoch)
        if self._outbound is not None:
            self._outbound.record(event, event_epoch)

        cep_score = result["cep_score"]
        ml_score = self._ml_score(result["features"])
        # One call: score_and_decide knows whether the score is a probability.
        final, decision = fusion.score_and_decide(
            cep_score, ml_score, result["rule_hits"])
        predicted_type = fusion.classify_type(result["rule_hits"]) if decision != "ALLOW" else None

        out = {
            "transaction_id": event.get("transaction_id"),
            "event_time": event.get("event_time"),
            "sender_card": event.get("sender_card"),
            "receiver_card": event.get("receiver_card"),
            "sender_pinfl": event.get("sender_pinfl"),
            # No receiver_pinfl: a sending bank cannot resolve the destination PAN to
            # a person, so the payee is keyed by card.
            "amount_uzs": event.get("amount_uzs"),
            "sender_region": event.get("sender_region"),
            "is_new_payee": result["is_new_payee"],
            "receiver_account_age_days": result["receiver_account_age_days"],
            "cep_score": cep_score,
            "ml_score": round(ml_score, 4) if ml_score is not None else None,
            "final_score": round(final, 4),
            "decision": decision,
            "predicted_type": predicted_type,
            "rule_hits": result["rule_hits"],
            "active_call": result["active_call"],
            "secs_login_z": result["secs_login_z"],
            # Raw, from the event: the job does not recompute what the app sent.
            "secs_login_to_confirm": event.get("secs_login_to_confirm"),
            # From what ran, not from configuration: a degraded run used to be stored
            # as a fused one, indistinguishable afterwards.
            "model_version": (C.MODEL_VERSION if self._sess is not None
                              else C.MODEL_VERSION_CEP_ONLY),
            # Latency instrumentation (wall clock, never a feature). t0 from the
            # producer, t1 here, t2 at the sink - so a breach points at a stage.
            "ingested_at": event.get("ingested_at"),
            "scored_at_job": time.time(),
            "scoring_ms": round((time.time() - scoring_started) * 1000.0, 3),
            # Forwarded untouched. Recomputing here would let the job mint a hash for
            # a substituted event.
            "ingress_hash": event.get("ingress_hash"),
        }
        # The feature vector, on alerts only: case-manager explains from it and cannot
        # recompute sender state. NaN -> None, as a bare NaN is not valid JSON.
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
            if self._outbound is not None:
                self._outbound.close()


def _apply_security(builder):
    """Add the transport-security properties, if any, to a Kafka source or sink."""
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
            # Committed offsets, the topic start only on a first run;
            # earliest() rescored the whole topic on every restart.
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.EARLIEST))
            .set_property("commit.offsets.on.checkpoint", "true")
            .set_value_only_deserializer(SimpleStringSchema())
            # fetch.max.wait.ms bounds how long a fetch parks on an empty topic.
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
    """Trade throughput for latency; each value is explained in config.py."""
    # Job-level options go through Configuration and env.configure();
    # env.get_config() is an ExecutionConfig with no string interface.
    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.time", str(C.PY_BUNDLE_TIME_MS))
    conf.set_string("python.fn-execution.bundle.size", str(C.PY_BUNDLE_SIZE))

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

    # Retained beyond the job and outside the container - config.CHECKPOINT_DIR.
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
              # Partition on the clear routing field, without decrypting.
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
