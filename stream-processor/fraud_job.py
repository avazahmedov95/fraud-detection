"""The PyFlink job: apply the CEP rules, score with ONNX, decide.

    transactions.raw --(key by sender)--> transactions.scored, fraud.second_look

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
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
    DeliveryGuarantee, KafkaOffsetResetStrategy,
)

import config as C
from rules import SenderState, evaluate
import features as F
from receiver_store import ReceiverStore
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
    """Announce a rules-only run loudly; its records carry a version of their own."""
    # No flush=True: PyFlink's stdout shim rejects the keyword.
    bar = "!" * 72
    print(f"\n{bar}\n[fraud_job] RUNNING CEP-ONLY, NO ML SCORE - {reason}\n"
          f"[fraud_job] Rule scores only. Run `serve-prep` to place model.onnx\n"
          f"[fraud_job] in the mounted job directory, then resubmit.\n{bar}\n")


#: The decision path, stage by stage; every decision carries each stage's wall time
#: in `stage_ms`. kafka and handoff are timed against Kafka's own append time.
STAGES = ("kafka", "handoff", "decode", "state", "redis", "rules", "model", "decide")


def _lap(stages, name, mark):
    """Close one stage: its wall time in ms into `stages`; returns the next start."""
    now = time.perf_counter()
    stages[name] = round((now - mark) * 1000.0, 3)
    return now


def _before_operator(stages, ingested_at, appended_ms, arrived):
    """The two stages before this operator, from the producer's stamp, Kafka's append
    time (ms) and the arrival here. A missing stamp leaves its stage out."""
    if appended_ms is None:
        return
    appended = appended_ms / 1000.0
    if ingested_at is not None:
        stages["kafka"] = round((appended - float(ingested_at)) * 1000.0, 3)
    stages["handoff"] = round((arrived - appended) * 1000.0, 3)


def _positive_proba(outputs) -> float:
    """The fraud probability from the [1, 2] tensor ml/export_onnx.py writes."""
    import numpy as np
    for out in outputs:
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return float(arr[0, 1])
    raise RuntimeError("no [1, 2] probability tensor in the ONNX output")


class FraudDetector(KeyedProcessFunction):
    """Per-sender CEP and the ONNX model, fused into one decision."""

    def open(self, ctx: RuntimeContext):
        self._state = ctx.get_state(
            ValueStateDescriptor("sender_state", Types.PICKLED_BYTE_ARRAY()))
        # The payee's side lives in Redis: the stream is keyed by sender.
        self._receivers = ReceiverStore(C.REDIS_HOST, C.REDIS_PORT)
        self._receivers.open()
        import redis
        self._lease = redis.Redis(host=C.REDIS_HOST, port=C.REDIS_PORT,
                                  socket_timeout=0.2, socket_connect_timeout=0.2)
        self._answering = True

        self._unusable = 0
        self._crypto_key = None
        if os.getenv("PAYLOAD_KEY_HEX"):
            # A key that is set but unusable fails here, not on every record.
            self._crypto_key = payload_crypto.key_from_env()
            print("[fraud_job] payload decryption enabled (AES-256-GCM)")

        self._sess = None
        self._in_name = None
        try:
            import onnxruntime as ort
            if os.path.exists(C.MODEL_ONNX_PATH):
                # One thread: more gain a quarter of a millisecond on one row, and
                # their spinning took five cores at 100 calls a second.
                opts = ort.SessionOptions()
                opts.intra_op_num_threads = 1
                self._sess = ort.InferenceSession(
                    C.MODEL_ONNX_PATH, sess_options=opts, providers=["CPUExecutionProvider"])
                self._in_name = self._sess.get_inputs()[0].name
                print(f"[fraud_job] ONNX model loaded from {C.MODEL_ONNX_PATH}")
            else:
                _warn_cep_only(f"model file not found at {C.MODEL_ONNX_PATH}")
        except Exception as exc:                          # noqa: BLE001
            _warn_cep_only(f"ONNX init failed ({exc})")
        print(f"[fraud_job] second look from {C.SECOND_LOOK_FROM:.4f} to the cut-off"
              if C.SECOND_LOOK_FROM is not None else
              f"[fraud_job] second look off: no {C.SECOND_LOOK_PATH} chosen under this cut-off")

    def _second_look_answering(self):
        """Whether the second look renewed its key in time. No Redis reads as no: the
        transfer is held rather than sent to wait."""
        try:
            answering = bool(self._lease.exists(C.SECOND_LOOK_ALIVE_KEY))
        except Exception:                              # noqa: BLE001
            answering = False
        if answering != self._answering:
            self._answering = answering
            print("[fraud_job] second look answering again" if answering else
                  "[fraud_job] second look NOT answering: its band is held here")
        return answering

    def _ml_score(self, feature_vector):
        if self._sess is None:
            return None
        import numpy as np
        x = np.asarray([feature_vector], dtype=np.float32)
        return _positive_proba(self._sess.run(None, {self._in_name: x}))

    def process_element(self, value, ctx):
        scoring_started = time.time()
        stages = {}
        mark = time.perf_counter()
        try:
            # Encrypted or plain; decryption is part of the measured time.
            event = payload_crypto.loads_maybe_encrypted(value, self._crypto_key)
            problem = F.unusable(event)
            if problem:
                raise ValueError(problem)
        except Exception as exc:                          # noqa: BLE001
            # Counted aloud: a wrong key makes every record undecodable.
            self._unusable += 1
            if self._unusable in (1, 10, 100) or self._unusable % 1000 == 0:
                print(f"[fraud_job] UNUSABLE RECORD "
                      f"({self._unusable} so far): {type(exc).__name__}: {exc}")
            return
        mark = _lap(stages, "decode", mark)

        state = self._state.value() or SenderState()
        # The event's own (simulated) time, as in training; wall time only measures.
        event_epoch = _event_epoch(event)
        mark = _lap(stages, "state", mark)
        receiver_state = self._receivers.load(F.payee_key(event), event_epoch)
        sender_inbound = self._receivers.last_inbound(F.sender_key(event), event_epoch)
        confirmed = self._receivers.confirmed_among(event, receiver_state, event_epoch)
        mark = _lap(stages, "redis", mark)

        result = evaluate(event, state, event_epoch, receiver_state,
                          sender_inbound_ts=sender_inbound, confirmed=confirmed)
        self._state.update(state)
        self._receivers.record(event, event_epoch)
        mark = _lap(stages, "rules", mark)

        cep_score = result["cep_score"]
        ml_score = self._ml_score(result["features"])
        mark = _lap(stages, "model", mark)
        final, decision = fusion.score_and_decide(cep_score, ml_score, result["rule_hits"])
        # From what ran: a rules-only run must not be stored as a fused one.
        model_version = C.MODEL_VERSION if self._sess is not None else C.MODEL_VERSION_CEP_ONLY
        if decision == "SECOND_LOOK" and not self._second_look_answering():
            decision, model_version = "REVIEW", C.SECOND_LOOK_UNSCORED
        predicted_type = fusion.classify_type(result["rule_hits"]) if decision != "ALLOW" else None

        out = {
            "transaction_id": event.get("transaction_id"),
            "event_time": event.get("event_time"),
            "sender_card": event.get("sender_card"),
            "receiver_card": event.get("receiver_card"),
            "sender_pinfl": event.get("sender_pinfl"),
            "amount_uzs": event.get("amount_uzs"),
            "sender_region": event.get("sender_region"),
            "is_new_payee": result["is_new_payee"],
            "cep_score": cep_score,
            "ml_score": round(ml_score, 4) if ml_score is not None else None,
            "final_score": round(final, 4),
            "decision": decision,
            "predicted_type": predicted_type,
            "rule_hits": result["rule_hits"],
            "active_call": result["active_call"],
            "secs_login_z": result["secs_login_z"],
            "secs_login_to_confirm": event.get("secs_login_to_confirm"),
            "model_version": model_version,
            "ingested_at": event.get("ingested_at"),
            "scored_at_job": time.time(),
            "scoring_ms": round((time.time() - scoring_started) * 1000.0, 3),
            # Forwarded untouched: recomputed here, it could vouch for a substituted event.
            "ingress_hash": event.get("ingress_hash"),
            # Explained, rescored by the second look, retrained on; NaN is not JSON.
            "features": [None if v != v else round(float(v), 6) for v in result["features"]],
        }
        _lap(stages, "decide", mark)
        _before_operator(stages, event.get("ingested_at"), ctx.timestamp(), scoring_started)
        out["stage_ms"] = stages
        yield json.dumps(out)

    def close(self):
        if hasattr(self, "_receivers"):
            self._receivers.close()
            self._lease.close()


def _apply_security(builder):
    """The transport-security properties, if any, on a Kafka source or sink."""
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
            # Committed offsets; the topic's start only on a first run.
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.EARLIEST))
            .set_property("commit.offsets.on.checkpoint", "true")
            .set_value_only_deserializer(SimpleStringSchema())
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
    """Latency over throughput (config.py, Latency)."""
    # Job options go through Configuration: ExecutionConfig takes no strings.
    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.time", str(C.PY_BUNDLE_TIME_MS))
    conf.set_string("python.fn-execution.bundle.size", str(C.PY_BUNDLE_SIZE))
    conf.set_string("restart-strategy.type", "failure-rate")
    conf.set_string("restart-strategy.failure-rate.max-failures-per-interval",
                    str(C.RESTART_ATTEMPTS))
    conf.set_string("restart-strategy.failure-rate.failure-rate-interval",
                    f"{C.RESTART_WINDOW_MS} ms")
    conf.set_string("restart-strategy.failure-rate.delay", f"{C.RESTART_DELAY_MS} ms")
    env.configure(conf)
    env.set_buffer_timeout(C.BUFFER_TIMEOUT_MS)
    env.set_parallelism(C.JOB_PARALLELISM)
    return env


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(C.CHECKPOINT_INTERVAL_MS)
    chk = env.get_checkpoint_config()
    chk.set_checkpoint_storage_dir(C.CHECKPOINT_DIR)
    chk.set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    _tune_for_latency(env)

    raw = env.from_source(
        _kafka_source(), WatermarkStrategy.no_watermarks(), "transactions.raw")
    scored = (raw
              # Partitioned on the clear routing field, without decrypting.
              .key_by(lambda v: payload_crypto.routing_key(v), key_type=Types.STRING())
              .process(FraudDetector(), output_type=Types.STRING()))
    scored.sink_to(_kafka_sink(C.TOPIC_SCORED)).name("scored-sink")
    (scored
     .filter(lambda v: json.loads(v)["decision"] == "SECOND_LOOK")
     .sink_to(_kafka_sink(C.TOPIC_SECOND_LOOK))
     .name("second-look-sink"))
    env.execute("fraud-detection-cep-ml")


if __name__ == "__main__":
    main()
