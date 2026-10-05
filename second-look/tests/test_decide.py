"""The second look's decision and what it publishes, without TabPFN or Kafka."""

import config as C
import consumer
from decide import verdict

EVENT = {"transaction_id": "t_1", "decision": "SECOND_LOOK", "final_score": 0.06,
         "ml_score": 0.06, "features": [1.0, None, 3.0], "stage_ms": {"model": 0.4},
         "model_version": "committee-v3", "scored_at_job": 1_772_000_000.5}


def test_tabpfn_at_or_above_its_cutoff_holds_the_transfer():
    out = verdict(EVENT, 0.91, 0.88, "ckpt")
    assert out["decision"] == "REVIEW" and out["final_score"] == 0.91
    assert out["ml_score"] == 0.06                 # the served model's: the explanation guard
    assert out["features"] == EVENT["features"]    # the case explains from them
    assert out["model_version"] == "second-look:ckpt"
    assert "stage_ms" not in out                   # the job's stage times are the job's
    assert out["scored_at_job"] == EVENT["scored_at_job"]   # the hold began at the job


def test_under_its_cutoff_the_transfer_goes():
    out = verdict(EVENT, 0.40, 0.88, "ckpt")
    assert out["decision"] == "ALLOW"
    assert out["features"] == EVENT["features"]    # every decision carries them, as the job's do


def test_no_score_holds_the_transfer_and_says_so():
    out = verdict(EVENT, None, 0.88, "ckpt")
    assert out["decision"] == "REVIEW" and out["model_version"].endswith(":unscored")


class Look:
    spec = {"cut": 0.88, "checkpoint": "ckpt"}

    def __init__(self, value=None, fail=False):
        self.value, self.fail = value, fail

    def score(self, features):
        if self.fail:
            raise RuntimeError("TabPFN is down")
        return self.value


def _published(look, event=EVENT, waited=0.0):
    sent = []
    consumer.handle(event, look, lambda topic, out: sent.append((topic, out["decision"])),
                    now=event["scored_at_job"] + waited)
    return sent


def test_a_hold_goes_to_the_warehouse_and_opens_a_case():
    assert _published(Look(0.95)) == [(C.TOPIC_SCORED, "REVIEW"), (C.TOPIC_ALERTS, "REVIEW")]


def test_a_release_goes_to_the_warehouse_only():
    assert _published(Look(0.10)) == [(C.TOPIC_SCORED, "ALLOW")]


def test_a_failure_or_a_missing_vector_holds_rather_than_releases():
    assert _published(Look(fail=True))[-1] == (C.TOPIC_ALERTS, "REVIEW")
    assert _published(Look(0.10), dict(EVENT, features=None))[-1] == (C.TOPIC_ALERTS, "REVIEW")


def test_past_the_deadline_the_transfer_is_held_without_asking_tabpfn():
    """TabPFN would let this one go - after the transfers queued behind it had
    waited for the answer too."""
    assert _published(Look(0.10), waited=C.DEADLINE_S - 0.1) == [(C.TOPIC_SCORED, "ALLOW")]
    assert _published(Look(0.10), waited=C.DEADLINE_S) == [(C.TOPIC_SCORED, "REVIEW"),
                                                           (C.TOPIC_ALERTS, "REVIEW")]
