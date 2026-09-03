"""Loss accounting for the dependency matrix: offered minus stored, not expected
minus stored. Getting it wrong hid the single real data-loss finding in the
whole matrix - see offered_count.
"""

import types

import dependency_failure as dep


def args(service, expect=1000, sent=None):
    return types.SimpleNamespace(service=service, expect=expect, sent=sent)


def test_producer_count_wins_when_present():
    # The only direct evidence of what reached the topic; --expect is an intention.
    n, _ = dep.offered_count(args("clickhouse", expect=1000, sent=997))
    assert n == 997


def test_downstream_outage_trusts_expect():
    # Stopping the warehouse cannot stop the producer, so a missing row is one the
    # pipeline dropped. This used to print "check whether the producer could send".
    n, how = dep.offered_count(args("clickhouse", expect=1000, sent=-1))
    assert n == 1000
    assert "cannot stop the producer" in how


def test_transport_outage_without_a_send_count_is_inconclusive():
    # Stopping Kafka stops the offer too; calling that 100% loss invents a failure.
    n, how = dep.offered_count(args("kafka", expect=1000, sent=-1))
    assert n is None
    assert "--sent" in how


def test_transport_outage_with_a_send_count_is_measurable():
    n, _ = dep.offered_count(args("kafka", expect=1000, sent=612))
    assert n == 612


def test_no_denominator_at_all():
    n, _ = dep.offered_count(args("redis", expect=None, sent=None))
    assert n is None


# --- the reference comparison ---------------------------------------------

def test_pass_mix_is_a_delta_not_a_total():
    # The bug this replaces compared a 1,000-row delta against a 79,200-row
    # table and called the difference "degradation".
    before = {"types": {"MULE": 500, "APP": 40}}
    snap = {"types": {"MULE": 543, "APP": 40, "ATO": 1}}
    assert dep.pass_mix(before, snap) == {"MULE": 43, "APP": 0, "ATO": 1}


def test_silenced_type_is_named(capsys):
    dep.report_mix({"MULE": 0, "STRUCTURING": 4}, {"MULE": 45, "STRUCTURING": 4})
    out = capsys.readouterr().out
    assert "STOPPED FIRING: MULE" in out
    assert "SILENCED" in out


def test_a_type_absent_from_both_is_not_listed(capsys):
    dep.report_mix({"MULE": 3}, {"MULE": 3, "APP": 0})
    out = capsys.readouterr().out
    assert "APP" not in out
    assert "no type lost ground" in out


def test_partial_reduction_is_not_reported_as_silence(capsys):
    dep.report_mix({"MULE": 20}, {"MULE": 40})
    out = capsys.readouterr().out
    assert "STOPPED FIRING" not in out
    assert "reduced but still firing: MULE" in out


def test_control_arm_has_a_written_prediction():
    # Every arm's expectation is recorded before the run; the control is an arm.
    assert "control" in dep.EXPECTED
    assert dep.EXPECTED["control"]
