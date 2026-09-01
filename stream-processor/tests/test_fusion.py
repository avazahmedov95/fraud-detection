"""Unit tests for fusion.py. Run: python test_fusion.py"""

from fusion import final_score, decide, classify_type


def test_final_score_is_model_with_cep_fallback():
    # model present -> graded risk is the model probability (not blended)
    assert final_score(0.9, 0.2) == 0.2
    assert final_score(0.0, 0.7) == 0.7
    # model down -> fall back to the CEP score
    assert final_score(0.55, None) == 0.55
    # clamped to [0, 1]
    assert final_score(0.0, 1.4) == 1.0
    assert final_score(-0.3, None) == 0.0


def test_decision_thresholds():
    assert decide(0.90, []) == "BLOCK"
    assert decide(0.50, []) == "REVIEW"
    assert decide(0.10, []) == "ALLOW"


def test_mandatory_floor_forces_review():
    # compliance must-flags escalate even when the model score is low
    assert decide(0.10, ["STRUCTURING"]) == "REVIEW"
    assert decide(0.10, ["DAILY_LIMIT_BREACH"]) == "REVIEW"
    # a non-mandatory rule does NOT force escalation on its own
    assert decide(0.10, ["GEO_ANOMALY"]) == "ALLOW"


def test_type_priority():
    assert classify_type(["STRUCTURING", "VELOCITY"]) == "STRUCTURING"
    assert classify_type(["DEVICE_CHANGE"]) == "ATO"
    assert classify_type(["VELOCITY", "DISTINCT_PAYEE_BURST"]) == "MULE"
    assert classify_type(["NEW_PAYEE_HIGH_AMOUNT"]) == "APP"
    assert classify_type([]) is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\n{len(fns)} tests passed")
