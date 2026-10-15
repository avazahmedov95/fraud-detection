"""fraud_job.py forgets a silent sender. Read from its source: PyFlink is absent here."""
import ast
import pathlib

import config as C


def test_the_sender_state_expires():
    src = (pathlib.Path(__file__).resolve().parent.parent / "fraud_job.py").read_text(
        encoding="utf-8")
    calls = {n.func.attr for n in ast.walk(ast.parse(src))
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert {"register_processing_time_timer", "delete_processing_time_timer"} <= calls
    assert "def on_timer" in src and "C.SENDER_STATE_TTL_DAYS" in src


def test_it_outlasts_every_window_a_feature_reads():
    assert C.SENDER_STATE_TTL_DAYS * 86_400 > C.LINK_MONTH_S
