"""payload_crypto.py and integrity.py deploy in two packages each. Their tests
live beside the other copy (stream-processor/, sink-writer/); these must match
that copy byte for byte."""

import pathlib

import pytest

HERE = pathlib.Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("name, other", [("payload_crypto.py", "stream-processor"),
                                         ("integrity.py", "sink-writer")])
def test_identical_to_the_tested_copy(name, other):
    theirs = HERE.parent / other / name
    if not theirs.exists():
        pytest.skip(f"{other}/ is not checked out")
    ours = (HERE / name).read_bytes().replace(b"\r\n", b"\n")
    assert ours == theirs.read_bytes().replace(b"\r\n", b"\n"), f"{name} drifted"
