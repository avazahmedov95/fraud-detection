"""The package directory on sys.path: pytest adds only the test file's own."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# case.py and explain.py, from the sink writer.
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sink-writer"))
