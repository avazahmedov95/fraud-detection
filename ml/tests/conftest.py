"""The package directory on sys.path: pytest adds only the test file's own."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
