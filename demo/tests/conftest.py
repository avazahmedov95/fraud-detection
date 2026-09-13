"""Path shim for the demo tests: pytest inserts the test file's OWN directory, not
its parent, so the package dir is added here - as the other packages do."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
