"""Package dir onto sys.path: pytest inserts the test file's OWN directory, not
its parent, so `import features` needs this. In a conftest because the five
packages deploy as separate units with no shared tooling.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# case.py, explain.py and integrity.py, from the packages that own them (the image
# copies them in).
for _owner in ("case-manager", "data-generator"):
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", _owner))
