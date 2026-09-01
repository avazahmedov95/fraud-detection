"""Make the package importable from tests/.

The tests live one directory below the modules they exercise, so the package
directory has to be on sys.path for `import features` to resolve. pytest adds
the test file's OWN directory (rootdir insertion), not its parent.

Kept here rather than in a pytest.ini or a package __init__.py: these five
packages deploy as separate units with no shared tooling, and a conftest is the
one file pytest loads without any configuration at all.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
