"""The version is declared once, in pyproject.toml.

0.7.4 shipped reporting 0.7.3 because a second copy lived as a literal in
``__init__.py`` and only one of the two was bumped. PyPI releases are
immutable, so that could not be corrected in place.
"""

import re
import subprocess
import sys
import unittest
from pathlib import Path

import beam_network_sdk

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def declared_version() -> str:
    """Read the declared version without tomllib, which is 3.11 and newer only."""
    match = re.search(r'^version\s*=\s*"([^"]+)"', PYPROJECT.read_text(), re.MULTILINE)
    if match is None:
        raise AssertionError(f"no version declared in {PYPROJECT}")
    return match.group(1)


class VersionReportingTest(unittest.TestCase):
    def test_reported_version_matches_pyproject(self) -> None:
        self.assertEqual(beam_network_sdk.__version__, declared_version())

    def test_cli_reports_the_same_version(self) -> None:
        """Run the console script the way a user does."""
        result = subprocess.run(
            [sys.executable, "-m", "beam_network_sdk.cli", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )

        self.assertIn(declared_version(), result.stdout)
