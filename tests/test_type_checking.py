import subprocess
import sys

from django.test import SimpleTestCase


class TestTypeAnnotations(SimpleTestCase):
    def test_mypy_type_checking(self) -> None:
        """Verify mypy catches type errors in our decorators and registry.

        ``sandbox/type_checking.py`` contains:

        - **Positive assertions**: valid code that must type-check cleanly.
        - **Negative assertions**: invalid code with ``# type: ignore[error-code]``.

        If a negative assertion's ``# type: ignore`` becomes unused (the
        expected error disappears), mypy's ``warn_unused_ignores`` setting
        reports it — meaning our types have regressed.
        """
        result = subprocess.run(
            [sys.executable, "-m", "mypy", "sandbox/type_checking.py"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"mypy found unexpected type errors:\n{result.stdout}{result.stderr}",
        )
