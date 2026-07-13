"""Regression tests for the holistic-review 'quick win' fixes.

- redaction.mask_secret is the single source of truth for {first4}…{last2}
- generate_deployment_summary imports it (no duplicate _mask)
- setup_logging returns the CALLER's module logger, not logging_utils'
- generate_batches.py is deprecated and refuses to clobber without opt-in
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


def test_mask_secret_shape_and_none_safety():
    from scripts.common.redaction import mask_secret

    assert mask_secret(None) == "***"
    assert mask_secret("") == "***"
    assert mask_secret("short1") == "***"  # < 8 chars
    assert mask_secret("abcdefghij") == "abcd…ij"


def test_summary_module_has_no_local_mask_def():
    """The nested _mask must be gone — it should import from redaction."""
    import scripts.common.generate_deployment_summary as gds

    src = Path(gds.__file__).read_text()
    assert "def _mask(" not in src, "duplicate _mask must be removed"
    assert "mask_secret" in src, "summary must import the shared mask"


def test_setup_logging_returns_caller_module_logger():
    """setup_logging must name the logger after the CALLER's module, not
    scripts.common.logging_utils."""
    from scripts.common.logging_utils import setup_logging

    lg = setup_logging(name="my.caller.module")
    assert lg.name == "my.caller.module"
    # Without an explicit name it derives from the calling frame — here that is
    # this test module, definitely NOT logging_utils.
    lg2 = setup_logging()
    assert lg2.name != "scripts.common.logging_utils"


def test_generate_batches_refuses_without_optin():
    """The deprecated generator must not clobber output without the explicit
    opt-in flag."""
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "scripts" / "generate_batches.py")],
        capture_output=True,
        text=True,
        cwd=root,
    )
    assert result.returncode == 2
    assert "DEPRECATED" in result.stderr
