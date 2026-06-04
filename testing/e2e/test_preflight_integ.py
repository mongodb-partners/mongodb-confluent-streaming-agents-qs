"""Tests that run_deployment integrates with the preflight framework.

Spec: REQ-E-335.
"""

from __future__ import annotations

import importlib
import inspect


def test_TC_PRE_INTEG_001_skip_preflight_flag_declared():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--skip-preflight"' in src, \
        "main() must declare --skip-preflight"


def test_TC_PRE_INTEG_001_run_deployment_calls_run_preflight():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    assert "run_preflight" in src, \
        "run_deployment must invoke run_preflight (unless --skip-preflight)"
