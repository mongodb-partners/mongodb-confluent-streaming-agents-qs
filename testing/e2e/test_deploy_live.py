"""Tests for deploy integration of the live SSE sidecar + new entrypoints.

Traceability: TC-E-040 (launch sidecar, binds, /api/health 200), TC-E-041
(port range busy -> warn+continue), TC-REG-002 (all entrypoints importable).
"""
from __future__ import annotations

import importlib

import pytest

# --- TC-REG-002: all uv entrypoints (incl. live, surge) are importable -----

def test_all_entrypoints_import():
    """INV-002: existing + new console scripts resolve to real callables."""
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    with open(root / "pyproject.toml", "rb") as f:
        scripts = tomllib.load(f)["project"]["scripts"]

    assert "live" in scripts and scripts["live"] == "scripts.live_server:main"
    assert "surge" in scripts and scripts["surge"] == "scripts.surge:main"

    for entry in scripts.values():
        mod_name, _, attr = entry.partition(":")
        mod = importlib.import_module(mod_name)
        assert callable(getattr(mod, attr)), f"{entry} is not callable"


# --- TC-E-040 / TC-E-041: deploy sidecar launcher -------------------------

def test_deploy_exposes_live_launcher():
    deploy = importlib.import_module("scripts.deploy")
    assert hasattr(deploy, "_launch_live_server")


def test_launch_live_server_binds_and_serves_health(monkeypatch):
    """TC-E-040 (boundary B7): _launch_live_server spawns a process that binds a
    port; /api/health returns 200. Uses a URI so the app starts (change stream
    will just fail to connect against a fake host — that's fine for B7)."""
    import time
    import urllib.request
    from pathlib import Path

    deploy = importlib.import_module("scripts.deploy")
    root = Path(__file__).resolve().parents[2]

    # Isolation contract: do NOT mutate the real credentials.env (REQ test rule).
    saved = {}
    monkeypatch.setattr(deploy, "_save_env_many", lambda pairs: saved.update(pairs))

    # Provide a resolvable-but-harmless URI via env so the sidecar starts.
    import os
    monkeypatch.setenv("MONGODB_URI", "mongodb://127.0.0.1:27099/demo")

    proc, port = deploy._launch_live_server(root)
    try:
        assert proc is not None and port is not None
        # poll for health
        ok = False
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1) as r:
                    if r.status == 200:
                        ok = True
                        break
            except Exception:
                time.sleep(0.4)
        assert ok, "sidecar did not serve /api/health"
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


def test_launch_live_server_warns_when_ports_busy(monkeypatch, tmp_path, capsys):
    """TC-E-041: when no free port is available, warn and return (None, None)
    rather than raising."""
    deploy = importlib.import_module("scripts.deploy")
    monkeypatch.setattr(deploy, "_find_free_live_port", lambda *a, **k: None)
    proc, port = deploy._launch_live_server(tmp_path)
    assert proc is None and port is None
    out = capsys.readouterr().out.lower()
    assert "live" in out or "port" in out
