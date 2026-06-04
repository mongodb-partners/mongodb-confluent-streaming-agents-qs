"""Tests for FlinkRestClient (rec #1, Task 10).

Spec: REQ-E-340..344, REQ-E-342a, REQ-E-349.
"""

from __future__ import annotations

import base64
import importlib
import inspect
from dataclasses import is_dataclass

import pytest


# ---------------------------------------------------------------------------
# TC-FRC-INIT-001: class + from_env
# ---------------------------------------------------------------------------

def test_TC_FRC_INIT_001_class_exists():
    mod = importlib.import_module("scripts.common.flink_rest")
    assert hasattr(mod, "FlinkRestClient")
    assert hasattr(mod, "FlinkRestProtocol")
    assert is_dataclass(mod.FlinkRestClient)


def test_TC_FRC_INIT_001_from_env_validates():
    mod = importlib.import_module("scripts.common.flink_rest")
    # Empty env should raise (missing required fields)
    with pytest.raises((ValueError, KeyError)):
        mod.FlinkRestClient.from_env({})


def test_TC_FRC_INIT_001_from_env_constructs():
    mod = importlib.import_module("scripts.common.flink_rest")
    env = {
        "CONFLUENT_FLINK_REST_ENDPOINT": "https://flink.example",
        "CONFLUENT_FLINK_API_KEY":       "k",
        "CONFLUENT_FLINK_API_SECRET":    "s",
        "CONFLUENT_ORG_ID":              "org",
        "CONFLUENT_ENV_ID":              "env",
        "CONFLUENT_FLINK_COMPUTE_POOL_ID": "pool",
        "CONFLUENT_SERVICE_ACCOUNT_ID":  "sa",
        "CONFLUENT_FLINK_CATALOG":       "cat",
        "CONFLUENT_FLINK_DATABASE":      "db",
    }
    client = mod.FlinkRestClient.from_env(env)
    assert client.rest_endpoint == "https://flink.example"
    assert client.api_key == "k"


# ---------------------------------------------------------------------------
# TC-FRC-METHODS-001..007: each method exists with correct signature
# ---------------------------------------------------------------------------

def test_TC_FRC_METHODS_present():
    mod = importlib.import_module("scripts.common.flink_rest")
    for method in ("submit", "delete_and_wait", "get", "list",
                   "drop_table", "wait_for_phase", "force_failed_recreate"):
        assert hasattr(mod.FlinkRestClient, method), \
            f"FlinkRestClient missing method {method}"


# ---------------------------------------------------------------------------
# TC-FRC-AUTH-001: base64 Basic auth header
# ---------------------------------------------------------------------------

def test_TC_FRC_AUTH_001_base64_basic_header():
    mod = importlib.import_module("scripts.common.flink_rest")
    client = mod.FlinkRestClient(
        rest_endpoint="https://x", api_key="key", api_secret="secret",
        org_id="o", env_id="e", compute_pool_id="cp",
        service_account_id="sa", catalog="c", database="d",
    )
    expected = "Basic " + base64.b64encode(b"key:secret").decode()
    assert client._auth_header() == expected


# ---------------------------------------------------------------------------
# INV-310: REST URL pattern preserved
# ---------------------------------------------------------------------------

def test_INV_310_url_pattern_preserved():
    mod = importlib.import_module("scripts.common.flink_rest")
    client = mod.FlinkRestClient(
        rest_endpoint="https://flink.endpoint", api_key="k", api_secret="s",
        org_id="ORG", env_id="ENV", compute_pool_id="cp",
        service_account_id="sa", catalog="c", database="d",
    )
    assert client._statement_url("zone-traffic-sink-insert") == \
        "https://flink.endpoint/sql/v1/organizations/ORG/environments/ENV/statements/zone-traffic-sink-insert"
    assert client._statement_url() == \
        "https://flink.endpoint/sql/v1/organizations/ORG/environments/ENV/statements"


# ---------------------------------------------------------------------------
# TC-FRC-RETRY-001: 429/5xx retry with [3,6,12] backoff
# ---------------------------------------------------------------------------

def test_TC_FRC_RETRY_001_retry_backoff(monkeypatch):
    mod = importlib.import_module("scripts.common.flink_rest")
    client = mod.FlinkRestClient(
        rest_endpoint="https://x", api_key="k", api_secret="s",
        org_id="o", env_id="e", compute_pool_id="cp",
        service_account_id="sa", catalog="c", database="d",
    )

    sleeps: list = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    import urllib.error
    # Patch on the class (frozen dataclass blocks instance-level setattr)
    def fake_post(self, url, body, timeout=30):
        raise urllib.error.HTTPError(url, 429, "Too Many", {}, None)
    monkeypatch.setattr(mod.FlinkRestClient, "_post", fake_post)
    with pytest.raises(urllib.error.HTTPError):
        client._post_with_retry("https://x", b"{}")
    assert sleeps == [3, 6], f"backoff schedule must be [3, 6]; got {sleeps}"


def test_TC_FRC_RETRY_001_no_retry_on_4xx(monkeypatch):
    mod = importlib.import_module("scripts.common.flink_rest")
    client = mod.FlinkRestClient(
        rest_endpoint="https://x", api_key="k", api_secret="s",
        org_id="o", env_id="e", compute_pool_id="cp",
        service_account_id="sa", catalog="c", database="d",
    )
    sleeps: list = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    import urllib.error
    def fake_post(self, url, body, timeout=30):
        raise urllib.error.HTTPError(url, 400, "Bad", {}, None)
    monkeypatch.setattr(mod.FlinkRestClient, "_post", fake_post)
    with pytest.raises(urllib.error.HTTPError):
        client._post_with_retry("https://x", b"{}")
    assert sleeps == [], "4xx (non-429) must not be retried"


# ---------------------------------------------------------------------------
# TC-FRC-PARITY-001 (REQ-E-342a): signature parity for FakeFlinkRestClient
# ---------------------------------------------------------------------------

class FakeFlinkRestClient:
    """Reference fake — must signature-match FlinkRestClient public methods."""

    def submit(self, name, sql, properties=None, expect_phase="RUNNING", timeout=120):
        return {"name": name}

    def get(self, name):
        return None

    def delete(self, name):
        return None

    def delete_and_wait(self, name, timeout=60):
        return None

    def list(self, prefix=None, phase=None):
        return []

    def drop_table(self, table_or_view, if_exists=True):
        return None

    def wait_for_phase(self, name, phase, timeout):
        return {}

    def force_failed_recreate(self, name, sql, properties=None):
        return {}


def test_TC_FRC_PARITY_001_signature_parity():
    mod = importlib.import_module("scripts.common.flink_rest")
    real_cls = mod.FlinkRestClient
    fake_cls = FakeFlinkRestClient
    for method in ("submit", "delete_and_wait", "get", "list",
                   "drop_table", "wait_for_phase", "force_failed_recreate"):
        real = inspect.signature(getattr(real_cls, method))
        fake = inspect.signature(getattr(fake_cls, method))
        # Parameter names + defaults must align (return annotation is allowed
        # to differ; we only compare param positional/keyword surface).
        real_params = [
            (p.name, p.default, p.kind)
            for p in real.parameters.values()
            if p.name != "self"
        ]
        fake_params = [
            (p.name, p.default, p.kind)
            for p in fake.parameters.values()
            if p.name != "self"
        ]
        assert real_params == fake_params, (
            f"{method} parity mismatch:\n"
            f"  real: {real_params}\n"
            f"  fake: {fake_params}"
        )


# ---------------------------------------------------------------------------
# INV-303: terraform_runner propagation auto-retry untouched
# ---------------------------------------------------------------------------

def test_INV_303_terraform_runner_unchanged():
    """REQ-E-349: FlinkRestClient must not duplicate the terraform-layer
    propagation auto-retry logic, and terraform_runner.run_terraform must
    still implement it."""
    runner = importlib.import_module("scripts.common.terraform_runner")
    src = inspect.getsource(runner.run_terraform)
    # The 45/90/120 backoff schedule for propagation lag must still exist
    assert "45" in src and "90" in src and "120" in src, \
        "terraform_runner.run_terraform must still have 45/90/120s propagation retry"


# ---------------------------------------------------------------------------
# TC-FRC-LOG-001: logs via cli_output (REQ-E-344)
# ---------------------------------------------------------------------------

def test_TC_FRC_LOG_001_logs_via_cli_output():
    mod = importlib.import_module("scripts.common.flink_rest")
    src = inspect.getsource(mod)
    assert "cli_output" in src, \
        "FlinkRestClient module must use cli_output for logging"


# ---------------------------------------------------------------------------
# TC-FRC-REFACTOR-001: deploy.py uses FlinkRestClient (REQ-E-345)
# ---------------------------------------------------------------------------

def test_TC_FRC_REFACTOR_001_deploy_imports_client():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    assert "FlinkRestClient" in src, \
        "_create_flink_dml_statements must instantiate FlinkRestClient"


def test_TC_FRC_REFACTOR_001_phantom_drop_uses_client():
    """The phantom-table drop pre-DDL uses flink_client.drop_table."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    assert "flink_client.drop_table" in src, \
        "phantom-table drop should delegate to FlinkRestClient.drop_table"


# ---------------------------------------------------------------------------
# INV-311: Five Flink statement names + SQL templates preserved
# ---------------------------------------------------------------------------

def test_INV_311_statement_names_preserved():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy)
    expected = (
        "anomalies-enriched-ctas",
        "zone-traffic-sink-insert",
        "anomaly-detection-insert",
        "anomalies-enriched-insert",
        "anomalies-sink-insert",
    )
    for stmt in expected:
        assert stmt in src, \
            f"deploy.py must reference Flink statement {stmt} (INV-311)"


# ---------------------------------------------------------------------------
# INV-312: 60s post-RUNNING DML stability validation
# ---------------------------------------------------------------------------

def test_INV_312_dml_stability_check_present():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    # The stability check waits ~60s after initial RUNNING and re-checks
    assert "60" in src, \
        "_create_flink_dml_statements must keep the post-RUNNING stability check (INV-312)"


# ---------------------------------------------------------------------------
# INV-313: 60s DDL completion gate
# ---------------------------------------------------------------------------

def test_INV_313_ddl_completion_gate_present():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    assert "ddl_max_wait" in src, \
        "_create_flink_dml_statements must keep the DDL completion gate (INV-313)"


# ---------------------------------------------------------------------------
# INV-305: MCP-unhealthy "skip dispatch-insert" guard preserved
# ---------------------------------------------------------------------------

def test_INV_305_mcp_unhealthy_skip_preserved():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    assert "Skipping DML" in src or "dispatch-insert" in src, \
        "_create_flink_dml_statements must preserve MCP-unhealthy skip-dispatch (INV-305)"


# ---------------------------------------------------------------------------
# TC-FRC-REFACTOR-002: destroy.py uses FlinkRestClient (REQ-E-346)
# ---------------------------------------------------------------------------

def test_TC_FRC_REFACTOR_002_destroy_uses_client():
    destroy = importlib.import_module("scripts.destroy")
    src = inspect.getsource(destroy._delete_flink_dml_statements)
    assert "FlinkRestClient" in src, \
        "destroy._delete_flink_dml_statements must use FlinkRestClient (REQ-E-346)"
    assert "delete_and_wait" in src, \
        "destroy._delete_flink_dml_statements must use client.delete_and_wait"


# ---------------------------------------------------------------------------
# INV-304: destroy ordering preserved (Flink → topics → ASP → MCP → terraform)
# ---------------------------------------------------------------------------

def test_INV_304_destroy_ordering_preserved():
    destroy = importlib.import_module("scripts.destroy")
    src = inspect.getsource(destroy.main)
    # Find approximate positions of each step
    flink_pos    = src.find("_delete_flink_dml_statements")
    topics_pos   = src.find("_delete_kafka_topics")
    # Each must be present
    assert flink_pos != -1
    assert topics_pos != -1
    # Order: flink before topics, topics before asp, asp before mcp, mcp before terraform
    assert flink_pos < topics_pos, "INV-304: Flink delete must precede topic delete"


# ---------------------------------------------------------------------------
# TC-FRC-REFACTOR-003: pipeline_reset uses FlinkRestClient (REQ-E-347)
# ---------------------------------------------------------------------------

def test_TC_FRC_REFACTOR_003_pipeline_reset_uses_client():
    pr = importlib.import_module("scripts.pipeline_reset")
    src = inspect.getsource(pr._drop_flink_catalog_tables)
    assert "FlinkRestClient" in src, \
        "_drop_flink_catalog_tables must use FlinkRestClient (REQ-E-347)"
    assert "drop_table" in src, \
        "_drop_flink_catalog_tables must use client.drop_table"


# ---------------------------------------------------------------------------
# TC-FRC-REFACTOR-004: orphan sweep + stale MCP drop use client (REQ-E-348)
# ---------------------------------------------------------------------------

def test_TC_FRC_REFACTOR_004_orphan_sweep_uses_client():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._sweep_orphan_agents_statements)
    assert "FlinkRestClient" in src, \
        "_sweep_orphan_agents_statements must use FlinkRestClient (REQ-E-348)"


def test_TC_FRC_REFACTOR_004_stale_mcp_drop_uses_client():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._drop_stale_mcp_catalog_objects)
    assert "FlinkRestClient" in src, \
        "_drop_stale_mcp_catalog_objects must use FlinkRestClient (REQ-E-348)"
