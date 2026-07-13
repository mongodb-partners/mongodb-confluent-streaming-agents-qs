"""Tests for individual preflight check implementations (rec #4 / Task 8).

Spec: REQ-E-332 (the per-check matrix), REQ-E-339 (port existing checks).
All HTTP checks use mocked responses — no live API calls.
"""

from __future__ import annotations

import importlib

import pytest

# ---------------------------------------------------------------------------
# TC-PRE-001: _check_atlas_admin_auth
# ---------------------------------------------------------------------------

class _StubResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text

    def json(self):
        import json as _json
        return _json.loads(self.text) if self.text else {}


def test_TC_PRE_001_atlas_admin_auth_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import requests

    def fake_get(*_a, **_kw):
        return _StubResponse(200)
    monkeypatch.setattr(requests, "get", fake_get)
    env = {"ATLAS_PUBLIC_KEY": "p", "ATLAS_PRIVATE_KEY": "s",
           "ATLAS_PROJECT_ID": "id"}
    res = pre._check_atlas_admin_auth(env)
    assert res.status == "pass"


def test_TC_PRE_001_atlas_admin_auth_401(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import requests

    def fake_get(*_a, **_kw):
        return _StubResponse(401, "Unauthorized")
    monkeypatch.setattr(requests, "get", fake_get)
    env = {"ATLAS_PUBLIC_KEY": "p", "ATLAS_PRIVATE_KEY": "s",
           "ATLAS_PROJECT_ID": "id"}
    res = pre._check_atlas_admin_auth(env)
    assert res.status == "fail"
    assert "401" in res.message


def test_TC_PRE_001_atlas_admin_auth_missing_keys():
    pre = importlib.import_module("scripts.preflight")
    res = pre._check_atlas_admin_auth({})
    assert res.status == "fail"
    assert "missing" in res.message.lower()


def test_TC_PRE_001_atlas_admin_uses_correct_api_version():
    """REQ-E-332: must use API_VERSION_DEFAULT from asp_setup, not a fictional date."""
    pre = importlib.import_module("scripts.preflight")
    import inspect
    src = inspect.getsource(pre._check_atlas_admin_auth)
    assert "vnd.atlas.2023-02-01+json" in src, \
        "Atlas Admin API check must use the verified API version 2023-02-01"


# ---------------------------------------------------------------------------
# TC-PRE-002: _check_mongodb_uri_format
# ---------------------------------------------------------------------------

def test_TC_PRE_002_mongodb_uri_valid():
    pre = importlib.import_module("scripts.preflight")
    res = pre._check_mongodb_uri_format(
        {"TF_VAR_mongodb_connection_string": "mongodb+srv://u:p@cluster.example.com/"}
    )
    assert res.status == "pass"


def test_TC_PRE_002_mongodb_uri_invalid():
    pre = importlib.import_module("scripts.preflight")
    res = pre._check_mongodb_uri_format(
        {"TF_VAR_mongodb_connection_string": "not-a-uri"}
    )
    assert res.status == "fail"


def test_TC_PRE_002_mongodb_uri_missing():
    """BYO mode (create_atlas_cluster!=true) with no URI → fail.

    This is the existing-behavior anchor: BYO mode requires the URI
    upfront. The workshop fresh-deploy skip (TC-PRE-URI-004) is the
    exception, not the rule.
    """
    pre = importlib.import_module("scripts.preflight")
    res = pre._check_mongodb_uri_format({})
    assert res.status == "fail"


def test_TC_PRE_URI_004_skip_on_fresh_create_cluster_deploy():
    """REQ-E-360 §7 (extended to mongodb_uri_format): fresh workshop
    deploy with TF_VAR_create_atlas_cluster=true and no DEPLOY_PHASE
    → SKIP without error.

    The connection string is populated by terraform/atlas AFTER
    preflight runs at deploy startup. Without this skip, the
    `uv run deploy` startup preflight aborts with exit 1 before the
    cluster can be provisioned — breaking the documented workshop
    flow. This is the SAME bug class as atlas_cluster_exists
    (TC-PRE-CLUSTER-014) but on a different check.
    """
    pre = importlib.import_module("scripts.preflight")
    env = {
        "TF_VAR_create_atlas_cluster": "true",
        "DEPLOY_PHASE": "",
        # URI deliberately absent — terraform/atlas will populate it
    }
    res = pre._check_mongodb_uri_format(env)
    assert res.status == "skip", \
        f"fresh create_cluster=true must skip mongodb_uri_format, got {res.status}"


def test_TC_PRE_URI_005_no_skip_after_atlas_terraform_completed():
    """REQ-E-360 §7: AFTER atlas_terraform completes (DEPLOY_PHASE set),
    the URI SHOULD be populated by _persist_atlas_cluster_connection_string.
    If it's still missing at that point, that's a real bug — fail."""
    pre = importlib.import_module("scripts.preflight")
    env = {
        "TF_VAR_create_atlas_cluster": "true",
        "DEPLOY_PHASE": "atlas_terraform",  # phase completed; URI must be set
        # URI still missing — that's a regression in _persist_...
    }
    res = pre._check_mongodb_uri_format(env)
    assert res.status == "fail", \
        "after atlas_terraform completes the URI must be set; missing = real bug"


def test_TC_PRE_URI_006_no_skip_for_byo_mode():
    """REQ-E-360 §7: BYO mode (create_atlas_cluster=false) must never
    skip — URI is required upfront regardless of DEPLOY_PHASE."""
    pre = importlib.import_module("scripts.preflight")
    env = {
        "TF_VAR_create_atlas_cluster": "false",
        "DEPLOY_PHASE": "",
        # URI missing → real config error
    }
    res = pre._check_mongodb_uri_format(env)
    assert res.status == "fail", \
        "BYO mode must always validate URI presence, never skip"


def test_TC_PRE_URI_007_run_preflight_does_not_abort_on_workshop_skip(monkeypatch):
    """REQ-E-360 §7: `run_preflight()` at `uv run deploy` startup must
    treat mongodb_uri_format as 'skip' (not 'fail') when in workshop
    fresh-deploy mode. Without this, the documented
    create_atlas_cluster=true path aborts with exit 1 because the
    URI hasn't been populated by terraform/atlas yet.

    Reproduces the bug a user hit immediately after my earlier 5-commit
    series: interactive prompts complete, preflight runs at deploy
    startup, and `mongodb_uri_format` fails because URI is legitimately
    absent (terraform/atlas will create the cluster + populate the URI
    in the next phase).

    Asserts only on the specific check entry — other checks may
    pass/fail in this stub env; we only care that THIS check no longer
    reports `fail` in the workshop case.
    """
    pre = importlib.import_module("scripts.preflight")
    env = {
        "TF_VAR_create_atlas_cluster": "true",
        "DEPLOY_PHASE":                "",
        # URI deliberately missing — terraform/atlas will populate
    }
    result = pre.run_preflight_with_results(env=env, skip_network=True)
    uri_check = next(
        c for c in result["checks"]
        if c["name"] == "mongodb_uri_format"
    )
    assert uri_check["status"] == "skip", (
        f"mongodb_uri_format must skip on workshop fresh deploy, got "
        f"{uri_check['status']!r} with message {uri_check['message']!r}"
    )


# ---------------------------------------------------------------------------
# TC-PRE-005: _check_docker_daemon
# ---------------------------------------------------------------------------

def test_TC_PRE_005_docker_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import subprocess

    class StubCompleted:
        returncode = 0
        stdout = "Client: Docker"

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: StubCompleted())
    res = pre._check_docker_daemon({})
    assert res.status == "pass"


def test_TC_PRE_005_docker_fail(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import subprocess

    class StubFailed:
        returncode = 1
        stdout = ""
        stderr = "Cannot connect"

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: StubFailed())
    res = pre._check_docker_daemon({})
    assert res.status == "fail"


# ---------------------------------------------------------------------------
# TC-PRE-006: _check_aws_caller_identity
# ---------------------------------------------------------------------------

def test_TC_PRE_006_aws_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import subprocess

    class StubCompleted:
        returncode = 0
        stdout = '{"UserId": "AID"}'

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: StubCompleted())
    res = pre._check_aws_caller_identity({})
    assert res.status == "pass"


def test_TC_PRE_006_aws_fail(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import subprocess

    class StubFailed:
        returncode = 255
        stdout = ""
        stderr = "Unable to locate credentials"

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: StubFailed())
    res = pre._check_aws_caller_identity({})
    assert res.status == "fail"


# ---------------------------------------------------------------------------
# TC-PRE-003: _check_flink_rest_reachable
# ---------------------------------------------------------------------------

def test_TC_PRE_003_flink_rest_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import requests

    def fake_get(*_a, **_kw):
        return _StubResponse(200)
    monkeypatch.setattr(requests, "get", fake_get)
    env = {
        "CONFLUENT_FLINK_REST_ENDPOINT": "https://flink.example/sql/v1",
        "CONFLUENT_FLINK_API_KEY": "k",
        "CONFLUENT_FLINK_API_SECRET": "s",
    }
    res = pre._check_flink_rest_reachable(env)
    # 200 or 401 both count as "endpoint is reachable" for this check
    assert res.status in ("pass", "warn")


def test_TC_PRE_003_flink_rest_unreachable(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import requests

    def fake_get(*_a, **_kw):
        raise requests.exceptions.ConnectionError("unreachable")
    monkeypatch.setattr(requests, "get", fake_get)
    env = {
        "CONFLUENT_FLINK_REST_ENDPOINT": "https://nope.example/",
        "CONFLUENT_FLINK_API_KEY": "k",
        "CONFLUENT_FLINK_API_SECRET": "s",
    }
    res = pre._check_flink_rest_reachable(env)
    assert res.status == "fail"


# ---------------------------------------------------------------------------
# TC-PRE-004: _check_kafka_rest_reachable
# ---------------------------------------------------------------------------

def test_TC_PRE_004_kafka_rest_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")
    import requests

    def fake_get(*_a, **_kw):
        return _StubResponse(200)
    monkeypatch.setattr(requests, "get", fake_get)
    env = {
        "CONFLUENT_KAFKA_REST_ENDPOINT": "https://kafka.example/kafka/v3",
        "CONFLUENT_KAFKA_API_KEY": "k",
        "CONFLUENT_KAFKA_API_SECRET": "s",
        "CONFLUENT_KAFKA_CLUSTER_ID": "lkc-x",
    }
    res = pre._check_kafka_rest_reachable(env)
    assert res.status in ("pass", "warn")


# ---------------------------------------------------------------------------
# TC-PRE-MIGRATE-001: existing checks ported (REQ-E-339)
# ---------------------------------------------------------------------------

def test_TC_PRE_MIGRATE_001_registry_includes_known_checks():
    pre = importlib.import_module("scripts.preflight")
    names = {c.name for c in pre.CHECKS}
    expected = {
        "atlas_admin_auth",
        "atlas_cluster_exists",
        "mongodb_uri_format",
        "flink_rest_reachable",
        "kafka_rest_reachable",
        "docker_daemon",
        "aws_caller_identity",
    }
    missing = expected - names
    assert not missing, f"preflight CHECKS missing: {missing}"


# ---------------------------------------------------------------------------
# TC-PRE-CLUSTER-001..009: _check_atlas_cluster_exists
#
# Verifies ATLAS_CLUSTER_NAME exists in the configured Atlas project before
# asp_setup attempts to create connections that reference it. Without this,
# the deploy fails late with three near-identical 400 errors from atlas_cluster
# + events_dlq + fleet_dlq (all reference the same cluster_name).
# ---------------------------------------------------------------------------

def _cluster_env(**overrides):
    env = {
        "ATLAS_PUBLIC_KEY":   "pub",
        "ATLAS_PRIVATE_KEY":  "priv",
        "ATLAS_PROJECT_ID":   "proj-1",
        "ATLAS_CLUSTER_NAME": "my-cluster",
    }
    env.update(overrides)
    return env


class _ListBackedSession:
    """Stub `requests` module: returns a sequence of responses across get() calls."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def __call__(self, url, *args, **kwargs):
        self.calls.append(url)
        if not self._responses:
            raise AssertionError(f"no stub response queued for {url}")
        return self._responses.pop(0)


def test_TC_PRE_CLUSTER_001_pass_when_cluster_exists(monkeypatch):
    """REQ-E-360 §3: 200 from GET /clusters/{name} → pass (single call)."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([_StubResponse(200, '{"name":"my-cluster"}')])
    monkeypatch.setattr(requests, "get", stub)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "pass"
    assert "my-cluster" in res.message
    # Should only call the specific-cluster endpoint (no need to enumerate)
    assert len(stub.calls) == 1
    assert "/clusters/my-cluster" in stub.calls[0]


def test_TC_PRE_CLUSTER_002_fail_with_available_clusters_listed(monkeypatch):
    """REQ-E-360 §4: 404 + project has other clusters → fail with actionable list."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([
        _StubResponse(404, '{"error":404}'),
        _StubResponse(200, '{"results":[{"name":"alpha"},{"name":"beta"},{"name":"gamma"}]}'),
    ])
    monkeypatch.setattr(requests, "get", stub)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "fail"
    assert "my-cluster" in res.message
    assert "not found" in res.message.lower()
    # Remediation must enumerate the actual cluster names so operator can fix .env
    assert res.remediation is not None
    for name in ("alpha", "beta", "gamma"):
        assert name in res.remediation, f"remediation must list {name!r}"


def test_TC_PRE_CLUSTER_003_fail_when_project_has_no_clusters(monkeypatch):
    """REQ-E-360 §4: 404 + empty cluster list → fail, points at TF_VAR_create_atlas_cluster."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([
        _StubResponse(404),
        _StubResponse(200, '{"results":[]}'),
    ])
    monkeypatch.setattr(requests, "get", stub)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "fail"
    assert res.remediation is not None
    assert "TF_VAR_create_atlas_cluster" in res.remediation


def test_TC_PRE_CLUSTER_004_fail_when_cluster_name_unset(monkeypatch):
    """REQ-E-360 §2: ATLAS_CLUSTER_NAME unset → fail without hitting the network."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    def must_not_call(*_a, **_kw):
        raise AssertionError("network must not be hit when ATLAS_CLUSTER_NAME is unset")
    monkeypatch.setattr(requests, "get", must_not_call)
    env = _cluster_env()
    env["ATLAS_CLUSTER_NAME"] = ""
    res = pre.check_atlas_cluster_exists(env)
    assert res.status == "fail"
    assert "ATLAS_CLUSTER_NAME" in res.message


def test_TC_PRE_CLUSTER_005_fail_on_401(monkeypatch):
    """REQ-E-360 §5: 401 → fail (defense in depth; primary signal is atlas_admin_auth)."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    monkeypatch.setattr(requests, "get",
                        _ListBackedSession([_StubResponse(401, "Unauthorized")]))
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "fail"
    assert "401" in res.message


def test_TC_PRE_CLUSTER_006_warn_on_connection_error(monkeypatch):
    """REQ-E-360 §6: Network error → warn (not fail; AtlasAPI retries handle transient)."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    def boom(*_a, **_kw):
        raise requests.exceptions.ConnectionError("unreachable")
    monkeypatch.setattr(requests, "get", boom)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "warn"


def test_TC_PRE_CLUSTER_007_warn_when_admin_keys_missing():
    """REQ-E-360 §1: missing admin keys → warn (handled by atlas_admin_auth check)."""
    pre = importlib.import_module("scripts.preflight")
    res = pre.check_atlas_cluster_exists({"ATLAS_CLUSTER_NAME": "x"})
    assert res.status == "warn"


def test_TC_PRE_CLUSTER_008_registered_on_asp_setup_phase():
    """REQ-E-332 / REQ-E-360: registered in CHECKS with phases=('asp_setup',)."""
    pre = importlib.import_module("scripts.preflight")
    entries = [c for c in pre.CHECKS if c.name == "atlas_cluster_exists"]
    assert len(entries) == 1
    c = entries[0]
    assert "asp_setup" in c.phases
    assert c.severity == "fail"
    assert c.network is True


def test_TC_PRE_CLUSTER_009_no_list_call_on_pass(monkeypatch):
    """REQ-E-360 §3: 200 on first call must NOT trigger the list-clusters call."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([_StubResponse(200)])
    monkeypatch.setattr(requests, "get", stub)
    pre.check_atlas_cluster_exists(_cluster_env())
    # Only one HTTP call — the optimistic specific-cluster GET
    assert len(stub.calls) == 1, \
        "pass path must not enumerate the cluster list (extra API call)"


def test_TC_PRE_CLUSTER_010_list_enum_401_does_not_report_empty(monkeypatch):
    """REQ-E-360 §4: enumeration call returning 401 must NOT report 'project has no clusters'.

    Previously the code wrapped the enumeration in `except Exception:
    available = []` which silently downgraded "couldn't list" to
    "empty project" — producing a misleading remediation when the
    key was revoked between the two calls.
    """
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([
        _StubResponse(404),
        _StubResponse(401, "key revoked between calls"),
    ])
    monkeypatch.setattr(requests, "get", stub)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "fail"
    # Message must NOT claim project is empty
    assert "no clusters" not in res.message.lower(), \
        "must not report 'no clusters' when enumeration call itself failed"
    # Remediation must distinguish this case from the "empty project" case
    assert res.remediation is not None
    assert "could not list" in res.message.lower() or \
           "could not list" in res.remediation.lower() or \
           "verify ATLAS" in res.remediation, \
        "remediation must signal that enumeration failed, not that project is empty"


def test_TC_PRE_CLUSTER_011_list_enum_network_error_does_not_report_empty(monkeypatch):
    """REQ-E-360 §4: enumeration network error must also not report 'project has no clusters'."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    calls = {"n": 0}
    def maybe_boom(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return _StubResponse(404)
        raise requests.exceptions.ConnectionError("network died mid-check")
    monkeypatch.setattr(requests, "get", maybe_boom)
    res = pre.check_atlas_cluster_exists(_cluster_env())
    assert res.status == "fail"
    assert "no clusters" not in res.message.lower()


def test_TC_PRE_CLUSTER_012_integration_via_run_preflight(monkeypatch):
    """REQ-E-360: check fires through the full registry + timeout wrapper.

    Unit tests call check_atlas_cluster_exists directly. This test
    verifies the check is actually invoked end-to-end when
    `run_preflight(phase='asp_setup')` runs — the filter, timeout
    wrapper, and result aggregation all see it.
    """
    pre = importlib.import_module("scripts.preflight")
    import requests

    # All other asp_setup-phase checks pass; only atlas_cluster_exists
    # should produce a fail.
    monkeypatch.setattr(requests, "get",
                        _ListBackedSession([
                            _StubResponse(200),                            # atlas_admin_auth
                            _StubResponse(404),                            # atlas_cluster_exists (specific)
                            _StubResponse(200, '{"results":[{"name":"only-one"}]}'),  # cluster list
                        ]))
    env = {
        "ATLAS_PUBLIC_KEY":   "p",
        "ATLAS_PRIVATE_KEY":  "s",
        "ATLAS_PROJECT_ID":   "proj",
        "ATLAS_CLUSTER_NAME": "missing-cluster",
    }
    result = pre.run_preflight_with_results(phase="asp_setup", env=env)
    names = [c["name"] for c in result["checks"]]
    assert "atlas_cluster_exists" in names, \
        "check must run when phase=asp_setup is selected"
    cluster_entry = next(c for c in result["checks"] if c["name"] == "atlas_cluster_exists")
    assert cluster_entry["status"] == "fail"
    assert "only-one" in (cluster_entry["remediation"] or ""), \
        "available-cluster list must appear in remediation via run_preflight"
    # Summary must count the failure
    assert result["summary"]["fail"] >= 1


def test_TC_PRE_CLUSTER_013_backwards_compat_underscore_alias(monkeypatch):
    """Renaming `_check_atlas_cluster_exists` → `check_atlas_cluster_exists` keeps the
    old private name as a backwards-compat alias so external monkeypatchers
    don't break across the boundary."""
    pre = importlib.import_module("scripts.preflight")
    assert hasattr(pre, "check_atlas_cluster_exists"), "public name must exist"
    assert hasattr(pre, "_check_atlas_cluster_exists"), \
        "private alias must exist for backwards-compat"
    assert pre._check_atlas_cluster_exists is pre.check_atlas_cluster_exists, \
        "alias must be the same object, not a separate copy"


def test_TC_PRE_CLUSTER_014_skip_on_fresh_create_cluster_deploy(monkeypatch):
    """REQ-E-360 §7: fresh workshop deploy with TF_VAR_create_atlas_cluster=true
    and no DEPLOY_PHASE set → check SKIPS without hitting the network.

    Without this skip, `uv run deploy` runs the preflight at startup BEFORE
    the atlas_terraform phase provisions the cluster — and a 404 from the
    not-yet-existent cluster aborts the deploy with exit 1. This regression
    would break the documented `create_atlas_cluster=true` workshop path.
    """
    pre = importlib.import_module("scripts.preflight")
    import requests
    def must_not_call(*_a, **_kw):
        raise AssertionError("must not hit network when cluster is being provisioned")
    monkeypatch.setattr(requests, "get", must_not_call)
    env = _cluster_env()
    env["TF_VAR_create_atlas_cluster"] = "true"
    env["DEPLOY_PHASE"] = ""  # fresh run; atlas_terraform hasn't completed
    res = pre.check_atlas_cluster_exists(env)
    assert res.status == "skip", \
        f"fresh create_cluster=true deploy must skip, got {res.status}"


def test_TC_PRE_CLUSTER_015_no_skip_after_atlas_terraform_completed(monkeypatch):
    """REQ-E-360 §7: AFTER atlas_terraform completes, the cluster exists and
    the check SHOULD run (and pass)."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([_StubResponse(200, '{"name":"my-cluster"}')])
    monkeypatch.setattr(requests, "get", stub)
    env = _cluster_env()
    env["TF_VAR_create_atlas_cluster"] = "true"
    env["DEPLOY_PHASE"] = "atlas_terraform"  # phase completed; cluster exists
    res = pre.check_atlas_cluster_exists(env)
    assert res.status == "pass", \
        "check must run (not skip) once atlas_terraform has completed"
    assert len(stub.calls) == 1, "must make the HTTP call when not skipping"


def test_TC_PRE_CLUSTER_016_no_skip_for_byo_cluster_mode(monkeypatch):
    """REQ-E-360 §7: BYO mode (create_atlas_cluster=false) must NEVER skip
    — the cluster must exist regardless of DEPLOY_PHASE."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([_StubResponse(404), _StubResponse(200, '{"results":[]}')])
    monkeypatch.setattr(requests, "get", stub)
    env = _cluster_env()
    env["TF_VAR_create_atlas_cluster"] = "false"
    env["DEPLOY_PHASE"] = ""  # fresh BYO run
    res = pre.check_atlas_cluster_exists(env)
    assert res.status == "fail", \
        "BYO mode must always validate cluster existence, never skip"


def test_TC_PRE_CLUSTER_017_no_skip_for_resumed_deploy(monkeypatch):
    """REQ-E-360 §7: resume from any post-atlas_terraform phase must NOT skip.
    Cluster has been provisioned by that point and must be verified."""
    pre = importlib.import_module("scripts.preflight")
    import requests
    stub = _ListBackedSession([_StubResponse(200)])
    monkeypatch.setattr(requests, "get", stub)
    for phase in ("mcp_server", "terraform", "credentials",
                  "publish_data", "asp_setup", "flink_dml", "complete"):
        env = _cluster_env()
        env["TF_VAR_create_atlas_cluster"] = "true"
        env["DEPLOY_PHASE"] = phase
        # Refresh stub per iteration
        stub = _ListBackedSession([_StubResponse(200)])
        monkeypatch.setattr(requests, "get", stub)
        res = pre.check_atlas_cluster_exists(env)
        assert res.status != "skip", \
            f"must not skip when DEPLOY_PHASE={phase!r} (cluster should exist)"


def test_TC_PRE_CLUSTER_018_run_preflight_aggregation_handles_skip(monkeypatch):
    """REQ-E-360 §7: run_preflight() at deploy startup must NOT count
    'skip' as a fail. Without this, the deploy would abort even though
    the check explicitly skipped itself."""
    pre = importlib.import_module("scripts.preflight")
    import requests

    # Stub responses for all asp_setup-phase checks. atlas_cluster_exists
    # is skipped (won't hit network). atlas_admin_auth needs ONE 200.
    stub = _ListBackedSession([_StubResponse(200)])
    monkeypatch.setattr(requests, "get", stub)
    env = {
        "ATLAS_PUBLIC_KEY":          "p",
        "ATLAS_PRIVATE_KEY":         "s",
        "ATLAS_PROJECT_ID":          "proj",
        "ATLAS_CLUSTER_NAME":        "future-cluster",
        "TF_VAR_create_atlas_cluster": "true",
        "DEPLOY_PHASE":              "",  # fresh deploy
    }
    passed, warned, failed = pre.run_preflight(phase="asp_setup", env=env)
    assert failed == 0, \
        f"fresh create_cluster=true deploy must not produce a fail (got {failed})"
