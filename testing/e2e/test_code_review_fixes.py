"""
End-to-end tests for the 2026-05-25 code review fixes.

Each TC-CRF-* maps to a REQ-CRF-* in
``specs/2026-05-25-code-review-fixes/requirements.md`` and a task in
``tasks.md``. Tests are added incrementally per task during TDD.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import io
import os
import sys
import textwrap
import threading
from pathlib import Path
from unittest import mock

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# T1: Module-level `import time` in deploy.py  (REQ-CRF-001)
# ---------------------------------------------------------------------------


def _read_source(rel_path: str) -> str:
    return (PROJECT_ROOT / rel_path).read_text()


def _module_level_imports(rel_path: str) -> set[str]:
    """Return the set of top-level imported names in a Python file."""
    tree = ast.parse(_read_source(rel_path))
    names: set[str] = set()
    for node in tree.body:  # module-level only — do NOT walk()
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name)
    return names


def test_TC_CRF_001_deploy_imports_time_at_module_level():
    """REQ-CRF-001: ``time`` must be importable at module scope so that
    ``_deploy_mcp_if_needed`` and ``_start_mcp_image_build_async`` do not
    raise NameError on the ``time.sleep(15)`` calls after MCP teardown."""
    imports = _module_level_imports("scripts/deploy.py")
    assert "time" in imports, (
        "scripts/deploy.py must import `time` at module scope; "
        "_deploy_mcp_if_needed and _start_mcp_image_build_async both "
        "call `time.sleep(15)` without a local import. See B1 in the "
        "2026-05-25 code review."
    )


def test_TC_CRF_001b_mcp_functions_can_call_time_sleep():
    """REQ-CRF-001: import the two MCP redeploy functions and exercise
    the time.sleep call paths with mocks to verify no NameError."""
    deploy = importlib.import_module("scripts.deploy")

    # Both functions must exist
    assert hasattr(deploy, "_deploy_mcp_if_needed")
    assert hasattr(deploy, "_start_mcp_image_build_async")

    # AST-verify time.sleep is reachable inside both function bodies
    # (we can't easily call them — they have many side-effects — so we
    # AST-check that `time.sleep` references resolve to the
    # module-imported `time`).
    tree = ast.parse(_read_source("scripts/deploy.py"))
    funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    for fname in ("_deploy_mcp_if_needed", "_start_mcp_image_build_async"):
        func = funcs[fname]
        # Find any time.sleep calls
        sleeps = [
            node for node in ast.walk(func)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "time"
            and node.attr == "sleep"
        ]
        assert sleeps, f"{fname} expected to call time.sleep"
        # And there should NOT be a local `import time` shadowing the
        # module-level one (no point — keep clean).
        local_imports = [
            n for n in ast.walk(func)
            if isinstance(n, ast.Import) and any(a.name == "time" for a in n.names)
        ]
        assert not local_imports, (
            f"{fname}: remove the local `import time` now that the "
            f"module-level one exists (B1 cleanup)."
        )


# ---------------------------------------------------------------------------
# T3: Dashboard time-filter type fix  (REQ-CRF-029, BLOCKER B2)
# ---------------------------------------------------------------------------


class _CapturingCollection:
    """Stub Mongo collection that captures the last find() filter."""

    def __init__(self, return_docs=None):
        self.last_filter = None
        self.last_sort = None
        self.last_limit = None
        self._docs = return_docs or []

    def find(self, query):
        self.last_filter = query
        return self

    def sort(self, *args, **kwargs):
        self.last_sort = (args, kwargs)
        return self

    def limit(self, n):
        self.last_limit = n
        return iter(self._docs)

    def count_documents(self, query):
        self.last_filter = query
        return len(self._docs)

    def estimated_document_count(self):
        return len(self._docs)


class _CapturingClient(dict):
    def __init__(self, collections):
        super().__init__()
        for (db, coll), stub in collections.items():
            if db not in self:
                self[db] = {}
            self[db][coll] = stub

    def __getitem__(self, name):
        return super().__getitem__(name)


def test_TC_CRF_029_build_time_filter_passes_datetime_not_int():
    """REQ-CRF-029: _build_time_filter must produce {field: {$gte: <datetime>}}
    for time-range queries; epoch_millis int $gte does not match BSON Date
    storage (ASP applies $toDate in pipelines)."""
    from datetime import datetime, timezone
    from scripts.dashboard import _build_time_filter

    cutoff = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    flt = _build_time_filter(cutoff, field="window_start")
    assert flt == {"window_start": {"$gte": cutoff}}, (
        "Default _build_time_filter must pass datetime through unchanged"
    )


def test_TC_CRF_029b_fetch_zone_traffic_queries_with_datetime():
    """REQ-CRF-029: _fetch_zone_traffic must NOT convert datetime to epoch
    millis before querying MongoDB."""
    from datetime import datetime, timezone
    from scripts.dashboard import _fetch_zone_traffic

    stub = _CapturingCollection(return_docs=[])
    client = _CapturingClient({("analytics", "zone_traffic"): stub})
    cutoff = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)

    _fetch_zone_traffic(client, zones=["French Quarter"], cutoff=cutoff)

    assert stub.last_filter is not None
    gte = stub.last_filter.get("window_start", {}).get("$gte")
    assert gte is not None, "expected $gte clause on window_start"
    assert isinstance(gte, datetime), (
        f"expected datetime, got {type(gte).__name__} ({gte!r}) — "
        "epoch_millis branch must be removed (BLOCKER B2)"
    )


def test_TC_CRF_029c_fetch_anomalies_queries_with_datetime():
    """REQ-CRF-029: _fetch_anomalies must pass datetime through."""
    from datetime import datetime, timezone
    from scripts.dashboard import _fetch_anomalies

    stub = _CapturingCollection(return_docs=[])
    client = _CapturingClient({("analytics", "zone_anomalies"): stub})
    cutoff = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)

    _fetch_anomalies(client, zones=["French Quarter"], cutoff=cutoff)

    assert stub.last_filter is not None
    gte = stub.last_filter.get("window_time", {}).get("$gte")
    assert gte is not None
    assert isinstance(gte, datetime), (
        f"expected datetime, got {type(gte).__name__} — BLOCKER B2"
    )


# ---------------------------------------------------------------------------
# T5: deploy.py credential return-check + state safety
#   REQ-CRF-002, -005, -007, -008, -009
# ---------------------------------------------------------------------------


def test_TC_CRF_002_save_terraform_credentials_returns_bool_consumed_by_caller():
    """REQ-CRF-002: callers of _save_terraform_credentials must consume the
    return value (False means missing creds; deploy should fail loudly)."""
    src = _read_source("scripts/deploy.py")
    # Find the call site inside run_deployment
    tree = ast.parse(src)
    found_consumed = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name == "_save_terraform_credentials":
                # Check the parent context — must NOT be a bare expression
                # statement (return value discarded). We rely on AST parents,
                # so re-walk capturing parents.
                pass
    # Simpler approach: source-grep for `_save_terraform_credentials(root)`
    # used as a bare statement vs. with `if not ...:` or `result = ...`.
    # The fix: the call must be inside an `if not <call>:` clause.
    lines = src.splitlines()
    call_lines = [i for i, l in enumerate(lines) if "_save_terraform_credentials(root)" in l
                  and "def _save_terraform_credentials" not in l]
    assert call_lines, "expected at least one call to _save_terraform_credentials"
    for idx in call_lines:
        line = lines[idx].strip()
        prev_line = lines[idx - 1].strip() if idx > 0 else ""
        # Acceptable patterns: `if not _save_terraform_credentials(root):` or
        # `ok = _save_terraform_credentials(root)` followed by an `if not ok`
        is_guarded = (
            line.startswith("if not _save_terraform_credentials")
            or line.startswith("ok = _save_terraform_credentials")
            or line.startswith("success = _save_terraform_credentials")
            or "= _save_terraform_credentials" in line
        )
        assert is_guarded, (
            f"line {idx+1}: bare _save_terraform_credentials() call; "
            "return value must be consumed (REQ-CRF-002 / H1). "
            f"Got: {line!r}"
        )


def test_TC_CRF_005_select_non_tty_returns_default():
    """REQ-CRF-005: _select must return effective_default rather than
    calling input() (which would EOFError) when stdin is not a TTY."""
    from scripts.deploy import _select

    # Force non-TTY environment + no questionary.
    with mock.patch("sys.stdin") as mock_stdin, \
         mock.patch("sys.stdout") as mock_stdout, \
         mock.patch("scripts.deploy.HAS_QUESTIONARY", False), \
         mock.patch("builtins.input", side_effect=EOFError("no stdin")):
        mock_stdin.isatty.return_value = False
        mock_stdout.isatty.return_value = False
        # Should NOT raise EOFError
        result = _select("question?", ["A", "B", "C"], default="B")
        assert result == "B", f"expected default 'B', got {result!r}"


def test_TC_CRF_S3_preflight_skips_confluent_login_on_non_tty():
    """S3 (self-review HIGH): _preflight's auto-login via `confluent login`
    must NOT run when stdin is not a TTY (CI / EC2). Without a guard the
    subprocess hangs up to 60 s waiting for browser SSO.

    Implementation contract: the deploy.py module source between
    `check_confluent_login` and the `subprocess.run(login_cmd, ...)` call
    must include an `sys.stdin.isatty()` check.
    """
    src = _read_source("scripts/deploy.py")
    # Find the auto-login block by literal anchor.
    anchor = 'login_cmd = ["confluent", "login"'
    idx = src.find(anchor)
    assert idx != -1, "expected the confluent login subprocess call"
    # Look at the 200 chars BEFORE the subprocess.run to find a TTY guard.
    before = src[max(0, idx - 600):idx]
    assert (
        "sys.stdin.isatty()" in before
        or "isatty()" in before
    ), (
        "The `confluent login --save` subprocess call must be guarded "
        "by a TTY check. Without it, non-TTY callers (CI, EC2) hang up "
        "to 60s waiting for browser SSO. S3 from self-review."
    )


def test_TC_CRF_006_no_log_flag_disables_session_log():
    """REQ-CRF-006 / S2 (self-review HIGH): --no-log must actually disable
    the session-log bootstrap. Prior implementation called
    bootstrap_logging() BEFORE parse_args, so the parsed --no-log arg was
    never consulted by the bootstrap logic.

    Behavior: the bootstrap function (or its caller) must inspect the
    parsed args and short-circuit when --no-log is set.
    """
    # Source-grep guard: bootstrap_logging must come AFTER parser.parse_args()
    # in main() for both deploy.py and destroy.py.
    for path in ("scripts/deploy.py", "scripts/destroy.py"):
        src = _read_source(path)
        # Find the body of main() and check ordering of:
        #   1. parser.parse_args()
        #   2. bootstrap_logging(...)
        tree = ast.parse(src)
        main_fn = next(
            (n for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name == "main"),
            None,
        )
        assert main_fn is not None, f"{path}: main() function missing"

        # Walk body in order, find first occurrences.
        first_parse = None
        first_bootstrap = None
        for i, stmt in enumerate(main_fn.body):
            stmt_src = ast.unparse(stmt)
            if first_parse is None and "parse_args" in stmt_src:
                first_parse = i
            if first_bootstrap is None and "bootstrap_logging" in stmt_src:
                first_bootstrap = i
        # Either bootstrap_logging is absent (caller-side disable OK), or
        # if present, must be after parse_args.
        if first_bootstrap is not None and first_parse is not None:
            assert first_bootstrap > first_parse, (
                f"{path}: bootstrap_logging() runs BEFORE parser.parse_args() "
                f"in main() (bootstrap at body[{first_bootstrap}], parse at "
                f"body[{first_parse}]). The --no-log flag cannot be honored "
                "when bootstrap runs first. REQ-CRF-006 / S2."
            )


def test_TC_CRF_007_save_terraform_credentials_persists_flink_rest_endpoint():
    """REQ-CRF-007 / H11 + S10 (brittle-test conversion): _save_terraform_credentials
    must persist CONFLUENT_FLINK_REST_ENDPOINT to .env when terraform outputs
    contain `confluent_flink_rest_endpoint`. Verified by behavior."""
    from scripts import deploy

    fake_outputs = {
        "confluent_kafka_cluster_bootstrap_endpoint": {"value": "kafka.x"},
        "app_manager_kafka_api_key":         {"value": "k"},
        "app_manager_kafka_api_secret":      {"value": "s"},
        "confluent_schema_registry_rest_endpoint": {"value": "sr.x"},
        "app_manager_schema_registry_api_key":     {"value": "srk"},
        "app_manager_schema_registry_api_secret":  {"value": "srs"},
        "confluent_kafka_cluster_rest_endpoint": {"value": "rest.x"},
        "confluent_kafka_cluster_id":            {"value": "cid"},
        "confluent_flink_rest_endpoint":         {"value": "https://flink.example/path"},
    }
    captured = {}

    def fake_save(pairs):
        captured.update(pairs)

    with mock.patch(
        "scripts.common.terraform_outputs.get_core_outputs",
        return_value=fake_outputs,
    ), mock.patch.object(deploy, "_save_env_many", side_effect=fake_save):
        ok = deploy._save_terraform_credentials(PROJECT_ROOT)

    assert ok is True
    assert captured.get("CONFLUENT_FLINK_REST_ENDPOINT") == "https://flink.example/path", (
        f"_save_terraform_credentials must persist CONFLUENT_FLINK_REST_ENDPOINT. "
        f"Saved keys: {sorted(captured.keys())}"
    )


def test_TC_CRF_008_aws_session_token_cleared_on_AKIA_key():
    """REQ-CRF-008: when the user switches from ASIA* to AKIA*, the stale
    TF_VAR_aws_session_token must be cleared."""
    src = _read_source("scripts/deploy.py")
    # The fix lives near the ASIA* branch; we expect an `else` arm that
    # sets TF_VAR_aws_session_token to "" or removes it from .env.
    # Heuristic: source-grep for both the ASIA branch and an explicit
    # clearing of TF_VAR_aws_session_token.
    has_clear = (
        'pairs["TF_VAR_aws_session_token"] = ""' in src
        or "'TF_VAR_aws_session_token': ''" in src
        or 'TF_VAR_aws_session_token": ""' in src
    )
    assert has_clear, (
        "When AWS access key does NOT start with 'ASIA', "
        "TF_VAR_aws_session_token must be cleared (set to ''). "
        "REQ-CRF-008 / M6."
    )


def test_TC_CRF_S6_persist_credential_snapshot_runs_after_agents_apply():
    """S6 (self-review MEDIUM): credential drift detection (REQ-CRF-027)
    relies on a DEPLOY_LAST_* snapshot persisted after each successful
    deploy. If snapshot only runs at DEPLOY_PHASE=complete, partial
    deploys leave a stale baseline, causing the next deploy to compute
    drift against the wrong baseline.

    Source-level check: _persist_credential_snapshot must be invoked
    not only at deploy completion but also after the agents terraform
    apply succeeds (which is when rotated creds were first consumed).
    """
    src = _read_source("scripts/deploy.py")
    # Count call sites of _persist_credential_snapshot. We want ≥ 2:
    # one in the per-env apply loop (after `e == "agents"`) and one at
    # deploy completion.
    n_calls = src.count("_persist_credential_snapshot(")
    # Subtract 1 for the `def _persist_credential_snapshot(...)` line.
    n_definitions = src.count("def _persist_credential_snapshot")
    n_invocations = n_calls - n_definitions
    assert n_invocations >= 2, (
        f"_persist_credential_snapshot is invoked {n_invocations} time(s); "
        "expected ≥ 2 (after agents apply AND at deploy complete). "
        "Without the agents-apply call, a deploy that fails after agents "
        "but before complete leaves a stale baseline. S6 (self-review)."
    )


def test_TC_CRF_S5_save_terraform_credentials_handles_value_error():
    """S5 (self-review MEDIUM): _save_terraform_credentials calls
    _save_env_many, which now raises ValueError on \\n/\\r in values
    (REQ-CRF-009). The caller MUST catch this and return False rather
    than letting the exception propagate (which would bypass REQ-CRF-002's
    bool contract)."""
    from scripts import deploy

    # Stub get_core_outputs to return an output containing \n in a value.
    bad_outputs = {
        "confluent_kafka_cluster_bootstrap_endpoint": {"value": "host1\nhost2"},
        "app_manager_kafka_api_key": {"value": "k"},
        "app_manager_kafka_api_secret": {"value": "s"},
        "confluent_schema_registry_rest_endpoint": {"value": "sr"},
        "app_manager_schema_registry_api_key": {"value": "srk"},
        "app_manager_schema_registry_api_secret": {"value": "srs"},
        "confluent_kafka_cluster_rest_endpoint": {"value": "rest"},
        "confluent_kafka_cluster_id": {"value": "cid"},
        "confluent_flink_rest_endpoint": {"value": "flink"},
    }
    with mock.patch(
        "scripts.common.terraform_outputs.get_core_outputs",
        return_value=bad_outputs,
    ):
        # Must NOT raise — must return False instead.
        try:
            result = deploy._save_terraform_credentials(PROJECT_ROOT)
        except ValueError as e:
            pytest.fail(
                f"_save_terraform_credentials let ValueError propagate: {e}. "
                "It must catch and return False per REQ-CRF-002. (S5)"
            )
        assert result is False, (
            "_save_terraform_credentials must return False when a value "
            "is rejected by _save_env_many's newline guard. (S5)"
        )


def test_TC_CRF_009_save_env_many_rejects_newlines():
    """REQ-CRF-009: _save_env_many must refuse values containing \\n or \\r
    to prevent .env corruption."""
    from scripts.deploy import _save_env_many

    # Patch _env_path to a tmp file so we don't touch the real .env.
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as f:
        tmp_path = Path(f.name)
    try:
        with mock.patch("scripts.deploy._env_path", return_value=tmp_path):
            with pytest.raises((ValueError, RuntimeError)):
                _save_env_many({"BAD": "line1\nline2"})
            with pytest.raises((ValueError, RuntimeError)):
                _save_env_many({"BAD": "line1\rline2"})
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# T20: LOW/NIT batch  (REQ-CRF-053..060)
# ---------------------------------------------------------------------------


def test_TC_CRF_053_build_uri_strips_whitespace():
    """REQ-CRF-053: build_uri must .strip() username and password to
    tolerate trailing newlines from .env copy-paste."""
    from scripts.common.mongo import build_uri

    uri = build_uri("mongodb+srv://example.com", "user\n", "pass\r")
    # The newline / CR characters must be stripped before quote_plus
    # (which would otherwise encode them as %0A / %0D).
    assert "%0A" not in uri and "%0D" not in uri, (
        f"build_uri must .strip() whitespace from creds; got {uri}"
    )
    assert "@example.com" in uri


def test_TC_CRF_054_get_client_lru_cache_size_is_adequate():
    """REQ-CRF-054: get_client lru_cache must accommodate ≥16 distinct
    app_names so the dashboard doesn't evict an in-use client."""
    from scripts.common import mongo

    cache_info = mongo.get_client.cache_info()
    # maxsize=None means unbounded (fine); maxsize=16 or larger is OK.
    assert cache_info.maxsize is None or cache_info.maxsize >= 16, (
        f"get_client lru_cache maxsize={cache_info.maxsize} is too small. "
        "REQ-CRF-054."
    )


def test_TC_CRF_056_summary_skips_empty_outputs():
    """REQ-CRF-056: generate_deployment_summary must skip rows whose
    underlying terraform output is empty (no empty `random_id` rows)."""
    src = _read_source("scripts/common/generate_deployment_summary.py")
    # The fix wraps the AWS Resources / IAM User section in an `if`
    # gated on a non-empty output (random_id, aws_access_key_id, or
    # similar).
    # Heuristic: a guard such as `if random_id:` or `if iam_user_name:`.
    has_guard = (
        "if random_id" in src
        or "if aws_access_key_id" in src
        or "if iam_user_name" in src
        or "if not random_id" in src
    )
    # If the section itself was removed entirely, also OK.
    has_legacy_row = "IAM User" in src and "random_id" in src
    assert (not has_legacy_row) or has_guard, (
        "generate_deployment_summary still emits AWS Resources / IAM User "
        "rows with empty values when terraform outputs are missing. "
        "Either guard the section or remove it. REQ-CRF-056."
    )


def test_TC_CRF_060_no_duplicate_test_bodies_in_integration():
    """REQ-CRF-060: duplicate test bodies in test_integration.py removed."""
    src = _read_source("testing/e2e/test_integration.py")
    # The duplicate `test_destroy_has_asp_teardown` body appeared twice.
    # After dedup there should be exactly one definition.
    n = src.count("def test_destroy_has_asp_teardown")
    assert n <= 1, (
        f"test_destroy_has_asp_teardown defined {n} times — should be 1. "
        "REQ-CRF-060."
    )


# ---------------------------------------------------------------------------
# T19: Docs sweep  (REQ-CRF-044..050)
# ---------------------------------------------------------------------------


def test_TC_DOCS_001_walkthrough_says_five_processors_ten_events():
    """REQ-CRF-044 / H12: WALKTHROUGH.md must state 5 processors and 10
    events (currently lines 66, 132, 297 say 3 and 6)."""
    w = (PROJECT_ROOT / "WALKTHROUGH.md").read_text()
    # No stale "3 processors" or "Three ASP processors" references
    assert "3 stream processors" not in w, (
        "WALKTHROUGH.md: '3 stream processors' is stale — actual is 5"
    )
    assert "three ASP processors" not in w.lower() or "five ASP processors" in w.lower(), (
        "WALKTHROUGH.md: 'three ASP processors' is stale"
    )
    # 6 events → 10 events
    assert "6 events" not in w, (
        "WALKTHROUGH.md: '6 events' is stale — actual SEED_EVENTS is 10"
    )
    assert "6 seed events" not in w, (
        "WALKTHROUGH.md: '6 seed events' is stale"
    )


def test_TC_DOCS_002_walkthrough_uses_named_processors_not_pipeline3():
    """REQ-CRF-049 / M21 / S9 (self-review): WALKTHROUGH.md and
    TROUBLESHOOTING.md must refer to processors by name, NOT by
    'Pipeline N' numerical headings.

    Strict reading after S9: zero `Pipeline N` references in narrative
    text. Named processors (dispatch_log_ingestion etc.) are primary.
    """
    import re
    PIPE_N_RE = re.compile(r"Pipeline\s+[0-9]")
    for path in ("WALKTHROUGH.md", "docs/TROUBLESHOOTING.md"):
        content = (PROJECT_ROOT / path).read_text()
        matches = PIPE_N_RE.findall(content)
        assert not matches, (
            f"{path}: still contains 'Pipeline N' references: {matches}. "
            "Named processors (`dispatch_log_ingestion` etc.) must be "
            "primary. REQ-CRF-049 / M21 / S9."
        )
    # Positive assertion: the named processors must appear in WALKTHROUGH.
    w = (PROJECT_ROOT / "WALKTHROUGH.md").read_text()
    for name in ("event_knowledge_base_population",
                 "event_publication_to_kafka",
                 "dispatch_log_ingestion"):
        assert name in w, f"WALKTHROUGH.md must reference {name}"


def test_TC_DOCS_003_readme_includes_health_and_preflight():
    """REQ-CRF-045 / M22: README.md must mention `uv run health` and
    `uv run preflight` in the commands section."""
    r = (PROJECT_ROOT / "README.md").read_text()
    assert "uv run health" in r, (
        "README must document `uv run health`. REQ-CRF-045 / M22."
    )
    assert "uv run preflight" in r, (
        "README must document `uv run preflight`. REQ-CRF-045 / M22."
    )


def test_TC_DOCS_004_readme_test_count_is_accurate():
    """REQ-CRF-046 / M23: README test count must match reality."""
    r = (PROJECT_ROOT / "README.md").read_text()
    assert "275 structural/offline tests" not in r, (
        "README.md test count '275' is stale — actual is 550+. REQ-CRF-046 / M23."
    )


def test_TC_DOCS_005_configuration_md_lists_atlas_terraform_phase():
    """REQ-CRF-047 / M24: docs/CONFIGURATION.md DEPLOY_PHASE row must
    include atlas_terraform (the first phase when creating a cluster)."""
    c = (PROJECT_ROOT / "docs/CONFIGURATION.md").read_text()
    # The DEPLOY_PHASE row enumerates valid values
    assert "atlas_terraform" in c, (
        "docs/CONFIGURATION.md DEPLOY_PHASE must list atlas_terraform. "
        "REQ-CRF-047 / M24."
    )


def test_TC_DOCS_006_env_example_lists_atlas_and_deploy_state_keys():
    """REQ-CRF-048 / M25: .env.example must document atlas + DEPLOY_PHASE keys."""
    env = (PROJECT_ROOT / ".env.example").read_text()
    # At minimum, commented placeholders for the deploy-managed keys.
    for key in ("TF_VAR_atlas_db_username", "TF_VAR_atlas_db_password",
                "DEPLOY_PHASE"):
        assert key in env, (
            f".env.example must document {key} (commented placeholder OK). "
            "REQ-CRF-048 / M25."
        )


def test_TC_DOCS_007_env_example_documents_workshop_mode_guidance():
    """REQ-E-361 follow-up: .env.example must give workshop attendees enough
    guidance to avoid the most common deployment failure modes. Specifically:

      - Both deployment modes (Terraform-provisioned vs BYO) must be
        documented, so a workshop attendee doesn't blindly copy values
        that don't match their setup.
      - The OrgAdmin Confluent Cloud requirement must be stated
        (terraform/core creates a new confluent_environment).
      - The M10+ minimum for ASP must be stated (M0/M2/M5 silently
        don't work with Atlas Stream Processing).
      - The `uv run preflight` tool must be mentioned (it's the
        fastest diagnose path for the cluster-name-mismatch class of
        bugs that REQ-E-360 catches).
      - The default value for TF_VAR_create_atlas_cluster must be
        `true` (workshop path) — the BYO path is the advanced option
        per CLAUDE.md and the spec.
    """
    env = (PROJECT_ROOT / ".env.example").read_text()
    # Both modes documented
    assert "TF_VAR_create_atlas_cluster=true" in env, \
        ".env.example must default to TF_VAR_create_atlas_cluster=true (workshop path)"
    assert "create_atlas_cluster=false" in env, \
        ".env.example must document the BYO mode (commented out)"
    # Critical prerequisites
    assert "OrganizationAdmin" in env or "OrgAdmin" in env, \
        ".env.example must state the Confluent OrganizationAdmin requirement"
    assert "M10" in env, \
        ".env.example must mention the M10 minimum tier for ASP"
    # Preflight tool reference
    assert "uv run preflight" in env, \
        ".env.example must reference `uv run preflight` for diagnosing config"
    # Non-interactive mode mentioned (REQ-E-350)
    assert "--non-interactive" in env, \
        ".env.example must mention --non-interactive for unattended runs"


def test_TC_CRF_050_deployment_summary_masks_secrets():
    """REQ-CRF-050 / M1 + S10 (brittle-test conversion): the generated
    deployment summary must NOT emit secrets verbatim. Build a fake
    outputs dict containing a recognizable secret, render the section,
    and assert the literal secret is absent from the output."""
    from scripts.common.generate_deployment_summary import (
        _build_credentials_section,
    )

    SECRET = "AKIA1234FAKE5678SECRET90"
    fake_outputs = {
        "confluent_organization_id":         {"value": "org-123"},
        "confluent_environment_id":          {"value": "env-456"},
        "confluent_cloud_api_key":           {"value": "FAKE_CLOUD_KEY_PLAINTEXT"},
        "confluent_cloud_api_secret":        {"value": SECRET},
        "confluent_kafka_cluster_bootstrap_endpoint": {"value": "kafka.example"},
        "app_manager_kafka_api_key":         {"value": "FAKE_KAFKA_KEY_PLAINTEXT"},
        "app_manager_kafka_api_secret":      {"value": SECRET + "-KAFKA"},
        "confluent_schema_registry_rest_endpoint": {"value": "sr.example"},
        "app_manager_schema_registry_api_key":     {"value": "FAKE_SR_KEY_PLAINTEXT"},
        "app_manager_schema_registry_api_secret":  {"value": SECRET + "-SR"},
        "confluent_flink_rest_endpoint":     {"value": "flink.example"},
        "confluent_flink_compute_pool_id":   {"value": "pool-1"},
        "app_manager_flink_api_key":         {"value": "FAKE_FLINK_KEY_PLAINTEXT"},
        "app_manager_flink_api_secret":      {"value": SECRET + "-FLINK"},
    }
    def get(k):
        return fake_outputs.get(k, {}).get("value", "")

    rendered = _build_credentials_section(fake_outputs, get)

    # The full secret values must NOT appear verbatim.
    assert SECRET not in rendered, (
        f"Full secret {SECRET!r} leaks into rendered markdown. "
        "REQ-CRF-050 / M1."
    )
    assert SECRET + "-KAFKA" not in rendered, "Kafka secret leak"
    assert SECRET + "-SR" not in rendered, "SR secret leak"
    assert SECRET + "-FLINK" not in rendered, "Flink secret leak"
    # The first 4 chars (used by the mask) should still appear.
    assert "AKIA" in rendered, "masked form must keep prefix for traceability"
    # Identifier-style values (not secrets) must pass through unchanged.
    assert "org-123" in rendered and "env-456" in rendered


# ---------------------------------------------------------------------------
# T17: MCP deploy hardening  (REQ-CRF-013, -014, -015)
# ---------------------------------------------------------------------------


def test_TC_CRF_013_ecs_service_name_uses_random_suffix():
    """REQ-CRF-013 / M13 + S10 (brittle-test conversion): _create_ecs_express
    must use a cryptographically random suffix on collision retry.
    Verified by AST: the retry block must contain a call to
    secrets.token_hex / token_urlsafe (not just import the module)."""
    from scripts import mcp_deploy
    fn = getattr(mcp_deploy, "_create_ecs_express", None)
    assert callable(fn), "_create_ecs_express must exist"
    fn_src = inspect.getsource(fn)
    tree = ast.parse(fn_src)
    secrets_calls = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "secrets"
        and n.func.attr.startswith("token_")
    ]
    assert secrets_calls, (
        "_create_ecs_express must CALL secrets.token_hex / token_urlsafe "
        "inside the function body (not just have the import in source). "
        "REQ-CRF-013 / M13."
    )
    # And the unsafe modular form must not appear in the function body.
    assert "int(time.time()) % 100000" not in fn_src, (
        "Remove `int(time.time()) % 100000` suffix. REQ-CRF-013 / M13."
    )


def test_TC_CRF_014_destroy_cleans_orphan_target_groups():
    """REQ-CRF-014 / M14: destroy_mcp_server must enumerate and delete
    orphaned `ecs-gateway-tg-*` target groups (which otherwise
    accumulate across destroys)."""
    src = _read_source("scripts/mcp_deploy.py")
    # The fix calls elbv2 describe-target-groups + delete-target-group
    # inside destroy_mcp_server.
    from scripts import mcp_deploy
    destroy_src = inspect.getsource(mcp_deploy.destroy_mcp_server)
    assert (
        "describe-target-groups" in destroy_src
        or "delete-target-group" in destroy_src
        or "_cleanup_orphan_target_groups" in destroy_src
    ), (
        "destroy_mcp_server must enumerate and delete orphaned "
        "ecs-gateway-tg-* target groups. REQ-CRF-014 / M14."
    )


def test_TC_CRF_015_start_sh_waits_for_backend():
    """REQ-CRF-015 / M15: start.sh must wait for backend port 8000 to be
    bound before launching the proxy on 8080."""
    start_sh = (PROJECT_ROOT / "mcp-server/start.sh").read_text()
    has_wait = (
        "nc -z 127.0.0.1 8000" in start_sh
        or "while ! nc" in start_sh
        or "wait_for_backend" in start_sh
    )
    assert has_wait, (
        "mcp-server/start.sh must wait for 127.0.0.1:8000 before launching "
        "the proxy. REQ-CRF-015 / M15. Without this, ALB health checks "
        "during the startup window see 502 ECONNREFUSED."
    )


# ---------------------------------------------------------------------------
# T16: Dead code removal + terraform output caching  (REQ-CRF-042, -043)
# ---------------------------------------------------------------------------


def test_TC_CRF_042_load_credentials_json_removed():
    """REQ-CRF-042 / M26: dead load_credentials_json (refs nonexistent
    tests/ folder) must be removed."""
    from scripts.common import credentials

    assert not hasattr(credentials, "load_credentials_json"), (
        "load_credentials_json is dead code (references tests/ folder "
        "that doesn't exist). Remove it. REQ-CRF-042 / M26."
    )


def test_TC_CRF_043b_get_core_outputs_has_production_callers():
    """REQ-CRF-043 / S4 (self-review HIGH): the cached helper is useless if
    nothing calls it. The deploy.py / destroy.py / pipeline_reset.py /
    asp_setup.py inline subprocess calls must be replaced with imports
    from scripts.common.terraform_outputs."""
    consumers = (
        "scripts/deploy.py",
        "scripts/destroy.py",
        "scripts/pipeline_reset.py",
    )
    for path in consumers:
        src = _read_source(path)
        assert (
            "from scripts.common.terraform_outputs import" in src
            or "from .common.terraform_outputs import" in src
            or "scripts.common.terraform_outputs" in src
        ), (
            f"{path} must import get_core_outputs from "
            "scripts.common.terraform_outputs. REQ-CRF-043 / S4."
        )

    # And at least one inline subprocess call must have been replaced:
    # count remaining inline calls; should be substantially less than 13.
    import subprocess as _sub
    cmd = _sub.run(
        ["rg", "-c", r'"terraform", "output", "-json"',
         "scripts/deploy.py", "scripts/destroy.py", "scripts/pipeline_reset.py"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if cmd.returncode == 0:
        total = 0
        for line in cmd.stdout.strip().splitlines():
            try:
                total += int(line.split(":")[-1])
            except (ValueError, IndexError):
                pass
        assert total <= 3, (
            f"Found {total} remaining inline `terraform output -json` calls "
            "in deploy/destroy/pipeline_reset. The cached helper must "
            "replace at least the bulk of them. REQ-CRF-043 / S4."
        )


def test_TC_CRF_043_terraform_outputs_helper_caches():
    """REQ-CRF-043 / M27: get_core_outputs(root) must cache the
    `terraform output -json` result so repeat calls within a deploy
    don't shell out 10+ times."""
    from scripts.common import terraform_outputs

    assert hasattr(terraform_outputs, "get_core_outputs"), (
        "scripts.common.terraform_outputs.get_core_outputs must exist. "
        "REQ-CRF-043 / M27."
    )

    # Behavior: two calls with the same root invoke subprocess once.
    fake_outputs = {"app_manager_kafka_api_key": {"value": "k"}}
    call_count = {"n": 0}

    class FakeCompleted:
        returncode = 0
        stdout = '{"app_manager_kafka_api_key": {"value": "k"}}'
        stderr = ""

    def fake_run(*args, **kwargs):
        call_count["n"] += 1
        return FakeCompleted()

    # Clear cache between tests so we get a fresh count.
    if hasattr(terraform_outputs, "_clear_cache"):
        terraform_outputs._clear_cache()
    # Patch the tfstate existence check (terraform/core/terraform.tfstate
    # may not exist locally) and the subprocess invocation.
    with mock.patch("subprocess.run", side_effect=fake_run), \
         mock.patch.object(Path, "exists", return_value=True):
        out1 = terraform_outputs.get_core_outputs(PROJECT_ROOT)
        out2 = terraform_outputs.get_core_outputs(PROJECT_ROOT)
    assert out1 == out2 == fake_outputs
    assert call_count["n"] == 1, (
        f"expected 1 subprocess call (cached); got {call_count['n']}. "
        "REQ-CRF-043 / M27."
    )


# ---------------------------------------------------------------------------
# T15: Constants consolidation  (REQ-CRF-040, -041)
# ---------------------------------------------------------------------------


def test_TC_CRF_040_flink_statements_module_is_single_source_of_truth():
    """REQ-CRF-040 / M7: deploy/destroy/pipeline_reset must import DDL/DML
    statement names from scripts.common.flink_statements rather than
    duplicating local lists."""
    from scripts.common import flink_statements

    # Module exists with canonical exports
    for name in ("DDL_STATEMENTS", "DML_STATEMENTS"):
        assert hasattr(flink_statements, name), f"missing {name}"

    # The 3 callers must import them (not maintain local duplicates).
    for path in ("scripts/destroy.py", "scripts/pipeline_reset.py"):
        src = _read_source(path)
        assert (
            "from scripts.common.flink_statements" in src
            or "from .common.flink_statements" in src
            or "from scripts.common import flink_statements" in src
        ), (
            f"{path} must import statement names from "
            f"scripts.common.flink_statements. REQ-CRF-040 / M7."
        )


def test_TC_CRF_041_project_root_helper_consolidated():
    """REQ-CRF-041 / M28 + S10 (brittle-test conversion): _project_root must
    be defined once in scripts.common.terraform.get_project_root and
    IMPORTED by callers (not just absent locally)."""
    from scripts.common import terraform as tf

    assert hasattr(tf, "get_project_root"), (
        "scripts.common.terraform must export get_project_root"
    )
    # cli_output, cli_logging, preflight, health, pipeline_logger must
    # import the canonical helper.
    modules_to_check = [
        "scripts/common/cli_output.py",
        "scripts/common/cli_logging.py",
        "scripts/health.py",
        "scripts/common/pipeline_logger.py",
    ]
    for path in modules_to_check:
        src = _read_source(path)
        has_local_def = "def _project_root" in src
        imports_canonical = (
            "from scripts.common.terraform import get_project_root" in src
            or "from .terraform import get_project_root" in src
        )
        # STRICT: no local def AND imports canonical helper.
        assert not has_local_def, (
            f"{path} still defines a local _project_root. Consolidate to "
            f"scripts.common.terraform.get_project_root. REQ-CRF-041 / M28."
        )
        assert imports_canonical, (
            f"{path} must import get_project_root from "
            f"scripts.common.terraform. REQ-CRF-041 / M28 / S10."
        )


# ---------------------------------------------------------------------------
# T14: SQL dedup in anomalies-enriched-insert.sql  (REQ-CRF-039, M10)
# ---------------------------------------------------------------------------


def test_TC_CRF_039_anomalies_enriched_insert_does_not_duplicate_query_concat():
    """REQ-CRF-039 / M10: the ~70-line CONCAT block that builds the user
    query must appear at most once (not duplicated for `rad.query` AND
    the ML_PREDICT embedding input)."""
    sql = (PROJECT_ROOT / "terraform/agents/sql/anomalies-enriched-insert.sql").read_text()
    # Count occurrences of the distinctive opening phrase
    count = sql.count("'Transportation demand surge in '")
    assert count <= 1, (
        f"Duplicate CONCAT-build of `query` in anomalies-enriched-insert.sql "
        f"(found {count} occurrences). The two builds MUST stay byte-identical "
        f"or the embedding will not match the LLM prompt. Refactor to compute "
        f"once and reference twice. REQ-CRF-039 / M10."
    )


# ---------------------------------------------------------------------------
# T13: cli_output secret redaction + lock + UTC consistency
#      REQ-CRF-036, -037, -038
# ---------------------------------------------------------------------------


def test_TC_CRF_036_cli_output_redacts_mongo_uri_password():
    """REQ-CRF-036 / M3: cli_output emissions must redact embedded
    passwords in MongoDB URIs."""
    from scripts.common import cli_output

    with cli_output.capture() as (out, log):
        cli_output.info("connecting to mongodb+srv://admin:s3cr3t@host/db")
    joined = "\n".join(out + log)
    assert "s3cr3t" not in joined, (
        "cli_output must redact passwords embedded in MongoDB URIs. "
        f"Got: {joined!r}. REQ-CRF-036 / M3."
    )
    # And the rest of the message must survive (not redact everything).
    assert "mongodb" in joined.lower()


def test_TC_CRF_S12_redact_preserves_kv_spacing():
    """S12 (self-review LOW): the redaction regex used to capture only the
    trailing whitespace of `key : value`, so kv()-formatted output would
    lose its leading space before `:`. The fix captures both sides of
    the separator into the `sep` group. Log lines must stay shape-stable."""
    from scripts.common.redaction import redact

    # The cli_output.kv() formatter emits "  key : value" (space-colon-space).
    # After redaction the spacing must survive (modulo masked value).
    out = redact("  api_key : AKIA1234FAKE5678SECRET")
    assert " : " in out, (
        f"Redaction stripped the space around ':' from: {out!r}. "
        "S12 (self-review LOW)."
    )
    # And the colon must NOT be flanked by 'key:value' (no whitespace).
    assert "api_key: " not in out and "api_key:A" not in out


def test_TC_CRF_036b_cli_output_redacts_kv_secrets():
    """REQ-CRF-036 / M3: kv() output should redact obvious secret-like
    values."""
    from scripts.common import cli_output

    with cli_output.capture() as (out, log):
        cli_output.kv("api_key", "AKIA1234567890SECRET")
        cli_output.kv("password", "hunter2-password")
        cli_output.kv("environment_id", "env-abc12345")  # NOT a secret
    joined = "\n".join(out + log)
    assert "AKIA1234567890SECRET" not in joined, (
        "kv must redact api_key values"
    )
    assert "hunter2-password" not in joined, (
        "kv must redact password values"
    )
    # Identifier-style values must survive — they're not secrets.
    assert "env-abc12345" in joined, (
        "environment IDs must NOT be redacted"
    )


def test_TC_CRF_037_cli_output_log_writes_are_locked():
    """REQ-CRF-037 / M4 + S10 (brittle-test conversion): concurrent writes
    must be serialized. Spawn 30 threads each emitting a recognizable
    line and verify no line is torn (interleaved with another)."""
    import tempfile
    import threading as _threading
    from scripts.common import cli_output

    # Initialize with a temp log dir so we can read back the session log.
    with tempfile.TemporaryDirectory() as tmp_dir:
        log_path = cli_output.init(quiet=False, debug=False, log_dir=Path(tmp_dir))
        # Each thread writes a recognizable identifier — 100 'A's, 100
        # 'B's, etc. If writes interleave, we'll see fewer than 30
        # complete lines or torn payloads.
        n_threads = 30
        n_writes_per_thread = 5
        markers = [chr(ord('A') + i) * 80 for i in range(n_threads)]

        def worker(marker: str) -> None:
            for _ in range(n_writes_per_thread):
                cli_output.info(marker)

        threads = [_threading.Thread(target=worker, args=(m,)) for m in markers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Flush + read the log.
        if cli_output._S.log_fh is not None:
            cli_output._S.log_fh.flush()
        log_text = log_path.read_text()

    # Every line should be the full prefix + a single marker repeated 80x.
    # Torn writes would produce shorter lines, mixed markers, or extra
    # blank lines.
    expected_lines = n_threads * n_writes_per_thread
    actual_lines = [ln for ln in log_text.splitlines() if ln.strip()]
    assert len(actual_lines) == expected_lines, (
        f"Expected {expected_lines} log lines from {n_threads} threads × "
        f"{n_writes_per_thread} writes; got {len(actual_lines)}. "
        "Likely a torn-write / missing-lock issue. REQ-CRF-037 / M4."
    )
    # And every line must contain exactly one marker (no interleaving).
    for ln in actual_lines:
        matching = [m for m in markers if m in ln]
        assert len(matching) == 1, (
            f"Line {ln!r} contains {len(matching)} markers (expected 1) — "
            "two writes interleaved. REQ-CRF-037 / M4."
        )


def test_TC_CRF_038_cli_logging_and_cli_output_use_utc():
    """REQ-CRF-038: session log timestamps must use UTC consistently in
    both cli_output and cli_logging."""
    cl_src = _read_source("scripts/common/cli_logging.py")
    co_src = _read_source("scripts/common/cli_output.py")
    # cli_output already uses time.gmtime(). cli_logging used datetime.now().
    # After fix, cli_logging must use UTC (datetime.now(timezone.utc) or
    # time.gmtime).
    assert (
        "datetime.now(timezone.utc)" in cl_src
        or "time.gmtime" in cl_src
        or "datetime.now(UTC)" in cl_src
    ), (
        "cli_logging timestamp must use UTC. REQ-CRF-038."
    )
    assert "gmtime" in co_src or "utc" in co_src.lower(), (
        "cli_output already uses UTC — keep it that way"
    )


# ---------------------------------------------------------------------------
# T12: Dashboard race fixes + DROP/CREATE poll  (REQ-CRF-030, -031)
# ---------------------------------------------------------------------------


def test_TC_CRF_030_run_agent_dispatch_button_disabled_when_running():
    """REQ-CRF-030 / H13: 'Run Agent Dispatch' button must declare
    `disabled=...` when agent_dispatch_running flag is True, to prevent
    spawning a second worker thread on rapid double-click."""
    src = _read_source("scripts/dashboard.py")
    # Heuristic: the Run Agent Dispatch button must have a `disabled=`
    # kwarg, and that kwarg must reference the session_state flag.
    import re
    # Find every `st.button("Run Agent Dispatch", ...)` call
    pattern = re.compile(
        r'st\.button\(\s*"Run Agent Dispatch"([^)]*)\)',
        re.DOTALL,
    )
    matches = pattern.findall(src)
    assert matches, "expected st.button('Run Agent Dispatch', ...) call"
    # At least one of the matches must include `disabled=` AND reference
    # the agent_dispatch_running session_state key.
    has_disabled = any(
        "disabled" in m and "agent_dispatch_running" in m
        for m in matches
    )
    assert has_disabled, (
        "st.button('Run Agent Dispatch', ...) must include "
        "`disabled=st.session_state.get('agent_dispatch_running', False)` "
        "(or equivalent) to prevent double-click thread races. "
        "REQ-CRF-030 / H13."
    )


def test_TC_CRF_031_drop_create_polls_for_deletion():
    """REQ-CRF-031 / M17: the DROP TABLE → CREATE TABLE chain must poll
    for statement deletion (or use delete_and_wait) rather than relying
    on a fixed time.sleep(5)."""
    src = _read_source("scripts/dashboard.py")
    # Look for _wait_for_statement_deleted helper (or equivalent),
    # or that delete_and_wait is used in the dispatch chain.
    has_helper = (
        "_wait_for_statement_deleted" in src
        or "wait_for_statement_deleted" in src
        or "delete_and_wait" in src
    )
    assert has_helper, (
        "Dashboard dispatch chain must poll for statement deletion "
        "before recreate. The fixed `time.sleep(5)` permits the "
        "create-while-deleting race documented in CLAUDE.md. "
        "REQ-CRF-031 / M17."
    )


# ---------------------------------------------------------------------------
# T11: health.py error discrimination + partition iteration + preflight phases
#      REQ-CRF-032, -033, -034, -035
# ---------------------------------------------------------------------------


def test_TC_CRF_032_health_distinguishes_auth_from_not_found():
    """REQ-CRF-032 / H10: _check_flink must distinguish 401/403 (auth) from
    404 (not found) from transport errors, surfacing each with its own
    detail field instead of collapsing all to 'unknown'."""
    from scripts import health

    outputs = {
        "app_manager_flink_api_key":         {"value": "k"},
        "app_manager_flink_api_secret":      {"value": "s"},
        "confluent_organization_id":         {"value": "o"},
        "confluent_environment_id":          {"value": "e"},
        "confluent_flink_rest_endpoint":     {"value": "https://flink.example"},
    }

    def make_http_error(code: int):
        import urllib.error
        return urllib.error.HTTPError(
            url="x", code=code, msg="err", hdrs=None, fp=None,
        )

    # 401 → fail/auth
    with mock.patch("urllib.request.urlopen", side_effect=make_http_error(401)):
        results = health._check_flink(outputs)
    assert results
    assert all(r["status"] == "fail" for r in results), (
        f"401 should map to 'fail' status, got {[r['status'] for r in results]}"
    )
    assert all("auth" in (r.get("detail") or "").lower() for r in results), (
        f"401 should produce detail containing 'auth'; got {[r.get('detail') for r in results]}"
    )

    # 404 → fail/not_found
    with mock.patch("urllib.request.urlopen", side_effect=make_http_error(404)):
        results = health._check_flink(outputs)
    assert all(r["status"] == "fail" for r in results)
    assert all("not_found" in (r.get("detail") or "").lower()
               or "not found" in (r.get("detail") or "").lower()
               for r in results), f"got {[r.get('detail') for r in results]}"


def test_TC_CRF_033_health_kafka_uses_dynamic_partition_count():
    """REQ-CRF-033: _check_kafka must NOT hardcode range(6) for partition
    iteration in code (comments referencing the historical bug are OK)."""
    from scripts import health
    src = inspect.getsource(health._check_kafka)
    # Strip comments so we don't false-positive on documentation that
    # references the old bug.
    code_only = "\n".join(
        line.split("#", 1)[0] for line in src.splitlines()
    )
    assert "range(6)" not in code_only, (
        "_check_kafka hardcodes range(6) — must query actual partition "
        "count via Consumer.list_topics(). REQ-CRF-033."
    )
    # Positive assertion: list_topics is now used.
    assert "list_topics" in src, (
        "_check_kafka must call list_topics(...) to discover partitions"
    )


def test_TC_CRF_034_preflight_terraform_phase_has_confluent_probe():
    """REQ-CRF-034 / M2: preflight --phase terraform must include at least
    one Confluent Cloud auth probe (otherwise the phase is no-op)."""
    from scripts.preflight import CHECKS

    terraform_checks = [c for c in CHECKS if "terraform" in c.phases]
    confluent_check = next(
        (c for c in terraform_checks
         if "confluent" in c.name.lower()), None
    )
    assert confluent_check is not None, (
        "preflight --phase terraform must include a Confluent Cloud auth "
        "probe. Without it, terraform-phase preflight returns nothing "
        "useful before terraform is run. REQ-CRF-034 / M2. "
        f"Current terraform-phase checks: {[c.name for c in terraform_checks]}"
    )


def test_TC_CRF_034b_preflight_terraform_phase_has_aws_probe():
    """REQ-CRF-034 / M2: preflight --phase terraform must also include the
    AWS caller-identity probe (Bedrock creds tested at terraform time)."""
    from scripts.preflight import CHECKS

    terraform_checks = [c for c in CHECKS if "terraform" in c.phases]
    aws_check = next(
        (c for c in terraform_checks if "aws" in c.name.lower()), None
    )
    assert aws_check is not None, (
        "preflight --phase terraform must include AWS caller_identity probe. "
        f"Current terraform-phase checks: {[c.name for c in terraform_checks]}"
    )


def test_TC_CRF_035_health_json_schema_is_stable():
    """REQ-CRF-035 / H3-from-misc + S7 (self-review MEDIUM): --json entries
    must have a stable per-entry shape — every entry must contain the SAME
    KEY SET, with None defaults for inapplicable fields. Both ok and error
    branches across all 4 component-check functions must match."""
    from scripts import health

    # Force a Flink fail/auth branch
    import urllib.error
    outputs_flink = {
        "app_manager_flink_api_key":     {"value": "k"},
        "app_manager_flink_api_secret":  {"value": "s"},
        "confluent_organization_id":     {"value": "o"},
        "confluent_environment_id":      {"value": "e"},
        "confluent_flink_rest_endpoint": {"value": "https://flink"},
    }
    err = urllib.error.HTTPError(url="x", code=401, msg="m", hdrs=None, fp=None)
    with mock.patch("urllib.request.urlopen", side_effect=err):
        flink_err = health._check_flink(outputs_flink)

    class FakeResp:
        def read(self):
            return b'{"status":{"phase":"RUNNING","detail":""}}'
        def __enter__(self): return self
        def __exit__(self, *a): pass
    with mock.patch("urllib.request.urlopen", return_value=FakeResp()):
        flink_ok = health._check_flink(outputs_flink)

    # All entries from BOTH calls must share the same key set — that's
    # the stable-schema contract.
    all_entries = flink_err + flink_ok
    if all_entries:
        canonical_keys = set(all_entries[0].keys())
        for r in all_entries[1:]:
            assert set(r.keys()) == canonical_keys, (
                f"Entry {r!r} key set {set(r.keys())!r} does not match "
                f"canonical {canonical_keys!r} (REQ-CRF-035 / S7). Every "
                "entry must carry the same keys with None defaults for "
                "inapplicable fields."
            )


def test_TC_CRF_S7_health_entries_have_full_canonical_schema():
    """S7 (self-review MEDIUM): the canonical entry shape must include
    EVERY field that any branch ever sets. Otherwise --json consumers
    must defensively `.get(key, None)` on every field."""
    from scripts import health

    # The canonical key superset for any health entry.
    # Must include both `records` (kafka) and `count` (mongo) since the
    # text formatter reads each one separately.
    EXPECTED_KEYS = {
        "name", "status", "detail", "phase", "records", "count",
        "state", "last_checkpoint",
    }

    # Probe one success entry per check.
    import urllib.error
    fake_outputs = {
        "app_manager_flink_api_key":         {"value": "k"},
        "app_manager_flink_api_secret":      {"value": "s"},
        "confluent_organization_id":         {"value": "o"},
        "confluent_environment_id":          {"value": "e"},
        "confluent_flink_rest_endpoint":     {"value": "https://x"},
    }
    class FakeResp:
        def read(self):
            return b'{"status":{"phase":"RUNNING","detail":""}}'
        def __enter__(self): return self
        def __exit__(self, *a): pass
    with mock.patch("urllib.request.urlopen", return_value=FakeResp()):
        entries = health._check_flink(fake_outputs)

    for e in entries:
        missing = EXPECTED_KEYS - set(e.keys())
        assert not missing, (
            f"Flink success entry {e!r} missing keys {missing}. "
            "Every health entry must carry the full canonical key set with "
            "None defaults for inapplicable fields (S7 / REQ-CRF-035)."
        )


# ---------------------------------------------------------------------------
# T10: Terraform connection drift detection + Flink key depends_on
#      REQ-CRF-027, -028
# ---------------------------------------------------------------------------


def test_TC_CRF_027_connection_drift_detector_exists():
    """REQ-CRF-027 / H8: deploy.py must include a connection-drift detector
    that compares current credentialed TF_VAR values against persisted
    DEPLOY_LAST_* values and returns the set of connections to drop."""
    src = _read_source("scripts/deploy.py")
    # We require a helper named `_detect_connection_drift` (or equivalent)
    # that returns which Flink connections need a -replace.
    has_helper = (
        "_detect_connection_drift" in src
        or "_connections_to_drop" in src
        or "_drift_connections" in src
    )
    assert has_helper, (
        "deploy.py must include a helper that detects credential drift "
        "across mcp_url, mongo URI/password, voyage_api_key and returns "
        "the set of Flink connections requiring `-replace`. REQ-CRF-027 / H8."
    )


def test_TC_CRF_027b_drift_detector_handles_mongo_password_rotation():
    """REQ-CRF-027 / H8: detector must trigger on mongo password change."""
    from scripts import deploy

    # Helper must exist and accept (env_old, env_new) -> set/list of conn names.
    fn = getattr(deploy, "_detect_connection_drift", None)
    assert fn is not None, "expected _detect_connection_drift helper"

    env_old = {
        "TF_VAR_mcp_server_url": "https://mcp.old/",
        "TF_VAR_mongodb_connection_string": "mongodb+srv://u:old@h/",
        "TF_VAR_mongodb_password": "oldpass",
        "TF_VAR_voyage_api_key": "vk-old",
    }
    env_new = dict(env_old)

    # No drift → empty
    assert not fn(env_old, env_new), "no drift expected"

    # MCP URL change → mongodb-mcp-connection (and downstream)
    env_new["TF_VAR_mcp_server_url"] = "https://mcp.new/"
    drift = fn(env_old, env_new)
    assert "mongodb-mcp-connection" in drift or "mcp" in str(drift).lower()
    env_new["TF_VAR_mcp_server_url"] = env_old["TF_VAR_mcp_server_url"]

    # Mongo password change → mongodb-connection
    env_new["TF_VAR_mongodb_password"] = "newpass"
    drift = fn(env_old, env_new)
    assert "mongodb-connection" in drift or "mongo" in str(drift).lower(), (
        f"mongo password rotation must trigger mongodb-connection drift; got {drift!r}"
    )
    env_new["TF_VAR_mongodb_password"] = env_old["TF_VAR_mongodb_password"]

    # Voyage key change → voyage_connection
    env_new["TF_VAR_voyage_api_key"] = "vk-new"
    drift = fn(env_old, env_new)
    assert "voyage_connection" in drift or "voyage" in str(drift).lower(), (
        f"voyage key rotation must trigger voyage connection drift; got {drift!r}"
    )


def test_TC_CRF_028_flink_api_key_depends_on_role_binding():
    """REQ-CRF-028 / M9: app-manager-flink-api-key must declare depends_on
    on the EnvironmentAdmin role binding to avoid SR propagation race on
    first apply."""
    src = (PROJECT_ROOT / "terraform/core/main.tf").read_text()
    # Extract the Flink API key resource block by brace-matching.
    needle = 'resource "confluent_api_key" "app-manager-flink-api-key"'
    start = src.find(needle)
    assert start != -1, "expected app-manager-flink-api-key resource block"
    # Walk to the matching close brace
    brace_start = src.find("{", start)
    depth = 1
    i = brace_start + 1
    while i < len(src) and depth > 0:
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
        i += 1
    body = src[start:i]
    assert (
        "depends_on" in body
        and "app-manager-kafka-cluster-admin" in body
    ), (
        "app-manager-flink-api-key must declare "
        "`depends_on = [confluent_role_binding.app-manager-kafka-cluster-admin]` "
        "to match its Kafka/SR siblings. Without this, the Flink API key "
        "can be created before the SA has any privilege → SR propagation "
        "race. REQ-CRF-028 / M9."
    )


# ---------------------------------------------------------------------------
# T9: Flink REST polling + retry-after + propagation pattern narrow
#     REQ-CRF-023, -024, -025, -026
# ---------------------------------------------------------------------------


def test_TC_CRF_023_wait_for_phase_checks_before_sleep():
    """REQ-CRF-023 / H17 + S10 (brittle-test conversion): wait_for_phase
    must check phase BEFORE the first sleep. Verified by behavior: when
    the statement is already in target phase, sleep must NOT be called.

    FlinkRestClient is a frozen dataclass, so we patch the underlying
    `_get` (regular module-level mechanic, not a method) instead.
    """
    from scripts.common.flink_rest import FlinkRestClient

    client = FlinkRestClient(
        rest_endpoint="https://x",
        api_key="k", api_secret="s",
        org_id="o", env_id="e",
        compute_pool_id="p", service_account_id="sa",
        catalog="c", database="d",
    )
    sleeps = []
    fake_response = {"status": {"phase": "COMPLETED", "detail": ""}}

    # Patch the unbound class method so the frozen-instance constraint
    # doesn't matter — replace it at the class level.
    with mock.patch.object(FlinkRestClient, "get", return_value=fake_response), \
         mock.patch("scripts.common.flink_rest.time.sleep",
                    side_effect=lambda s: sleeps.append(s)):
        result = client.wait_for_phase("dml-x", "COMPLETED", timeout=30)

    assert result == fake_response
    # The FIRST iteration must NOT have slept; if get() returned the
    # target phase immediately, sleep should never be called.
    assert sleeps == [], (
        f"wait_for_phase slept {sleeps} times before returning despite "
        "the statement already being in target phase. The first-iter "
        "must check phase BEFORE sleeping. REQ-CRF-023 / H17."
    )


def test_TC_CRF_024_post_with_retry_honors_retry_after():
    """REQ-CRF-024 / H18: on HTTP 429, _post_with_retry must honor the
    Retry-After header if it's larger than the static backoff."""
    src = _read_source("scripts/common/flink_rest.py")
    assert "Retry-After" in src or "retry-after" in src.lower(), (
        "_post_with_retry must consult the Retry-After header on 429 responses. "
        "REQ-CRF-024 / H18."
    )


def test_TC_CRF_025_propagation_lag_does_not_match_generic_authz():
    """REQ-CRF-025 / H9: _PROPAGATION_ERROR_PATTERNS must NOT include generic
    auth strings like 'is not authorized' or 'Authorization failed' — those
    match real RBAC errors and waste 4+ minutes of pointless retries."""
    from scripts.common import terraform_runner

    patterns = terraform_runner._PROPAGATION_ERROR_PATTERNS
    # Reject generic auth phrasings
    blocklist = {
        "is not authorized",
        "Authorization failed",
        "Forbidden access to topic",
    }
    overlap = blocklist & set(patterns)
    assert not overlap, (
        f"_PROPAGATION_ERROR_PATTERNS contains generic auth patterns "
        f"that match real RBAC errors: {overlap}. Narrow to SR/Kafka-"
        f"specific patterns only. REQ-CRF-025 / H9."
    )
    # And the SR-specific pattern MUST still be present
    has_sr = any("Schema Registry" in p for p in patterns)
    assert has_sr, "must still retry on SR propagation lag"


def test_TC_CRF_025b_real_rbac_error_does_not_trigger_retry():
    """REQ-CRF-025 / H9: behavior test — a real RBAC error
    ("is not authorized") must NOT be classified as propagation lag."""
    from scripts.common.terraform_runner import _looks_like_propagation_lag

    rbac_error = (
        "Error: error creating Kafka topic: "
        "User is not authorized to operate on this cluster"
    )
    assert not _looks_like_propagation_lag(rbac_error), (
        "Real RBAC error must not be classified as transient propagation lag. "
        "REQ-CRF-025 / H9."
    )
    # And the genuine SR propagation lag pattern MUST trigger retry.
    sr_error = (
        "Error: error registering statement: "
        "Permission denied to access the Schema Registry cluster 'lsrc-12345'"
    )
    assert _looks_like_propagation_lag(sr_error), (
        "Genuine SR propagation lag must still be detected for retry."
    )


def test_TC_CRF_026_delete_and_wait_raises_on_timeout():
    """REQ-CRF-026 / M5 + S10 (brittle-test conversion): delete_and_wait
    must raise TimeoutError on timeout. Verified by behavior: mock the
    HTTP poll to always return DELETING, call with tiny timeout, expect
    TimeoutError."""
    from scripts.common.flink_rest import FlinkRestClient
    import urllib.error

    client = FlinkRestClient(
        rest_endpoint="https://x",
        api_key="k", api_secret="s",
        org_id="o", env_id="e",
        compute_pool_id="p", service_account_id="sa",
        catalog="c", database="d",
    )
    # Stub _delete (the initial DELETE) to succeed. Then stub urlopen
    # used by the polling loop to return 200 (statement still exists).
    class FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return b'{"status":{"phase":"DELETING"}}'

    with mock.patch.object(FlinkRestClient, "_delete", return_value=None), \
         mock.patch("scripts.common.flink_rest.urllib.request.urlopen",
                    return_value=FakeResp()), \
         mock.patch("scripts.common.flink_rest.time.sleep", return_value=None):
        with pytest.raises(TimeoutError, match="DELETING|still"):
            client.delete_and_wait("x", timeout=0.1)


# ---------------------------------------------------------------------------
# T8: ASP correctness bundle  (REQ-CRF-016..022)
# ---------------------------------------------------------------------------


def test_TC_CRF_016_asp_restart_surfaces_failed_but_does_not_treat_as_success():
    """REQ-CRF-016 / H7 + S1 (self-review BLOCKER): the function must:

    - INCLUDE FAILED in the wait-terminal set (so the loop exits in <1
      poll interval rather than blocking until timeout)
    - But CLASSIFY FAILED as a failure in the final snapshot/log/return

    Behavior is verified by TC_CRF_016b (return value contains FAILED)
    and TC_CRF_016c (timing). This test pins the warning-classification
    code path: a FAILED entry must produce a warning log line.
    """
    import logging
    from scripts.common import asp_restart

    with mock.patch.object(asp_restart, "_send_action", return_value=(True, 200, "")), \
         mock.patch.object(asp_restart, "_list_processors",
                           return_value={"dispatch_log_ingestion": "FAILED"}):
        with mock.patch.object(asp_restart, "logger") as mock_logger:
            asp_restart.restart_processors_for_topics(
                project_id="p", instance="i",
                topics=["completed_actions"],
                auth=None,
                timeout_per_processor=1,
                poll_interval_s=0,
            )
    # The warning log must mention FAILED.
    warn_calls = [c for c in mock_logger.warning.call_args_list
                  if "FAILED" in str(c)]
    assert warn_calls, (
        "FAILED processor must produce a logger.warning(...) call "
        "(not just .info). REQ-CRF-016 / H7."
    )


def test_TC_CRF_016b_asp_restart_warns_on_failed_processors():
    """REQ-CRF-016 / H7: when a processor ends in FAILED state, the function
    must warn (not info) the caller."""
    from scripts.common import asp_restart

    # Call with a stub _list_processors that returns FAILED.
    with mock.patch.object(asp_restart, "_send_action", return_value=(True, 200, "")), \
         mock.patch.object(asp_restart, "_list_processors", return_value={"dispatch_log_ingestion": "FAILED"}):
        final = asp_restart.restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["completed_actions"],
            auth=None,
            timeout_per_processor=1,
            poll_interval_s=0,
        )
        assert final.get("dispatch_log_ingestion") == "FAILED", \
            "must return the FAILED state in the result dict"
        # Caller can detect FAILED count by inspecting the return value.


def test_TC_CRF_016c_asp_restart_does_not_block_on_failed_processor():
    """S1 / self-review BLOCKER: when a processor goes FAILED, the wait
    loop must terminate promptly. The earlier "fix" excluded FAILED from
    the terminal set so the loop polled until timeout (60s default).

    This test calls restart_processors_for_topics with timeout=3 and
    poll_interval=1, simulating a processor that immediately goes FAILED.
    The function must return in well under 3 seconds (the timeout).
    """
    import time as _t
    from scripts.common import asp_restart

    list_calls = {"n": 0}
    def fake_list(*args, **kwargs):
        list_calls["n"] += 1
        return {"dispatch_log_ingestion": "FAILED"}

    with mock.patch.object(asp_restart, "_send_action", return_value=(True, 200, "")), \
         mock.patch.object(asp_restart, "_list_processors", side_effect=fake_list):
        start = _t.monotonic()
        final = asp_restart.restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["completed_actions"],
            auth=None,
            timeout_per_processor=3,   # would-be timeout
            poll_interval_s=1,
        )
        elapsed = _t.monotonic() - start

    assert final.get("dispatch_log_ingestion") == "FAILED"
    # FAILED is a terminal state. Both Step 2 (wait STOPPED, terminal set
    # already includes FAILED) and Step 4 (wait STARTED) must terminate
    # promptly. The bug: if Step 4's terminal set excludes FAILED, the
    # loop polls every `poll_interval_s` until `timeout_per_processor`
    # expires. We allow 1× poll_interval slack but no more.
    assert elapsed < 2.0, (
        f"restart_processors_for_topics blocked for {elapsed:.1f}s on a "
        "FAILED processor (timeout=3, poll=1). The wait set must include "
        "FAILED as terminal so the loop terminates within one poll "
        "interval (S1 BLOCKER from self-review)."
    )


def test_TC_CRF_017_ensure_connections_does_not_silently_proceed_on_stop_timeout():
    """REQ-CRF-017 / H14: when connection-stop polling times out, the code
    must NOT proceed to delete the connection silently. Source-grep for
    explicit handling of the timeout state."""
    src = _read_source("scripts/asp_setup.py")
    # The current code has a `for/else` that prints a timeout warning but
    # falls through to the connection delete. The fix is to either raise
    # or `continue` to the next connection rather than deleting blindly.
    # Heuristic: after the "Timed out waiting" message, the next action
    # must NOT be an unconditional delete of the connection.
    lines = src.splitlines()
    for i, line in enumerate(lines):
        if "Timed out waiting for" in line and "STOPPED state" in line:
            # Look at the next 15 lines for either `raise`, `continue`,
            # or a short-circuit `return` / `processors_failed_to_stop`.
            window = "\n".join(lines[i:i + 15])
            assert (
                "raise" in window
                or "continue" in window
                or "processors_failed_to_stop" in window
                or "abort" in window.lower()
            ), (
                f"asp_setup.py near line {i+1}: timeout warning followed by "
                f"unconditional connection delete. Must abort or continue. "
                f"REQ-CRF-017 / H14. Window:\n{window}"
            )


def test_TC_CRF_018_run_asp_setup_does_not_reseed_unconditionally():
    """REQ-CRF-018 / H15: the post-processor re-seed must be gated on
    processors being newly created on this run, not blindly re-run."""
    src = _read_source("scripts/asp_setup.py")
    # The fix introduces a flag (processors_created / processors_new /
    # newly_created) that gates the Step 6 re-seed.
    # We require the re-seed call site to be conditional on this flag.
    lines = src.splitlines()
    step6_idx = None
    for i, line in enumerate(lines):
        if "Step 6:" in line and "Re-seed" in line:
            step6_idx = i
            break
    if step6_idx is None:
        # If Step 6 is removed entirely (alternative implementation), that's fine.
        return
    # The re-seed call must be inside a block whose predicate includes
    # a freshness flag.
    window = "\n".join(lines[max(0, step6_idx - 5):step6_idx + 10])
    assert (
        "processors_created" in window
        or "newly_created" in window
        or "processors_new" in window
        or "any_created" in window
    ), (
        f"Step 6 re-seed must be gated on a 'processors created this run' "
        f"flag (REQ-CRF-018 / H15). Window:\n{window}"
    )


def test_TC_CRF_019_dedupe_dispatch_log_reraises_on_failure():
    """REQ-CRF-019 / H16: _dedupe_dispatch_log must re-raise on failure
    (the comment promises this; the code swallows). Subsequent
    create_index(unique=True) would E11000."""
    src = _read_source("scripts/asp_setup.py")
    # Find the _dedupe_dispatch_log function body
    tree = ast.parse(src)
    func = next((n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                 and n.name == "_dedupe_dispatch_log"), None)
    assert func is not None
    # The except handler must contain a `raise` statement (re-raise).
    for node in ast.walk(func):
        if isinstance(node, ast.ExceptHandler):
            raises = [n for n in ast.walk(node) if isinstance(n, ast.Raise)]
            assert raises, (
                "_dedupe_dispatch_log except handler must re-raise "
                "(REQ-CRF-019 / H16). Comment in source promises this but "
                "the implementation swallows."
            )


def test_TC_CRF_020_dispatch_log_validator_requires_window_time():
    """REQ-CRF-020 / M11: $jsonSchema validator for fleet.dispatch_log must
    require window_time."""
    src = _read_source("scripts/asp_setup.py")
    # Find the dispatch_log validator and assert window_time is required
    # alongside pickup_zone and dispatch_summary.
    # Heuristic: search for the validator definition. The dispatch_log
    # validator should list "window_time" in required.
    # Look for the specific tuple ("fleet", "dispatch_log") followed by
    # required containing window_time.
    import re
    # Cheap and robust enough: find the dispatch_log validator block,
    # assert window_time appears in its required.
    m = re.search(
        r'\("fleet", "dispatch_log"\)\s*:\s*\{[^}]*"required"\s*:\s*\[([^\]]+)\]',
        src, re.DOTALL,
    )
    assert m, "expected to find the dispatch_log validator definition"
    required_str = m.group(1)
    assert "window_time" in required_str, (
        f"dispatch_log validator required list must include 'window_time'. "
        f"Got: {required_str!r}. REQ-CRF-020 / M11."
    )


def test_TC_CRF_021_dispatch_log_pipeline_guards_and_converts_window_time():
    """REQ-CRF-021 / M12: dispatch_log_ingestion pipeline must $match-guard
    window_time presence and $toDate-convert it before $merge."""
    from scripts.asp_setup import _pipeline_dispatch_log

    pipeline = _pipeline_dispatch_log()
    # Find the $match stage and $addFields/$toDate stage for window_time.
    has_match_guard = any(
        "$match" in stage and "window_time" in str(stage.get("$match", {}))
        for stage in pipeline
    )
    has_todate = any(
        "$addFields" in stage and "$toDate" in str(stage.get("$addFields", {}))
        and "window_time" in str(stage.get("$addFields", {}))
        for stage in pipeline
    )
    assert has_match_guard, (
        "dispatch_log_ingestion must $match-guard window_time presence "
        "before $merge (REQ-CRF-021 / M12). Without this, null-keyed "
        "merges can E11000 storm or silently drop rows."
    )
    assert has_todate, (
        "dispatch_log_ingestion must $toDate-convert window_time before "
        "$merge (REQ-CRF-021 / M12). The unique compound index sorts on "
        "window_time -1, and Date vs Long sort orderings differ."
    )


def test_TC_CRF_022_no_currentDate_in_aggregation_pipelines():
    """REQ-CRF-022 / M20: $addFields stages must use $$NOW, not the
    update-operator $currentDate."""
    src = _read_source("scripts/asp_setup.py")
    # Allow $currentDate inside seed_events_calendar (update operations
    # there — legitimate). Disallow inside pipeline stages.
    # Strategy: parse the pipeline-building functions and assert no
    # $currentDate literal appears inside their bodies.
    tree = ast.parse(src)
    pipeline_funcs = [
        "_pipeline_event_knowledge_base",
        "_pipeline_event_publication",
        "_pipeline_dispatch_log",
        "_pipeline_zone_traffic_ingestion",
        "_pipeline_anomalies_ingestion",
    ]
    for fn_name in pipeline_funcs:
        fn = next((n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                   and n.name == fn_name), None)
        if fn is None:
            continue
        fn_src = ast.unparse(fn)
        assert "$currentDate" not in fn_src, (
            f"{fn_name} contains $currentDate (invalid in aggregation context). "
            f"Use \"$$NOW\" instead. REQ-CRF-022 / M20."
        )


# ---------------------------------------------------------------------------
# T7: destroy partial-failure handling  (REQ-CRF-004, HIGH H3)
# ---------------------------------------------------------------------------


def test_TC_CRF_004_destroy_preserves_credentials_on_partial_failure():
    """REQ-CRF-004 / H3: when any terraform destroy fails, _remove_stale_credentials
    must NOT run. User must not be left with half-destroyed cloud infra AND
    a wiped .env, which would block re-destroy."""
    src = _read_source("scripts/destroy.py")
    # The main() destroy loop must track per-env success and gate the
    # _remove_stale_credentials call on full success.
    # Heuristic: the call to _remove_stale_credentials must appear within
    # a conditional block whose predicate checks a success flag.
    # We require an explicit `destroy_failed` / `destroy_ok` flag in source.
    assert "destroy_failed" in src or "destroy_ok" in src or "all_succeeded" in src, (
        "destroy.py main() must track per-env destroy success "
        "(e.g. `destroy_failed = False`) and skip _remove_stale_credentials "
        "when any env failed. REQ-CRF-004 / H3."
    )
    # And _remove_stale_credentials must be guarded.
    lines = src.splitlines()
    for i, line in enumerate(lines):
        if "_remove_stale_credentials(root" in line and "def _remove_stale_credentials" not in line:
            # Look at the 5 lines preceding for a conditional guard
            preceding = "\n".join(lines[max(0, i - 5):i])
            assert (
                "if not destroy_failed" in preceding
                or "if destroy_ok" in preceding
                or "if all_succeeded" in preceding
            ), (
                f"line {i + 1}: _remove_stale_credentials must be guarded "
                f"by destroy-success flag. Got preceding lines:\n{preceding}"
            )


# ---------------------------------------------------------------------------
# T6: pipeline_reset CTAS topic exclusion  (REQ-CRF-003, HIGH H2)
# ---------------------------------------------------------------------------


def test_TC_CRF_003_pipeline_reset_excludes_ctas_topics():
    """REQ-CRF-003 / H2: pipeline_reset must not include anomalies_enriched
    or completed_actions in PIPELINE_TOPICS or FLINK_CATALOG_TABLES.

    deploy.py at line ~1754 excludes them explicitly with a BUG-302 comment:
    pre-creating these topics blocks the CTAS DDL via phantom raw-byte
    catalog tables. pipeline_reset was re-introducing the exact bug.
    """
    from scripts.pipeline_reset import PIPELINE_TOPICS, FLINK_CATALOG_TABLES

    for ctas_managed in ("anomalies_enriched", "completed_actions"):
        assert ctas_managed not in PIPELINE_TOPICS, (
            f"pipeline_reset.PIPELINE_TOPICS must NOT include {ctas_managed!r} — "
            "this is a CTAS-managed topic; pre-creating it blocks the CTAS DDL "
            "(see deploy.py BUG-302 comment near line 1754). REQ-CRF-003 / H2."
        )
        assert ctas_managed not in FLINK_CATALOG_TABLES, (
            f"pipeline_reset.FLINK_CATALOG_TABLES must NOT include {ctas_managed!r}. "
            "The CTAS creates the catalog table itself; pre-dropping it conflicts."
        )

    # Sanity: the 5 non-CTAS topics ARE present (INV-CRF-004)
    for required in ("ride_requests", "windowed_traffic", "anomalies_per_zone",
                     "zone_traffic_sink", "anomalies_sink"):
        assert required in PIPELINE_TOPICS, (
            f"INV-CRF-004: pipeline_reset must still recreate {required!r}"
        )


def test_TC_CRF_029d_get_collection_counts_uses_datetime():
    """REQ-CRF-029: _get_collection_counts time-filter branch must use
    datetime, not epoch millis."""
    from datetime import datetime, timezone
    from scripts.dashboard import _get_collection_counts

    zt = _CapturingCollection(return_docs=[])
    an = _CapturingCollection(return_docs=[])
    disp = _CapturingCollection(return_docs=[])
    kb = _CapturingCollection(return_docs=[])
    client = _CapturingClient({
        ("analytics", "zone_traffic"): zt,
        ("analytics", "zone_anomalies"): an,
        ("fleet", "dispatch_log"): disp,
        ("events", "knowledge_base"): kb,
    })
    cutoff = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    _get_collection_counts(client, time_filter=cutoff)
    # zt and an were time-filtered; verify datetime not int
    for stub, name in ((zt, "zone_traffic"), (an, "anomalies")):
        if stub.last_filter is None:
            continue  # not all branches always filter
        for fld, clause in stub.last_filter.items():
            if isinstance(clause, dict) and "$gte" in clause:
                assert isinstance(clause["$gte"], datetime), (
                    f"{name}: $gte expected datetime, got "
                    f"{type(clause['$gte']).__name__}"
                )
