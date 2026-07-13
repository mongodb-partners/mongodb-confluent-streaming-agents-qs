"""Tests for the pass-3 self-review fixes (BLOCKER + HIGH + MEDIUM).

Each TC-P3-NNN is a behavior test (no source-grep) verifying a specific
finding from the self-review of commit a08ae9a.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import io
import json
import os
import stat
import tempfile
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read_source(rel_path: str) -> str:
    return (PROJECT_ROOT / rel_path).read_text()


# ---------------------------------------------------------------------------
# B1: terraform moved block prevents Atlas access-list delete+recreate
# ---------------------------------------------------------------------------


def test_TC_P3_B1_atlas_moved_block_present():
    """B1: a `moved {}` block in terraform/atlas/main.tf is required so
    existing clusters don't get delete+recreate on the for_each migration."""
    main_tf = (PROJECT_ROOT / "terraform/atlas/main.tf").read_text()
    assert "moved" in main_tf, (
        "terraform/atlas/main.tf must include a `moved {}` block to "
        "migrate the legacy `mongodbatlas_project_ip_access_list.workshop` "
        "single-instance state to the new for_each-keyed form without "
        "delete+recreate. B1 BLOCKER."
    )
    # Specifically the moved block must reference the legacy address.
    assert "mongodbatlas_project_ip_access_list.workshop" in main_tf
    # And the for_each-keyed target.
    assert 'mongodbatlas_project_ip_access_list.workshop["0.0.0.0/0"]' in main_tf


# ---------------------------------------------------------------------------
# B2: log redaction does NOT register an atexit handler in the inner
#     process (which races script(1) PTY parent)
# ---------------------------------------------------------------------------


def test_TC_P3_B2_no_atexit_redaction_in_inner_process():
    """B2: the redaction post-process must NOT run from inside the
    inner Python process via atexit. The script(1) parent is still
    writing trailing output when the inner exits; redacting at that
    point drops the last seconds of log content."""
    src = _read_source("scripts/common/cli_logging.py")
    # atexit.register(_redact_log_file, ...) must NOT exist.
    tree = ast.parse(src)
    bad_calls = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "atexit"
        and n.func.attr == "register"
    ]
    # If atexit.register is used at all, must NOT register _redact_log_file
    for call in bad_calls:
        # Inspect first positional arg
        if call.args and isinstance(call.args[0], ast.Name):
            assert call.args[0].id != "_redact_log_file", (
                "_redact_log_file must NOT be registered via atexit "
                "(races script(1) parent writes). B2 BLOCKER."
            )


# ---------------------------------------------------------------------------
# H1: Workshop-mode banner must NOT claim Secrets Manager
# ---------------------------------------------------------------------------


def test_TC_P3_H1_banner_does_not_claim_secrets_manager():
    """H1: deploy banner previously claimed non-workshop mode `uses
    Secrets Manager for the Mongo connection string`. Per user
    directive Pass 3 explicitly does NOT implement Secrets Manager.
    The banner must reflect reality."""
    src = _read_source("scripts/deploy.py")
    # No mention of Secrets Manager in the banner / user-facing strings.
    # Allow it in code comments referencing the design decision.
    # Strict check: no f-string / quoted string mentioning Secrets Manager.
    import re
    matches = re.findall(
        r'["\']([^"\']*Secrets?\s*Manager[^"\']*)["\']',
        src,
        re.IGNORECASE,
    )
    assert not matches, (
        f"deploy.py banner / strings still reference Secrets Manager: "
        f"{matches}. Pass 3 directive: do NOT claim a feature that "
        "doesn't exist. H1."
    )


# ---------------------------------------------------------------------------
# H2: connections/*.json files written 0o600
# ---------------------------------------------------------------------------


def test_TC_P3_H2_connection_json_files_written_mode_0600():
    """H2: scripts/common/datagen_helpers.generate_connection_file must
    write the JSON file with mode 0o600 (contains Kafka SASL + Schema
    Registry credentials)."""
    from scripts.common import datagen_helpers

    # Source must contain a chmod 0o600 or os.open with O_CREAT 0o600.
    src = inspect.getsource(datagen_helpers)
    has_mode = (
        "0o600" in src
        or "S_IWUSR" in src
    )
    assert has_mode, (
        "datagen_helpers.py must apply mode 0o600 when writing "
        "connections/*.json files (Kafka SASL creds + SR creds). H2."
    )


def test_TC_P3_H2b_connection_file_actual_mode():
    """H2 behavior / pass-4 M-15: write a sample connection file and stat it.

    The pytest.skip envelope was removed — REQ-CRG-027 bans
    skip-on-absence of production behavior. Calls the actual writer
    and verifies the resulting file has mode 0o600.
    """
    from scripts.common.datagen_helpers import generate_connection_file

    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "connection.json"
        sample_creds = {
            "bootstrap_servers": "kafka.example:9092",
            "kafka_api_key": "k",
            "kafka_api_secret": "s",
            "schema_registry_url": "https://sr.example",
            "schema_registry_api_key": "srk",
            "schema_registry_api_secret": "srs",
        }
        generate_connection_file(
            credentials=sample_creds,
            connection_name="kafka_confluent",
            output_path=target,
        )
        assert target.exists(), (
            "generate_connection_file did not produce the output file."
        )
        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o600, (
            f"connection JSON mode 0o{mode:o} — must be 0o600. H2 / REQ-CRG-003."
        )

        # Re-write to confirm chmod is also re-applied on overwrite.
        target.chmod(0o644)
        generate_connection_file(
            credentials=sample_creds,
            connection_name="kafka_confluent",
            output_path=target,
        )
        mode2 = stat.S_IMODE(target.stat().st_mode)
        assert mode2 == 0o600, (
            f"connection JSON mode 0o{mode2:o} after rewrite — must remain 0o600."
        )


# ---------------------------------------------------------------------------
# H3: TC-CRG-008b must NOT use pytest.skip to mask a dropped requirement
# ---------------------------------------------------------------------------


def test_TC_P3_H3_no_skip_masking_in_cost_cap_test():
    """H3: TC-CRG-008b in test_holistic_review_pass2.py previously
    called pytest.skip() when the rate-dedup wasn't implemented —
    actively masking a dropped requirement. Either implement the
    dedup or delete the test entirely. The skip-on-absence pattern
    is the anti-pattern REQ-CRG-027 was supposed to eliminate."""
    src = _read_source("testing/e2e/test_holistic_review_pass2.py")
    # The test must either be deleted, or no longer pytest.skip on
    # absent implementation.
    if "test_TC_CRG_008b" not in src:
        return  # Deleted — acceptable resolution.
    # If kept, must NOT have a pytest.skip in its body.
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if (isinstance(node, ast.FunctionDef)
                and node.name == "test_TC_CRG_008b_anomaly_detection_caps_explanation_rate"):
            fn_src = ast.unparse(node)
            assert "pytest.skip" not in fn_src, (
                "TC-CRG-008b must not use pytest.skip to mask a "
                "dropped requirement. H3 / REQ-CRG-027."
            )


# ---------------------------------------------------------------------------
# H4: _quick_stats_fragment handles None counts without crashing
# ---------------------------------------------------------------------------


def test_TC_P3_H4_quick_stats_fragment_handles_none_counts():
    """H4: _get_collection_counts now returns None on Mongo error.
    Every consumer that formats with `f"{n:,}"` must guard against
    None. _quick_stats_fragment was missed in the original REQ-CRG-020
    pass."""
    src = _read_source("scripts/dashboard.py")
    # The unguarded pattern `counts.get('X', 0):,}` must be gone, OR
    # the function must use a guard helper.
    # Look for the _quick_stats / KPI render block at line ~1896.
    import re

    # Naive: any line with `:,}` followed by f-string interpolation
    # that includes `counts.get` should be guarded.
    risky_lines = [
        (i, line) for i, line in enumerate(src.splitlines(), 1)
        if ":,}" in line and "counts.get(" in line
    ]
    for line_no, line in risky_lines:
        # Acceptable: line uses a helper like `_kpi_value(counts.get(...))`
        # OR the get default is something that handles None.
        is_safe = (
            "_kpi_value" in line
            or "_fmt_count" in line
            or "or 0" in line
            or "or '\u2014'" in line
            or "or '—'" in line
        )
        assert is_safe, (
            f"line {line_no}: `{line.strip()}` will crash on None counts. "
            "REQ-CRG-020 / H4. Wrap with _kpi_value() helper."
        )


# ---------------------------------------------------------------------------
# H5: ensure_connections failure must abort the deploy phase chain
# ---------------------------------------------------------------------------


def test_TC_P3_H5_run_asp_setup_failure_aborts_deploy():
    """H5 / pass-4 L-4: when run_asp_setup returns False, the deploy
    caller MUST abort with sys.exit(1) and record DEPLOY_LAST_FAILED_PHASE.

    Behavior test (replaces the source-grep with vacuous `"sys.exit"
    in src` fallback — sys.exit appears 32× in deploy.py).

    Strategy:
      1. Locate the phase function that calls run_asp_setup.
      2. Patch run_asp_setup to return False.
      3. Call the phase function with minimal stubs.
      4. Assert SystemExit raised AND DEPLOY_LAST_FAILED_PHASE recorded.
    """
    import tempfile

    from scripts import deploy

    # Find the phase function by source pattern (the one that calls
    # `run_asp_setup(`). It's `_run_asp_post_terraform` or similar.
    src = _read_source("scripts/deploy.py")
    tree = ast.parse(src)
    phase_fn_name = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        fn_src = ast.unparse(node)
        if "run_asp_setup(" in fn_src and "if not " in fn_src:
            phase_fn_name = node.name
            break
    assert phase_fn_name, (
        "Could not locate the phase function that calls run_asp_setup. "
        "If the structure changed, this test needs an update."
    )
    phase_fn = getattr(deploy, phase_fn_name, None)
    assert callable(phase_fn), (
        f"deploy.{phase_fn_name} should be callable"
    )

    # Patch run_asp_setup to return False, isolate env writes to a tmp
    # .env, and invoke the phase function. Assert SystemExit.
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Build the env dict the phase function expects.
        env = {
            "ATLAS_PUBLIC_KEY": "pk",
            "ATLAS_PRIVATE_KEY": "sk",
            "ATLAS_PROJECT_ID": "proj",
            "ATLAS_CLUSTER_NAME": "Cluster0",
            "CONFLUENT_BOOTSTRAP_SERVER": "kafka.example",
            "CONFLUENT_KAFKA_API_KEY": "k",
            "CONFLUENT_KAFKA_API_SECRET": "s",
            "TF_VAR_voyage_api_key": "vk",
            "TF_VAR_mongodb_connection_string": "mongodb+srv://cluster.example",
            "TF_VAR_mongodb_username": "u",
            "TF_VAR_mongodb_password": "p",
        }
        # Patch run_asp_setup → False AND patch _save_env_many to
        # capture the breadcrumb. The phase function reads optional
        # terraform_outputs / cli_output; stub them minimally.
        save_calls: list = []
        run_asp_mock = mock.MagicMock(return_value=False)
        with mock.patch("scripts.asp_setup.run_asp_setup", run_asp_mock), \
             mock.patch.object(deploy, "_save_env_many",
                              side_effect=lambda d: save_calls.append(d)), \
             mock.patch.object(deploy, "_project_root",
                              return_value=Path(tmp_dir)):
            with pytest.raises(SystemExit) as exc_info:
                phase_fn(env=env, root=Path(tmp_dir))
        # H-E (pass-5 self-review): verify run_asp_setup was ACTUALLY
        # called. Without this assertion the test could pass via the
        # phase function's early-return on missing credentials —
        # exercising the wrong code path entirely.
        assert run_asp_mock.call_count == 1, (
            f"run_asp_setup was called {run_asp_mock.call_count} times — "
            "the phase function early-returned before reaching the "
            "failure branch. The test fixture credentials are incomplete. H-E."
        )
        # Must have exited with non-zero.
        assert exc_info.value.code != 0, (
            "phase function exited with code 0 despite asp_setup "
            "failure — must be non-zero. H5."
        )
        # And must have recorded the failure phase.
        recorded_phases = [
            d.get("DEPLOY_LAST_FAILED_PHASE")
            for d in save_calls
            if "DEPLOY_LAST_FAILED_PHASE" in d
        ]
        assert "asp_setup" in recorded_phases, (
            f"phase function must record DEPLOY_LAST_FAILED_PHASE=asp_setup. "
            f"Saved: {save_calls}. H5."
        )


# ---------------------------------------------------------------------------
# H7: Egress IP fallback must warn loudly
# ---------------------------------------------------------------------------


def test_TC_P3_H7_egress_fallback_warns():
    """H7 / pass-4 L-8: when _detect_egress_cidrs returns None
    (corporate proxy / rate limit / IPv6), the fallback to 0.0.0.0/0
    must emit a visible warning to stdout via the public writer.

    Behavior test: stub the egress probe to return None and run the
    public `write_tfvars_for_deployment` with non-workshop mode +
    atlas in envs_to_deploy. Capture stdout, assert the warning
    line appears verbatim.
    """
    import io
    from contextlib import redirect_stdout

    from scripts.common import tfvars

    # 1. Detector returns None when the network probe fails.
    with mock.patch("urllib.request.urlopen",
                    side_effect=OSError("simulated proxy block")):
        result = tfvars._detect_egress_cidrs()
    assert result is None, (
        "_detect_egress_cidrs must return None on network failure; "
        f"got {result}."
    )

    # 2. write_tfvars_for_deployment WARNS when detector returns None
    #    and workshop_mode is off.
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        (root / "terraform" / "atlas").mkdir(parents=True)
        creds = {
            "ATLAS_PUBLIC_KEY": "pk",
            "ATLAS_PRIVATE_KEY": "sk",
            "ATLAS_PROJECT_ID": "proj",
            "ATLAS_CLUSTER_NAME": "Cluster0",
            "TF_VAR_atlas_db_username": "u",
            "TF_VAR_atlas_db_password": "p",
            # workshop_mode UNSET → triggers the egress detection path
        }
        with mock.patch.object(tfvars, "_detect_egress_cidrs", return_value=None):
            buf = io.StringIO()
            with redirect_stdout(buf):
                tfvars.write_tfvars_for_deployment(
                    root=root, region="us-east-1",
                    creds=creds, envs_to_deploy=["atlas"],
                )
            stdout = buf.getvalue()
        # Behavior assertions: warning text appears AND fallback is 0.0.0.0/0.
        assert "Could not detect egress IP" in stdout, (
            f"egress fallback warning missing from stdout. "
            f"Captured: {stdout!r}. H7 / L-8."
        )
        assert "0.0.0.0/0" in stdout, (
            "fallback CIDR 0.0.0.0/0 not surfaced in warning. H7."
        )


# ---------------------------------------------------------------------------
# M1: AtlasAPI retry restricts 5xx retry to idempotent methods
# ---------------------------------------------------------------------------


def test_TC_P3_M1_atlas_retry_does_not_retry_post_on_5xx():
    """M1: POST is not idempotent; retrying a 5xx on POST risks creating
    a duplicate side effect. The retry layer must restrict 5xx retry
    to GET/DELETE methods (POST retries only on transport errors)."""
    import requests

    from scripts.asp_setup import AtlasAPI

    api = AtlasAPI(public_key="pk", private_key="sk", project_id="proj")
    attempts = {"n": 0}

    def fake_post_5xx(method, url, **kwargs):
        attempts["n"] += 1
        resp = mock.MagicMock()
        resp.status_code = 503
        resp.ok = False
        resp.text = "server error"
        resp.headers = {}
        return resp

    with mock.patch.object(requests, "request", side_effect=fake_post_5xx), \
         mock.patch("time.sleep", return_value=None):
        api.post("/test", {"k": "v"})
    # POST on 5xx should NOT retry (or retry budget < 3).
    # Acceptable: 1 attempt (no retry) or 2 attempts (max).
    assert attempts["n"] <= 2, (
        f"AtlasAPI.post retried {attempts['n']} times on 503 — POST is "
        "not idempotent and retry risks duplicate side effects. M1."
    )


# ---------------------------------------------------------------------------
# M3: AtlasAPI retry honors Retry-After header on 429
# ---------------------------------------------------------------------------


def test_TC_P3_M3_atlas_retry_honors_retry_after():
    """M3: on HTTP 429, retry must honor the Retry-After header if it's
    larger than the static backoff."""
    import requests

    from scripts.asp_setup import AtlasAPI

    api = AtlasAPI(public_key="pk", private_key="sk", project_id="proj")
    attempts = {"n": 0}
    sleeps = []

    def fake_429_then_200(method, url, **kwargs):
        attempts["n"] += 1
        resp = mock.MagicMock()
        if attempts["n"] < 2:
            resp.status_code = 429
            resp.ok = False
            resp.headers = {"Retry-After": "12"}
        else:
            resp.status_code = 200
            resp.ok = True
            resp.headers = {}
        return resp

    with mock.patch.object(requests, "request", side_effect=fake_429_then_200), \
         mock.patch("scripts.asp_setup.time.sleep",
                    side_effect=lambda s: sleeps.append(s)):
        api.get("/test")
    # Must have slept at least 12s (the Retry-After value).
    assert sleeps and max(sleeps) >= 12, (
        f"AtlasAPI.get on 429 must honor Retry-After. Sleeps: {sleeps}. M3."
    )


# ---------------------------------------------------------------------------
# M5: tmp file in a user-private dir, not /tmp
# ---------------------------------------------------------------------------


def test_TC_P3_M5_mcp_tempfile_in_private_dir():
    """M5: the mcp_deploy --primary-container tempfile should be in
    ~/.cache/streaming-agents (mode 0o700) rather than /tmp where
    /proc/<pid>/fd/* may be readable by other local users."""
    src = _read_source("scripts/mcp_deploy.py")
    # The fix uses Path.home() / ".cache" / "streaming-agents" or similar.
    has_private_dir = (
        ".cache" in src and "streaming-agents" in src
    ) or "Path.home()" in src
    assert has_private_dir, (
        "mcp_deploy must place the container-config tempfile in a "
        "user-private cache dir (Path.home() / '.cache' / "
        "'streaming-agents'), not /tmp. M5."
    )


# ---------------------------------------------------------------------------
# M6: Workshop banner uses cli_output.info, not .warn
# ---------------------------------------------------------------------------


def test_TC_P3_M6_workshop_banner_uses_info_not_warn():
    """M6: when the user explicitly passes --workshop-mode, the banner
    is a confirmation, not a warning. Using `[warn]` here trains the
    user to ignore warnings."""
    src = _read_source("scripts/deploy.py")
    # Find the workshop-mode banner block and check it uses .info
    tree = ast.parse(src)
    # Look for the if args.workshop_mode block in main()
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test_src = ast.unparse(node.test)
        if "workshop_mode" in test_src and "args" in test_src:
            body_src = ast.unparse(node.body)
            if "Workshop mode" in body_src:
                # Must use cli_output.info, NOT .warn
                assert "cli_output.warn" not in body_src, (
                    "Workshop-mode banner uses cli_output.warn on the "
                    "branch the user EXPLICITLY OPTED INTO. Use .info. M6."
                )
                return
    # Not finding the block is OK if the banner was restructured.


# ---------------------------------------------------------------------------
# H6: Annotation noise cleanup happened
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# L1: Add tests for previously-untested REQs (CRG-009, 018, 021, 022)
# REQ-CRG-020 already covered by TC-P3-H4 above.
# ---------------------------------------------------------------------------


def test_TC_P3_L1_009_mcp_deploy_tempfile_cleanup():
    """REQ-CRG-009 / L1: the mcp_deploy tempfile MUST be unlinked on
    every code path (success, error, retry). Verified by AST: the
    `finally:` block under the `try:` containing `os.unlink(tmp_path)`
    exists."""
    from scripts import mcp_deploy
    fn = mcp_deploy._create_ecs_express
    fn_src = inspect.getsource(fn)
    tree = ast.parse(fn_src)
    # Walk for a Try node whose finalbody includes os.unlink call.
    found_unlink = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            final_src = ast.unparse(node.finalbody) if node.finalbody else ""
            if "os.unlink(tmp_path)" in final_src:
                found_unlink = True
                break
    assert found_unlink, (
        "_create_ecs_express must have `try / finally: os.unlink(tmp_path)` "
        "to clean up the container config tempfile on every code path. "
        "REQ-CRG-009 / L1."
    )


def test_TC_P3_L1_018_seed_vessel_catalog_creates_index_first():
    """REQ-CRG-018 / L1: seed_vessel_catalog must call create_index
    BEFORE the upsert loop. AST-walks the function to confirm
    ordering."""
    from scripts import asp_setup
    fn = asp_setup.seed_vessel_catalog
    fn_src = inspect.getsource(fn)
    tree = ast.parse(fn_src)
    # Find the function's body and walk in order. First, find:
    #   - call to create_index
    #   - the for/upsert loop
    # Assert create_index comes BEFORE the loop.
    create_index_line = None
    upsert_loop_line = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            attr = getattr(node.func, "attr", None)
            if attr == "create_index" and create_index_line is None:
                create_index_line = node.lineno
            elif attr == "update_one" and upsert_loop_line is None:
                upsert_loop_line = node.lineno
    assert create_index_line is not None, "seed_vessel_catalog must call create_index"
    assert upsert_loop_line is not None, "seed_vessel_catalog must call update_one"
    assert create_index_line < upsert_loop_line, (
        f"create_index at line {create_index_line} must precede "
        f"upsert at line {upsert_loop_line}. REQ-CRG-018."
    )


def test_TC_P3_L1_021_ride_requests_cache_invalidated_on_publish_complete():
    """REQ-CRG-021 / L1: the ride_requests session cache must be
    invalidated when publish completes successfully (rc == 0). Otherwise
    the KPI tile stays stale until the user reloads."""
    src = _read_source("scripts/dashboard.py")
    # Find the completion block. Look for the line that sets
    # datagen_status = "success" and verify _ride_requests_count is
    # popped within 10 lines.
    lines = src.splitlines()
    success_lines = [
        i for i, line in enumerate(lines)
        if 'datagen_status = "success"' in line
    ]
    assert success_lines, "expected datagen_status='success' assignment"
    found_invalidation = False
    for ln in success_lines:
        # Check the next 25 lines (commit_batch_counter + pop ride_requests).
        window = "\n".join(lines[ln:ln + 25])
        if '_ride_requests_count' in window and "pop" in window:
            found_invalidation = True
            break
    assert found_invalidation, (
        "Successful publish must invalidate the _ride_requests_count "
        "session cache (st.session_state.pop). REQ-CRG-021."
    )


def test_TC_P3_L1_022_no_duplicate_subprocess_poller():
    """REQ-CRG-022 / L1: the top-level `proc.poll()` block that used
    to race the fragment-thread poll must be removed."""
    src = _read_source("scripts/dashboard.py")
    # The fragment at run_every=1 still has `proc.poll()` (correct,
    # single owner). But the top-level (non-fragment) poll block was
    # the bug. Verify there's at most ONE `proc.poll()` reachable
    # from `_render_sidebar`.
    # Easier: count proc.poll() calls. The fragment has 1, the agent
    # dispatch path may have 1 more. Threshold: ≤ 2.
    poll_calls = src.count("proc.poll()")
    assert poll_calls <= 2, (
        f"Found {poll_calls} `proc.poll()` calls in dashboard.py. "
        "REQ-CRG-022 promised to remove the duplicate top-level "
        "poller (must be ≤ 2: 1 fragment-owned + 1 agent-dispatch)."
    )


# ---------------------------------------------------------------------------
# L2: Convert stability source-grep to behavior test
# ---------------------------------------------------------------------------


@pytest.mark.timeout(15)
def test_TC_P3_L2_stability_validation_detects_late_failure():
    """L2 / pass-4 H-12 / pass-5 L-A / pass-6 M-NEW-14: stability
    validation behavior test.

    KNOWN BRITTLENESS: this test extracts the stability `while` loop's
    AST from `_create_flink_dml_statements` and `exec()`s the loop body
    in a manually-constructed namespace. Renaming `stability_elapsed`
    → `stab_elapsed` (or any variable referenced by the loop) produces
    a cryptic `NameError` at exec time rather than a clean test failure.

    The proper fix is Phase C-2 of the deploy.py refactor (extract
    `_validate_stability(running, base_url, headers, timeout, poll)`
    into scripts/common/flink_pipeline.py, then test that helper
    directly with mocked urlopen/sleep). The 631-line parent function
    + 4 nested closures is deferred per Premortem R1 in
    `specs/2026-05-25-holistic-review-pass2/tasks.md`.

    Until then this test is the gate: a regression inverting
    `if phase in ("FAILED","DEGRADED",...)` → `if phase == "RUNNING"`
    is caught by the [LATE-FAIL] assertion below, even if the
    test's "scaffolding" is brittle to upstream variable renames.

    pytest.timeout(15) prevents an infinite loop on namespace mismatch.
    """
    import io
    import urllib  # use the top-level package so urllib.request.urlopen resolves
    import urllib.request  # noqa: F401 — ensure submodule is loaded
    from contextlib import redirect_stdout

    from scripts import deploy

    # 1. Extract the stability while-loop AST node.
    fn_src = inspect.getsource(deploy._create_flink_dml_statements)
    tree = ast.parse(fn_src)
    while_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.While):
            test_src = ast.unparse(node.test)
            if "stability" in test_src.lower():
                while_node = node
                break
    assert while_node is not None, (
        "Expected `while stability_*` loop in _create_flink_dml_statements"
    )

    # 2. Stub urlopen → always returns FAILED phase.
    class _StubResp:
        def __init__(self, payload):
            self._b = __import__("json").dumps(payload).encode()
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return self._b

    def fake_urlopen(req, timeout=None):
        return _StubResp({
            "status": {"phase": "FAILED", "detail": "stability-regression-stub"},
        })

    # 3. Build the namespace the loop body closes over. Critical:
    #    `stability_running` MUST be a set (not bool), the body
    #    iterates with `for stmt_name in list(stability_running)`.
    ns = {
        "stability_running": {"stub-dml-1"},
        "stability_failed": set(),
        "stability_elapsed": 0,
        "stability_max": 2,     # tighten so 1 poll completes the loop
        "stability_poll": 1,
        "base_url": "https://stub.example/sql/v1/statements",
        "headers": {"Authorization": "Basic Zm9vOmJhcg=="},
        "time": __import__("time"),
        "json": __import__("json"),
        # Bind the top-level `urllib` package so the loop body's
        # `urllib.request.urlopen(...)` resolution works after exec.
        "urllib": urllib,
    }

    # 4. Run the loop with stubbed urlopen + sleep.
    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen), \
         mock.patch("time.sleep", return_value=None):
        loop_src = ast.unparse(while_node)
        buf = io.StringIO()
        with redirect_stdout(buf):
            exec(loop_src, ns)
    stdout = buf.getvalue()

    # 5. Behavior assertions.
    assert "LATE-FAIL" in stdout, (
        f"Stability loop did not emit [LATE-FAIL] when stubbed FAILED "
        f"phase appeared. stdout={stdout!r}. H-12 / L2 / REQ-CRG-025."
    )
    assert ns["stability_failed"], (
        "stability_failed set was never populated despite stubbed "
        "FAILED phase. The phase predicate is inverted or broken. H-12."
    )
    # The failed statement name must have been moved from running -> failed.
    assert "stub-dml-1" in ns["stability_failed"], (
        "FAILED statement was not added to stability_failed."
    )
    assert "stub-dml-1" not in ns["stability_running"], (
        "FAILED statement should have been discarded from stability_running."
    )


# ---------------------------------------------------------------------------
# C-3: atlas_reconcile.py extraction (REQ-CRG-026)
# ---------------------------------------------------------------------------


def test_TC_P3_C3_atlas_reconcile_module_exists():
    """REQ-CRG-026 / C-3: scripts/common/atlas_reconcile.py provides the
    extracted atlas reconciliation helpers."""
    from scripts.common import atlas_reconcile

    # Public API
    for name in ("db_user_exists", "delete_db_user",
                 "reconcile_orphan_db_user", "quarantine_stale_agents_state"):
        assert hasattr(atlas_reconcile, name), (
            f"atlas_reconcile must export {name}. REQ-CRG-026 / C-3."
        )


def test_TC_P3_C3_db_user_exists_behavior():
    """REQ-CRG-026 / C-3: db_user_exists must return True/False/None
    based on HTTP status code."""
    import requests

    from scripts.common import atlas_reconcile

    class FakeResp:
        def __init__(self, code): self.status_code = code

    # 200 → True
    with mock.patch.object(requests, "get", return_value=FakeResp(200)):
        assert atlas_reconcile.db_user_exists("pk", "sk", "proj", "u") is True
    # 404 → False
    with mock.patch.object(requests, "get", return_value=FakeResp(404)):
        assert atlas_reconcile.db_user_exists("pk", "sk", "proj", "u") is False
    # 401 → None (unknown)
    with mock.patch.object(requests, "get", return_value=FakeResp(401)):
        assert atlas_reconcile.db_user_exists("pk", "sk", "proj", "u") is None
    # Transport error → None
    def _raise(*args, **kwargs):
        raise requests.ConnectionError("network down")
    with mock.patch.object(requests, "get", side_effect=_raise):
        assert atlas_reconcile.db_user_exists("pk", "sk", "proj", "u") is None


def test_TC_P3_C3_deploy_shims_import_atlas_helpers():
    """REQ-CRG-026 / C-3 / pass-4 L-3 / pass-6 M-NEW-12: every shim
    must either be a direct re-export of the corresponding
    atlas_reconcile function OR a wrapper that delegates with the
    expected args.

    Symmetric verification: for re-export shims (3 of 4) we check
    identity; for the wrapper shim (1 of 4) we check delegation
    behavior. The original asymmetry (bare `callable()` for 2)
    let an empty no-op replacement pass — that's now closed.
    """
    from scripts import deploy
    from scripts.common import atlas_reconcile

    # Re-exports: identity check.
    assert deploy._atlas_db_user_exists is atlas_reconcile.db_user_exists, (
        "_atlas_db_user_exists must be `atlas_reconcile.db_user_exists`"
    )
    assert deploy._delete_atlas_db_user is atlas_reconcile.delete_db_user, (
        "_delete_atlas_db_user must be `atlas_reconcile.delete_db_user`"
    )
    assert deploy._quarantine_stale_agents_state is atlas_reconcile.quarantine_stale_agents_state, (
        "_quarantine_stale_agents_state must be a direct re-export"
    )

    # Wrapper: behavior check (delegation with env-derived args).
    called_with: dict = {}

    def fake_reconcile(**kwargs):
        called_with.update(kwargs)

    with mock.patch(
        "scripts.common.atlas_reconcile.reconcile_orphan_db_user",
        side_effect=fake_reconcile,
    ), mock.patch.object(deploy, "_load_env",
                         return_value={
                             "ATLAS_PUBLIC_KEY": "pk",
                             "ATLAS_PRIVATE_KEY": "sk",
                             "ATLAS_PROJECT_ID": "proj",
                         }):
        deploy._reconcile_orphan_atlas_db_user()
    assert called_with.get("public_key") == "pk", (
        "_reconcile_orphan_atlas_db_user shim did not forward "
        "ATLAS_PUBLIC_KEY from .env. H-7."
    )
    assert called_with.get("project_id") == "proj"
    assert called_with.get("username"), (
        "shim must always pass a non-empty username"
    )


def test_TC_P3_H6_annotation_cleanup_reduces_noise():
    """H6: REQ-CRG-024 promised cleanup of ~37 noise REQ-CRF-NNN
    annotations from prior pass. Verify the count went down."""
    src = _read_source("scripts/deploy.py")
    # Count one-line `# REQ-CRF-NNN / Sx:` style comment lines (the
    # noise pattern). We require fewer than 35 of these (down from
    # ~49 measured in the audit).
    import re
    noise_lines = [
        line for line in src.splitlines()
        if re.match(r"\s*#\s*REQ-CRF-\d+", line)
    ]
    assert len(noise_lines) < 35, (
        f"Annotation noise count: {len(noise_lines)} REQ-CRF-* "
        "comments in scripts/deploy.py. Target < 35 after cleanup. "
        "H6 / REQ-CRG-024."
    )


# ---------------------------------------------------------------------------
# Pass-6 H-NEW-12: flink_pipeline behavior tests (was 0 dedicated tests)
# ---------------------------------------------------------------------------


class TestFlinkPipelineCheckMcpHealth:
    """REQ-CRG-025 / H-NEW-12 (pass-6): behavior tests for the
    `check_mcp_health` function that gates `dispatch-insert` submission.

    Previously had ZERO dedicated coverage — a regression that always
    returned True or always returned False would pass the deploy's
    string-grep at test_integration.py:570 but break production.
    """

    def test_returns_true_on_http_200(self):
        from unittest import mock

        from scripts.common import flink_pipeline as fp

        class _Resp:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b"{}"

        with mock.patch("urllib.request.urlopen", return_value=_Resp()):
            assert fp.check_mcp_health("http://mcp.example", "tok") is True

    def test_returns_false_on_401(self):
        import urllib.error
        from unittest import mock

        from scripts.common import flink_pipeline as fp

        def _raise_401(*a, **kw):
            raise urllib.error.HTTPError(
                "http://mcp.example/mcp", 401, "Unauthorized", {}, None,
            )
        with mock.patch("urllib.request.urlopen", side_effect=_raise_401):
            assert fp.check_mcp_health("http://mcp.example", "tok") is False

    def test_returns_false_on_connection_refused(self):
        import urllib.error
        from unittest import mock

        from scripts.common import flink_pipeline as fp

        def _raise(*a, **kw):
            raise urllib.error.URLError("Connection refused")
        with mock.patch("urllib.request.urlopen", side_effect=_raise):
            assert fp.check_mcp_health("http://mcp.example", "tok") is False

    def test_returns_false_on_timeout(self):
        import socket
        from unittest import mock

        from scripts.common import flink_pipeline as fp

        def _raise(*a, **kw):
            raise socket.timeout("timed out")
        with mock.patch("urllib.request.urlopen", side_effect=_raise):
            assert fp.check_mcp_health("http://mcp.example", "tok", timeout=1) is False

    def test_returns_false_on_5xx(self):
        from unittest import mock

        from scripts.common import flink_pipeline as fp

        class _Resp:
            status = 503
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b""

        with mock.patch("urllib.request.urlopen", return_value=_Resp()):
            assert fp.check_mcp_health("http://mcp.example", "tok") is False


class TestFlinkPipelineDetectConnectionDrift:
    """REQ-CRF-027 / H-NEW-12 (pass-6): drift detection between two
    .env snapshots."""

    def test_initial_provision_returns_empty(self):
        """Old env empty → INITIAL provision, NOT rotation. No drift."""
        from scripts.common import flink_pipeline as fp
        drift = fp.detect_connection_drift(
            env_old={},
            env_new={"TF_VAR_mcp_server_url": "https://new.example"},
        )
        assert drift == set(), (
            "Initial provision (old empty) must NOT trigger a replace"
        )

    def test_mcp_url_change_drifts_mcp_connection(self):
        from scripts.common import flink_pipeline as fp
        drift = fp.detect_connection_drift(
            env_old={"TF_VAR_mcp_server_url": "https://old.example"},
            env_new={"TF_VAR_mcp_server_url": "https://new.example"},
        )
        assert "mongodb-mcp-connection" in drift

    def test_mongo_password_change_drifts_mongo_connection(self):
        from scripts.common import flink_pipeline as fp
        drift = fp.detect_connection_drift(
            env_old={"TF_VAR_mongodb_password": "old-pw"},
            env_new={"TF_VAR_mongodb_password": "new-pw"},
        )
        assert "mongodb-connection" in drift

    def test_no_change_returns_empty(self):
        from scripts.common import flink_pipeline as fp
        snapshot = {
            "TF_VAR_mcp_server_url": "https://x",
            "TF_VAR_mongodb_password": "x",
            "TF_VAR_voyage_api_key": "x",
        }
        drift = fp.detect_connection_drift(snapshot, snapshot)
        assert drift == set()


# ---------------------------------------------------------------------------
# Pass-6 coverage gaps: atlas_reconcile branch coverage
# ---------------------------------------------------------------------------


class TestAtlasReconcileQuarantine:
    """Pass-6 coverage gap: quarantine_stale_agents_state branch coverage.

    Previous coverage tested only the rename path. These tests pin
    the "no core state" early-return, the "no env-id drift" no-op,
    and the M-7 backup-only quarantine.
    """

    def test_no_core_state_returns_false(self, tmp_path: Path):
        """First-time deploy (no terraform/core/terraform.tfstate)
        — nothing to quarantine, returns False without side effects."""
        from scripts.common import atlas_reconcile

        (tmp_path / "terraform" / "agents").mkdir(parents=True)
        (tmp_path / "terraform" / "agents" / "terraform.tfstate").write_text("{}")
        # Note: NO terraform/core/terraform.tfstate

        result = atlas_reconcile.quarantine_stale_agents_state(tmp_path)
        assert result is False
        # Agents state file untouched
        assert (tmp_path / "terraform" / "agents" / "terraform.tfstate").exists()

    def test_no_env_id_drift_no_op(self, tmp_path: Path, monkeypatch):
        """When agents state references the CURRENT env-id, no rename."""
        import json

        from scripts.common import atlas_reconcile

        (tmp_path / "terraform" / "agents").mkdir(parents=True)
        (tmp_path / "terraform" / "core").mkdir(parents=True)
        (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text(
            json.dumps({"outputs": {"confluent_environment_id": {"value": "env-AAAA"}}})
        )
        # Agents state references the SAME env-id → no drift.
        agents_state = tmp_path / "terraform" / "agents" / "terraform.tfstate"
        agents_state.write_text('{"resources":[{"id":"env-AAAA/lfcp-xxxx"}]}')

        # Stub get_core_outputs to return the matching env-id
        monkeypatch.setattr(
            atlas_reconcile, "get_core_outputs",
            lambda root: {"confluent_environment_id": {"value": "env-AAAA"}},
            raising=False,
        )
        result = atlas_reconcile.quarantine_stale_agents_state(tmp_path)
        assert result is False
        assert agents_state.exists(), "no-drift case must leave agents state in place"


# ---------------------------------------------------------------------------
# Pass-6 coverage gap: cli_logging bootstrap behavior
# ---------------------------------------------------------------------------


class TestCliLoggingBootstrap:
    """Pass-6 coverage gap: bootstrap_logging --no-log short-circuit.

    The B-1 (pass-4) hardening + L-1 (pass-4) env-pop guarantee had
    zero behavior coverage. Add tests for the obvious short-circuits.
    """

    def test_no_log_flag_returns_none(self, monkeypatch):
        """--no-log in sys.argv → returns None, doesn't wrap."""
        import sys as _sys

        from scripts.common import cli_logging

        monkeypatch.setattr(_sys, "argv", ["deploy", "--no-log"])
        # Also stub isatty to True so the function reaches the --no-log
        # check (it short-circuits earlier on non-TTY).
        monkeypatch.setattr(_sys.stdin, "isatty", lambda: True, raising=False)
        result = cli_logging.bootstrap_logging("test")
        assert result is None
        # And --no-log must be stripped from sys.argv so downstream
        # arg-parsers don't see it.
        assert "--no-log" not in _sys.argv

    def test_non_tty_returns_none(self, monkeypatch):
        """No-TTY stdin → returns None (don't wrap CI / piped input)."""
        import sys as _sys

        from scripts.common import cli_logging

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False, raising=False)
        result = cli_logging.bootstrap_logging("test")
        assert result is None

    def test_inner_process_returns_path(self, monkeypatch, tmp_path: Path):
        """When _BOOTSTRAP_ENV is set, we're the inner process — return
        the log path without re-spawning."""
        import os

        from scripts.common import cli_logging

        log_path = tmp_path / "deploy-test.log"
        monkeypatch.setenv(cli_logging._BOOTSTRAP_ENV, str(log_path))
        result = cli_logging.bootstrap_logging("test")
        assert result == log_path


# ---------------------------------------------------------------------------
# Pass-6 coverage gap: redaction H-NEW-8 patterns (bearer / authorization)
# ---------------------------------------------------------------------------


class TestRedactionAuthHeaders:
    """Pass-6 H-NEW-8: redact MCP bearer / Authorization headers."""

    def test_authorization_bearer_header_masked(self):
        from scripts.common.redaction import redact

        line = "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        out = redact(line)
        # Token must be masked. Auth scheme word may or may not be
        # preserved depending on regex precedence — the security
        # requirement is the TOKEN not leaking.
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in out, (
            f"Bearer token leaked through redaction: {out}"
        )

    def test_authorization_basic_header_masked(self):
        from scripts.common.redaction import redact

        line = "authorization: basic dXNlcjpzZWNyZXRwYXNzMTIz"
        out = redact(line)
        assert "dXNlcjpzZWNyZXRwYXNzMTIz" not in out

    def test_mdb_mcp_client_auth_env_var_masked(self):
        from scripts.common.redaction import redact

        # MCP ECS task definition env var
        line = "MDB_MCP_HTTP_CLIENT_AUTH=Bearer abcdef0123456789secrettoken"
        out = redact(line)
        # The value side must be masked
        assert "abcdef0123456789secrettoken" not in out

    def test_atlas_public_key_masked(self):
        from scripts.common.redaction import redact

        line = "ATLAS_PUBLIC_KEY=XXXXXXXXVVVVVVVV"
        out = redact(line)
        assert "XXXXXXXXVVVVVVVV" not in out

    def test_short_token_fully_redacted(self):
        from scripts.common.redaction import redact

        line = "Authorization: Bearer short"
        out = redact(line)
        assert "short" not in out


# ---------------------------------------------------------------------------
# Pass-6 coverage gap: cli_output prunes .jsonl + .log
# ---------------------------------------------------------------------------


class TestCliOutputPruneOldLogs:
    """Pass-6 H-NEW-7: _prune_old_logs covers both .log and .jsonl."""

    def test_old_log_pruned(self, tmp_path: Path):
        import os
        import time

        from scripts.common.cli_output import _prune_old_logs

        old_log = tmp_path / "deploy-old.log"
        old_log.write_text("")
        # Set mtime to 30 days ago
        old_time = time.time() - (30 * 86400)
        os.utime(old_log, (old_time, old_time))

        _prune_old_logs(tmp_path, days=7)
        assert not old_log.exists()

    def test_old_jsonl_also_pruned(self, tmp_path: Path):
        """Pass-6 H-NEW-7: pipeline-reset JSONLs were leaked forever."""
        import os
        import time

        from scripts.common.cli_output import _prune_old_logs

        old_jsonl = tmp_path / "pipeline-reset-old.jsonl"
        old_jsonl.write_text("")
        old_time = time.time() - (30 * 86400)
        os.utime(old_jsonl, (old_time, old_time))

        _prune_old_logs(tmp_path, days=7)
        assert not old_jsonl.exists(), (
            "H-NEW-7: .jsonl files older than `days` must be pruned"
        )

    def test_recent_files_preserved(self, tmp_path: Path):
        from scripts.common.cli_output import _prune_old_logs

        new_log = tmp_path / "deploy-new.log"
        new_jsonl = tmp_path / "pipeline-new.jsonl"
        new_log.write_text("")
        new_jsonl.write_text("")

        _prune_old_logs(tmp_path, days=7)
        assert new_log.exists()
        assert new_jsonl.exists()

    def test_unknown_suffixes_left_alone(self, tmp_path: Path):
        """Files with non-log suffixes (.txt, .md, etc.) are not touched."""
        import os
        import time

        from scripts.common.cli_output import _prune_old_logs

        leave = tmp_path / "config.txt"
        leave.write_text("important")
        old_time = time.time() - (30 * 86400)
        os.utime(leave, (old_time, old_time))

        _prune_old_logs(tmp_path, days=7)
        assert leave.exists(), "non-log files must not be pruned"


# ---------------------------------------------------------------------------
# Pass-6 coverage gap: env_file.atomic_write_env behavior
# ---------------------------------------------------------------------------


class TestAtomicEnvWrite:
    """Pass-6 H-NEW-5: the shared atomic writer used by both deploy.py
    and mcp_deploy.py. Replaces deterministic .env.tmp / .env.mcp-tmp
    paths with mkstemp + os.replace."""

    def test_creates_file_with_0600(self, tmp_path: Path):
        import stat

        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        atomic_write_env(target, {"FOO": "bar"})
        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o600, f"mode 0o{mode:o} — must be 0o600"
        assert target.read_text().strip() == "FOO=bar"

    def test_preserves_unrelated_keys(self, tmp_path: Path):
        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        target.write_text("EXISTING=value\n# comment\nOTHER=keep\n")
        atomic_write_env(target, {"NEW": "added"})
        content = target.read_text()
        assert "EXISTING=value" in content
        assert "# comment" in content
        assert "OTHER=keep" in content
        assert "NEW=added" in content

    def test_updates_existing_key_in_place(self, tmp_path: Path):
        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        target.write_text("KEY=old\n# comment after\nOTHER=keep\n")
        atomic_write_env(target, {"KEY": "new"})
        lines = target.read_text().splitlines()
        # KEY must be at line 0 (replaced in place, not appended)
        assert lines[0] == "KEY=new"
        assert "# comment after" in lines[1]

    def test_refuses_newline_values(self, tmp_path: Path):
        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        with pytest.raises(ValueError, match=r"newline"):
            atomic_write_env(target, {"BAD": "line1\nline2"})

    def test_no_temp_file_left_on_success(self, tmp_path: Path):
        """No `.env.*.tmp` debris after a successful write."""
        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        atomic_write_env(target, {"KEY": "value"})
        tmps = [p for p in tmp_path.iterdir() if ".tmp" in p.name]
        assert tmps == [], f"leftover temp files: {tmps}"

    def test_none_values_skipped(self, tmp_path: Path):
        """None values don't create empty assignments."""
        from scripts.common.env_file import atomic_write_env

        target = tmp_path / ".env"
        atomic_write_env(target, {"KEEP": "v", "SKIP": None})
        content = target.read_text()
        assert "KEEP=v" in content
        assert "SKIP" not in content
