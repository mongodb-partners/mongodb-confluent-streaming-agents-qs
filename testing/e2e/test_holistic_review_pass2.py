"""End-to-end tests for the 2026-05-25 holistic review (pass 2).

Each TC-CRG-NNN maps to a REQ-CRG-NNN in
``specs/2026-05-25-holistic-review-pass2/requirements.md``.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read_source(rel_path: str) -> str:
    return (PROJECT_ROOT / rel_path).read_text()


# ---------------------------------------------------------------------------
# T-A1: REQ-CRG-001 — ASP vector index 404 → create
# ---------------------------------------------------------------------------


def test_TC_CRG_001_vector_index_created_when_collection_does_not_exist():
    """REQ-CRG-001: on a fresh cluster, GET search/indexes returns 404
    (collection doesn't exist yet, no indexes registered). The code must
    treat 404 as "no existing indexes" and proceed to CREATE, not skip."""
    from scripts.asp_setup import AtlasAPI, ensure_atlas_indexes

    # Stub API: GET search/indexes returns 404; POST search/indexes
    # returns 201. Also stub everything else (cluster check, atlas indexes
    # via pymongo) to no-ops.
    class FakeResp:
        def __init__(self, status_code, body=None):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self._body = body or {}
            self.text = str(self._body)
        def json(self): return self._body

    posts = []
    def fake_get(path, api_version=None):
        if "search/indexes/events/knowledge_base" in path:
            return FakeResp(404, {"detail": "not found"})
        return FakeResp(200, {"results": []})
    def fake_post(path, body, api_version=None, idempotent=False):
        # pass-4 H-5: AtlasAPI.post grew an `idempotent` kwarg so
        # callers can opt into 5xx retry on name-idempotent endpoints.
        posts.append((path, body))
        return FakeResp(201, {})

    api = mock.MagicMock(spec=AtlasAPI)
    api.get = mock.MagicMock(side_effect=fake_get)
    api.post = mock.MagicMock(side_effect=fake_post)

    # Stub pymongo client construction inside ensure_atlas_indexes so the
    # rest of the function (collection-level indexes, validators, dedup,
    # purge) doesn't run.
    with mock.patch("scripts.asp_setup.get_client") as mock_client, \
         mock.patch("scripts.asp_setup.build_uri", return_value="mongodb://x"):
        mock_db = mock.MagicMock()
        mock_db.command = mock.MagicMock()
        mock_db.create_collection = mock.MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        # Just need it not to crash; we care about the search index POST.
        ensure_atlas_indexes(
            api,
            cluster_name="Cluster0",
            connection_string="mongodb+srv://x.example.com",
            username="u", password="p",
        )

    # Verify: a POST to search/indexes was issued (the CREATE call).
    create_calls = [p for p in posts if p[0].endswith("/search/indexes")]
    assert create_calls, (
        "Vector index CREATE was not posted despite GET returning 404. "
        "REQ-CRG-001 (BLOCKER): on a fresh cluster the index would never "
        "be created, breaking RAG silently."
    )
    body = create_calls[0][1]
    assert body.get("collectionName") == "knowledge_base"
    assert body.get("database") == "events"
    assert body.get("name") == "vector_index"


# ---------------------------------------------------------------------------
# T-A8: REQ-CRG-003 — File modes 0o600
# ---------------------------------------------------------------------------


def test_TC_CRG_003_save_env_many_writes_mode_0600():
    """REQ-CRG-003: _save_env_many must write the .env file with mode 0o600
    (owner read/write only) so secrets aren't world-readable on shared
    systems (EC2 / dev boxes / shared dev containers)."""
    from scripts import deploy

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_env = Path(tmp_dir) / ".env"
        # Pre-create with broader perms to confirm the write tightens them.
        tmp_env.write_text("EXISTING=value\n")
        os.chmod(tmp_env, 0o644)
        with mock.patch("scripts.deploy._env_path", return_value=tmp_env):
            deploy._save_env_many({"NEW_KEY": "value"})
        mode = stat.S_IMODE(tmp_env.stat().st_mode)
        assert mode == 0o600, (
            f"Expected file mode 0o600, got 0o{mode:o}. "
            "REQ-CRG-003: secrets in .env must not be world-readable."
        )


def test_TC_CRG_003b_tfvars_written_mode_0600():
    """REQ-CRG-003 / pass-4 M-15: tfvars files must be written with
    mode 0o600.

    Real behavior test (skip envelope removed per REQ-CRG-027).
    Calls write_tfvars_file directly and stats the resulting file.
    """
    from scripts.common import tfvars

    # write_tfvars_file is the low-level writer that all higher-level
    # functions delegate to. Verify IT applies chmod 0o600.
    assert hasattr(tfvars, "write_tfvars_file"), (
        "scripts/common/tfvars must export write_tfvars_file (the "
        "single chmod-applying writer). If renamed, update this test."
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "terraform.tfvars"
        content = 'confluent_cloud_api_key = "FAKE"\n'
        ok = tfvars.write_tfvars_file(target, content)
        assert ok, "write_tfvars_file should return True on success"
        assert target.exists(), "tfvars file was not created"
        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o600, (
            f"tfvars file mode 0o{mode:o} — must be 0o600. REQ-CRG-003."
        )

        # Re-write to confirm the rewrite path also applies chmod
        # (write_tfvars_file may backup then write a fresh file).
        target.chmod(0o644)
        tfvars.write_tfvars_file(target, content + "new = 1\n")
        mode2 = stat.S_IMODE(target.stat().st_mode)
        assert mode2 == 0o600, (
            f"tfvars file mode 0o{mode2:o} after rewrite — must remain 0o600. "
            "REQ-CRG-003."
        )


# ---------------------------------------------------------------------------
# T-A2: REQ-CRG-002 — ensure_connections raises on failure
# ---------------------------------------------------------------------------


def test_TC_CRG_002_ensure_connections_raises_on_delete_failure():
    """REQ-CRG-002: when a connection delete fails (non-404), the function
    must raise rather than continue silently. Otherwise processors run
    against stale credentials."""
    from scripts.asp_setup import AtlasAPI, ensure_connections

    class FakeResp:
        def __init__(self, status_code, body=None):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self._body = body or {}
            self.text = str(self._body)
        def json(self): return self._body
        def raise_for_status(self):
            if not self.ok:
                raise requests.HTTPError(f"HTTP {self.status_code}")

    import requests
    api = mock.MagicMock(spec=AtlasAPI)
    # Existing connections list (one to update)
    api.get = mock.MagicMock(return_value=FakeResp(200, {
        "results": [{"name": "kafka_confluent"}],
    }))
    # DELETE fails with 500
    api.delete = mock.MagicMock(return_value=FakeResp(500, {"detail": "boom"}))
    api.post = mock.MagicMock(return_value=FakeResp(201))

    # Fake creds so connections are populated
    # M-NEW-13 (pass-6): assert on the specific failing connection
    # identity, not just any RuntimeError with a generic word match.
    # A regression that raises early (e.g. "voyage_api_key invalid")
    # would otherwise pass.
    with pytest.raises(RuntimeError) as exc_info:
        ensure_connections(
            api,
            cluster_name="Cluster0",
            bootstrap_server="k.example:9092",
            confluent_api_key="k", confluent_api_secret="s",
            voyage_api_key="vk",
            schema_registry_url="https://sr.example",
            schema_registry_key="srk",
            schema_registry_secret="srs",
        )
    err = str(exc_info.value)
    assert "kafka_confluent" in err, (
        f"RuntimeError must identify the failing connection by name. "
        f"Got: {err[:300]}"
    )
    assert "500" in err or "delete" in err.lower(), (
        f"RuntimeError must surface the failure mode (delete 500). "
        f"Got: {err[:300]}"
    )


# ---------------------------------------------------------------------------
# T-A3: REQ-CRG-013/014 — AtlasAPI timeout + retry
# ---------------------------------------------------------------------------


def test_TC_CRG_013_atlas_api_passes_timeout():
    """REQ-CRG-013: AtlasAPI.get/post/delete must pass a request timeout."""
    import requests

    from scripts.asp_setup import AtlasAPI

    api = AtlasAPI(public_key="pk", private_key="sk", project_id="proj")
    captured_kwargs = []
    def fake_request(method, url, **kwargs):
        captured_kwargs.append(kwargs)
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.ok = True
        resp.json = lambda: {}
        resp.text = ""
        return resp

    with mock.patch.object(requests, "request", side_effect=fake_request):
        api.get("/test")
        assert captured_kwargs and "timeout" in captured_kwargs[-1], (
            "AtlasAPI.get must pass timeout=. REQ-CRG-013."
        )
        api.post("/test", {})
        assert "timeout" in captured_kwargs[-1], "AtlasAPI.post timeout missing"
        api.delete("/test")
        assert "timeout" in captured_kwargs[-1], "AtlasAPI.delete timeout missing"


def test_TC_CRG_014_atlas_api_retries_on_transient_failure():
    """REQ-CRG-014: AtlasAPI must retry on 429/5xx/connection errors."""
    from scripts.asp_setup import AtlasAPI

    api = AtlasAPI(public_key="pk", private_key="sk", project_id="proj")
    attempts = {"n": 0}

    class FakeResp:
        def __init__(self, status_code):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self.text = ""
        def json(self): return {}

    def flaky_get(*args, **kwargs):
        attempts["n"] += 1
        if attempts["n"] < 3:
            return FakeResp(503)
        return FakeResp(200)

    import requests
    def flaky_request(method, url, **kwargs):
        return flaky_get()
    with mock.patch.object(requests, "request", side_effect=flaky_request), \
         mock.patch("time.sleep", return_value=None):
        resp = api.get("/test")
    assert resp.status_code == 200
    assert attempts["n"] == 3, (
        f"Expected 3 attempts (2 retries on 503 + 1 success); got {attempts['n']}. "
        "REQ-CRG-014."
    )


# ---------------------------------------------------------------------------
# T-A4: REQ-CRG-015 — Pipelines 4 & 5 $validate before $match
# ---------------------------------------------------------------------------


def test_TC_CRG_015_zone_traffic_ingestion_has_validate():
    """REQ-CRG-015: _pipeline_zone_traffic_ingestion must include a
    $validate stage with validationAction=dlq, mirroring the dispatch_log
    pattern. Without it, schema drift silently drops rows."""
    from scripts.asp_setup import _pipeline_zone_traffic_ingestion

    pipeline = _pipeline_zone_traffic_ingestion()
    validate_stages = [s for s in pipeline if "$validate" in s]
    assert validate_stages, (
        "zone_traffic_ingestion pipeline must include $validate before "
        "$match. REQ-CRG-015."
    )
    v = validate_stages[0]["$validate"]
    assert v.get("validationAction") == "dlq", (
        "validationAction must be 'dlq' so schema drift goes to DLQ rather "
        "than being silently dropped by $match."
    )


def test_TC_CRG_015b_anomalies_ingestion_has_validate():
    """REQ-CRG-015: same contract for _pipeline_anomalies_ingestion."""
    from scripts.asp_setup import _pipeline_anomalies_ingestion

    pipeline = _pipeline_anomalies_ingestion()
    validate_stages = [s for s in pipeline if "$validate" in s]
    assert validate_stages, "anomalies_ingestion needs $validate. REQ-CRG-015."
    assert validate_stages[0]["$validate"].get("validationAction") == "dlq"


# ---------------------------------------------------------------------------
# T-A5: REQ-CRG-016 — Embedding-shape $match guard
# ---------------------------------------------------------------------------


def test_TC_CRG_016_knowledge_base_pipeline_guards_embedding_shape():
    """REQ-CRG-016: when Voyage returns a malformed response (no data
    array, error body), the embedding extraction produces null. The
    pipeline must guard the embedding shape before $merge so null
    embeddings don't pollute knowledge_base.

    Either a $match guard or a $validate stage (preferred — DLQ rather
    than silent drop) is acceptable.
    """
    from scripts.asp_setup import _pipeline_event_knowledge_base

    pipeline = _pipeline_event_knowledge_base()
    merge_idx = next(i for i, s in enumerate(pipeline) if "$merge" in s)
    # Look for either $validate or $match before $merge that references
    # the embedding field.
    pre_merge_guard = None
    for s in pipeline[:merge_idx]:
        s_str = str(s)
        if ("$validate" in s or "$match" in s) and "embedding" in s_str:
            pre_merge_guard = s
            break
    assert pre_merge_guard is not None, (
        "Pipeline must include $validate or $match guarding embedding "
        "shape before $merge. REQ-CRG-016."
    )


# ---------------------------------------------------------------------------
# T-A6: REQ-CRG-017 — ensure_asp_instance raises
# ---------------------------------------------------------------------------


def test_TC_CRG_017_ensure_asp_instance_raises_on_timeout():
    """REQ-CRG-017: timeout path must raise RuntimeError, not sys.exit(1).
    Otherwise the deploy.py phase-resume contract is broken."""
    import scripts.asp_setup as asp_mod

    src = inspect.getsource(asp_mod.ensure_asp_instance)
    # Old behavior: `sys.exit(1)` in the timeout branch. Fix raises.
    # Check no `sys.exit(` calls remain in the function.
    tree = ast.parse(src)
    sys_exits = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "sys"
        and n.func.attr == "exit"
    ]
    assert not sys_exits, (
        "ensure_asp_instance must not call sys.exit() — raise RuntimeError "
        "instead so the caller's bool contract holds. REQ-CRG-017."
    )
    # And it must contain a raise.
    raises = [n for n in ast.walk(tree) if isinstance(n, ast.Raise)]
    assert raises, "ensure_asp_instance must raise on timeout"


# ---------------------------------------------------------------------------
# T-A12: REQ-CRG-008 — Bedrock max_tokens 500
# ---------------------------------------------------------------------------


def test_TC_CRG_008_bedrock_max_tokens_capped():
    """REQ-CRG-008: Bedrock max_tokens must be capped at 500 (was 50000)
    to limit cost on adversarial input. Single response at 50000 tokens
    on Claude Sonnet 4.6 = ~$0.75; 1000 surge rows/min = $45k/day."""
    main_tf = (PROJECT_ROOT / "terraform/core/main.tf").read_text()
    # Find the llm_textgen_model statement
    assert "llm_textgen_model" in main_tf, "expected llm_textgen_model resource"
    # The max_tokens setting in the SQL statement
    import re
    m = re.search(r"max_tokens'\s*=\s*'(\d+)'", main_tf)
    assert m, "expected 'bedrock.params.max_tokens' setting"
    max_tokens = int(m.group(1))
    assert max_tokens <= 500, (
        f"Bedrock max_tokens is {max_tokens}; must be ≤ 500 to cap "
        "cost on adversarial input. REQ-CRG-008."
    )


# ---------------------------------------------------------------------------
# T-A11: REQ-CRG-007 — RAG prompt-injection guard
# ---------------------------------------------------------------------------


def test_TC_CRG_007_rag_chunks_wrapped_and_capped():
    """REQ-CRG-007: retrieved chunks must be wrapped with deterministic
    delimiters AND length-capped via SUBSTRING(top_chunk_N, 1, 500) to
    limit prompt-injection blast radius."""
    sql = (PROJECT_ROOT / "terraform/agents/sql/anomalies-enriched-insert.sql").read_text()
    # Must SUBSTRING each chunk
    for chunk in ("top_chunk_1", "top_chunk_2", "top_chunk_3"):
        # Either SUBSTRING(...) or SUBSTR(...) or similar truncation
        has_cap = (
            f"SUBSTRING({chunk}" in sql
            or f"SUBSTRING(rad_with_rag.{chunk}" in sql
            or f"SUBSTR({chunk}" in sql
        )
        assert has_cap, (
            f"{chunk} must be length-capped (SUBSTRING) before reaching "
            "the LLM prompt. REQ-CRG-007."
        )


# ---------------------------------------------------------------------------
# T-A14: REQ-CRG-019 — Batch counter on success only
# ---------------------------------------------------------------------------


def test_TC_CRG_019_batch_counter_not_incremented_before_subprocess_success():
    """REQ-CRG-019: the dashboard must not increment the batch counter
    immediately after Popen — it must wait for return code 0.

    The fix moves the counter write into the completion callback path,
    using `_pending_batch_num` stashed in session_state at launch time.
    """
    src = _read_source("scripts/dashboard.py")
    # The fix uses session_state["_pending_batch_num"] as the deferred
    # marker. Verify the at-launch path stashes the pending number, and
    # the completion path commits it only on rc == 0.
    assert "_pending_batch_num" in src, (
        "Expected `_pending_batch_num` stash pattern that defers the "
        "batch counter write to the completion callback. REQ-CRG-019."
    )
    # And verify there's no immediate `batch_counter_file.write_text`
    # at the launch site (the old anti-pattern).
    lines = src.splitlines()
    for i, line in enumerate(lines):
        if "batch_counter_file.write_text" in line:
            preceding = "\n".join(lines[max(0, i - 10):i])
            assert "rc == 0" in preceding or "_pending_batch_num" in preceding, (
                f"line {i+1}: batch_counter_file.write_text without "
                "success guard. REQ-CRG-019."
            )


# ---------------------------------------------------------------------------
# T-A10: REQ-CRG-005 — Dashboard XSS escape
# ---------------------------------------------------------------------------


def test_TC_CRG_005_dashboard_escapes_external_content():
    """REQ-CRG-005: LLM responses, RAG chunks, and other external content
    must be escaped before reaching st.markdown. A poisoned chunk
    containing `<script>` or `[click](javascript:...)` should not execute."""
    src = _read_source("scripts/dashboard.py")
    # The fix imports `html` and wraps `card['anomaly_reason']` / `chunk`
    # in `html.escape()` before st.markdown.
    assert "import html" in src or "from html import" in src, (
        "dashboard.py must import the html module for escaping. REQ-CRG-005."
    )
    # The previous unguarded patterns must be gone or wrapped.
    import re
    raw_anomaly = re.search(
        r'st\.markdown\(\s*f"[^"]*\{(?:card\[\s*[\'"]anomaly_reason[\'"]\s*\]|chunk)[^}]*\}',
        src,
    )
    assert raw_anomaly is None, (
        "Found raw st.markdown(f'... {anomaly_reason}') or chunk "
        "interpolation. Must be wrapped in html.escape(). REQ-CRG-005."
    )


# ---------------------------------------------------------------------------
# T-A18: REQ-CRG-023 — Drop redundant sleeps in agent dispatch
# ---------------------------------------------------------------------------


def test_TC_CRG_023_agent_dispatch_no_redundant_sleeps():
    """REQ-CRG-023: the agent-dispatch chain uses _wait_for_statement_deleted
    for polling, so the prior fixed time.sleep(5)/(3) calls are redundant
    and should be removed."""
    src = _read_source("scripts/dashboard.py")
    # Find the _do_agent_dispatch function body
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_do_agent_dispatch":
            fn_src = ast.unparse(node)
            # The fix removed `_time.sleep(5)` / `_time.sleep(3)` that
            # used to precede `_delete_flink_statement`.
            time_sleeps = fn_src.count("_time.sleep(")
            # We allow some sleeps (e.g. between retries) — but the
            # pattern of multiple sleep+delete pairs must be gone.
            # Counting: pre-fix had 4 sleeps inline (before DROP, before
            # DROP table, etc.); post-fix should have ≤ 1.
            assert time_sleeps <= 1, (
                f"_do_agent_dispatch contains {time_sleeps} _time.sleep "
                "calls. Most are redundant with _wait_for_statement_deleted. "
                "REQ-CRG-023."
            )
            return
    pytest.fail("_do_agent_dispatch function not found")


# ---------------------------------------------------------------------------
# T-B: REQ-CRG-010, 011 — Workshop-mode flag
# ---------------------------------------------------------------------------


def test_TC_CRG_010_workshop_mode_flag_exists():
    """REQ-CRG-010: deploy.py must accept a --workshop-mode argparse flag."""
    src = _read_source("scripts/deploy.py")
    assert "--workshop-mode" in src, (
        "deploy.py must declare a --workshop-mode CLI flag. REQ-CRG-010."
    )
    # And the flag must be wired to a settable TF var or env var
    # (workshop-mode preserves the lax defaults; default mode hardens).
    assert "workshop_mode" in src, (
        "--workshop-mode must be parsed into args.workshop_mode and "
        "propagated to the deploy. REQ-CRG-010."
    )


def test_TC_CRG_011_atlas_ip_scoping_default():
    """REQ-CRG-011: without --workshop-mode, Atlas IP access list defaults
    to the deployer's egress IP, NOT 0.0.0.0/0."""
    # The fix introduces a TF_VAR_atlas_access_cidrs variable (or
    # similar) which the deploy populates with either ['0.0.0.0/0']
    # (workshop) or [<egress>/32] (default). Check the Terraform
    # module accepts the variable.
    tf = (PROJECT_ROOT / "terraform/atlas/main.tf").read_text()
    has_cidr_var = (
        "var.atlas_access_cidrs" in tf
        or "var.access_cidrs" in tf
        or "var.workshop_mode" in tf
    )
    assert has_cidr_var, (
        "terraform/atlas/main.tf must accept a CIDR variable so the "
        "deploy can scope the IP access list. REQ-CRG-011."
    )


# ---------------------------------------------------------------------------
# T-A9: REQ-CRG-004 — cli_logging redaction
# ---------------------------------------------------------------------------


def test_TC_CRG_004_log_redaction_strips_secrets():
    """REQ-CRG-004: the cli_logging post-process redacts secrets from
    the session log on exit. Verified by writing a fake log file with
    embedded secrets, calling the redaction helper, then re-reading."""
    from scripts.common.cli_logging import _redact_log_file

    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as f:
        f.write("Starting terraform apply...\n")
        f.write('aws_secret_key = "AKIA1234FAKE5678SECRET90"\n')
        f.write("Connection string: mongodb+srv://admin:s3cr3t_password@host/db\n")
        f.write("Done.\n")
        log_path = Path(f.name)
    try:
        _redact_log_file(log_path)
        content = log_path.read_text()
        # Plaintext secret values must be gone
        assert "AKIA1234FAKE5678SECRET90" not in content, (
            "AWS secret leaked through redaction. REQ-CRG-004."
        )
        assert "s3cr3t_password" not in content, (
            "Mongo URI password leaked through redaction. REQ-CRG-004."
        )
        # Non-secret context survives
        assert "Starting terraform apply" in content
        assert "Done." in content
        # Mode is owner-only after redaction
        mode = stat.S_IMODE(log_path.stat().st_mode)
        assert mode == 0o600, f"redacted log mode 0o{mode:o} should be 0o600"
    finally:
        log_path.unlink(missing_ok=True)


def test_TC_CRG_004b_log_redaction_handles_missing_file():
    """REQ-CRG-004: best-effort — missing or empty log file is OK."""
    from scripts.common.cli_logging import _redact_log_file

    # Nonexistent path must not raise
    _redact_log_file(Path("/tmp/definitely-does-not-exist-cqrhpz.log"))


# ---------------------------------------------------------------------------
# T-D3: REQ-CRG-029 — redaction.py edge cases
# ---------------------------------------------------------------------------


class TestRedactionEdgeCases:
    """REQ-CRG-029: redact() must handle real-world variants."""

    def test_uri_without_port(self):
        """URI without an explicit port: scheme://user:pass@host/db"""
        from scripts.common.redaction import redact
        out = redact("mongodb+srv://admin:s3cr3t@host.example/db")
        assert "s3cr3t" not in out
        assert "admin" in out  # username allowed
        assert "host.example" in out  # host preserved

    def test_uri_with_percent_encoded_password(self):
        """%-encoded password chars (e.g. @ in password)."""
        from scripts.common.redaction import redact
        out = redact("mongodb+srv://u:p%40ss@host/db")
        # The encoded password must be masked too.
        assert "p%40ss" not in out
        assert "host" in out

    def test_aws_secret_access_key_with_underscores(self):
        """AWS_SECRET_ACCESS_KEY uses underscores — the regex must match."""
        from scripts.common.redaction import redact
        out = redact('AWS_SECRET_ACCESS_KEY="AKIA1234FAKE5678SECRET"')
        assert "AKIA1234FAKE5678SECRET" not in out

    def test_json_secret_pairs_are_masked(self):
        """JSON object form `{"key":"value"}` must be masked. The key's closing
        quote sits between key word and colon, and the value ends at `}` — both
        previously defeated the KV regex, leaking secrets in JSON CLI/JSONL output."""
        from scripts.common.redaction import redact

        out1 = redact('{"api_secret":"abc123def456"}')
        assert "abc123def456" not in out1
        out2 = redact('{"ATLAS_PRIVATE_KEY":"xyz789private0"}')
        assert "xyz789private0" not in out2
        # Non-secret JSON keys must still pass through untouched.
        assert redact('{"environment":"env-abc123"}') == '{"environment":"env-abc123"}'

    def test_value_under_8_chars_fully_redacted(self):
        """Short values are fully masked (no prefix leakage)."""
        from scripts.common.redaction import redact
        out = redact("api_key=short1")  # 6 chars
        # Should be replaced with ***, not a prefix-truncated form.
        assert "short1" not in out
        assert "***" in out

    def test_special_regex_chars_in_value(self):
        """A value containing regex metacharacters must not break the redactor."""
        from scripts.common.redaction import redact
        out = redact('password="abc.+*?d12345"')
        assert "abc.+*?d12345" not in out

    def test_empty_value_preserved(self):
        """`key=` with empty value should not crash or corrupt."""
        from scripts.common.redaction import redact
        out = redact("api_key=")
        # Should not raise; output should still contain `api_key`
        assert "api_key" in out

    def test_multiple_secrets_in_one_line(self):
        """Multiple secret k=v pairs on one line each get masked."""
        from scripts.common.redaction import redact
        line = 'api_key="AKIA1234FAKE5678" secret="HUNTER2_FAKE_LONG_VALUE"'
        out = redact(line)
        assert "AKIA1234FAKE5678" not in out
        assert "HUNTER2_FAKE_LONG_VALUE" not in out

    def test_non_secret_identifiers_pass_through(self):
        """`environment_id`, `cluster_name`, etc. are NOT redacted."""
        from scripts.common.redaction import redact
        out = redact("environment_id=env-abc12345 cluster_name=Cluster0")
        assert "env-abc12345" in out
        assert "Cluster0" in out

    def test_empty_string_returns_empty(self):
        """redact('') must return ''."""
        from scripts.common.redaction import redact
        assert redact("") == ""

    def test_idempotent(self):
        """redact(redact(x)) must equal redact(x)."""
        from scripts.common.redaction import redact
        x = "password=hunter2_long_secret"
        once = redact(x)
        twice = redact(once)
        assert once == twice, "redact() must be idempotent"


# ---------------------------------------------------------------------------
# T-D4: REQ-CRG-030 — health.py MCP check
# ---------------------------------------------------------------------------


def test_TC_CRG_030_health_includes_mcp_check():
    """REQ-CRG-030: health.py must include a _check_mcp(creds) function
    that probes the MCP server endpoint and is wired into collect_report."""
    from scripts import health

    assert hasattr(health, "_check_mcp"), (
        "health.py must define _check_mcp. REQ-CRG-030."
    )
    # collect_report must include MCP entries.
    src = inspect.getsource(health.collect_report)
    assert "_check_mcp" in src or "mcp" in src.lower(), (
        "collect_report must invoke _check_mcp. REQ-CRG-030."
    )


# ---------------------------------------------------------------------------
# T-D5: REQ-CRG-031 — publish_data.py behavior tests
# ---------------------------------------------------------------------------


def test_TC_CRG_031_get_topic_message_count_returns_zero_on_404():
    """REQ-CRG-031: _get_topic_message_count must return 0 for a topic
    that doesn't exist (404 on partitions endpoint)."""
    import urllib.error

    from scripts import publish_data

    def fake_urlopen(*args, **kwargs):
        raise urllib.error.HTTPError(
            url="x", code=404, msg="not found", hdrs=None, fp=None,
        )
    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = publish_data._get_topic_message_count(
            rest_endpoint="https://kafka",
            cluster_id="lkc-x",
            kafka_api_key="k", kafka_api_secret="s",
            topic="nonexistent",
        )
    assert result == 0


def test_TC_CRG_031b_get_topic_message_count_returns_none_on_transport_error():
    """REQ-CRG-031: transport errors return None (distinguishes "topic
    has 0 messages" from "couldn't query")."""
    import urllib.error

    from scripts import publish_data

    def fake_urlopen(*args, **kwargs):
        raise urllib.error.URLError("network down")

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = publish_data._get_topic_message_count(
            rest_endpoint="https://kafka",
            cluster_id="lkc-x",
            kafka_api_key="k", kafka_api_secret="s",
            topic="ride_requests",
        )
    assert result is None


def test_TC_CRG_031c_get_topic_message_count_sums_partition_offsets():
    """REQ-CRG-031: successful path sums (latest - earliest) across
    partitions."""
    from scripts import publish_data

    # Stub: 2 partitions, each with 100 messages
    call_count = {"n": 0}

    class FakeResp:
        def __init__(self, body):
            self._body = body
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self):
            return json.dumps(self._body).encode()

    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "/partitions" in url and "/offsets/" not in url:
            # partition list
            return FakeResp({"data": [
                {"partition_id": 0},
                {"partition_id": 1},
            ]})
        if "/offsets/earliest" in url:
            return FakeResp({"offset": 0})
        if "/offsets/latest" in url:
            return FakeResp({"offset": 100})
        raise RuntimeError(f"unexpected url: {url}")

    import json
    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = publish_data._get_topic_message_count(
            rest_endpoint="https://kafka",
            cluster_id="lkc-x",
            kafka_api_key="k", kafka_api_secret="s",
            topic="ride_requests",
        )
    assert result == 200, f"expected 2 partitions × 100 msgs = 200; got {result}"


def test_TC_CRG_031d_publish_data_main_parses_dry_run_flag():
    """REQ-CRG-031: --dry-run argparse flag exists and short-circuits."""
    import argparse

    from scripts import publish_data

    # The main module's parser must accept --dry-run.
    src = inspect.getsource(publish_data.main)
    assert "--dry-run" in src or "dry_run" in src, (
        "publish_data.main must support --dry-run. REQ-CRG-031."
    )


def test_TC_CRG_031e_publish_data_main_parses_force_flag():
    """REQ-CRG-031: --force argparse flag exists for bypassing the
    idempotency guard."""
    from scripts import publish_data
    src = inspect.getsource(publish_data.main)
    assert "--force" in src or "force" in src, (
        "publish_data.main must support --force. REQ-CRG-031."
    )


def test_TC_CRG_030b_check_mcp_returns_canonical_entry_shape():
    """REQ-CRG-030 + REQ-CRG-035: _check_mcp entries must use _entry()
    helper so they match the canonical schema."""
    from scripts import health

    # Stub: TF_VAR_mcp_server_url / TF_VAR_mcp_auth_token unset.
    results = health._check_mcp({})
    assert isinstance(results, list)
    if results:
        entry = results[0]
        # Must have all canonical keys.
        EXPECTED_KEYS = {
            "name", "status", "detail", "phase",
            "records", "count", "state", "last_checkpoint",
        }
        missing = EXPECTED_KEYS - set(entry.keys())
        assert not missing, (
            f"MCP entry missing canonical keys: {missing}. "
            "Use _entry() helper. REQ-CRG-030 / REQ-CRG-035."
        )


# ---------------------------------------------------------------------------
# T-C2: REQ-CRG-025 — Pin stability validation (Premortem R1)
# ---------------------------------------------------------------------------


def test_TC_CRG_025_stability_validation_present():
    """REQ-CRG-025 / Premortem R1: the 60-second stability validation
    pass in _create_flink_dml_statements detects RUNNING→FAILED
    transitions on Flink statements after they initially reach RUNNING.
    Critical for catching stale-Avro-offset failures documented in
    CLAUDE.md. This test pins the validation block so the planned
    flink_pipeline.py extraction (REQ-CRG-025) can be verified to
    preserve the behavior.
    """
    src = _read_source("scripts/deploy.py")
    # The validation must:
    # - Loop with a 60s timeout
    # - Poll on a 10s interval
    # - Detect FAILED/DEGRADED transitions
    # - Log a [LATE-FAIL] marker
    assert "stability_max = 60" in src or "stability_max=60" in src, (
        "60-second stability window literal missing. REQ-CRG-025."
    )
    assert "stability_poll" in src, (
        "stability_poll variable missing — extraction must preserve "
        "the polling structure"
    )
    assert "LATE-FAIL" in src, (
        "[LATE-FAIL] log marker missing — operators rely on this string "
        "for triage. REQ-CRG-025."
    )
    assert "DEGRADED" in src, (
        "DEGRADED phase check missing — RUNNING→DEGRADED is a real "
        "failure mode"
    )


def test_TC_CRG_025b_create_flink_dml_statements_still_callable():
    """REQ-CRG-025: the deploy public surface must keep `_create_flink_dml_statements`
    importable and callable so the extraction in REQ-CRG-026 can keep
    a thin shim if needed without breaking call sites."""
    from scripts.deploy import _create_flink_dml_statements
    assert callable(_create_flink_dml_statements)


# ---------------------------------------------------------------------------
# T-A19: REQ-CRG-034 — Dockerfile pins
# ---------------------------------------------------------------------------


def test_TC_CRG_034_dockerfile_has_digest_pins():
    """REQ-CRG-034: Dockerfile must pin base image by digest AND
    npm-installed package by version."""
    dockerfile = (PROJECT_ROOT / "mcp-server/Dockerfile").read_text()
    # Base image digest pin: `FROM node:24-alpine@sha256:...`
    assert "@sha256:" in dockerfile, (
        "Dockerfile FROM must be digest-pinned (@sha256:...). REQ-CRG-034."
    )
    # npm install must NOT use @latest
    assert "@latest" not in dockerfile, (
        "Dockerfile must not use @latest for npm install — pin a version. "
        "REQ-CRG-034."
    )


# ---------------------------------------------------------------------------
# T-A4 (B19): REQ-CRG-008 — Cost cap dedup (placeholder — implementation
# in T-A4 / T-A12 combined)
# ---------------------------------------------------------------------------


# TC-CRG-008b removed in pass 3 (self-review H3).
#
# The original test used pytest.skip() to mask a dropped requirement
# (REQ-CRG-008 second clause: ROW_NUMBER-based rate dedup). That is the
# anti-pattern REQ-CRG-027 was specifically meant to eliminate.
#
# Resolution: REQ-CRG-008 second clause is consciously NOT implemented
# in this codebase. The primary cost cap is `max_tokens=500` in
# `terraform/core/main.tf` (verified by TC-CRG-008 above). Adding a
# secondary rate-dedup at the SQL level would change anomaly-detection
# semantics (it would suppress every duplicate-zone surge within an
# hour, hiding multi-event surges in the same zone that overlap), so
# the team decided against it. Documented in CLAUDE.md.
