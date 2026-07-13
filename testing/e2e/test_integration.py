#!/usr/bin/env python3
"""
Deployment Integration & Invariant Tests

Validates deploy, destroy, tfvars, data generation, entry points,
and regression invariants for the standalone project.

Test IDs map to: TC-DEPLOY-*, TC-DATAGEN-*, TC-ENTRY-*, TC-INV-*
"""

import ast
import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Import modules under test
sys.path.insert(0, str(PROJECT_ROOT))
from scripts.common.tfvars import (
    generate_agents_tfvars_content,
    generate_atlas_tfvars_content,
    generate_core_tfvars_content,
)


# -- TC-DEPLOY-003: tfvars generation ----------------------------------------
class TestTfvars:
    def test_generate_agents_tfvars_basic(self):
        """TC-DEPLOY-003: generate_agents_tfvars_content requires all credentials."""
        content = generate_agents_tfvars_content(
            mcp_server_url="https://mo-123.ecs.us-east-1.on.aws",
            mcp_auth_token="tok123",
            voyage_api_key="vk456",
            mongo_conn="mongodb+srv://test.mongodb.net/",
            mongo_user="testuser",
            mongo_pass="testpass",
        )
        assert 'mcp_server_url = "https://mo-123.ecs.us-east-1.on.aws"' in content
        assert 'mcp_auth_token = "tok123"' in content
        assert 'voyage_api_key = "vk456"' in content
        assert (
            'mongodb_connection_string = "mongodb+srv://test.mongodb.net/"' in content
        )
        assert 'mongodb_username = "testuser"' in content
        assert 'mongodb_password = "testpass"' in content

    def test_generate_agents_tfvars_with_mongo(self):
        """TC-DEPLOY-003b: generate_agents_tfvars_content with mongo credentials."""
        content = generate_agents_tfvars_content(
            mcp_server_url="https://mo-456.ecs.us-east-1.on.aws",
            mcp_auth_token="tok",
            voyage_api_key="vk",
            mongo_conn="mongodb+srv://host",
            mongo_user="admin",
            mongo_pass="secret",
        )
        assert 'mcp_server_url = "https://mo-456.ecs.us-east-1.on.aws"' in content
        assert 'mcp_auth_token = "tok"' in content
        assert 'voyage_api_key = "vk"' in content
        assert 'mongodb_connection_string = "mongodb+srv://host"' in content
        assert 'mongodb_username = "admin"' in content
        assert 'mongodb_password = "secret"' in content

    def test_generate_atlas_tfvars_emits_all_keys(self):
        """TC-TFVARS-ATLAS-001: standalone atlas module tfvars emits all atlas keys."""
        content = generate_atlas_tfvars_content(
            atlas_public_key="pub-AAAA",
            atlas_private_key="priv-BBBB",
            atlas_project_id="proj-9999",
            atlas_cluster_name="my-m10",
            atlas_db_username="streaming_agents_app",
            atlas_db_password="hunter2",
            cloud_region="us-east-1",
            owner_email="me@example.com",
        )
        assert 'atlas_public_key = "pub-AAAA"' in content
        assert 'atlas_private_key = "priv-BBBB"' in content
        assert 'atlas_project_id = "proj-9999"' in content
        assert 'atlas_cluster_name = "my-m10"' in content
        assert 'atlas_db_username = "streaming_agents_app"' in content
        assert 'atlas_db_password = "hunter2"' in content
        assert 'cloud_region = "us-east-1"' in content
        assert 'owner_email = "me@example.com"' in content

    def test_hcl_escape_neutralizes_dangerous_chars(self):
        """A credential value with quotes/backslashes/interpolation must not
        break out of the HCL string literal or alter the generated config."""
        from scripts.common.tfvars import _hcl_escape

        assert _hcl_escape('a"b') == 'a\\"b'
        assert _hcl_escape("a\\b") == "a\\\\b"
        assert _hcl_escape("p${var}") == "p$${var}"
        assert _hcl_escape("p%{x}") == "p%%{x}"
        assert _hcl_escape("line1\nline2") == "line1\\nline2"

    def test_agents_tfvars_escapes_password_with_quote(self):
        """A password containing a double-quote must be escaped, not emitted
        raw (which would terminate the HCL string and corrupt the file)."""
        content = generate_agents_tfvars_content(
            mcp_server_url="https://x",
            mcp_auth_token="tok",
            voyage_api_key="vk",
            mongo_conn="mongodb+srv://host",
            mongo_user="admin",
            mongo_pass='pa"ss\\word',
        )
        # The raw unescaped form must NOT appear on the password line.
        assert 'mongodb_password = "pa"ss\\word"' not in content
        # The escaped form must appear.
        assert 'mongodb_password = "pa\\"ss\\\\word"' in content

    def test_atlas_tfvars_escapes_interpolation_in_password(self):
        """A password containing ${...} must have the marker doubled so HCL
        does not treat it as a template interpolation."""
        content = generate_atlas_tfvars_content(
            atlas_public_key="pub",
            atlas_private_key="priv",
            atlas_project_id="proj",
            atlas_cluster_name="m10",
            atlas_db_username="u",
            atlas_db_password="p${secret}q",
        )
        assert 'atlas_db_password = "p$${secret}q"' in content

    def test_generate_core_tfvars_no_longer_emits_atlas(self):
        """TC-TFVARS-ATLAS-002: core tfvars never emits atlas keys (atlas moved to its own module)."""
        # Even when legacy callers pass atlas_* args, core tfvars must not contain them
        content = generate_core_tfvars_content(
            region="us-east-1",
            api_key="ck",
            api_secret="cs",
            create_atlas_cluster=True,  # legacy arg, ignored
            atlas_public_key="pub-AAAA",
            atlas_private_key="priv-BBBB",
            atlas_project_id="proj-9999",
            atlas_db_password="hunter2",
        )
        assert "atlas_public_key" not in content
        assert "atlas_private_key" not in content
        assert "atlas_db_password" not in content
        assert "create_atlas_cluster" not in content

    def test_generate_core_tfvars_backwards_compatible_signature(self):
        """TC-TFVARS-ATLAS-003: legacy call (no atlas args) still works (INV-204)."""
        content = generate_core_tfvars_content(
            region="us-east-1",
            api_key="ck",
            api_secret="cs",
        )
        assert 'cloud_region = "us-east-1"' in content
        assert 'confluent_cloud_api_key = "ck"' in content
        assert 'confluent_cloud_api_secret = "cs"' in content
        assert "atlas_public_key" not in content


# -- TC-ENTRY-001: pyproject.toml entry points --------------------------------
class TestEntryPoints:
    def test_entry_points_defined(self):
        """TC-ENTRY-001: pyproject.toml defines datagen, asp-setup, dashboard."""
        pyproject = (PROJECT_ROOT / "pyproject.toml").read_text()
        assert "datagen" in pyproject
        assert "asp-setup" in pyproject
        assert "dashboard" in pyproject
        assert "deploy" in pyproject
        assert "destroy" in pyproject


# -- Invariant / Regression Tests ---------------------------------------------


class TestInvariantTerraformValidate:
    """TC-INV-001 / TC-INV-002: Terraform modules validate."""

    @pytest.mark.parametrize(
        "tf_dir_name",
        [
            "atlas",
            "core",
            "agents",
        ],
    )
    def test_each_module_validates(self, tf_dir_name):
        """TC-INV-002: Each terraform module validates."""
        tf_dir = PROJECT_ROOT / "terraform" / tf_dir_name
        if not tf_dir.exists():
            pytest.skip(f"{tf_dir_name} terraform dir not found")
        if not (tf_dir / "main.tf").exists():
            pytest.skip(f"{tf_dir_name} has no main.tf")

        init = subprocess.run(
            ["terraform", "init", "-backend=false"],
            cwd=tf_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert (
            init.returncode == 0
        ), f"terraform init failed for {tf_dir_name}: {init.stderr}"

        result = subprocess.run(
            ["terraform", "validate"],
            cwd=tf_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            result.returncode == 0
        ), f"terraform validate failed for {tf_dir_name}: {result.stderr}"


class TestInvariantDeploy:
    def test_deploy_module_importable(self):
        """TC-INV-003: scripts.deploy module is importable."""
        deploy = importlib.import_module("scripts.deploy")
        assert callable(deploy.main)


class TestInvariantDestroy:
    def test_destroy_has_asp_teardown(self):
        """TC-INV-004: destroy.py includes ASP teardown code."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        assert (
            "run_asp_teardown" in source
        ), "destroy should import/call run_asp_teardown"
        assert "ASP" in source, "destroy should reference ASP teardown"


class TestDeployAtlasAdmin:
    def test_deploy_has_atlas_admin_references(self):
        """TC-DEPLOY-005: deploy.py references Atlas Admin API keys."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert "ATLAS_PUBLIC_KEY" in source, "deploy should check ATLAS_PUBLIC_KEY"
        assert "ATLAS_PRIVATE_KEY" in source, "deploy should check ATLAS_PRIVATE_KEY"
        assert "ATLAS_PROJECT_ID" in source, "deploy should check ATLAS_PROJECT_ID"


class TestMCPParallelBuild:
    """REQ-E-240..243 — MCP image build runs in parallel with cluster create."""

    def test_mcp_deploy_split_into_build_and_service(self):
        """TC-MCP-PAR-001: mcp_deploy exposes build_mcp_image and create_mcp_service."""
        mcp = importlib.import_module("scripts.mcp_deploy")
        assert hasattr(
            mcp, "build_mcp_image"
        ), "mcp_deploy must expose build_mcp_image (Phase A: ECR + Docker + IAM, no connection string)"
        assert hasattr(
            mcp, "create_mcp_service"
        ), "mcp_deploy must expose create_mcp_service (Phase B: ECS + ALB + health, needs connection string)"

    def test_build_mcp_image_signature_no_mongo(self):
        """TC-MCP-PAR-002: build_mcp_image accepts NO mongo_conn arg (parallel-safe)."""
        mcp = importlib.import_module("scripts.mcp_deploy")
        import inspect as _i

        sig = _i.signature(mcp.build_mcp_image)
        params = set(sig.parameters.keys())
        assert (
            "mongo_conn" not in params
        ), "build_mcp_image must NOT accept mongo_conn (it runs before cluster exists)"

    def test_create_mcp_service_takes_mongo_conn(self):
        """TC-MCP-PAR-003: create_mcp_service requires mongo_conn."""
        mcp = importlib.import_module("scripts.mcp_deploy")
        import inspect as _i

        sig = _i.signature(mcp.create_mcp_service)
        params = set(sig.parameters.keys())
        assert "mongo_conn" in params, "create_mcp_service must accept mongo_conn"

    def test_deploy_uses_threading_when_creating_cluster(self):
        """TC-MCP-PAR-004: deploy.py spawns a Thread for the MCP image build when creating a cluster.

        REQ-CRF-051 (brittle-test conversion): the previous source-grep
        for `"Thread" or "threading"` passed if the word appeared in a
        docstring. Now we AST-walk the helper and assert it actually
        instantiates a Thread.
        """
        deploy = importlib.import_module("scripts.deploy")
        # Must export a helper that starts the parallel build.
        assert hasattr(
            deploy, "_start_mcp_image_build_async"
        ), "deploy must define _start_mcp_image_build_async"
        # The helper must reference threading.Thread (AST-level, not just
        # a string match) AND call build_mcp_image.
        import ast as _ast

        fn_src = inspect.getsource(deploy._start_mcp_image_build_async)
        tree = _ast.parse(fn_src)
        thread_calls = [
            n
            for n in _ast.walk(tree)
            if isinstance(n, _ast.Call)
            and isinstance(n.func, _ast.Attribute)
            and n.func.attr == "Thread"
        ]
        assert thread_calls, (
            "_start_mcp_image_build_async must instantiate a Thread "
            "(REQ-CRF-051 / H19 brittle conversion)."
        )
        # Helper imports build_mcp_image from mcp_deploy.
        source = inspect.getsource(deploy)
        assert (
            "build_mcp_image" in source
        ), "deploy must call build_mcp_image (the parallel-safe phase)"
        assert (
            "create_mcp_service" in source
        ), "deploy must call create_mcp_service after cluster IDLE"


class TestDeployAtlasClusterProvisioning:
    """REQ-E-220..223 — optional Atlas cluster provisioning via deploy."""

    def test_prompt_offers_create_or_byo_branches(self):
        """TC-DEPLOY-ATLAS-001: prompt_mongodb_atlas offers create vs BYO branches."""
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy.prompt_mongodb_atlas)
        # Both choice strings must appear in the prompt source
        assert (
            "Create" in src and "M10" in src
        ), "prompt must offer 'Create a new M10 cluster' option"
        assert (
            "existing" in src.lower()
            or "own" in src.lower()
            or "BYO" in src.upper()
            or "provide" in src.lower()
        ), "prompt must offer the BYO/existing-cluster option"

    def test_create_branch_generates_password(self):
        """TC-DEPLOY-ATLAS-002: create branch generates a random password
        and saves TF_VAR_atlas_db_password.

        REQ-CRF-051 brittle conversion: source-grep for `"secrets"` was
        too permissive (matched docstrings, the literal word in any
        comment). Verify the password-gen helper uses the secrets module
        AT FUNCTION LEVEL via AST and that it persists the result.
        """
        deploy = importlib.import_module("scripts.deploy")
        helper = getattr(deploy, "_gen_atlas_password", None)
        assert callable(helper), "deploy must define a _gen_atlas_password helper"
        # The helper must use secrets.token_* via AST (not just any
        # function whose source contains the word "secrets").
        helper_src = inspect.getsource(helper)
        helper_tree = ast.parse(helper_src)
        secrets_calls = [
            n
            for n in ast.walk(helper_tree)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "secrets"
            and n.func.attr.startswith("token_")
        ]
        assert secrets_calls, (
            "_gen_atlas_password must call secrets.token_hex / token_urlsafe "
            "(not just reference the word 'secrets')"
        )
        # And the run_deployment flow must persist the result.
        source = inspect.getsource(deploy)
        assert (
            "TF_VAR_atlas_db_password" in source
        ), "deploy must persist TF_VAR_atlas_db_password to .env"

    def test_provision_helper_reads_terraform_output(self):
        """TC-DEPLOY-ATLAS-003: deploy has helper that reads atlas_cluster_connection_string."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "atlas_cluster_connection_string" in source
        ), "deploy should read atlas_cluster_connection_string output from terraform"
        # It should persist the result to TF_VAR_mongodb_connection_string
        assert (
            "TF_VAR_mongodb_connection_string" in source
        ), "deploy should persist connection string to .env"

    def test_byo_branch_sets_create_atlas_cluster_false(self):
        """TC-DEPLOY-ATLAS-004: BYO branch persists create_atlas_cluster=false.

        REQ-CRF-051 brittle conversion: assert that the prompt helper
        writes the expected pair instead of grepping for a `"false"`
        literal that could appear anywhere.
        """
        deploy = importlib.import_module("scripts.deploy")
        # The prompt that writes create_atlas_cluster must reference both
        # the variable name and the literal "false" within the same
        # function body (not just the same module).
        helper = getattr(deploy, "prompt_mongodb_atlas", None)
        assert callable(helper), "prompt_mongodb_atlas must exist"
        helper_src = inspect.getsource(helper)
        assert (
            "TF_VAR_create_atlas_cluster" in helper_src
        ), "prompt_mongodb_atlas must reference TF_VAR_create_atlas_cluster"
        # The BYO branch (where the user picks an existing cluster) must
        # set the variable to "false" — look for that specific assignment.
        assert (
            '"TF_VAR_create_atlas_cluster": "false"' in helper_src
            or "'TF_VAR_create_atlas_cluster': 'false'" in helper_src
        ), (
            "BYO branch must persist `TF_VAR_create_atlas_cluster=false`. "
            "REQ-CRF-051 brittle conversion."
        )


# REQ-CRF-060: TestDestroyASPTeardown removed — the
# test_destroy_has_asp_teardown body was identical to the one already
# defined in TestInvariantDestroy at line 169. Single source of truth.


class TestDestroyAtlasClusterCleanup:
    """REQ-E-231 — destroy clears deploy-managed Atlas creds."""

    def test_destroy_clears_atlas_managed_creds(self):
        """TC-DESTROY-ATLAS-001: when create_atlas_cluster=true, destroy clears the deploy-generated mongo creds."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        # The stale-credentials cleanup must conditionally include the
        # mongo connection-string / username / password keys when the
        # deploy was the one that generated them.
        assert (
            "TF_VAR_create_atlas_cluster" in source
        ), "destroy should check TF_VAR_create_atlas_cluster"
        assert (
            "TF_VAR_mongodb_connection_string" in source
        ), "destroy should clear TF_VAR_mongodb_connection_string when cluster was Terraform-managed"
        assert (
            "TF_VAR_atlas_db_password" in source
        ), "destroy should clear TF_VAR_atlas_db_password (was generated by deploy)"


class TestOrphanAgentsStatementSweep:
    """BUG-307: TF-managed Flink statements existing server-side but not in
    state cause `terraform apply` to fail with HTTP 409 'already exists'."""

    def test_sweep_helper_exists(self):
        deploy = importlib.import_module("scripts.deploy")
        assert hasattr(
            deploy, "_sweep_orphan_agents_statements"
        ), "deploy must expose _sweep_orphan_agents_statements"
        assert hasattr(
            deploy, "_AGENTS_TF_STATEMENT_NAMES"
        ), "deploy must expose _AGENTS_TF_STATEMENT_NAMES list"

    def test_sweep_covers_all_terraform_statements(self):
        """The sweep list must include every confluent_flink_statement
        defined in terraform/agents/main.tf."""
        deploy = importlib.import_module("scripts.deploy")
        agents_main = (PROJECT_ROOT / "terraform" / "agents" / "main.tf").read_text()
        # Extract statement_name = "..." values from the .tf file
        import re as _re

        tf_names = set(_re.findall(r'statement_name\s*=\s*"([^"]+)"', agents_main))
        sweep_names = set(deploy._AGENTS_TF_STATEMENT_NAMES)
        missing = tf_names - sweep_names
        assert not missing, (
            f"_AGENTS_TF_STATEMENT_NAMES is missing {missing}. "
            f"Every TF-managed statement must be sweepable."
        )

    def test_sweep_invoked_before_agents_apply(self):
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "_sweep_orphan_agents_statements(root)" in source
        ), "deploy must call _sweep_orphan_agents_statements before applying agents"

    def test_sweep_skips_when_no_core_state(self, tmp_path):
        """No core state → no Flink creds → no-op (no crash)."""
        deploy = importlib.import_module("scripts.deploy")
        # Layout exists but core has no state file
        (tmp_path / "terraform" / "core").mkdir(parents=True)
        (tmp_path / "terraform" / "agents").mkdir(parents=True)
        result = deploy._sweep_orphan_agents_statements(tmp_path)
        assert result == 0

    def test_sweep_skips_names_already_in_state(self, tmp_path, monkeypatch):
        """Names tracked in agents/terraform.tfstate must NOT be deleted."""
        import json as _json

        deploy = importlib.import_module("scripts.deploy")

        core = tmp_path / "terraform" / "core"
        agents = tmp_path / "terraform" / "agents"
        core.mkdir(parents=True)
        agents.mkdir(parents=True)
        (core / "terraform.tfstate").write_text("{}")
        # Agents state lists 2 of the 11 — the other 9 are candidates
        (agents / "terraform.tfstate").write_text(
            _json.dumps(
                {
                    "resources": [
                        {
                            "type": "confluent_flink_statement",
                            "instances": [
                                {
                                    "attributes": {
                                        "statement_name": "mongodb-mcp-connection-create"
                                    },
                                },
                                {
                                    "attributes": {
                                        "statement_name": "mongodb-mcp-model-create"
                                    },
                                },
                            ],
                        }
                    ],
                }
            )
        )

        # Stub `terraform output -json` so we have flink creds
        class _R:
            returncode = 0
            stdout = _json.dumps(
                {
                    "app_manager_flink_api_key": {"value": "k"},
                    "app_manager_flink_api_secret": {"value": "s"},
                    "confluent_organization_id": {"value": "org-1"},
                    "confluent_environment_id": {"value": "env-1"},
                    "confluent_flink_rest_endpoint": {"value": "https://flink"},
                }
            )
            stderr = ""

        monkeypatch.setattr(deploy.subprocess, "run", lambda *a, **kw: _R())

        # All HTTP calls return 404 (nothing on server) → 0 deletions
        import urllib.request

        class _NotFound(Exception):
            code = 404

            def read(self):
                return b""

            fp = None

        def _raise404(*a, **kw):
            raise urllib.error.HTTPError(
                url="x", code=404, msg="NF", hdrs=None, fp=None
            )

        monkeypatch.setattr(urllib.request, "urlopen", _raise404)

        result = deploy._sweep_orphan_agents_statements(tmp_path)
        assert result == 0


class TestStaleAgentsStateQuarantine:
    """Defense against stale agents state pinned to a destroyed Confluent env."""

    def test_quarantine_helper_exists(self):
        """TC-DEPLOY-DRIFT-001: _quarantine_stale_agents_state helper exists."""
        deploy = importlib.import_module("scripts.deploy")
        assert hasattr(
            deploy, "_quarantine_stale_agents_state"
        ), "deploy must expose _quarantine_stale_agents_state to remediate env-id drift"

    def test_quarantine_invoked_before_agents_apply(self):
        """TC-DEPLOY-DRIFT-002: deploy invokes the quarantine helper for the agents env."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "_quarantine_stale_agents_state(root)" in source
        ), "deploy must call _quarantine_stale_agents_state before applying agents"

    def test_quarantine_renames_does_not_delete(self, tmp_path, monkeypatch):
        """TC-DEPLOY-DRIFT-003: quarantine renames stale state files, never deletes them."""
        deploy = importlib.import_module("scripts.deploy")

        # Build a fake project layout with stale env-id in agents state
        core = tmp_path / "terraform" / "core"
        agents = tmp_path / "terraform" / "agents"
        core.mkdir(parents=True)
        agents.mkdir(parents=True)
        (core / "terraform.tfstate").write_text('{"current": true}')
        agents_state = agents / "terraform.tfstate"
        agents_state.write_text('{"id": "env-OLD123/lfcp-x/foo"}')
        (agents / "terraform.tfstate.backup").write_text("backup")

        # REQ-CRG-024 (C-1): _quarantine_stale_agents_state now uses
        # `get_core_outputs(root)` instead of an inline subprocess call.
        # Stub the helper to return the new env-id.
        from scripts.common import terraform_outputs as tf_outputs

        tf_outputs._clear_cache()
        monkeypatch.setattr(
            tf_outputs,
            "get_core_outputs",
            lambda root: {"confluent_environment_id": {"value": "env-NEW999"}},
        )
        # Also stub the deploy.py import path (some test runs may have
        # already-resolved references).
        monkeypatch.setattr(
            "scripts.common.terraform_outputs.get_core_outputs",
            lambda root: {"confluent_environment_id": {"value": "env-NEW999"}},
        )

        moved = deploy._quarantine_stale_agents_state(tmp_path)
        assert moved is True
        assert not agents_state.exists(), "stale state must be moved aside"
        # Both files preserved with the .stale-* suffix
        leftovers = sorted(p.name for p in agents.iterdir())
        assert any(
            ".tfstate.stale-" in n for n in leftovers
        ), f"expected quarantined files, got {leftovers}"

    def test_quarantine_no_op_when_env_matches(self, tmp_path, monkeypatch):
        """TC-DEPLOY-DRIFT-004: when env-id matches, state is left alone."""
        deploy = importlib.import_module("scripts.deploy")

        core = tmp_path / "terraform" / "core"
        agents = tmp_path / "terraform" / "agents"
        core.mkdir(parents=True)
        agents.mkdir(parents=True)
        (core / "terraform.tfstate").write_text('{"current": true}')
        agents_state = agents / "terraform.tfstate"
        agents_state.write_text('{"id": "env-CURRENT1/lfcp-x/foo"}')

        class _R:
            returncode = 0
            stdout = "env-CURRENT1\n"
            stderr = ""

        monkeypatch.setattr(deploy.subprocess, "run", lambda *a, **kw: _R())

        moved = deploy._quarantine_stale_agents_state(tmp_path)
        assert moved is False
        assert agents_state.exists(), "matching state must NOT be moved"


class TestDashboardPortFallback:
    """BUG-305: don't assume an in-use port is our dashboard."""

    def test_find_free_dashboard_port_helper_exists(self):
        deploy = importlib.import_module("scripts.deploy")
        assert hasattr(
            deploy, "_find_free_dashboard_port"
        ), "deploy must expose _find_free_dashboard_port()"

    def test_launch_dashboard_uses_fallback_port_search(self):
        """deploy._launch_dashboard scans for a free port instead of opening browser to whatever is bound."""
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._launch_dashboard)
        assert (
            "_find_free_dashboard_port" in src
        ), "deploy must call _find_free_dashboard_port from _launch_dashboard"
        # Sanity: don't blindly open a browser when port is in use without verifying
        assert (
            "Opening existing dashboard" not in src
        ), "deploy must not assume an in-use port is our dashboard"

    def test_find_free_port_returns_first_free(self, monkeypatch):
        deploy = importlib.import_module("scripts.deploy")
        # Pretend 8501 + 8502 are in use; 8503 free
        in_use = {8501, 8502}
        monkeypatch.setattr(deploy, "_is_port_in_use", lambda p: p in in_use)
        assert deploy._find_free_dashboard_port(8501, 8510) == 8503

    def test_find_free_port_returns_none_when_all_taken(self, monkeypatch):
        deploy = importlib.import_module("scripts.deploy")
        monkeypatch.setattr(deploy, "_is_port_in_use", lambda p: True)
        assert deploy._find_free_dashboard_port(8501, 8510) is None


class TestDispatchInsertMCPGate:
    """BUG-306: dispatch-insert needs MCP healthy at submit time."""

    def test_deploy_probes_mcp_before_dispatch_insert(self):
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._create_flink_dml_statements)
        # Probe + delayed submit must both be present
        assert (
            "_check_mcp_health" in src
        ), "deploy must probe MCP before submitting dispatch-insert"
        assert (
            "MCP_DEPENDENT" in src or '"dispatch-insert"' in src
        ), "deploy must single out dispatch-insert as MCP-dependent"

    def test_dispatch_insert_submitted_after_other_dml(self):
        """REQ-CRG-027 / Phase D-1 (behavior conversion): dispatch-insert
        must SUBMIT only AFTER the early DML loop has run. Verified by
        AST analysis of the late-DML / MCP-dependent block — early-DML
        loop body line must precede the late-DML / dispatch-insert
        submit line, AND the MCP gating predicate must appear between
        them. The previous string-find variant would have passed even
        if a refactor reversed the order but kept the literal strings.
        """
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._create_flink_dml_statements)
        # Step 1: early DML loop must exist and submit the non-dispatch
        # DML names. Step 2: an MCP health check must occur. Step 3:
        # the late_dml / MCP-dependent block (containing dispatch-insert)
        # must execute AFTER the gate.
        lines = src.splitlines()
        early_loop_line = None
        mcp_health_line = None
        late_loop_line = None
        for i, line in enumerate(lines):
            if early_loop_line is None and "for stmt_name in early_dml" in line:
                early_loop_line = i
            if mcp_health_line is None and (
                "_check_mcp_health" in line or "MCP_DEPENDENT" in line
            ):
                mcp_health_line = i
            if late_loop_line is None and "for stmt_name in late_dml" in line:
                late_loop_line = i
        assert early_loop_line is not None, "early_dml loop missing"
        assert late_loop_line is not None, "late_dml loop missing"
        # Order constraint: early < late, with MCP health check before
        # late (it gates whether dispatch-insert runs at all).
        assert early_loop_line < late_loop_line, (
            "early DML loop must come before late DML loop in source order. "
            "REQ-CRG-027."
        )
        if mcp_health_line is not None:
            assert mcp_health_line < late_loop_line, (
                "MCP health check must precede the late DML loop "
                "(it's the gate). REQ-CRG-027."
            )


class TestMCPHealthCheckTimeout:
    """ECS Express cold image pulls regularly take 10-12 min; bump health timeout.

    Raised in BUG-304 from 480s → 720s after observing repeated 466s timeouts.
    """

    def test_health_timeout_at_least_12_minutes(self):
        """TC-BUG-304: HEALTH_CHECK_TIMEOUT raised to 12 minutes (720s)."""
        mcp = importlib.import_module("scripts.mcp_deploy")
        assert (
            mcp.HEALTH_CHECK_TIMEOUT >= 720
        ), f"HEALTH_CHECK_TIMEOUT must be >=720s for ECS Express cold pulls; got {mcp.HEALTH_CHECK_TIMEOUT}"


class TestPhantomCatalogTableProtection:
    """BUG-302: phantom raw-byte catalog tables block CTAS."""

    def test_ensure_topics_excludes_ctas_managed(self):
        """TC-BUG-302a: deploy must NOT pre-create CTAS-managed topics.

        anomalies_enriched and completed_actions are created by their CTAS
        DDL. Pre-creating the topic causes Confluent to register a phantom
        raw-byte catalog table that blocks CTAS.
        """
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._create_flink_dml_statements)
        # The topic list inside _ensure_flink_topics must not include CTAS-managed topics
        # Look for the topics list literal
        # The list is defined inline; find it and assert exclusions
        topic_list_section = src.split("topics = [")[1].split("]")[0]
        assert (
            '"anomalies_enriched"' not in topic_list_section
        ), "BUG-302: anomalies_enriched must not be pre-created (CTAS owns it)"
        assert (
            '"completed_actions"' not in topic_list_section
        ), "BUG-302: completed_actions must not be pre-created (CTAS owns it)"
        # Sanity: other 5 must still be there
        for topic in (
            "ride_requests",
            "windowed_traffic",
            "anomalies_per_zone",
            "zone_traffic_sink",
            "anomalies_sink",
        ):
            assert (
                f'"{topic}"' in topic_list_section
            ), f"INV-302a: {topic} must continue to be pre-created"

    def test_phantom_drop_before_ctas(self):
        """TC-BUG-302b: deploy drops phantom catalog tables for CTAS targets before DDL."""
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._create_flink_dml_statements)
        # Must issue DROP TABLE IF EXISTS for the CTAS-managed tables
        assert (
            "DROP TABLE IF EXISTS" in src
        ), "BUG-302: deploy must drop phantom catalog tables before CTAS"
        assert (
            "anomalies_enriched" in src
        ), "BUG-302: phantom-drop must cover anomalies_enriched"
        assert (
            "completed_actions" in src
        ), "BUG-302: phantom-drop must cover completed_actions"


class TestAgentCreatedAtDeploy:
    """BUG-303: dispatch-insert needs boat_dispatch_agent at deploy time."""

    def test_deploy_creates_agent_and_tool(self):
        """TC-BUG-303a: deploy.py creates the tool and agent before dispatch-insert."""
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy)
        assert (
            "AGENT_SQL_CREATE_TOOL" in src or "create-tool-mongodb-fleet" in src
        ), "BUG-303: deploy must reference the CREATE TOOL step"
        assert (
            "AGENT_SQL_CREATE_AGENT" in src or "create-agent-boat-dispatch" in src
        ), "BUG-303: deploy must reference the CREATE AGENT step"

    def test_dashboard_sql_idempotent(self):
        """TC-BUG-303b: dashboard SQL uses IF NOT EXISTS so deploy + dashboard converge."""
        dashboard = importlib.import_module("scripts.dashboard")
        assert (
            "IF NOT EXISTS" in dashboard.AGENT_SQL_CREATE_TOOL
        ), "BUG-303: AGENT_SQL_CREATE_TOOL must be idempotent (IF NOT EXISTS)"
        assert (
            "IF NOT EXISTS" in dashboard.AGENT_SQL_CREATE_AGENT
        ), "BUG-303: AGENT_SQL_CREATE_AGENT must be idempotent (IF NOT EXISTS)"

    def test_deploy_imports_dashboard_sql(self):
        """TC-BUG-303c: deploy imports SQL from dashboard (single source of truth)."""
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy)
        # Either an import line or a usage referencing dashboard's symbols
        assert (
            "from scripts.dashboard import" in src
            or "scripts.dashboard.AGENT_SQL" in src
        ), "BUG-303: deploy must import AGENT_SQL constants from dashboard, not duplicate them"


class TestDestroyIncludeCluster:
    """REQ-E-253 — destroy preserves the atlas module by default; --include-cluster opts in."""

    def test_destroy_has_include_cluster_flag(self):
        """TC-DESTROY-INCLUDE-001: destroy.py defines --include-cluster flag."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        assert (
            "--include-cluster" in source
        ), "destroy must define --include-cluster flag"
        assert "include_cluster" in source, "destroy must use args.include_cluster"

    def test_destroy_skips_atlas_by_default(self):
        """TC-DESTROY-INCLUDE-002: by default, atlas module is NOT in the destroy list."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        # The default destroy envs must be agents + core (atlas is opt-in)
        # If we see a literal envs list, "atlas" should appear only in the
        # gated branch.
        assert (
            'envs = ["agents", "core"]' in source or '"agents", "core"' in source
        ), "default destroy envs should be agents + core only"
        # And the atlas module should be appended only when the flag is set
        assert (
            "atlas" in source
        ), "destroy must reference the atlas module under the include-cluster branch"


class TestDeployASPPostTerraform:
    def test_deploy_has_asp_post_terraform(self):
        """TC-DEPLOY-005c: deploy.py has ASP post-terraform function."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert "run_asp_setup" in source, "deploy should call run_asp_setup"


class TestDeployCredentialPersistence:
    def test_deploy_saves_terraform_credentials(self):
        """TC-DEPLOY-007: deploy.py saves Kafka/SR credentials to .env."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "_save_terraform_credentials" in source
        ), "deploy should have _save_terraform_credentials function"
        assert (
            "CONFLUENT_BOOTSTRAP_SERVER" in source
        ), "deploy should persist Kafka bootstrap server"
        assert (
            "CONFLUENT_KAFKA_API_KEY" in source
        ), "deploy should persist Kafka API key"
        assert (
            "CONFLUENT_SCHEMA_REGISTRY_URL" in source
        ), "deploy should persist Schema Registry URL"


class TestDeployFlinkStatements:
    def test_deploy_waits_for_deletion_before_recreate(self):
        """TC-DEPLOY-008: deploy.py polls for deletion before recreating statements."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "_wait_for_deletion" in source
        ), "deploy should have _wait_for_deletion helper"
        assert (
            "_delete_and_wait" in source
        ), "deploy should have _delete_and_wait helper"
        assert "404" in source, "deletion polling should check for 404 status"

    def test_deploy_separates_ddl_and_dml(self):
        """TC-DEPLOY-009: deploy.py handles DDL and DML statements separately."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert "DDL_STATEMENTS" in source, "deploy should have DDL_STATEMENTS list"
        assert "DML_STATEMENTS" in source, "deploy should have DML_STATEMENTS list"
        assert (
            "anomalies-enriched-ctas" in source
        ), "DDL list should contain anomalies-enriched-ctas"
        assert (
            "anomalies-enriched-insert" in source
        ), "DML list should contain anomalies-enriched-insert"

    def test_deploy_waits_for_running(self):
        """TC-DEPLOY-010 / Phase D-2 (behavior conversion): deploy.py
        must wait for DML statements to reach RUNNING state.

        Original test only checked `"RUNNING" in source` — would pass
        for any module containing the literal RUNNING anywhere
        (including comments). Behavior test verifies the wait-for-phase
        logic exists at expected indices.
        """
        deploy = importlib.import_module("scripts.deploy")
        src = inspect.getsource(deploy._create_flink_dml_statements)
        # Must reference RUNNING as the expected DML phase.
        assert '"RUNNING"' in src, "Expected RUNNING literal in DML helper"
        # AND must use a phase-wait construct (either time.sleep loop
        # checking phase, or FlinkRestClient.wait_for_phase call).
        assert (
            "wait_for_phase" in src or "elapsed" in src.lower()  # legacy inline loop
        ), (
            "deploy must implement a phase-wait loop or call "
            "FlinkRestClient.wait_for_phase. REQ-CRG-028."
        )


# ── TC-BUG-002: Flink DML status reporting ─────────────────────────────────
class TestFlinkDMLStatusReporting:
    """Tests for BUG-002: deploy.py must not report success when statements FAILED."""

    def test_deploy_tracks_failed_statements_separately(self):
        """TC-BUG-002a: deploy.py tracks failed DML statements separately.

        REQ-CRF-051 brittle conversion: source-grep for `"failed"` is
        meaningless (matches comments, docstrings, any non-trivial
        module). Use AST to verify _create_flink_dml_statements builds
        a list/set named *failed* / *failures* used in a conditional.
        """
        deploy = importlib.import_module("scripts.deploy")
        # The DML helper must exist and reference a "failed" tracking
        # variable in a meaningful way (assignment, not just text).
        import ast as _ast

        fn_src = inspect.getsource(deploy._create_flink_dml_statements)
        tree = _ast.parse(fn_src)
        # Walk for an Assign / AugAssign whose target name contains "failed".
        failed_assigns = [
            n
            for n in _ast.walk(tree)
            if isinstance(n, (_ast.Assign, _ast.AugAssign))
            and any(
                "failed" in (getattr(t, "id", "") or "").lower()
                for t in (n.targets if isinstance(n, _ast.Assign) else [n.target])
            )
        ]
        assert failed_assigns, (
            "_create_flink_dml_statements must assign to a *failed* "
            "tracking variable. Source-grep for the literal word was "
            "too permissive (REQ-CRF-051)."
        )

    def test_deploy_success_requires_no_failures(self):
        """TC-BUG-002b: success message only prints when no statements failed."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        # The success message condition must check both pending AND failed
        assert (
            "not failed" in source
        ), "success condition must verify no failures occurred"
        assert (
            "failed.add" in source or "failed |=" in source
        ), "failed statements must be tracked in a separate collection"

    def test_deploy_reports_running_when_all_succeed(self):
        """TC-INV-005: success message still prints when all DML statements are RUNNING."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "All DML statements are RUNNING" in source
        ), "deploy must still have the success message"

    def test_deploy_reports_timeout(self):
        """TC-INV-006: timeout warning still prints when statements are still pending."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert "Timed out waiting for" in source, "deploy must still warn on timeout"


# ── TC-BUG-003: Flink DML recreation logic ─────────────────────────────────
class TestFlinkDMLRecreation:
    """Tests for BUG-003: deploy.py must properly wait for deletion and not
    create on timeout."""

    def test_wait_for_deletion_only_trusts_404(self):
        """TC-BUG-003a / REQ-CRG-027 Phase D-1 (behavior conversion):
        wait_for_deletion (now in FlinkRestClient.delete_and_wait) must
        return only when it sees an actual 404, NOT on transport errors
        that could mean the statement still exists.

        Original test asserted `"assume deleted" not in source` — that
        only protected against one historical bug-specific comment.
        This test exercises the timeout-on-500 path and asserts the
        function does NOT silently succeed."""
        import urllib.error
        from unittest import mock as _mock

        from scripts.common.flink_rest import FlinkRestClient

        client = FlinkRestClient(
            rest_endpoint="https://x",
            api_key="k",
            api_secret="s",
            org_id="o",
            env_id="e",
            compute_pool_id="p",
            service_account_id="sa",
            catalog="c",
            database="d",
        )

        # Stub the polling urlopen to ALWAYS return HTTPError(500)
        # (transport / server error — NOT 404). The function must
        # NOT treat this as "deleted" and must raise TimeoutError on
        # the timeout fallthrough.
        def fake_urlopen(*args, **kwargs):
            raise urllib.error.HTTPError(
                url="x",
                code=500,
                msg="server",
                hdrs=None,
                fp=None,
            )

        with (
            _mock.patch.object(FlinkRestClient, "_delete", return_value=None),
            _mock.patch(
                "scripts.common.flink_rest.urllib.request.urlopen",
                side_effect=fake_urlopen,
            ),
            _mock.patch("scripts.common.flink_rest.time.sleep", return_value=None),
        ):
            import pytest

            with pytest.raises(TimeoutError):
                client.delete_and_wait("x", timeout=0.1)
        # PASS condition: raised TimeoutError. If it had silently
        # returned (the bug), the with-raises would have failed.

    def test_delete_and_wait_returns_status(self):
        """TC-BUG-003b / Phase D-1 (behavior): delete_and_wait must
        return None on confirmed 404 (clean deletion) and raise
        TimeoutError on poll timeout."""
        import urllib.error
        from unittest import mock as _mock

        from scripts.common.flink_rest import FlinkRestClient

        client = FlinkRestClient(
            rest_endpoint="https://x",
            api_key="k",
            api_secret="s",
            org_id="o",
            env_id="e",
            compute_pool_id="p",
            service_account_id="sa",
            catalog="c",
            database="d",
        )

        # 404 on first poll → clean deletion → returns None
        def fake_urlopen_404(*args, **kwargs):
            raise urllib.error.HTTPError(
                url="x",
                code=404,
                msg="not found",
                hdrs=None,
                fp=None,
            )

        with (
            _mock.patch.object(FlinkRestClient, "_delete", return_value=None),
            _mock.patch(
                "scripts.common.flink_rest.urllib.request.urlopen",
                side_effect=fake_urlopen_404,
            ),
            _mock.patch("scripts.common.flink_rest.time.sleep", return_value=None),
        ):
            result = client.delete_and_wait("x", timeout=5)
            assert result is None, "clean 404 must return None"

    def test_submit_statement_aborts_on_deletion_timeout(self):
        """TC-BUG-003c: _submit_statement does not create when deletion timed out."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        # The create should be conditional on successful deletion
        assert (
            "if not _delete_and_wait" in source
        ), "_submit_statement must check _delete_and_wait return value"

    def test_submit_creates_when_not_exists(self):
        """TC-INV-007: statement is created directly when it doesn't exist (404)."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert "404" in source, "deploy must handle 404 (not found) to create fresh"


class TestDestroyFlinkStatements:
    def test_destroy_deletes_all_flink_statements(self):
        """TC-DEPLOY-011: destroy.py covers all DML + DDL Flink statement names.

        REQ-CRF-051 (brittle-test conversion): rather than source-grep
        for literals, assert against the canonical statement registry
        imported from scripts.common.flink_statements (REQ-CRF-040 / M7).
        This catches drift in BOTH directions: if a new statement is
        added to deploy.py without registering it, this test fails.
        """
        from scripts.common.flink_statements import (
            ALL_DELETABLE_STATEMENTS,
            DDL_STATEMENTS,
            DML_STATEMENTS,
        )

        # The canonical registry must list every statement the destroy
        # path needs to clean up.
        expected = {
            "anomalies-sink-insert",
            "anomalies-enriched-insert",
            "anomalies-enriched-ctas",
            "anomaly-detection-insert",
            "zone-traffic-sink-insert",
            "completed-actions-ctas",
            "dispatch-insert",
        }
        registered = set(DDL_STATEMENTS) | set(DML_STATEMENTS)
        missing = expected - registered
        assert not missing, (
            f"flink_statements registry is missing: {missing}. "
            "Update scripts/common/flink_statements.py."
        )
        # And destroy.py must reference the canonical registry.
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        assert (
            "ALL_DELETABLE_STATEMENTS" in source
            or "from scripts.common.flink_statements" in source
        ), (
            "destroy.py must import statement names from "
            "scripts.common.flink_statements (REQ-CRF-040)."
        )
        # The full deletable set must include both DDL and DML.
        for stmt in expected:
            assert (
                stmt in ALL_DELETABLE_STATEMENTS
            ), f"{stmt!r} must appear in ALL_DELETABLE_STATEMENTS"


# ── Iteration 2: Pipeline Hardening ──────────────────────────────────────────


class TestDeployTopicPreCreation:
    """Tests for REQ-E-001: deploy.py pre-creates Kafka topics before Flink DML."""

    def test_deploy_has_ensure_flink_topics(self):
        """TC-E-001a: deploy.py has _ensure_flink_topics helper.

        REQ-CRF-051 (brittle-test conversion): AST-walk for the function
        definition rather than source-grep — `_ensure_flink_topics` is a
        nested helper inside `_create_flink_dml_statements`, so getattr
        doesn't reach it. The AST check ensures it's actually a function
        def, not just a string somewhere.
        """
        deploy = importlib.import_module("scripts.deploy")
        tree = ast.parse(inspect.getsource(deploy))
        helpers = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "_ensure_flink_topics"
        ]
        assert helpers, (
            "deploy must define a function named _ensure_flink_topics. "
            "Source-grep would have passed for a docstring or comment "
            "containing the name — the AST check is stricter."
        )
        # The helper must take credential parameters (not be a no-arg stub).
        helper = helpers[0]
        param_names = [a.arg for a in helper.args.args]
        assert len(param_names) >= 4, (
            f"_ensure_flink_topics expected to take Kafka REST creds; "
            f"got params {param_names}"
        )

    def test_deploy_topic_list_includes_ride_requests(self):
        """TC-E-001b: topic pre-creation list includes key Flink topics."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        # The function must reference the key topics that Flink reads
        assert "ride_requests" in source, "deploy should reference ride_requests topic"
        assert (
            "zone_traffic_sink" in source
        ), "deploy should reference zone_traffic_sink topic"
        assert (
            "anomalies_sink" in source
        ), "deploy should reference anomalies_sink topic"


class TestDeployRetryLogic:
    """Tests for REQ-E-002: deploy.py retries Flink statement creation."""

    def test_deploy_has_retry_in_submit(self):
        """TC-E-002a / Phase D-2 (behavior conversion): _post_with_retry
        on FlinkRestClient must retry on 429/5xx with backoff.

        Original test grepped for `"retry"` or `"attempt"` anywhere —
        matched comments and unrelated words. Behavior test stubs the
        underlying _post to fail twice then succeed, asserts 3 attempts.
        """
        import urllib.error
        from unittest import mock as _mock

        from scripts.common.flink_rest import FlinkRestClient

        client = FlinkRestClient(
            rest_endpoint="https://x",
            api_key="k",
            api_secret="s",
            org_id="o",
            env_id="e",
            compute_pool_id="p",
            service_account_id="sa",
            catalog="c",
            database="d",
        )
        attempts = {"n": 0}

        def fake_post(url, body, timeout=None):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise urllib.error.HTTPError(
                    url="x",
                    code=503,
                    msg="server",
                    hdrs=None,
                    fp=None,
                )
            return {"ok": True}

        with (
            _mock.patch.object(FlinkRestClient, "_post", side_effect=fake_post),
            _mock.patch("scripts.common.flink_rest.time.sleep", return_value=None),
        ):
            result = client._post_with_retry("https://x", b"{}")
        assert result == {"ok": True}
        assert attempts["n"] == 3, (
            f"expected 3 attempts (2 retries + 1 success); got {attempts['n']}. "
            "REQ-CRG-028."
        )


class TestDeployExecutionOrder:
    """Tests for REQ-E-003: deploy.py publishes data before creating Flink DML."""

    def test_deploy_publishes_before_flink_dml(self):
        """TC-E-003a: run_deployment calls _publish_local_data before _create_flink_dml_statements."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        publish_pos = source.find("_publish_local_data")
        flink_pos = source.find("_create_flink_dml_statements")
        assert publish_pos != -1, "run_deployment should call _publish_local_data"
        assert (
            flink_pos != -1
        ), "run_deployment should call _create_flink_dml_statements"
        assert (
            publish_pos < flink_pos
        ), "run_deployment must call _publish_local_data BEFORE _create_flink_dml_statements"


class TestDestroyKafkaTopics:
    """Tests for REQ-E-004: destroy.py deletes Kafka topics."""

    def test_destroy_has_delete_kafka_topics(self):
        """TC-E-004a: destroy.py has _delete_kafka_topics function."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        assert (
            "_delete_kafka_topics" in source
        ), "destroy should have _delete_kafka_topics function"

    def test_destroy_topic_list_is_complete(self):
        """TC-E-004b: the full-teardown topic set includes all pipeline topics.

        The list now lives in the canonical scripts.common.pipeline_topics
        module (imported by destroy.py) — assert against the resolved values."""
        from scripts.common.pipeline_topics import ALL_PIPELINE_TOPICS

        expected_topics = [
            "ride_requests",
            "zone_traffic_sink",
            "anomalies_sink",
            "anomalies_enriched",
            "event_documents",
            "completed_actions",
        ]
        for topic in expected_topics:
            assert (
                topic in ALL_PIPELINE_TOPICS
            ), f"full teardown should include '{topic}'"


class TestASPProcessorStopPolling:
    """Tests for REQ-E-005: asp_setup.py polls for processor STOPPED state."""

    def test_ensure_connections_polls_for_stopped(self):
        """TC-E-005a: ensure_connections polls processor state after stop."""
        asp = importlib.import_module("scripts.asp_setup")
        source = inspect.getsource(asp.ensure_connections)
        assert "STOPPED" in source, "ensure_connections should check for STOPPED state"
        assert (
            "poll" in source.lower() or "wait" in source.lower()
        ), "ensure_connections should poll/wait for processor to stop"


class TestDestroyTopicBeforeTerraform:
    """Invariant tests for destroy ordering."""

    def test_destroy_calls_topic_cleanup_before_terraform(self):
        """TC-INV-011: destroy.py calls _delete_kafka_topics before terraform destroy."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy.main)
        topic_pos = source.find("_delete_kafka_topics")
        terraform_pos = source.find("run_terraform_destroy")
        assert topic_pos != -1, "main should call _delete_kafka_topics"
        assert terraform_pos != -1, "main should call run_terraform_destroy"
        assert (
            topic_pos < terraform_pos
        ), "topic cleanup must happen before terraform destroy"


class TestDestroyCredentialCleanup:
    """Tests for stale credential cleanup after destroy."""

    def test_destroy_removes_stale_credentials(self):
        """TC-INV-012: destroy.py removes infra-generated keys from .env."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy.main)
        assert (
            "_remove_stale_credentials" in source
        ), "main should call _remove_stale_credentials after terraform destroy"

    def test_stale_keys_include_all_infra_generated(self):
        """TC-INV-012b: STALE_KEYS covers all deploy-generated credential keys."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy._remove_stale_credentials)
        expected_keys = [
            "CONFLUENT_BOOTSTRAP_SERVER",
            "CONFLUENT_KAFKA_API_KEY",
            "CONFLUENT_KAFKA_API_SECRET",
            "CONFLUENT_KAFKA_REST_ENDPOINT",
            "CONFLUENT_KAFKA_CLUSTER_ID",
            "CONFLUENT_SCHEMA_REGISTRY_URL",
            "TF_VAR_mcp_server_url",
            "TF_VAR_mcp_auth_token",
            "DEPLOY_PHASE",
        ]
        for key in expected_keys:
            assert key in source, f"STALE_KEYS should include {key}"


class TestPublishDataTopicPreCreation:
    """Invariant tests for publish_data.py."""

    def test_publish_data_has_ensure_topic_exists(self):
        """TC-INV-013: publish_data.py has _ensure_topic_exists function."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert (
            "_ensure_topic_exists" in source
        ), "publish_data should have _ensure_topic_exists function"
        assert (
            "_get_kafka_rest_endpoint" in source
        ), "publish_data should have _get_kafka_rest_endpoint function"


# ── Iteration 3: Data Generation Pipeline Reset ─────────────────────────────


class TestPipelineResetModule:
    """Tests for REQ-BUG-004a: pipeline_reset.py provides pipeline reset functionality."""

    def test_pipeline_reset_module_importable(self):
        """TC-BUG-004a: scripts.pipeline_reset module is importable with reset_pipeline."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "reset_pipeline" in source
        ), "pipeline_reset should have reset_pipeline function"
        assert callable(
            pipeline_reset.reset_pipeline
        ), "reset_pipeline should be callable"

    def test_pipeline_reset_stops_dml_statements(self):
        """TC-BUG-004b: reset_pipeline stops the DML streaming statements.

        REQ-CRF-051 (brittle-test conversion): check the canonical
        statement registry, not source-grepped literals.
        """
        from scripts import pipeline_reset

        expected_stmts = [
            "zone-traffic-sink-insert",
            "anomaly-detection-insert",
            "anomalies-enriched-insert",
            "anomalies-sink-insert",
        ]
        for stmt in expected_stmts:
            assert (
                stmt in pipeline_reset.DML_STATEMENTS
            ), f"pipeline_reset.DML_STATEMENTS must include {stmt!r}"
        # Must stop before deleting
        source = inspect.getsource(pipeline_reset)
        assert (
            "stopped" in source.lower()
        ), "pipeline_reset should stop statements before deleting"

    def test_pipeline_reset_deletes_and_recreates_topics(self):
        """TC-BUG-004c: reset_pipeline deletes and recreates pipeline Kafka topics."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        expected_topics = [
            "ride_requests",
            "windowed_traffic",
            "anomalies_per_zone",
            "anomalies_enriched",
            "zone_traffic_sink",
            "anomalies_sink",
        ]
        for topic in expected_topics:
            assert topic in source, f"pipeline_reset should reference topic '{topic}'"
        # Must both delete and create topics
        assert "DELETE" in source, "pipeline_reset should delete topics"
        assert "POST" in source, "pipeline_reset should create topics (POST)"

    def test_pipeline_reset_clears_mongodb_collections(self):
        """TC-BUG-004h: reset_pipeline clears MongoDB sink collections."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "_clear_mongodb_collections" in source
        ), "pipeline_reset should have _clear_mongodb_collections function"
        assert (
            "MONGODB_SINK_COLLECTIONS" in source
        ), "pipeline_reset should define MONGODB_SINK_COLLECTIONS"
        # The names now come from the canonical pipeline_topics module —
        # assert against the resolved list rather than the source text.
        sink = {c[1] for c in pipeline_reset.MONGODB_SINK_COLLECTIONS}
        assert {
            "zone_traffic",
            "zone_anomalies",
            "dispatch_log",
        } <= sink, f"reset must clear the 3 sink collections; got {sink}"
        # Verify delete_many is used (not drop — preserve indexes)
        assert (
            "delete_many" in source
        ), "pipeline_reset should use delete_many to clear collections"

    def test_pipeline_reset_clears_mongodb_before_flink(self):
        """TC-BUG-004i: reset_pipeline clears MongoDB before stopping Flink."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset.reset_pipeline)
        mongo_pos = source.find("_clear_mongodb_collections")
        flink_pos = source.find("_stop_flink_statement")
        assert mongo_pos != -1, "reset_pipeline should call _clear_mongodb_collections"
        assert flink_pos != -1, "reset_pipeline should call _stop_flink_statement"
        assert (
            mongo_pos < flink_pos
        ), "MongoDB clearing must happen BEFORE Flink statement stops"

    def test_pipeline_reset_deletes_schema_subjects(self):
        """TC-BUG-004j: reset_pipeline deletes Schema Registry subjects.

        Pass-4 M-14: SCHEMA_SUBJECTS is now generated from PIPELINE_TOPICS
        rather than a literal list; assert against the value, not the
        source text.
        """
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        assert hasattr(
            pipeline_reset, "_delete_schema_subjects"
        ), "pipeline_reset should have _delete_schema_subjects function"
        assert hasattr(
            pipeline_reset, "SCHEMA_SUBJECTS"
        ), "pipeline_reset should define SCHEMA_SUBJECTS"
        # Behavior: SCHEMA_SUBJECTS must cover every pipeline topic's
        # -value subject AND every -key subject (the latter is the M-14
        # fix — previously only ride_requests-key was cleaned).
        for topic in pipeline_reset.PIPELINE_TOPICS:
            assert (
                f"{topic}-value" in pipeline_reset.SCHEMA_SUBJECTS
            ), f"SCHEMA_SUBJECTS missing {topic}-value"
            assert (
                f"{topic}-key" in pipeline_reset.SCHEMA_SUBJECTS
            ), f"SCHEMA_SUBJECTS missing {topic}-key (M-14)"
        # Verify it's called in reset_pipeline (behavior — function name in body).
        reset_source = inspect.getsource(pipeline_reset.reset_pipeline)
        assert (
            "_delete_schema_subjects" in reset_source
        ), "reset_pipeline should call _delete_schema_subjects"

    def test_pipeline_reset_has_restart_flink_dml(self):
        """TC-BUG-004m: pipeline_reset has restart_flink_dml for two-phase reset."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        assert callable(
            pipeline_reset.restart_flink_dml
        ), "restart_flink_dml should be callable"
        source = inspect.getsource(pipeline_reset.restart_flink_dml)
        assert (
            "_submit_flink_statement" in source
        ), "restart_flink_dml should submit Flink statements"
        assert (
            "_wait_for_dml_running" in source
        ), "restart_flink_dml should wait for DML to reach RUNNING"


class TestPipelineResetAgentBootstrap:
    """Issue #2 (2026-05-29): restart_flink_dml must bootstrap the dispatch
    agent + tool, otherwise `uv run datagen` leaves dispatch-insert FAILED
    forever. deploy.py creates create-tool-mongodb-fleet +
    create-agent-boat-dispatch before dispatch-insert; pipeline_reset did
    not, so every datagen run diverged from deploy.
    """

    def test_TC_FIX_002a_restart_imports_agent_bootstrap_names(self):
        """restart_flink_dml path must reference the canonical
        AGENT_BOOTSTRAP_STATEMENTS (not redefine the names locally)."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert "AGENT_BOOTSTRAP_STATEMENTS" in source, (
            "pipeline_reset must import AGENT_BOOTSTRAP_STATEMENTS from "
            "the canonical scripts.common.flink_statements"
        )

    def test_TC_FIX_002b_restart_creates_agent_and_tool(self):
        """restart_flink_dml must create the tool + agent statements
        (by name) before/around dispatch-insert."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset.restart_flink_dml)
        # The bootstrap helper must be invoked
        assert (
            "_bootstrap_agent_statements" in source
            or "create-tool-mongodb-fleet" in source
            or "AGENT_BOOTSTRAP_STATEMENTS" in source
        ), (
            "restart_flink_dml must create the dispatch agent + tool "
            "(create-tool-mongodb-fleet, create-agent-boat-dispatch)"
        )

    def test_TC_FIX_002c_dispatch_gated_on_mcp_health(self):
        """dispatch-insert must be gated on MCP health + agent bootstrap,
        not submitted unconditionally (a guaranteed-FAILED statement
        when MCP is unhealthy or the agent doesn't exist)."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert "check_mcp_health" in source, (
            "pipeline_reset must probe MCP health before submitting "
            "dispatch-insert (mirror deploy.py)"
        )

    def test_TC_FIX_002d_agent_sql_imported_from_dashboard(self):
        """Agent SQL must come from the dashboard single-source-of-truth
        (AGENT_SQL_CREATE_TOOL / AGENT_SQL_CREATE_AGENT), matching deploy.py."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "AGENT_SQL_CREATE_TOOL" in source and "AGENT_SQL_CREATE_AGENT" in source
        ), (
            "pipeline_reset must import agent SQL from dashboard "
            "(single source of truth shared with deploy.py)"
        )


class TestDeployFlinkDmlCatalogRestore:
    """Issues #6 + #7 (2026-05-29): deploy.py's _create_flink_dml_statements
    must not leave the Flink catalog in a broken state.

    #6: it drops the CTAS phantom tables (anomalies_enriched,
        completed_actions) but then _submit_statement sees the CTAS
        *statement* still COMPLETED and skips recreation — leaving the
        table dropped-but-not-recreated → "Table 'anomalies_enriched'
        does not exist".

    #7: _ensure_flink_topics deletes + recreates the 4 streaming-output
        topics, which makes Confluent auto-register them as raw-byte
        VARBINARY catalog tables, clobbering the terraform-typed
        tables/view. deploy.py never restores them via terraform
        apply -replace, so DML reading from them FAILS with "Column
        'pickup_zone'/'is_surge' not found".
    """

    def test_TC_FIX_006_ctas_statement_deleted_when_table_dropped(self):
        """#6: when the CTAS phantom table is dropped, the matching CTAS
        statement must ALSO be deleted, so re-submit recreates the table
        (rather than the COMPLETED-skip leaving it missing)."""
        import inspect

        from scripts import deploy

        src = inspect.getsource(deploy._create_flink_dml_statements)
        # The fix introduces a mapping table → CTAS statement, used to
        # delete the statement right after dropping the table. Assert that
        # specific coupling exists (not just that the names appear, which
        # they do elsewhere via DDL_STATEMENTS).
        assert "_ctas_stmt_for" in src, (
            "must map CTAS table → statement name so the statement is "
            "deleted after the table drop (Issue #6)"
        )
        # And the mapping must be indexed inside a _delete_and_wait call.
        assert "_delete_and_wait(_ctas_stmt_for[" in src, (
            "must call _delete_and_wait(_ctas_stmt_for[table]) so the CTAS "
            "statement is recreated, not skipped as already-COMPLETED"
        )

    def test_TC_FIX_007_restores_typed_tables_after_topic_recreate(self):
        """#7: after _ensure_flink_topics recreates output topics (which
        clobbers terraform-typed tables with VARBINARY), deploy must
        restore them — drop the clobbered catalog tables AND run
        terraform apply -replace (reusing pipeline_reset's helper)."""
        import inspect

        from scripts import deploy

        src = inspect.getsource(deploy._create_flink_dml_statements)
        # Must run terraform -replace to restore typed tables. We reuse
        # pipeline_reset._run_terraform_ddl_replace.
        assert "_run_terraform_ddl_replace" in src, (
            "deploy flink_dml must run terraform apply -replace to restore "
            "terraform-typed tables clobbered by topic recreation (Issue #7)"
        )

    def test_TC_FIX_009_force_recreates_dml_after_topic_recreate(self):
        """Issue #9 (2026-05-29): _ensure_flink_topics deletes+recreates the
        output topics' Avro schema subjects every deploy. A DML statement
        left 'already running' (skipped by _submit_statement) keeps its
        STALE compiled output schema and FAILS at write time with
        SerializationErrorValue (2200) "Cannot write AVRO record".

        Fix: force-delete the streaming DML statements before the submit
        loop so they recreate fresh against the new schemas (mirrors
        pipeline_reset, which deletes all DML before recreating).
        """
        import inspect

        from scripts import deploy

        src = inspect.getsource(deploy._create_flink_dml_statements)
        # There must be a force-delete of the DML statements before the
        # submit loop. We look for a delete loop over the streaming DML
        # names that precedes the `_submit_statement(... is_ddl=False)`
        # submit calls.
        force_marker = "_force_recreate_dml"
        assert force_marker in src or (
            "_delete_and_wait" in src and "SerializationError" in src
        ), (
            "must force-delete streaming DML statements before submit so "
            "they bind to the freshly-recreated output schemas (Issue #9)"
        )

    def test_TC_FIX_009b_force_delete_precedes_submit(self):
        """Issue #9: the DML force-delete must run BEFORE the early_dml
        submit loop."""
        import inspect

        from scripts import deploy

        src = inspect.getsource(deploy._create_flink_dml_statements)
        del_pos = src.find("_force_recreate_dml")
        submit_pos = src.find("for stmt_name in early_dml")
        assert del_pos != -1, "must define/call _force_recreate_dml"
        assert submit_pos != -1, "must have the early_dml submit loop"
        assert (
            del_pos < submit_pos
        ), "force-delete of stale DML must run BEFORE the submit loop"

    def test_TC_FIX_007b_drops_output_catalog_tables(self):
        """#7: the 4 output catalog tables (windowed_traffic,
        anomalies_per_zone, zone_traffic_sink, anomalies_sink) must be
        dropped before the terraform -replace so the replace installs
        clean typed definitions."""
        import inspect

        from scripts import deploy

        src = inspect.getsource(deploy._create_flink_dml_statements)
        # The fix reuses pipeline_reset.FLINK_CATALOG_TABLES and iterates it
        # calling flink_client.drop_table before the terraform -replace.
        assert "FLINK_CATALOG_TABLES" in src, (
            "deploy flink_dml must reuse pipeline_reset.FLINK_CATALOG_TABLES "
            "to drop the clobbered output tables (Issue #7)"
        )
        # The drop loop must call drop_table on each, then run the replace.
        drop_pos = src.find("_PR_CATALOG_TABLES")
        replace_pos = src.find("_run_terraform_ddl_replace")
        assert (
            drop_pos != -1 and replace_pos != -1
        ), "must drop catalog tables then run terraform -replace"
        assert drop_pos < replace_pos, (
            "must DROP the clobbered catalog tables BEFORE terraform "
            "-replace reinstalls the typed definitions"
        )


class TestPipelineResetDropsCtasTables:
    """Issue #10 (2026-05-29): restart_flink_dml recreates the CTAS DDL with
    `CREATE TABLE IF NOT EXISTS`. If anomalies_enriched / completed_actions
    already exist as auto-registered raw-byte phantoms ([val: BYTES]), the
    IF NOT EXISTS NO-OPS against the phantom — so anomalies-enriched-insert
    FAILS with:
        Sink schema: [val: BYTES]   (vs the 8-column query schema)

    deploy.py drops these CTAS tables before recreating (Issue #6).
    pipeline_reset deletes the CTAS STATEMENTS but never dropped the
    catalog TABLES — so `uv run datagen` left the phantom in place.
    """

    def test_TC_FIX_010a_ctas_tables_constant_exists(self):
        """A constant enumerating the CTAS-managed catalog tables must exist."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        assert hasattr(pipeline_reset, "CTAS_CATALOG_TABLES"), (
            "pipeline_reset must define CTAS_CATALOG_TABLES "
            "(anomalies_enriched, completed_actions)"
        )
        tables = set(pipeline_reset.CTAS_CATALOG_TABLES)
        assert "anomalies_enriched" in tables and "completed_actions" in tables, (
            "CTAS_CATALOG_TABLES must include both anomalies_enriched and "
            "completed_actions"
        )

    def test_TC_FIX_010b_restart_drops_ctas_before_ddl_recreate(self):
        """restart_flink_dml must DROP the CTAS catalog tables BEFORE
        recreating the CTAS DDL, so CREATE TABLE IF NOT EXISTS makes the
        typed table instead of no-op'ing against the phantom."""
        import inspect

        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        src = inspect.getsource(pipeline_reset.restart_flink_dml)
        assert "_drop_ctas_catalog_tables" in src, (
            "restart_flink_dml must call _drop_ctas_catalog_tables before "
            "recreating the CTAS DDL (Issue #10)"
        )
        drop_pos = src.find("_drop_ctas_catalog_tables")
        # The DDL recreate loop iterates DDL_STATEMENTS with is_ddl=True
        ddl_recreate_pos = src.find("is_ddl=True")
        assert drop_pos != -1 and ddl_recreate_pos != -1
        assert (
            drop_pos < ddl_recreate_pos
        ), "CTAS table drop must run BEFORE the CTAS DDL recreate loop"

    def test_TC_FIX_010c_ctas_drop_uses_drop_table(self):
        """The CTAS drop must issue DROP TABLE IF EXISTS (via FlinkRestClient
        drop_table) for each CTAS table."""
        import inspect

        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        # A helper that drops the CTAS tables must exist and use drop_table.
        assert hasattr(
            pipeline_reset, "_drop_ctas_catalog_tables"
        ), "pipeline_reset must expose _drop_ctas_catalog_tables helper"
        src = inspect.getsource(pipeline_reset._drop_ctas_catalog_tables)
        assert (
            "drop_table" in src
        ), "_drop_ctas_catalog_tables must call FlinkRestClient.drop_table"
        assert "CTAS_CATALOG_TABLES" in src


class TestPipelineResetReturnsFailure:
    """Issue #3 (2026-05-29): restart_flink_dml returned True + printed
    '[ok] Flink statements recreated' even when DML statements FAILED.
    _wait_for_dml_running returned None (no signal). Operators saw a
    success message on a broken pipeline.
    """

    def test_TC_FIX_003a_wait_for_dml_running_returns_bool(self):
        """_wait_for_dml_running must return a bool (True = all RUNNING,
        False = some FAILED/timed-out), not None."""
        import inspect

        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        sig = inspect.signature(pipeline_reset._wait_for_dml_running)
        assert (
            sig.return_annotation is bool
        ), "_wait_for_dml_running must be annotated -> bool"
        src = inspect.getsource(pipeline_reset._wait_for_dml_running)
        assert (
            "return False" in src and "return True" in src
        ), "_wait_for_dml_running must return True/False based on outcome"

    def test_TC_FIX_003b_restart_returns_false_on_dml_failure(self):
        """restart_flink_dml must propagate _wait_for_dml_running's result
        (return False when DML failed) rather than unconditionally True."""
        import inspect

        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        src = inspect.getsource(pipeline_reset.restart_flink_dml)
        # The final return must be tied to the wait result, not a bare
        # `return True`.
        assert "_wait_for_dml_running(" in src
        # Heuristic: the result of _wait_for_dml_running must be captured
        # into a variable that influences the return.
        assert (
            "dml_ok" in src
            or "all_running" in src
            or "return _wait_for_dml_running" in src
            or "return (" in src
        ), (
            "restart_flink_dml must return the DML-running outcome, not a "
            "hardcoded True"
        )

    def test_TC_FIX_003c_success_message_gated_on_outcome(self):
        """The '[ok] Flink statements recreated' message must not print
        when DML failed (it should be conditional or replaced with a
        warning on failure)."""
        import inspect

        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        src = inspect.getsource(pipeline_reset.restart_flink_dml)
        ok_pos = src.find("Flink statements recreated")
        # Must be inside a conditional (an `if` referencing the outcome
        # variable must appear before the success print)
        assert ok_pos != -1, "success message should still exist"
        prefix = src[:ok_pos]
        assert (
            "if dml_ok" in prefix
            or "if all_running" in prefix
            or "if ok" in prefix
            or "else" in prefix
        ), (
            "success message must be gated on the DML outcome, not "
            "printed unconditionally"
        )

    def test_pipeline_reset_drops_flink_catalog_tables(self):
        """TC-BUG-004n: reset_pipeline drops Flink catalog tables to prevent stale entries."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "_drop_flink_catalog_tables" in source
        ), "pipeline_reset should have _drop_flink_catalog_tables function"
        assert (
            "FLINK_CATALOG_TABLES" in source
        ), "pipeline_reset should define FLINK_CATALOG_TABLES"
        # Verify it's called in reset_pipeline
        reset_source = inspect.getsource(pipeline_reset.reset_pipeline)
        assert (
            "_drop_flink_catalog_tables" in reset_source
        ), "reset_pipeline should call _drop_flink_catalog_tables"

    def test_pipeline_reset_runs_terraform_ddl_replace(self):
        """TC-BUG-004o: restart_flink_dml runs terraform apply -replace for DDL statements."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "_run_terraform_ddl_replace" in source
        ), "pipeline_reset should have _run_terraform_ddl_replace function"
        assert (
            "TERRAFORM_DDL_RESOURCES" in source
        ), "pipeline_reset should define TERRAFORM_DDL_RESOURCES"
        # Verify terraform apply -replace is used
        func_source = inspect.getsource(pipeline_reset._run_terraform_ddl_replace)
        assert (
            "-replace" in func_source
        ), "_run_terraform_ddl_replace should use terraform apply -replace"
        # Verify it's called in restart_flink_dml (DDL recreation happens after
        # schemas are registered, not during reset_pipeline cleanup)
        restart_source = inspect.getsource(pipeline_reset.restart_flink_dml)
        assert (
            "_run_terraform_ddl_replace" in restart_source
        ), "restart_flink_dml should call _run_terraform_ddl_replace"
        assert (
            "_drop_flink_catalog_tables" in restart_source
        ), "restart_flink_dml should call _drop_flink_catalog_tables"


class TestPipelineResetDockerCheck:
    """Tests for REQ-BUG-004b: ShadowTraffic container management."""

    def test_pipeline_reset_has_check_shadowtraffic_running(self):
        """TC-BUG-004d / Phase D-2 (behavior conversion): pipeline_reset
        has a check_shadowtraffic_running function that probes Docker.

        Original test only grepped for `"docker"` anywhere — matched
        any comment/docstring mentioning Docker. Behavior test stubs
        subprocess and asserts the function actually checks Docker.
        """
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        # Must be a callable.
        assert callable(
            pipeline_reset.check_shadowtraffic_running
        ), "check_shadowtraffic_running should be callable"
        # Stub subprocess.run to verify it's invoked with `docker ps`.
        from unittest import mock as _mock

        with _mock.patch("scripts.pipeline_reset.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="",
                stderr="",
            )
            pipeline_reset.check_shadowtraffic_running()
        # Verify subprocess.run was called with a docker command.
        assert mock_run.called, "must invoke subprocess.run"
        call_args = mock_run.call_args
        invoked_cmd = call_args[0][0] if call_args[0] else call_args[1].get("args", [])
        assert any(
            "docker" in str(arg) for arg in invoked_cmd
        ), f"check_shadowtraffic_running must invoke docker; got {invoked_cmd}"

    def test_pipeline_reset_has_stop_shadowtraffic(self):
        """TC-BUG-004e: pipeline_reset has stop_shadowtraffic function."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset)
        assert (
            "stop_shadowtraffic" in source
        ), "pipeline_reset should have stop_shadowtraffic function"
        assert callable(
            pipeline_reset.stop_shadowtraffic
        ), "stop_shadowtraffic should be callable"


class TestDatagenPipelineReset:
    """Tests for REQ-BUG-004b: datagen.py integrates pipeline reset."""

    def test_datagen_calls_pipeline_reset(self):
        """TC-BUG-004d-integration: datagen.py calls reset_pipeline before ShadowTraffic."""
        datagen = importlib.import_module("scripts.datagen")
        source = inspect.getsource(datagen)
        assert "reset_pipeline" in source, "datagen should call reset_pipeline"
        assert "stop_shadowtraffic" in source, "datagen should call stop_shadowtraffic"

    def test_datagen_reset_before_shadowtraffic(self):
        """TC-BUG-004d-order: datagen calls reset before run_shadowtraffic_docker."""
        datagen = importlib.import_module("scripts.datagen")
        source = inspect.getsource(datagen.run_datagen)
        reset_pos = source.find("reset_pipeline")
        docker_pos = source.find("run_shadowtraffic_docker")
        assert reset_pos != -1, "run_datagen should call reset_pipeline"
        assert docker_pos != -1, "run_datagen should call run_shadowtraffic_docker"
        assert (
            reset_pos < docker_pos
        ), "reset_pipeline must be called BEFORE run_shadowtraffic_docker"

    def test_datagen_schedules_flink_restart_after_reset(self):
        """TC-BUG-004k: datagen schedules Flink DML restart after pipeline reset."""
        datagen = importlib.import_module("scripts.datagen")
        source = inspect.getsource(datagen)
        assert "restart_flink_dml" in source, "datagen should import restart_flink_dml"
        assert (
            "_schedule_flink_restart" in source
        ), "datagen should have _schedule_flink_restart helper"
        # Verify the schedule happens before ShadowTraffic blocks
        run_source = inspect.getsource(datagen.run_datagen)
        sched_pos = run_source.find("_schedule_flink_restart")
        docker_pos = run_source.find("run_shadowtraffic_docker")
        assert sched_pos != -1, "run_datagen should call _schedule_flink_restart"
        assert docker_pos != -1, "run_datagen should call run_shadowtraffic_docker"
        assert (
            sched_pos < docker_pos
        ), "_schedule_flink_restart must be called BEFORE run_shadowtraffic_docker"

    def test_datagen_flink_restart_uses_threading(self):
        """TC-BUG-004l: _schedule_flink_restart uses a daemon thread for non-blocking restart."""
        datagen = importlib.import_module("scripts.datagen")
        source = inspect.getsource(datagen._schedule_flink_restart)
        assert (
            "threading.Thread" in source
        ), "_schedule_flink_restart should use threading.Thread"
        assert (
            "daemon=True" in source
        ), "_schedule_flink_restart thread should be a daemon thread"
        assert (
            "restart_flink_dml" in source
        ), "_schedule_flink_restart should call restart_flink_dml"

    # ── TC-DATAGEN-LOCAL-001..005: --local mode must run the FULL pipeline ──
    # reset sequence, not just publish_data. Before this contract, --local
    # was useless for any recovery scenario because the auto-registered raw-
    # byte tables stayed in the Flink catalog and DML statements failed.

    def test_TC_DATAGEN_LOCAL_001_calls_reset_pipeline(self):
        """`uv run datagen --local` must call reset_pipeline before publishing.

        Without this, an operator running --local for recovery has no way to
        clear FAILED Flink statements, auto-registered raw-byte catalog
        tables, or stale Kafka topics — the very class of bug that --local
        is supposed to help recover from.
        """
        import re

        datagen = importlib.import_module("scripts.datagen")
        src = inspect.getsource(datagen.main)
        # Find the args.local branch
        local_branch_match = re.search(
            r"if use_local:(.+?)(?=\n    try:|\nif __name__)", src, re.DOTALL
        )
        assert local_branch_match, "could not locate args.local branch in main()"
        local_branch = local_branch_match.group(1)
        assert "reset_pipeline" in local_branch, (
            "--local branch must call reset_pipeline (otherwise --local "
            "cannot recover from broken pipeline state)"
        )

    def test_TC_DATAGEN_LOCAL_002_calls_restart_flink_dml(self):
        """`uv run datagen --local` must call restart_flink_dml after
        publish_data (Phase 2). Without it, the DDL/DML statements that
        Phase 1 deleted are never recreated."""
        import re

        datagen = importlib.import_module("scripts.datagen")
        src = inspect.getsource(datagen.main)
        local_branch_match = re.search(
            r"if use_local:(.+?)(?=\n    try:|\nif __name__)", src, re.DOTALL
        )
        assert local_branch_match
        local_branch = local_branch_match.group(1)
        assert "restart_flink_dml" in local_branch, (
            "--local branch must call restart_flink_dml (Phase 2 of "
            "pipeline reset — recreates Flink DDL/DML statements)"
        )

    def test_TC_DATAGEN_LOCAL_003_ordering_reset_then_publish_then_restart(self):
        """Order matters: reset (drops everything) → publish (registers
        Avro schemas, triggers auto-registration) → restart (drops auto-
        registered tables, force-recreates via terraform -replace,
        recreates DML). Wrong order = broken pipeline."""
        import re

        datagen = importlib.import_module("scripts.datagen")
        src = inspect.getsource(datagen.main)
        local_branch_match = re.search(
            r"if use_local:(.+?)(?=\n    try:|\nif __name__)", src, re.DOTALL
        )
        assert local_branch_match
        local_branch = local_branch_match.group(1)
        reset_pos = local_branch.find("reset_pipeline")
        publish_pos = local_branch.find("publish_data")
        restart_pos = local_branch.find("restart_flink_dml")
        assert (
            reset_pos != -1 and publish_pos != -1 and restart_pos != -1
        ), "all three steps must be called"
        assert reset_pos < publish_pos, (
            "reset_pipeline must run BEFORE publish_data (publish on a "
            "topic with stale schemas fails)"
        )
        assert publish_pos < restart_pos, (
            "publish_data must run BEFORE restart_flink_dml (the 30s of "
            "publishing is the timing window for Schema Registry "
            "registration that restart_flink_dml relies on)"
        )

    def test_TC_DATAGEN_LOCAL_004_publish_uses_force(self):
        """publish_data has an idempotency guard that refuses to publish
        when a topic already has messages. After reset_pipeline deletes the
        topic and recreates it, the topic is empty — but the guard's
        offset-fetch path may be flaky for newly-created topics. --force
        bypasses the guard and is the correct flag for this controlled
        reset-then-publish sequence."""
        import re

        datagen = importlib.import_module("scripts.datagen")
        src = inspect.getsource(datagen.main)
        local_branch_match = re.search(
            r"if use_local:(.+?)(?=\n    try:|\nif __name__)", src, re.DOTALL
        )
        assert local_branch_match
        local_branch = local_branch_match.group(1)
        assert "--force" in local_branch, (
            "--local publish_data invocation must use --force "
            "(reset_pipeline just emptied the topic; the idempotency "
            "guard would block a legitimate publish)"
        )

    def test_TC_DATAGEN_DEFAULT_006_default_is_local_not_shadowtraffic(self):
        """REQ-DATAGEN-DEFAULT (2026-05-29): `uv run datagen` with NO flags
        must default to local pre-generated data, not ShadowTraffic Docker.

        Before this contract was inverted, the default required Docker,
        which is unavailable on many corporate-managed laptops (the very
        machines workshop attendees use). The default is now the
        permission-less, dependency-light path.
        """
        import importlib

        datagen = importlib.import_module("scripts.datagen")
        # The argparse declaration: --shadowtraffic must be opt-in
        # (action="store_true" with no `default=True`)
        src = inspect.getsource(datagen.create_argument_parser)
        assert (
            '"--shadowtraffic"' in src
        ), "create_argument_parser must declare --shadowtraffic flag"
        # The main() dispatch must invert the check so absence of
        # --shadowtraffic routes to local
        main_src = inspect.getsource(datagen.main)
        assert (
            "not args.shadowtraffic" in main_src or "use_local" in main_src
        ), "main() must default to local when --shadowtraffic is absent"

    def test_TC_DATAGEN_DEFAULT_007_local_flag_is_deprecated_alias(self):
        """REQ-DATAGEN-DEFAULT: --local is kept as a no-op alias for
        backwards compatibility (no breakage for users who scripted
        `uv run datagen --local` before the default was inverted),
        but it must emit a deprecation warning to stdout/log."""
        import importlib

        datagen = importlib.import_module("scripts.datagen")
        # Flag still exists for argparse
        parser = datagen.create_argument_parser()
        args = parser.parse_args(["--local"])
        assert args.local is True, "--local flag must still be accepted"
        # And main() must warn when it's set
        main_src = inspect.getsource(datagen.main)
        assert (
            "deprecated" in main_src.lower()
        ), "main() must emit a deprecation message when --local is set"

    def test_TC_DATAGEN_DEFAULT_008_shadowtraffic_flag_routes_to_docker(self):
        """REQ-DATAGEN-DEFAULT: --shadowtraffic explicitly opts INTO the
        legacy Docker path. Operators who genuinely want continuous
        live streaming still have a way to get it."""
        import importlib

        datagen = importlib.import_module("scripts.datagen")
        # The --shadowtraffic branch must lead to run_datagen (the Docker
        # orchestrator), not the local-publish branch
        main_src = inspect.getsource(datagen.main)
        # When use_local is False (i.e. --shadowtraffic is set), the code
        # should reach the run_datagen call. Easiest behavior check:
        # run_datagen is still referenced from main().
        assert "run_datagen" in main_src, (
            "main() must still wire --shadowtraffic to run_datagen "
            "(the Docker orchestration entry point)"
        )

    def test_TC_DATAGEN_LOCAL_005_dry_run_skips_real_operations(self):
        """--dry-run must skip the real reset/publish/restart so an operator
        can validate setup without touching cloud resources."""
        import re

        datagen = importlib.import_module("scripts.datagen")
        src = inspect.getsource(datagen.main)
        local_branch_match = re.search(
            r"if use_local:(.+?)(?=\n    try:|\nif __name__)", src, re.DOTALL
        )
        assert local_branch_match
        local_branch = local_branch_match.group(1)
        # reset_pipeline and restart_flink_dml must be gated on `not args.dry_run`
        assert (
            "args.dry_run" in local_branch or "dry_run" in local_branch
        ), "--local branch must check dry_run before calling reset/restart"


class TestDashboardDataGenButton:
    """Tests for dashboard data generation button using local JSONL publish."""

    def test_dashboard_uses_publish_data(self):
        """TC-BUG-004f: dashboard.py uses publish_data for data generation."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        assert (
            "scripts.publish_data" in source
        ), "dashboard should use scripts.publish_data for data generation"

    def test_dashboard_references_jsonl_data_file(self):
        """TC-BUG-004g: dashboard references the ride_requests.jsonl data file."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        assert (
            "ride_requests.jsonl" in source
        ), "dashboard should reference ride_requests.jsonl data file"


class TestInvariantDeployNoReset:
    """Invariant test: deploy.py publish does NOT call pipeline_reset."""

    def test_deploy_publish_does_not_call_pipeline_reset(self):
        """TC-INV-017: deploy.py _publish_local_data does not call pipeline reset."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._publish_local_data)
        assert (
            "reset_pipeline" not in source
        ), "deploy._publish_local_data must NOT call reset_pipeline"


class TestInvariantDatagenDryRun:
    """Invariant test: datagen --dry-run skips pipeline reset.

    REQ-CRG-027 / Phase D-1: converted from source-grep to behavior test.
    The original test would pass if the function source merely *mentioned*
    `dry_run` anywhere — including in a docstring or unrelated check.
    """

    def test_datagen_dry_run_skips_reset(self):
        """TC-INV-019 (behavior): run_datagen with dry_run=True must NOT
        invoke reset_pipeline or stop_shadowtraffic."""
        from unittest import mock as _mock

        import scripts.datagen as datagen_mod

        with (
            _mock.patch.object(datagen_mod, "reset_pipeline") as mock_reset,
            _mock.patch.object(datagen_mod, "stop_shadowtraffic") as mock_stop,
            _mock.patch.object(datagen_mod, "_schedule_flink_restart"),
            _mock.patch.object(
                datagen_mod, "validate_dependencies", return_value=[], create=True
            ),
            _mock.patch.object(
                datagen_mod, "run_shadowtraffic_docker", return_value=0, create=True
            ),
            _mock.patch.object(
                datagen_mod, "get_project_root", return_value=Path("/tmp/x")
            ),
            _mock.patch("pathlib.Path.exists", return_value=True),
        ):
            datagen_mod.run_datagen(dry_run=True)

        mock_reset.assert_not_called()
        mock_stop.assert_not_called()


class TestInvariantDatagenLocal:
    """Behavior test: `uv run datagen --local` MUST run the full 3-phase
    pipeline reset sequence (reset_pipeline → publish_data → restart_flink_dml),
    not just publish data.

    Previously this class asserted the INVERSE — that --local skips
    reset_pipeline. That contract was the bug. Operators ran --local
    to recover from broken pipeline state and got nothing but a republish
    of stale data. TC-DATAGEN-LOCAL-001..005 in TestDatagenPipelineReset
    add the contract from the source-grep side; this test adds the
    runtime-behavior side (mocked subprocess + reset/restart asserts).
    """

    def test_datagen_local_runs_full_pipeline_reset(self):
        """TC-INV-020 (behavior, inverted from prior contract): when --local
        is set and --dry-run is NOT set, main() must call reset_pipeline,
        then publish_data, then restart_flink_dml — in that order."""
        import argparse
        from unittest import mock as _mock

        import scripts.datagen as datagen_mod

        # Track call order so we can assert reset BEFORE publish BEFORE restart.
        call_order: list[str] = []

        def _record_reset(*a, **kw):
            call_order.append("reset_pipeline")
            return True

        def _record_run(*a, **kw):
            call_order.append("publish_data")
            return subprocess.CompletedProcess(args=[], returncode=0)

        def _record_restart(*a, **kw):
            call_order.append("restart_flink_dml")
            return True

        # sys.exit must actually exit (raise SystemExit) so execution doesn't
        # fall through into the non-local code path after the --local branch.
        with (
            _mock.patch.object(
                datagen_mod, "reset_pipeline", side_effect=_record_reset
            ) as mock_reset,
            _mock.patch.object(
                datagen_mod, "restart_flink_dml", side_effect=_record_restart
            ) as mock_restart,
            _mock.patch.object(datagen_mod, "stop_shadowtraffic", return_value=True),
            _mock.patch("subprocess.run", side_effect=_record_run) as mock_run,
            _mock.patch.object(
                argparse.ArgumentParser,
                "parse_args",
                return_value=argparse.Namespace(
                    shadowtraffic=False,
                    local=False,
                    dry_run=False,
                    verbose=False,
                ),
            ),
            _mock.patch("pathlib.Path.exists", return_value=True),
            _mock.patch.object(
                datagen_mod, "get_project_root", return_value=Path("/tmp/x")
            ),
            _mock.patch("sys.exit", side_effect=SystemExit),
        ):
            with pytest.raises(SystemExit):
                datagen_mod.main()

        # All steps must have been called
        mock_reset.assert_called_once()
        assert mock_run.called, "publish_data subprocess must be called"
        mock_restart.assert_called_once()
        # And in the correct order. 2026-07-14: a Phase-4 re-publish follows
        # restart_flink_dml — its DROP TABLE ride_requests deletes the backing
        # topic (and Phase 2's records), so datagen must publish again or it
        # ends with an empty pipeline (see test_fresh_deploy_hardening).
        assert call_order == [
            "reset_pipeline",
            "publish_data",
            "restart_flink_dml",
            "publish_data",
        ], f"default mode must call reset → publish → restart → re-publish, got: {call_order}"

    def test_datagen_local_dry_run_skips_real_operations(self):
        """TC-INV-020b: --dry-run flag must short-circuit the real
        reset/restart calls (subprocess.run for publish_data still
        runs, but with --dry-run propagated)."""
        import argparse
        from unittest import mock as _mock

        import scripts.datagen as datagen_mod

        # Capture the publish_data invocation specifically (first call args)
        captured_cmds: list[list] = []

        def _record_run(*a, **kw):
            captured_cmds.append(list(a[0]) if a else [])
            return subprocess.CompletedProcess(args=[], returncode=0)

        with (
            _mock.patch.object(datagen_mod, "reset_pipeline") as mock_reset,
            _mock.patch.object(datagen_mod, "restart_flink_dml") as mock_restart,
            _mock.patch.object(datagen_mod, "stop_shadowtraffic") as mock_stop,
            _mock.patch("subprocess.run", side_effect=_record_run),
            _mock.patch.object(
                argparse.ArgumentParser,
                "parse_args",
                return_value=argparse.Namespace(
                    shadowtraffic=False,
                    local=False,
                    dry_run=True,
                    verbose=False,
                ),
            ),
            _mock.patch("pathlib.Path.exists", return_value=True),
            _mock.patch.object(
                datagen_mod, "get_project_root", return_value=Path("/tmp/x")
            ),
            _mock.patch("sys.exit", side_effect=SystemExit),
        ):
            with pytest.raises(SystemExit):
                datagen_mod.main()

        # dry-run: neither reset nor restart should actually fire
        mock_reset.assert_not_called()
        mock_restart.assert_not_called()
        mock_stop.assert_not_called()
        # publish_data subprocess does run (the FIRST subprocess call —
        # there should only be one in --dry-run mode since reset/restart
        # are skipped), with --dry-run propagated
        assert captured_cmds, "publish_data subprocess must be called"
        publish_cmd = captured_cmds[0]
        assert (
            "publish_data" in publish_cmd
        ), f"first subprocess call must be publish_data, got: {publish_cmd}"
        assert "--dry-run" in publish_cmd, (
            "publish_data invocation must include --dry-run when "
            "datagen --dry-run is set"
        )


class TestInvariantShadowTrafficConfigUnchanged:
    """Invariant test: ShadowTraffic config files are not modified."""

    def test_advance_time_computes_24h_backfill(self):
        """TC-INV-022: advance_time.py still computes 24h historical backfill."""
        advance_time_path = (
            PROJECT_ROOT
            / "terraform"
            / "agents"
            / "data-gen"
            / "functions"
            / "advance_time.py"
        )
        assert advance_time_path.exists(), "advance_time.py should exist"
        source = advance_time_path.read_text()
        assert (
            "1 * 24 * 60 * 60 * 1000" in source
        ), "advance_time.py should still compute 24h backfill"
        assert (
            "starting_timestamp" in source
        ), "advance_time.py should still have starting_timestamp function"
        assert (
            "advance_time" in source
        ), "advance_time.py should still have advance_time function"

    def test_root_json_unchanged(self):
        """TC-INV-022b: root.json ShadowTraffic config structure is unchanged."""
        root_json_path = (
            PROJECT_ROOT / "terraform" / "agents" / "data-gen" / "root.json"
        )
        assert root_json_path.exists(), "root.json should exist"
        import json

        config = json.loads(root_json_path.read_text())
        assert "generators" in config, "root.json should have generators"
        assert "connections" in config, "root.json should have connections"
        assert "schedule" in config, "root.json should have schedule"


# ── Iteration 4: Fault-Tolerant One-Command Deployment ───────────────────────


class TestPublishDataIdempotency:
    """Tests for REQ-E-007: Idempotent data generation (publish_data guard)."""

    def test_publish_data_has_get_topic_message_count(self):
        """TC-E-007a: publish_data.py has _get_topic_message_count function."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert (
            "_get_topic_message_count" in source
        ), "publish_data should have _get_topic_message_count function"

    def test_publish_data_has_force_flag(self):
        """TC-E-007b: publish_data.py accepts --force CLI argument."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert "--force" in source, "publish_data should accept --force argument"
        assert (
            'action="store_true"' in source or "action='store_true'" in source
        ), "--force should be a boolean flag"

    def test_publish_data_checks_count_before_publishing(self):
        """TC-E-007c: publish_data checks topic message count before publishing."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert "msg_count" in source, "publish_data should check message count"
        assert (
            "already contains" in source or "already has" in source
        ), "publish_data should warn about existing messages"

    def test_publish_data_force_bypasses_check(self):
        """TC-E-007d: --force flag bypasses the duplicate data check."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert "args.force" in source, "publish_data should check args.force"
        # The check should be guarded by "not args.force"
        assert (
            "not args.force" in source
        ), "duplicate check should be skipped when --force is set"


class TestDashboardButtonMutex:
    """Tests for REQ-E-008: Dashboard data generation mutex."""

    def test_dashboard_hides_button_when_running(self):
        """TC-E-008a: dashboard shows spinner when datagen_running=True, hides button."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        # The button should only render in the else branch of datagen_running check
        assert (
            "datagen_running" in source
        ), "dashboard should track datagen_running state"
        assert (
            "Publishing ride requests" in source
        ), "dashboard should show progress message when datagen is running"

    def test_dashboard_passes_force_to_publish_data(self):
        """TC-E-008b: dashboard subprocess passes --force to publish_data."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        assert (
            '"--force"' in source or "'--force'" in source
        ), "dashboard should pass --force to publish_data subprocess"


class TestDeployCredentialValidation:
    """Tests for REQ-E-009: Deploy credential validation."""

    def test_save_terraform_credentials_returns_bool(self):
        """TC-E-009a: _save_terraform_credentials returns bool."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._save_terraform_credentials)
        assert "-> bool" in source, "_save_terraform_credentials should return bool"

    def test_save_terraform_credentials_validates_all_six(self):
        """TC-E-009b: _save_terraform_credentials validates all 6 required credentials."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._save_terraform_credentials)
        # Must check for missing credentials
        assert (
            "missing" in source.lower()
        ), "_save_terraform_credentials should track missing credentials"
        # Must return False on missing
        assert (
            "return False" in source
        ), "_save_terraform_credentials should return False when credentials are missing"
        # Must return True on success
        assert (
            "return True" in source
        ), "_save_terraform_credentials should return True when all credentials present"

    def test_save_terraform_credentials_logs_missing(self):
        """TC-E-009c: _save_terraform_credentials logs which credentials are missing."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._save_terraform_credentials)
        assert (
            "Missing credentials" in source or "missing" in source.lower()
        ), "_save_terraform_credentials should log which credentials are missing"


class TestDeployPortConflict:
    """Tests for REQ-E-010: Dashboard port conflict detection."""

    def test_deploy_has_is_port_in_use(self):
        """TC-E-010a: deploy.py has _is_port_in_use helper."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "_is_port_in_use" in source
        ), "deploy should have _is_port_in_use function"

    def test_launch_dashboard_checks_port(self):
        """TC-E-010b: _launch_dashboard checks port before spawning Streamlit.

        After BUG-305: port detection is delegated to _find_free_dashboard_port,
        which scans 8501..8510 instead of opening a browser to whatever
        process happens to be bound on 8501.
        """
        deploy = importlib.import_module("scripts.deploy")
        # The port check now lives in _find_free_dashboard_port; _launch_dashboard
        # delegates to it.
        launch_src = inspect.getsource(deploy._launch_dashboard)
        finder_src = inspect.getsource(deploy._find_free_dashboard_port)
        assert (
            "_find_free_dashboard_port" in launch_src
        ), "_launch_dashboard should delegate port selection to _find_free_dashboard_port"
        assert (
            "_is_port_in_use" in finder_src
        ), "_find_free_dashboard_port should use _is_port_in_use"

    def test_is_port_in_use_uses_socket(self):
        """TC-E-010c: _is_port_in_use uses socket to check port."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._is_port_in_use)
        assert "socket" in source, "_is_port_in_use should use socket module"


class TestDeployDDLWait:
    """Tests for REQ-E-011: Deploy DDL completion wait."""

    def test_deploy_waits_for_ddl_completion(self):
        """TC-E-011a: _create_flink_dml_statements waits for DDL to complete."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._create_flink_dml_statements)
        assert (
            "ddl_pending" in source or "DDL" in source
        ), "deploy should track DDL completion"
        assert "Waiting for DDL" in source, "deploy should log DDL wait status"

    def test_deploy_ddl_timeout_is_60s(self):
        """TC-E-011b: DDL wait timeout is 60 seconds."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._create_flink_dml_statements)
        assert "ddl_max_wait = 60" in source, "DDL wait timeout should be 60 seconds"

    def test_deploy_skips_dml_on_ddl_failure(self):
        """TC-E-011c: DML creation is skipped when DDL fails."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._create_flink_dml_statements)
        assert "Skipping DML" in source, "deploy should skip DML when DDL fails"


class TestDeployDMLHealthCheck:
    """Tests for REQ-E-012: Flink DML health check after deployment."""

    def test_deploy_dml_wait_is_120s(self):
        """TC-E-012a: DML wait timeout is 120 seconds."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._create_flink_dml_statements)
        assert "max_wait = 120" in source, "DML wait timeout should be 120 seconds"

    def test_deploy_logs_dml_failure_reason(self):
        """TC-E-012b: deploy.py logs failure reason from Flink API."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._create_flink_dml_statements)
        # Must extract detail from API response
        assert "detail" in source, "deploy should extract failure detail from API"
        assert "[FAIL]" in source, "deploy should log failure with [FAIL] prefix"


class TestDeployPhaseTracking:
    """Tests for REQ-E-013: Deploy resilience — partial failure recovery."""

    def test_deploy_saves_phase_to_credentials(self):
        """TC-E-013a: run_deployment saves DEPLOY_PHASE to .env."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        assert "DEPLOY_PHASE" in source, "run_deployment should save DEPLOY_PHASE"

    def test_deploy_tracks_all_phases(self):
        """TC-E-013b: run_deployment tracks all major deployment phases."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        phases = [
            "terraform",
            "credentials",
            "asp_setup",
            "publish_data",
            "flink_dml",
            "complete",
        ]
        for phase in phases:
            assert (
                f'"{phase}"' in source
            ), f"run_deployment should track phase '{phase}'"

    def test_deploy_marks_complete_at_end(self):
        """TC-E-013c: run_deployment marks DEPLOY_PHASE=complete after success."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        # "complete" phase should come after "flink_dml" phase
        flink_pos = source.find('"flink_dml"')
        complete_pos = source.find('"complete"')
        assert flink_pos != -1, "run_deployment should have flink_dml phase"
        assert complete_pos != -1, "run_deployment should have complete phase"
        assert (
            complete_pos > flink_pos
        ), "complete phase must come after flink_dml phase"


class TestAnomalyDetectionMinTrainingSize:
    """minTrainingSize in anomaly detection SQL.

    Lowered 50→15 alongside the 5→1 min window change: at a 1-min window,
    minTrainingSize=15 lets the detector reach its baseline in ~15 min (vs ~50)
    so anomalies actually surface during a demo. See the window-size change in
    windowed_traffic_view / dashboard.WINDOW_MINUTES.
    """

    def test_anomaly_detection_sql_has_min_training_size_15(self):
        """anomaly-detection-insert.sql uses minTrainingSize=15."""
        sql_path = (
            PROJECT_ROOT
            / "terraform"
            / "agents"
            / "sql"
            / "anomaly-detection-insert.sql"
        )
        assert sql_path.exists(), "anomaly-detection-insert.sql should exist"
        sql = sql_path.read_text()
        assert "minTrainingSize" in sql, "SQL should reference minTrainingSize"
        assert (
            "'minTrainingSize' VALUE 15" in sql
        ), "minTrainingSize should be set to 15 (was 50)"


# ── Iteration 4: Invariant Regression Tests ──────────────────────────────────


class TestInvariantDeployOrdering:
    """INV-023 through INV-025: Deploy execution ordering invariants."""

    def test_deploy_terraform_core_before_agents(self):
        """TC-INV-023: deploy runs terraform core before agents."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        core_pos = source.find("core")
        agents_pos = source.find("agents")
        assert core_pos != -1, "run_deployment should reference core module"
        assert agents_pos != -1, "run_deployment should reference agents module"
        assert core_pos < agents_pos, "core terraform must run before agents terraform"

    def test_deploy_credentials_before_asp(self):
        """TC-INV-024: deploy saves terraform credentials before ASP setup."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        cred_pos = source.find("_save_terraform_credentials")
        asp_pos = source.find("_run_asp_post_terraform")
        assert cred_pos != -1, "run_deployment should call _save_terraform_credentials"
        assert asp_pos != -1, "run_deployment should call _run_asp_post_terraform"
        assert cred_pos < asp_pos, "credentials must be saved before ASP setup"

    def test_deploy_publish_before_flink_dml(self):
        """TC-INV-025: deploy publishes data before Flink DML creation."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        publish_pos = source.find("_publish_local_data")
        flink_pos = source.find("_create_flink_dml_statements")
        assert publish_pos != -1, "run_deployment should call _publish_local_data"
        assert (
            flink_pos != -1
        ), "run_deployment should call _create_flink_dml_statements"
        assert (
            publish_pos < flink_pos
        ), "data publish must happen before Flink DML creation"


class TestInvariantDestroyOrdering:
    """INV-026: Destroy ordering invariant."""

    def test_destroy_order_flink_topics_asp_terraform(self):
        """TC-INV-026: destroy deletes Flink, topics, ASP, then terraform (in order)."""
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy.main)
        flink_pos = source.find("_delete_flink_dml_statements")
        topic_pos = source.find("_delete_kafka_topics")
        asp_pos = source.find("run_asp_teardown")
        terraform_pos = source.find("run_terraform_destroy")
        assert flink_pos != -1, "main should call _delete_flink_dml_statements"
        assert topic_pos != -1, "main should call _delete_kafka_topics"
        assert asp_pos != -1, "main should call run_asp_teardown"
        assert terraform_pos != -1, "main should call run_terraform_destroy"
        assert (
            flink_pos < topic_pos < asp_pos < terraform_pos
        ), "destroy order must be: Flink -> topics -> ASP -> terraform"


class TestInvariantPublishDataDryRun:
    """INV-027: publish_data --dry-run skips Kafka connections."""

    def test_publish_data_dry_run_skips_kafka(self):
        """TC-INV-027: publish_data --dry-run skips Kafka connections."""
        publish = importlib.import_module("scripts.publish_data")
        source = inspect.getsource(publish)
        assert "dry_run" in source, "publish_data should check dry_run flag"
        assert "args.dry_run" in source, "publish_data should check args.dry_run"


class TestInvariantASPSeedIdempotent:
    """INV-028: ASP seed events are upserted idempotently."""

    def test_asp_setup_upserts_seed_events(self):
        """TC-INV-028: asp_setup upserts seed events idempotently."""
        asp = importlib.import_module("scripts.asp_setup")
        source = inspect.getsource(asp)
        assert (
            "upsert" in source.lower() or "replace_one" in source.lower()
        ), "asp_setup should upsert seed events (not blind insert)"


class TestInvariantDashboardZones:
    """INV-029: Dashboard shows all 7 zone names."""

    def test_dashboard_has_all_seven_zones(self):
        """TC-INV-029: dashboard.py references all 7 New Orleans zones."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        zones = [
            "French Quarter",
            "Bywater",
            "Marigny",
            "Garden District",
            "Uptown",
            "Warehouse District",
            "Central Business District",
        ]
        for zone in zones:
            assert zone in source, f"dashboard should reference zone '{zone}'"


class TestInvariantPipelineResetMongoFirst:
    """INV-030: pipeline_reset clears MongoDB before stopping Flink."""

    def test_pipeline_reset_mongo_before_flink(self):
        """TC-INV-030: pipeline_reset clears MongoDB collections before Flink stops."""
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset.reset_pipeline)
        mongo_pos = source.find("_clear_mongodb_collections")
        flink_pos = source.find("_stop_flink_statement")
        assert mongo_pos != -1, "reset_pipeline should call _clear_mongodb_collections"
        assert flink_pos != -1, "reset_pipeline should call _stop_flink_statement"
        assert (
            mongo_pos < flink_pos
        ), "MongoDB clearing must happen BEFORE Flink statement stops"


class TestInvariantWatermark:
    """INV-031: Flink DDL uses WATERMARK with request_ts - INTERVAL '5' SECOND."""

    def test_watermark_uses_5_second_tolerance(self):
        """TC-INV-031: Flink DDL uses 5-second watermark tolerance."""
        # Check terraform DDL files for WATERMARK definition
        sql_dir = PROJECT_ROOT / "terraform" / "agents"
        found_watermark = False
        for tf_file in sql_dir.glob("*.tf"):
            content = tf_file.read_text()
            if "WATERMARK" in content and "request_ts" in content:
                assert (
                    "INTERVAL '5' SECOND" in content
                ), f"{tf_file.name} should use INTERVAL '5' SECOND watermark"
                found_watermark = True
        assert (
            found_watermark
        ), "At least one terraform file should define WATERMARK on request_ts"


class TestInvariantDeployForceFlag:
    """Additional invariant: deploy.py passes --force to publish_data."""

    def test_deploy_publish_passes_force(self):
        """TC-INV-033: deploy.py _publish_local_data passes --force to publish_data."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._publish_local_data)
        assert (
            "--force" in source
        ), "_publish_local_data should pass --force to publish_data"


class TestMCPDeployFaultTolerance:
    """Validates MCP deployment fault-tolerance fixes."""

    def test_docker_build_no_cache(self):
        """Fix-1: docker build uses --no-cache to prevent cross-arch layer reuse."""
        mcp_deploy = importlib.import_module("scripts.mcp_deploy")
        source = inspect.getsource(mcp_deploy._build_and_push)
        assert (
            "--no-cache" in source
        ), "_build_and_push must use --no-cache for reliable cross-platform builds"
        assert (
            "--pull" in source
        ), "_build_and_push must use --pull to fetch fresh base image"

    def test_health_check_fix_matches_by_port(self):
        """Fix-2: ALB health check fix uses HealthCheckPort for matching.

        Pass-4 H-2: matching by HealthCheckPort moved into the
        `_our_target_groups_for_endpoint` helper that scopes to TGs
        attached to OUR listener rule (host-header match). Check both
        functions together — the contract is that one of them filters
        by HealthCheckPort=8080.
        """
        mcp_deploy = importlib.import_module("scripts.mcp_deploy")
        combined = inspect.getsource(
            mcp_deploy._fix_alb_health_check
        ) + inspect.getsource(mcp_deploy._our_target_groups_for_endpoint)
        assert (
            "HealthCheckPort" in combined
        ), "Should match TG by HealthCheckPort (available immediately)"

    def test_ecs_draining_error_handled(self):
        """Fix-3: ECS 'still draining' error triggers name-suffix retry."""
        mcp_deploy = importlib.import_module("scripts.mcp_deploy")
        source = inspect.getsource(mcp_deploy._create_ecs_express)
        assert (
            "still draining" in source
        ), "Must catch 'still draining' error to retry with suffixed name"

    def test_mcp_deploy_saves_credentials(self):
        """Fix-4: standalone mcp-deploy saves URL/token to .env."""
        mcp_deploy = importlib.import_module("scripts.mcp_deploy")
        source = inspect.getsource(mcp_deploy.main)
        assert (
            "_save_mcp_credentials" in source
        ), "main() must auto-save credentials to .env"

    def test_save_mcp_credentials_function_exists(self):
        """Fix-4: _save_mcp_credentials helper handles create and update."""
        mcp_deploy = importlib.import_module("scripts.mcp_deploy")
        source = inspect.getsource(mcp_deploy._save_mcp_credentials)
        assert "TF_VAR_mcp_server_url" in source
        assert "TF_VAR_mcp_auth_token" in source


class TestDeployMCPURLChangeHandling:
    """Validates deploy.py handles MCP URL changes gracefully."""

    def test_deploy_detects_mcp_url_change(self):
        """Fix-5/6: deploy.py detects when MCP URL changes."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        assert "old_mcp_url" in source, "Must capture old URL before MCP deploy"
        assert (
            "mcp_url_changed" in source
        ), "Must detect URL change for cascade handling"

    def test_deploy_drops_stale_catalog_objects(self):
        """Fix-5/6: deploy.py drops stale Flink catalog objects on URL change."""
        deploy = importlib.import_module("scripts.deploy")
        assert hasattr(deploy, "_drop_stale_mcp_catalog_objects")
        source = inspect.getsource(deploy._drop_stale_mcp_catalog_objects)
        assert "DROP CONNECTION" in source
        assert "DROP MODEL" in source
        assert "DROP AGENT" in source
        assert "DROP TOOL" in source

    def test_deploy_uses_replace_on_url_change(self):
        """Fix-5/6: deploy.py passes replace_resources to terraform on URL change."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        assert (
            "replace_resources" in source
        ), "Must pass replace_resources to terraform when URL changes"

    def test_terraform_runner_supports_replace(self):
        """Fix-5/6: terraform_runner.run_terraform accepts replace_resources."""
        from scripts.common.terraform_runner import run_terraform

        source = inspect.getsource(run_terraform)
        assert "replace_resources" in source
        assert "-replace" in source


# ── Iteration 5: Pipeline Latency Optimization ────────────────────────────────


class TestComputePoolCFU:
    """Tests for REQ-E-001: Flink compute pool max_cfu increased to 50."""

    def test_compute_pool_max_cfu_is_50(self):
        """TC-E-001a: Terraform compute pool max_cfu is 50."""
        core_main = Path("terraform/core/main.tf").read_text()
        assert (
            "max_cfu      = 50" in core_main
        ), "Compute pool max_cfu should be 50 for higher parallelism"


class TestDirectDispatchPath:
    """Tests for REQ-E-002/003: dispatch reads from anomalies_per_zone directly."""

    def test_dispatch_sql_template_exists(self):
        """TC-E-002a: dispatch-insert.sql template exists."""
        sql_file = Path("terraform/agents/sql/dispatch-insert.sql")
        assert sql_file.exists(), "dispatch-insert.sql must exist"
        sql = sql_file.read_text()
        assert "INSERT INTO" in sql
        assert "completed_actions" in sql

    def test_dispatch_reads_from_anomalies_per_zone(self):
        """TC-E-002b: dispatch INSERT reads from anomalies_per_zone (not anomalies_enriched)."""
        sql = Path("terraform/agents/sql/dispatch-insert.sql").read_text()
        assert (
            "anomalies_per_zone" in sql
        ), "Dispatch must read directly from anomalies_per_zone"
        assert "AI_RUN_AGENT" in sql, "Dispatch must use AI_RUN_AGENT"
        assert "is_surge" in sql, "Dispatch must filter on is_surge"

    def test_dispatch_does_not_depend_on_enrichment(self):
        """TC-E-003a: dispatch path is independent of RAG enrichment."""
        sql = Path("terraform/agents/sql/dispatch-insert.sql").read_text()
        assert (
            "anomalies_enriched" not in sql
        ), "Dispatch must NOT read from anomalies_enriched (parallel path)"
        assert (
            "anomaly_reason" not in sql or "CONCAT" in sql
        ), "Dispatch must generate its own anomaly reason, not depend on RAG"

    def test_completed_actions_ctas_exists(self):
        """TC-E-003b: completed-actions-ctas.sql DDL template exists."""
        sql_file = Path("terraform/agents/sql/completed-actions-ctas.sql")
        assert sql_file.exists(), "completed-actions-ctas.sql must exist"
        sql = sql_file.read_text()
        assert "CREATE TABLE" in sql
        assert "completed_actions" in sql

    def test_deploy_includes_dispatch_in_dml_list(self):
        """TC-E-003c: deploy.py includes dispatch-insert in DML_STATEMENTS."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "dispatch-insert" in source
        ), "deploy.py must include dispatch-insert in DML_STATEMENTS"

    def test_deploy_includes_completed_actions_in_ddl_list(self):
        """TC-E-003d: deploy.py includes completed-actions-ctas in DDL_STATEMENTS."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy)
        assert (
            "completed-actions-ctas" in source
        ), "deploy.py must include completed-actions-ctas in DDL_STATEMENTS"

    def test_destroy_includes_dispatch_statements(self):
        """TC-E-003e: destroy.py covers dispatch + CTAS statements.

        REQ-CRF-051: assert against the canonical registry rather than
        source-grepping literals (statement names now live in
        scripts.common.flink_statements).
        """
        from scripts.common.flink_statements import ALL_DELETABLE_STATEMENTS

        assert "dispatch-insert" in ALL_DELETABLE_STATEMENTS
        assert "completed-actions-ctas" in ALL_DELETABLE_STATEMENTS

    def test_dashboard_dispatch_reads_anomalies_per_zone(self):
        """TC-E-003f: dashboard agent INSERT reads from anomalies_per_zone."""
        dashboard = importlib.import_module("scripts.dashboard")
        assert (
            "anomalies_per_zone" in dashboard.AGENT_SQL_INSERT_COMPLETED_ACTIONS
        ), "Dashboard agent INSERT must read from anomalies_per_zone"

    def test_dispatch_sql_has_nullif_protection(self):
        """TC-E-003g: dispatch SQL uses NULLIF to prevent division by zero."""
        sql = Path("terraform/agents/sql/dispatch-insert.sql").read_text()
        assert (
            "NULLIF" in sql
        ), "Dispatch SQL must use NULLIF to prevent division by zero"


class TestConfigurableLLM:
    """Tests for REQ-E-004/005: configurable LLM model via environment variable."""

    def test_terraform_has_bedrock_model_id_variable(self):
        """TC-E-004a: Terraform core has bedrock_model_id variable."""
        variables = Path("terraform/core/variables.tf").read_text()
        assert (
            "bedrock_model_id" in variables
        ), "variables.tf must define bedrock_model_id"

    def test_terraform_model_defaults_to_sonnet_4_6(self):
        """TC-E-004b: bedrock_model_id defaults to Sonnet 4.6 (global profile)."""
        variables = Path("terraform/core/variables.tf").read_text()
        assert (
            "global.anthropic.claude-sonnet-4-6" in variables
        ), "bedrock_model_id should default to global.anthropic.claude-sonnet-4-6"

    def test_terraform_connection_uses_variable(self):
        """TC-E-004c: Bedrock connection uses var.bedrock_model_id in endpoint."""
        main_tf = Path("terraform/core/main.tf").read_text()
        assert (
            "var.bedrock_model_id" in main_tf
        ), "Bedrock connection endpoint must reference var.bedrock_model_id"

    def test_tfvars_supports_bedrock_model_id(self):
        """TC-E-005a: tfvars.py passes bedrock_model_id to terraform."""
        from scripts.common import tfvars

        source = inspect.getsource(tfvars)
        assert "bedrock_model_id" in source, "tfvars.py must support bedrock_model_id"

    def test_tfvars_generates_model_id_in_output(self):
        """TC-E-005b: generate_core_tfvars_content includes bedrock_model_id."""
        from scripts.common.tfvars import generate_core_tfvars_content

        result = generate_core_tfvars_content(
            region="us-east-1",
            api_key="test",
            api_secret="test",
            aws_bedrock_access_key="ak",
            aws_bedrock_secret_key="sk",
            bedrock_model_id="anthropic.claude-haiku-4-5-20251001-v1:0",
        )
        assert (
            "bedrock_model_id" in result
        ), "Generated tfvars must include bedrock_model_id when provided"


class TestReducedToolTimeout:
    """Tests for REQ-E-006: MCP tool request_timeout reduced to 15s."""

    def test_dashboard_tool_timeout_is_15(self):
        """TC-E-006a: Dashboard CREATE TOOL has request_timeout = 15."""
        dashboard = importlib.import_module("scripts.dashboard")
        assert (
            "'request_timeout' = '15'" in dashboard.AGENT_SQL_CREATE_TOOL
        ), "MCP tool request_timeout must be 15 (not 30)"

    def test_dashboard_tool_timeout_not_30(self):
        """TC-E-006b: Dashboard CREATE TOOL does NOT have request_timeout = 30."""
        dashboard = importlib.import_module("scripts.dashboard")
        assert (
            "'request_timeout' = '30'" not in dashboard.AGENT_SQL_CREATE_TOOL
        ), "Old 30s timeout must be removed"


class TestBatchCounterReset:
    """Dashboard's `.batch_counter` must reset on pipeline_reset and destroy."""

    def test_helper_relative_path_is_correct(self):
        from scripts.common.datagen_helpers import BATCH_COUNTER_RELATIVE

        assert BATCH_COUNTER_RELATIVE == ("assets", "data", ".batch_counter")

    def test_helper_deletes_existing_file(self, tmp_path):
        from scripts.common.datagen_helpers import (
            BATCH_COUNTER_RELATIVE,
            reset_batch_counter,
        )

        target = tmp_path.joinpath(*BATCH_COUNTER_RELATIVE)
        target.parent.mkdir(parents=True)
        target.write_text("7")
        assert reset_batch_counter(tmp_path) is True
        assert not target.exists()

    def test_helper_returns_false_when_missing(self, tmp_path):
        from scripts.common.datagen_helpers import reset_batch_counter

        # File doesn't exist; helper must not raise.
        assert reset_batch_counter(tmp_path) is False

    def test_pipeline_reset_invokes_reset_batch_counter(self):
        pipeline_reset = importlib.import_module("scripts.pipeline_reset")
        source = inspect.getsource(pipeline_reset.reset_pipeline)
        assert "reset_batch_counter" in source, (
            "reset_pipeline must call reset_batch_counter so the dashboard "
            "starts the next seed run from batch 1 with an empty pipeline."
        )

    def test_destroy_invokes_reset_batch_counter(self):
        destroy = importlib.import_module("scripts.destroy")
        source = inspect.getsource(destroy)
        assert "reset_batch_counter" in source, (
            "destroy must call reset_batch_counter so a fresh deploy "
            "doesn't resume mid-sequence."
        )

    def test_dashboard_uses_shared_relative_path_constant(self):
        """Dashboard, pipeline_reset, and destroy must all derive the
        path from the same BATCH_COUNTER_RELATIVE tuple — otherwise a
        rename in one place silently breaks the other two."""
        dashboard = importlib.import_module("scripts.dashboard")
        source = inspect.getsource(dashboard)
        assert "BATCH_COUNTER_RELATIVE" in source, (
            "dashboard should import BATCH_COUNTER_RELATIVE from "
            "scripts.common.datagen_helpers, not hard-code the path."
        )
