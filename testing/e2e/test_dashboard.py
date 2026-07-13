"""Real-Time Workshop Dashboard Tests.

Tests for scripts/dashboard.py covering:
- Module imports and function signatures
- Credential resolution logic
- Data transformation (Decimal128, dates, JSON parsing)
- Architecture diagram rendering
- Panel rendering functions
- CLI entry point
- Filter/query building
"""

import importlib
import inspect
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Ensure project root is on sys.path so `from scripts import ...` works
# regardless of how pytest collects this file.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Task 1: Skeleton + Entry Point
# ---------------------------------------------------------------------------


class TestDashboardImport:
    """TC-DASH-001: Module imports and callable checks."""

    def test_module_imports(self):
        """TC-DASH-001a: dashboard module is importable."""
        from scripts import dashboard

        assert dashboard is not None

    def test_main_is_callable(self):
        """TC-DASH-001b: main function exists and is callable."""
        from scripts.dashboard import main

        assert callable(main)

    def test_run_dashboard_is_callable(self):
        """TC-DASH-001c: _run_dashboard function exists."""
        from scripts.dashboard import _run_dashboard

        assert callable(_run_dashboard)


class TestDeployDispatchDetection:
    """The dashboard must recognise a deploy-provisioned pipeline (canonical
    statement names) so 'Run Agent Dispatch' does not DROP shared Flink
    tool/agent/table objects the deployed dispatch-insert depends on."""

    def test_canonical_statement_names_defined(self):
        from scripts import dashboard

        assert dashboard.DEPLOY_DISPATCH_STATEMENT == "dispatch-insert"
        assert "create-tool-mongodb-fleet" in dashboard.DEPLOY_AGENT_STATEMENTS
        assert "create-agent-boat-dispatch" in dashboard.DEPLOY_AGENT_STATEMENTS

    def test_canonical_names_match_deploy(self):
        """The dashboard's canonical names must match deploy's DML statements —
        otherwise cross-detection silently checks the wrong statement."""
        from scripts import dashboard, deploy

        dml_src = inspect.getsource(deploy._create_flink_dml_statements)
        assert f'"{dashboard.DEPLOY_DISPATCH_STATEMENT}"' in dml_src

    def test_sidebar_checks_deploy_dispatch(self):
        """The detection code path must consult the canonical dispatch statement
        before offering to recreate the shared objects."""
        from scripts import dashboard

        src = inspect.getsource(dashboard._render_sidebar)
        assert "DEPLOY_DISPATCH_STATEMENT" in src
        assert "deploy_dispatch_active" in src
        # And the "active" verdict must incorporate the deploy check.
        assert "or deploy_dispatch_active" in src


class TestDashboardCLI:
    """TC-DASH-002: CLI entry point behavior."""

    def test_help_flag(self):
        """TC-DASH-002a: --help prints usage and exits 0."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.dashboard", "--help"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode == 0
        assert (
            "dashboard" in result.stdout.lower() or "streamlit" in result.stdout.lower()
        )


class TestDashboardEntryPoint:
    """TC-DASH-003: Streamlit detection logic."""

    def test_main_detects_non_streamlit_context(self):
        """TC-DASH-003a: main() detects CLI context (not inside Streamlit)."""
        from scripts.dashboard import _is_running_in_streamlit

        # When run from pytest, we are NOT inside Streamlit
        assert _is_running_in_streamlit() is False


class TestEntryPointRegistration:
    """TC-REG-004: pyproject.toml entry points preserved."""

    def test_dashboard_entry_point_exists(self):
        """TC-REG-004a: dashboard entry point registered."""
        import tomllib

        pyproject = PROJECT_ROOT / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        scripts = data["project"]["scripts"]
        assert "dashboard" in scripts
        assert scripts["dashboard"] == "scripts.dashboard:main"

    def test_existing_entry_points_preserved(self):
        """TC-REG-004b: All pre-existing entry points still present."""
        import tomllib

        pyproject = PROJECT_ROOT / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        scripts = data["project"]["scripts"]
        # Verify key existing entry points
        assert "asp-setup" in scripts
        assert "datagen" in scripts
        assert "deploy" in scripts
        assert "destroy" in scripts

    def test_streamlit_dependency_added(self):
        """TC-REG-004c: streamlit is in dependencies."""
        import tomllib

        pyproject = PROJECT_ROOT / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        deps = data["project"]["dependencies"]
        streamlit_deps = [d for d in deps if d.startswith("streamlit")]
        assert len(streamlit_deps) >= 1


# ---------------------------------------------------------------------------
# Task 2: Credential Resolution
# ---------------------------------------------------------------------------


class TestCredentialResolution:
    """TC-DASH-004 through TC-DASH-008: Credential resolution chain."""

    def test_resolve_from_credentials_env(self, tmp_path):
        """TC-DASH-004: Resolves URI from .env TF_VAR."""
        from scripts.dashboard import _resolve_mongodb_uri

        creds_file = tmp_path / ".env"
        creds_file.write_text(
            'TF_VAR_mongodb_connection_string="mongodb+srv://user:pass@cluster.mongodb.net"\n'
        )
        uri = _resolve_mongodb_uri(project_root=tmp_path)
        assert uri is not None
        assert "mongodb" in uri

    def test_resolve_from_terraform_tfvars(self, tmp_path, monkeypatch):
        """TC-DASH-005: Resolves URI from terraform.tfvars when .env missing."""
        from scripts.dashboard import _resolve_mongodb_uri

        # Create a pyproject.toml so tmp_path is treated as project root
        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        tf_dir = tmp_path / "terraform" / "agents"
        tf_dir.mkdir(parents=True)
        tfvars = tf_dir / "terraform.tfvars"
        tfvars.write_text(
            'mongodb_connection_string = "cluster.8iv8n.mongodb.net"\n'
            'mongodb_username = "testuser"\n'
            'mongodb_password = "testpass"\n'
        )
        monkeypatch.delenv("MONGODB_URI", raising=False)
        uri = _resolve_mongodb_uri(project_root=tmp_path)
        assert uri is not None
        assert "testuser" in uri
        assert "testpass" in uri

    def test_resolve_from_env_var(self, tmp_path, monkeypatch):
        """TC-DASH-006: Resolves URI from MONGODB_URI env var."""
        from scripts.dashboard import _resolve_mongodb_uri

        # Create a pyproject.toml so tmp_path is treated as project root
        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        monkeypatch.setenv("MONGODB_URI", "mongodb+srv://env:pass@cluster.mongodb.net")
        uri = _resolve_mongodb_uri(project_root=tmp_path)
        assert uri is not None
        assert "env:pass" in uri

    def test_resolve_returns_none_when_nothing_available(self, tmp_path, monkeypatch):
        """TC-DASH-007: Returns None when no credentials source available."""
        from scripts.dashboard import _resolve_mongodb_uri

        # Create a pyproject.toml so tmp_path is treated as project root
        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        monkeypatch.delenv("MONGODB_URI", raising=False)
        uri = _resolve_mongodb_uri(project_root=tmp_path)
        assert uri is None

    def test_connect_mongodb_returns_none_on_bad_uri(self):
        """TC-DASH-008: _connect_mongodb returns None on connection failure."""
        from scripts.dashboard import _connect_mongodb

        client = _connect_mongodb(
            "mongodb://invalid-host:27017/?serverSelectionTimeoutMS=500"
        )
        assert client is None


# ---------------------------------------------------------------------------
# Task 3: Data Fetching + Transformation
# ---------------------------------------------------------------------------


class TestDataTransformation:
    """TC-DASH-024, TC-DASH-025: Data type handling."""

    def test_decimal128_to_float(self):
        """TC-DASH-024: Decimal128 values are converted to float."""
        from scripts.dashboard import _convert_decimal128

        try:
            from bson.decimal128 import Decimal128

            val = Decimal128("123.45")
            result = _convert_decimal128(val)
            assert isinstance(result, float)
            assert result == pytest.approx(123.45)
        except ImportError:
            pytest.skip("bson not available")

    def test_decimal128_passthrough_for_non_decimal(self):
        """TC-DASH-024b: Non-Decimal128 values pass through unchanged."""
        from scripts.dashboard import _convert_decimal128

        assert _convert_decimal128(42) == 42
        assert _convert_decimal128("hello") == "hello"
        assert _convert_decimal128(3.14) == pytest.approx(3.14)

    def test_dates_formatted_as_utc(self):
        """TC-DASH-025: Dates are formatted in UTC."""
        from scripts.dashboard import _format_datetime

        dt = datetime(2025, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        formatted = _format_datetime(dt)
        assert "UTC" in formatted or "2025" in formatted
        assert "14:30" in formatted


class TestDispatchJsonParsing:
    """TC-DASH-026, TC-DASH-027: dispatch_json parsing."""

    def test_valid_json_parsed(self):
        """TC-DASH-026: Valid JSON string is parsed to dict."""
        from scripts.dashboard import _parse_dispatch_json

        result = _parse_dispatch_json('{"action": "dispatch", "zone": "CBD"}')
        assert isinstance(result, dict)
        assert result["action"] == "dispatch"

    def test_invalid_json_returns_raw(self):
        """TC-DASH-027: Invalid JSON returns raw string."""
        from scripts.dashboard import _parse_dispatch_json

        result = _parse_dispatch_json("not valid json {{{")
        assert isinstance(result, str)
        assert "not valid json" in result


class TestFilterBuilding:
    """TC-DASH-009, TC-DASH-010: Filter construction."""

    def test_zone_filter_builds_in_query(self):
        """TC-DASH-009: Zone filter produces $in query."""
        from scripts.dashboard import _build_zone_filter

        f = _build_zone_filter(["CBD", "French Quarter"], field="zone")
        assert f["zone"]["$in"] == ["CBD", "French Quarter"]

    def test_zone_filter_empty_returns_empty_dict(self):
        """TC-DASH-009b: Empty zone list returns no filter."""
        from scripts.dashboard import _build_zone_filter

        f = _build_zone_filter([], field="zone")
        assert f == {}

    def test_time_filter_builds_gte_query(self):
        """TC-DASH-010: Time range produces $gte query."""
        from scripts.dashboard import _build_time_filter

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        f = _build_time_filter(cutoff, field="window_start")
        assert f["window_start"]["$gte"] == cutoff

    def test_time_filter_none_returns_empty_dict(self):
        """TC-DASH-010b: None cutoff returns no filter."""
        from scripts.dashboard import _build_time_filter

        f = _build_time_filter(None, field="window_start")
        assert f == {}


class TestCacheDecorator:
    """TC-DASH-028: Cache decorator usage."""

    def test_fetch_functions_exist(self):
        """TC-DASH-028: All fetch functions are defined."""
        from scripts import dashboard

        assert callable(getattr(dashboard, "_fetch_zone_traffic", None))
        assert callable(getattr(dashboard, "_fetch_anomalies", None))
        assert callable(getattr(dashboard, "_fetch_dispatches", None))
        assert callable(getattr(dashboard, "_fetch_knowledge_base", None))
        assert callable(getattr(dashboard, "_get_collection_counts", None))


# ---------------------------------------------------------------------------
# Task 4: KPI Row + Architecture Diagram
# ---------------------------------------------------------------------------


class TestKPIRow:
    """TC-DASH-011: KPI metric rendering."""

    def test_kpi_data_structure(self):
        """TC-DASH-011: _build_kpi_data returns 5 metrics."""
        from scripts.dashboard import _build_kpi_data

        counts = {
            "zone_traffic": 42,
            "anomalies": 5,
            "dispatches": 3,
            "knowledge_base": 6,
            "ride_requests": 23289,
        }
        kpi = _build_kpi_data(counts)
        assert len(kpi) == 5
        assert all("label" in m and "value" in m for m in kpi)


class TestArchitectureDiagram:
    """TC-DASH-012, TC-DASH-013, TC-DASH-014: Architecture diagram HTML."""

    def test_diagram_contains_technology_colors(self):
        """TC-DASH-012: Diagram HTML has Kafka/Flink/MongoDB/ASP colors."""
        from scripts.dashboard import _build_architecture_html

        counts = {
            "zone_traffic": 0,
            "anomalies": 0,
            "dispatches": 0,
            "knowledge_base": 0,
        }
        html = _build_architecture_html(counts)
        # Kafka blue, Flink orange, MongoDB green, ASP purple
        assert "#0078FF" in html or "kafka" in html.lower()
        assert "#FF9800" in html or "flink" in html.lower()
        assert "#00ED64" in html or "mongodb" in html.lower()
        assert "#A855F7" in html or "asp" in html.lower()

    def test_diagram_green_dot_when_data_exists(self):
        """TC-DASH-013: Green status dot when collection has data."""
        from scripts.dashboard import _build_architecture_html

        counts = {
            "zone_traffic": 42,
            "anomalies": 0,
            "dispatches": 0,
            "knowledge_base": 6,
        }
        html = _build_architecture_html(counts)
        # Should contain active indicators for collections with data
        assert "42" in html
        assert "6" in html

    def test_diagram_gray_dot_when_empty(self):
        """TC-DASH-014: Muted status dot when collection is empty."""
        from scripts.dashboard import _build_architecture_html

        counts = {
            "zone_traffic": 0,
            "anomalies": 0,
            "dispatches": 0,
            "knowledge_base": 0,
        }
        html = _build_architecture_html(counts)
        assert (
            "gray" in html.lower()
            or "#9E9E9E" in html
            or "#888" in html
            or "#3d4f58" in html
        )


# ---------------------------------------------------------------------------
# Task 5: Zone Traffic + Heatmap Panels
# ---------------------------------------------------------------------------


class TestZoneTrafficPanel:
    """TC-DASH-015, TC-DASH-016: Zone traffic chart."""

    def test_traffic_chart_data_preparation(self):
        """TC-DASH-015: _prepare_traffic_chart_data produces plotly-ready data."""
        from scripts.dashboard import _prepare_traffic_chart_data

        raw = [
            {
                "zone": "CBD",
                "window_start": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "request_count": 10,
                "total_revenue": 100.0,
            },
            {
                "zone": "Uptown",
                "window_start": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "request_count": 5,
                "total_revenue": 50.0,
            },
        ]
        df = _prepare_traffic_chart_data(raw)
        assert len(df) == 2
        assert "zone" in df.columns
        assert "window_start" in df.columns
        assert "request_count" in df.columns

    def test_empty_traffic_message(self):
        """TC-DASH-016: Empty state returns correct help text."""
        from scripts.dashboard import EMPTY_STATE_MESSAGES

        assert "datagen" in EMPTY_STATE_MESSAGES["zone_traffic"]


class TestZoneHeatmap:
    """TC-DASH-017: Zone heatmap chart."""

    def test_heatmap_data_preparation(self):
        """TC-DASH-017: _prepare_heatmap_data aggregates by zone."""
        from scripts.dashboard import _prepare_heatmap_data

        raw = [
            {
                "zone": "CBD",
                "request_count": 10,
                "total_passengers": 20,
                "total_revenue": 100.0,
            },
            {
                "zone": "CBD",
                "request_count": 15,
                "total_passengers": 30,
                "total_revenue": 150.0,
            },
            {
                "zone": "Uptown",
                "request_count": 5,
                "total_passengers": 10,
                "total_revenue": 50.0,
            },
        ]
        df = _prepare_heatmap_data(raw)
        assert len(df) == 2  # Two unique zones
        cbd = df[df["zone"] == "CBD"].iloc[0]
        assert cbd["request_count"] == 25


# ---------------------------------------------------------------------------
# Task 6: Anomaly Detection Panel
# ---------------------------------------------------------------------------


class TestAnomalyPanel:
    """TC-DASH-018, TC-DASH-019: Anomaly detection display."""

    def test_anomaly_card_data_extraction(self):
        """TC-DASH-018: _prepare_anomaly_cards extracts display fields."""
        from scripts.dashboard import _prepare_anomaly_cards

        # Use real MongoDB field names (request_count, expected_requests)
        raw = [
            {
                "pickup_zone": "CBD",
                "window_time": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "request_count": 50,
                "expected_requests": 30,
                "anomaly_reason": "Event-driven surge due to Saints game",
                "top_chunk_1": "chunk1 text",
                "top_chunk_2": "chunk2 text",
                "top_chunk_3": "chunk3 text",
            }
        ]
        cards = _prepare_anomaly_cards(raw)
        assert len(cards) == 1
        card = cards[0]
        assert card["zone"] == "CBD"
        assert card["surplus"] == 20
        assert card["anomaly_reason"] == "Event-driven surge due to Saints game"
        assert len(card["rag_chunks"]) == 3

    def test_empty_anomalies_message(self):
        """TC-DASH-019: Empty state returns correct help text."""
        from scripts.dashboard import EMPTY_STATE_MESSAGES

        assert "Flink" in EMPTY_STATE_MESSAGES["anomalies"]


# ---------------------------------------------------------------------------
# Task 7: Dispatch Log + Knowledge Base Panels
# ---------------------------------------------------------------------------


class TestDispatchLogPanel:
    """TC-DASH-020, TC-DASH-021: Dispatch log display."""

    def test_dispatch_data_preparation(self):
        """TC-DASH-020: _prepare_dispatch_entries formats entries correctly."""
        from scripts.dashboard import _prepare_dispatch_entries

        raw = [
            {
                "pickup_zone": "CBD",
                "dispatched_at": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
                "dispatch_summary": "Dispatched 5 vehicles to CBD",
                "dispatch_json": '{"vehicles": 5}',
            }
        ]
        entries = _prepare_dispatch_entries(raw)
        assert len(entries) == 1
        assert entries[0]["zone"] == "CBD"
        assert isinstance(entries[0]["parsed_json"], dict)

    def test_empty_dispatches_message(self):
        """TC-DASH-021: Empty state returns correct help text."""
        from scripts.dashboard import EMPTY_STATE_MESSAGES

        msg = EMPTY_STATE_MESSAGES["dispatches"].lower()
        assert "dispatch" in msg and "pipeline" in msg


class TestKnowledgeBasePanel:
    """TC-DASH-022, TC-DASH-023: Knowledge base display."""

    def test_kb_card_data(self):
        """TC-DASH-022: _prepare_kb_cards formats event cards."""
        from scripts.dashboard import _prepare_kb_cards

        raw = [
            {
                "event_name": "Saints Game",
                "zone": "CBD",
                "venue": "Superdome",
                "expected_attendance": 73000,
                "impact_level": "high",
                "event_type": "sports",
            }
        ]
        cards = _prepare_kb_cards(raw)
        assert len(cards) == 1
        assert cards[0]["event_name"] == "Saints Game"
        assert cards[0]["impact_level"] == "high"

    def test_empty_kb_message(self):
        """TC-DASH-023: Empty state returns correct help text."""
        from scripts.dashboard import EMPTY_STATE_MESSAGES

        assert "asp-setup" in EMPTY_STATE_MESSAGES["knowledge_base"]


# ---------------------------------------------------------------------------
# Regression Tests
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Dynamic Zones
# ---------------------------------------------------------------------------


class TestDynamicZones:
    """TC-DASH-029: Dynamic zone list from MongoDB."""

    def test_fetch_zones_with_client(self):
        """TC-DASH-029a: Returns sorted zones from DB when client is available."""
        from scripts.dashboard import _fetch_distinct_zones

        mock_client = MagicMock()
        mock_client.__getitem__("analytics").__getitem__(
            "zone_traffic"
        ).distinct.return_value = [
            "Uptown",
            "Bywater",
            "French Quarter",
        ]
        result = _fetch_distinct_zones(mock_client)
        assert result == ["Bywater", "French Quarter", "Uptown"]

    def test_fetch_zones_without_client(self):
        """TC-DASH-029b: Returns FALLBACK_ZONES when client is None."""
        from scripts.dashboard import FALLBACK_ZONES, _fetch_distinct_zones

        result = _fetch_distinct_zones(None)
        assert result == FALLBACK_ZONES

    def test_fetch_zones_on_error(self):
        """TC-DASH-029c: Returns FALLBACK_ZONES on exception."""
        from scripts.dashboard import FALLBACK_ZONES, _fetch_distinct_zones

        mock_client = MagicMock()
        mock_client.__getitem__.side_effect = Exception("DB error")
        result = _fetch_distinct_zones(mock_client)
        assert result == FALLBACK_ZONES

    def test_fallback_zones_include_bywater_and_cbd(self):
        """TC-DASH-029d: FALLBACK_ZONES includes Bywater and Central Business District (CBD)."""
        from scripts.dashboard import FALLBACK_ZONES

        assert "Bywater" in FALLBACK_ZONES
        assert "Central Business District (CBD)" in FALLBACK_ZONES
        assert "CBD" not in FALLBACK_ZONES  # Should NOT have bare "CBD"


# ---------------------------------------------------------------------------
# Flink Credentials
# ---------------------------------------------------------------------------


class TestFlinkCredentials:
    """TC-DASH-030: Flink credential loading from terraform state."""

    def test_load_flink_credentials_from_state(self, tmp_path):
        """TC-DASH-030a: Loads credentials from valid terraform.tfstate."""
        from scripts.dashboard import _load_flink_credentials

        # Create minimal project structure
        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        tf_dir = tmp_path / "terraform" / "core"
        tf_dir.mkdir(parents=True)
        state = {
            "outputs": {
                "app_manager_flink_api_key": {"value": "fk-123"},
                "app_manager_flink_api_secret": {"value": "fs-456"},
                "confluent_flink_rest_endpoint": {"value": "https://flink.example.com"},
                "confluent_organization_id": {"value": "org-1"},
                "confluent_environment_id": {"value": "env-1"},
                "confluent_flink_compute_pool_id": {"value": "pool-1"},
                "confluent_environment_display_name": {"value": "my-env"},
                "confluent_kafka_cluster_display_name": {"value": "my-cluster"},
            }
        }
        (tf_dir / "terraform.tfstate").write_text(json.dumps(state))
        creds = _load_flink_credentials(project_root=tmp_path)
        assert creds is not None
        assert creds["flink_api_key"] == "fk-123"
        assert creds["flink_rest_endpoint"] == "https://flink.example.com"
        assert creds["compute_pool_id"] == "pool-1"

    def test_load_flink_credentials_missing_state(self, tmp_path):
        """TC-DASH-030b: Returns None when terraform.tfstate is missing."""
        from scripts.dashboard import _load_flink_credentials

        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        creds = _load_flink_credentials(project_root=tmp_path)
        assert creds is None

    def test_load_flink_credentials_incomplete_outputs(self, tmp_path):
        """TC-DASH-030c: Returns None when required outputs are missing."""
        from scripts.dashboard import _load_flink_credentials

        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        tf_dir = tmp_path / "terraform" / "core"
        tf_dir.mkdir(parents=True)
        state = {"outputs": {"app_manager_flink_api_key": {"value": "fk-123"}}}
        (tf_dir / "terraform.tfstate").write_text(json.dumps(state))
        creds = _load_flink_credentials(project_root=tmp_path)
        assert creds is None


# ---------------------------------------------------------------------------
# Flink SQL Submission
# ---------------------------------------------------------------------------


class TestFlinkSQLSubmission:
    """TC-DASH-031: Flink SQL REST API submission."""

    def test_submit_flink_sql_success(self):
        """TC-DASH-031a: Successful submission returns response JSON."""
        from scripts.dashboard import _submit_flink_sql

        creds = {
            "flink_api_key": "key",
            "flink_api_secret": "secret",
            "flink_rest_endpoint": "https://flink.example.com",
            "organization_id": "org-1",
            "environment_id": "env-1",
            "compute_pool_id": "pool-1",
            "environment_display_name": "my-env",
            "cluster_display_name": "my-cluster",
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "name": "test-stmt",
            "status": {"phase": "PENDING"},
        }
        mock_resp.raise_for_status.return_value = None
        with patch("scripts.dashboard._requests_mod") as mock_req:
            mock_req.post.return_value = mock_resp
            result = _submit_flink_sql(creds, "test-stmt", "SELECT 1")
        assert result["name"] == "test-stmt"

    def test_submit_flink_sql_error(self):
        """TC-DASH-031b: Failed submission returns error dict."""
        from scripts.dashboard import _submit_flink_sql

        creds = {
            "flink_api_key": "key",
            "flink_api_secret": "secret",
            "flink_rest_endpoint": "https://flink.example.com",
            "organization_id": "org-1",
            "environment_id": "env-1",
            "compute_pool_id": "pool-1",
            "environment_display_name": "my-env",
            "cluster_display_name": "my-cluster",
        }
        with patch("scripts.dashboard._requests_mod") as mock_req:
            mock_req.post.side_effect = Exception("Connection refused")
            result = _submit_flink_sql(creds, "test-stmt", "SELECT 1")
        assert "error" in result
        assert "Connection refused" in result["error"]


# ---------------------------------------------------------------------------
# Agent SQL Constants
# ---------------------------------------------------------------------------


class TestAgentSQLConstants:
    """TC-DASH-032: Agent SQL constants contain expected keywords."""

    def test_create_tool_sql(self):
        """TC-DASH-032a: AGENT_SQL_CREATE_TOOL contains CREATE TOOL."""
        from scripts.dashboard import AGENT_SQL_CREATE_TOOL

        assert "CREATE TOOL" in AGENT_SQL_CREATE_TOOL
        assert "mongodb-mcp-connection" in AGENT_SQL_CREATE_TOOL

    def test_create_agent_sql(self):
        """TC-DASH-032b: AGENT_SQL_CREATE_AGENT contains CREATE AGENT."""
        from scripts.dashboard import AGENT_SQL_CREATE_AGENT

        assert "CREATE AGENT" in AGENT_SQL_CREATE_AGENT
        assert "boat_dispatch_agent" in AGENT_SQL_CREATE_AGENT

    def test_create_completed_actions_sql(self):
        """TC-DASH-032c: Agent SQL contains CREATE TABLE + INSERT INTO with AI_RUN_AGENT."""
        from scripts.dashboard import (
            AGENT_SQL_CREATE_COMPLETED_ACTIONS_TABLE,
            AGENT_SQL_INSERT_COMPLETED_ACTIONS,
        )

        assert "CREATE TABLE" in AGENT_SQL_CREATE_COMPLETED_ACTIONS_TABLE
        assert "completed_actions" in AGENT_SQL_CREATE_COMPLETED_ACTIONS_TABLE
        assert "AI_RUN_AGENT" in AGENT_SQL_INSERT_COMPLETED_ACTIONS
        assert "INSERT INTO completed_actions" in AGENT_SQL_INSERT_COMPLETED_ACTIONS

    def test_agent_sql_steps_count(self):
        """TC-DASH-032d: AGENT_SQL_STEPS has 4 entries with dashboard- prefix."""
        from scripts.dashboard import AGENT_SQL_STEPS

        assert len(AGENT_SQL_STEPS) == 4
        for name, sql in AGENT_SQL_STEPS:
            assert name.startswith("dashboard-")
            assert len(sql) > 10


# ---------------------------------------------------------------------------
# Regression Tests
# ---------------------------------------------------------------------------


class TestRegressionExistingEntryPoints:
    """TC-REG-001, TC-REG-002: Existing commands still work."""

    def test_asp_setup_help(self):
        """TC-REG-001: asp-setup --help still works."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.asp_setup", "--help"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode == 0

    def test_datagen_importable(self):
        """TC-REG-002: datagen module still importable."""
        from scripts import datagen

        assert callable(datagen.main)


# ---------------------------------------------------------------------------
# Enhancement: Ride Requests Count + Window Timer
# ---------------------------------------------------------------------------


class TestRideRequestsKPI:
    """Tests for ride_requests Kafka topic message count in dashboard."""

    def test_kpi_data_includes_ride_requests(self):
        """TC-E-002a: _build_kpi_data includes a ride_requests metric."""
        from scripts.dashboard import _build_kpi_data

        counts = {
            "zone_traffic": 10,
            "anomalies": 2,
            "dispatches": 1,
            "knowledge_base": 6,
            "ride_requests": 23289,
        }
        kpi = _build_kpi_data(counts)
        labels = [m["label"] for m in kpi]
        assert any(
            "ride" in l.lower() or "request" in l.lower() for l in labels
        ), "KPI data must include a ride_requests metric"

    def test_kpi_data_ride_requests_value(self):
        """TC-E-002b: ride_requests KPI shows the correct count."""
        from scripts.dashboard import _build_kpi_data

        counts = {
            "zone_traffic": 10,
            "anomalies": 2,
            "dispatches": 1,
            "knowledge_base": 6,
            "ride_requests": 23289,
        }
        kpi = _build_kpi_data(counts)
        ride_kpi = [
            m
            for m in kpi
            if "ride" in m["label"].lower() or "request" in m["label"].lower()
        ]
        assert ride_kpi[0]["value"] == 23289

    def test_fetch_ride_requests_from_kafka_exists(self):
        """TC-E-002c: _fetch_ride_requests_from_kafka function exists."""
        from scripts.dashboard import _fetch_ride_requests_from_kafka

        assert callable(_fetch_ride_requests_from_kafka)

    def test_count_ride_requests_jsonl_fallback(self):
        """TC-E-002d: _count_ride_requests_jsonl returns line count from JSONL."""
        from scripts.dashboard import _count_ride_requests_jsonl

        count = _count_ride_requests_jsonl()
        assert count == 23289


class TestWindowTimer:
    """Tests for the tumbling-window countdown timer (WINDOW_MINUTES-aligned)."""

    def test_next_window_seconds_function_exists(self):
        """TC-E-003a: _seconds_to_next_window function exists."""
        from scripts.dashboard import _seconds_to_next_window

        assert callable(_seconds_to_next_window)

    def test_next_window_seconds_range(self):
        """TC-E-003b: _seconds_to_next_window returns 0..WINDOW_MINUTES*60."""
        from scripts.dashboard import WINDOW_MINUTES, _seconds_to_next_window

        result = _seconds_to_next_window()
        upper = WINDOW_MINUTES * 60
        assert 0 <= result <= upper, f"Expected 0-{upper}, got {result}"

    def test_next_window_seconds_deterministic(self):
        """TC-E-003c: at a known time, returns correct value for the 1-min window."""
        from datetime import datetime, timezone

        from scripts.dashboard import WINDOW_MINUTES, _seconds_to_next_window

        assert WINDOW_MINUTES == 1, "test written for the 1-minute window"
        # 12:03:40 UTC -> next 1-min boundary is 12:04:00 -> 20s remaining
        t = datetime(2026, 5, 6, 12, 3, 40, tzinfo=timezone.utc)
        result = _seconds_to_next_window(t)
        assert result == 20


class TestASPReseedAfterProcessors:
    """KB population via Python (supersedes REQ-E-001 change-stream re-seed).

    The event_knowledge_base_population ASP processor was removed (its
    Voyage $https call fails at the transport layer with HTTP 400 — the
    byte-identical request succeeds from curl/requests). The knowledge base
    is now populated directly in Python via populate_knowledge_base(), so
    the former "re-seed calendar after processors to wake the change stream"
    step is gone. These tests assert the new contract.
    """

    def test_run_asp_setup_populates_kb_after_seed(self):
        """run_asp_setup calls populate_knowledge_base after seed_events_calendar."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").run_asp_setup
        )
        seed_pos = source.find("seed_events_calendar")
        assert seed_pos != -1
        kb_pos = source.find("populate_knowledge_base", seed_pos)
        assert kb_pos != -1, (
            "run_asp_setup must call populate_knowledge_base AFTER "
            "seed_events_calendar"
        )

    def test_kb_population_respects_skip_seed(self):
        """KB population is gated by the same skip_seed / creds guard as seeding."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").run_asp_setup
        )
        kb_pos = source.find("populate_knowledge_base")
        assert kb_pos != -1
        guard = source.find("skip_seed")
        assert (
            guard != -1 and guard < kb_pos
        ), "populate_knowledge_base must be gated by the skip_seed guard"

    def test_kb_processor_removed_from_ensure_processors(self):
        """The broken event_knowledge_base_population processor must NOT be in
        the ensure_processors create list (its Voyage $https 400s)."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_processors
        )
        start = source.find("processors = [")
        assert start != -1
        depth = 0
        end = start
        for i in range(source.find("[", start), len(source)):
            if source[i] == "[":
                depth += 1
            elif source[i] == "]":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        create_list = source[start:end]
        assert "event_knowledge_base_population" not in create_list, (
            "event_knowledge_base_population must not be created — its Voyage "
            "$https call fails; KB is populated in Python instead"
        )


# ── TC-R-109: Dashboard count optimization ──────────────────────────────────


class TestDashboardCountOptimization:
    """REQ-R-109: dashboard uses estimatedDocumentCount for unfiltered KB count."""

    def test_kb_uses_estimated_document_count(self):
        """TC-R-109a: _get_collection_counts uses estimatedDocumentCount for knowledge_base."""
        import inspect

        from scripts import dashboard

        source = inspect.getsource(dashboard._get_collection_counts)
        assert (
            "estimated_document_count" in source.lower()
            or "estimateddocumentcount" in source.lower()
        ), (
            "REQ-R-109: knowledge_base count must use estimatedDocumentCount() "
            "to avoid O(n) collection scan"
        )

    def test_distinct_zones_caches_result(self):
        """TC-R-109b: _fetch_distinct_zones is wrapped with cache_data when streamlit available."""
        import inspect

        from scripts import dashboard

        # Either the function is decorated with @st.cache_data,
        # or the source references cache_data
        source = inspect.getsource(dashboard)
        # The cache decorator must appear in dashboard module
        assert (
            "cache_data" in source
        ), "REQ-R-109: dashboard must cache distinct zones via @st.cache_data"


class TestSharedMongoHelperUsage:
    """REQ-R-106: dashboard uses scripts.common.mongo helper."""

    def test_dashboard_imports_common_mongo(self):
        import inspect

        from scripts import dashboard

        source = inspect.getsource(dashboard)
        assert (
            "scripts.common.mongo" in source
        ), "dashboard must use shared MongoClient helper"


class TestLiveDispatchMap:
    """Live boat dispatch map (pydeck TripsLayer) on the dashboard."""

    def test_zone_coords_cover_all_fallback_zones(self):
        from scripts.dashboard import FALLBACK_ZONES, ZONE_COORDS

        for zone in FALLBACK_ZONES:
            assert zone in ZONE_COORDS, f"Missing coords for {zone}"
            lon, lat = ZONE_COORDS[zone]
            # New Orleans bounding box sanity
            assert -90.5 < lon < -89.5, f"{zone} lon out of NOLA range"
            assert 29.8 < lat < 30.2, f"{zone} lat out of NOLA range"

    def test_zone_coords_alias_cbd(self):
        """CBD short form (used by the agent prompt) maps to the same point as the long form."""
        from scripts.dashboard import ZONE_COORDS

        assert ZONE_COORDS["CBD"] == ZONE_COORDS["Central Business District (CBD)"]

    def test_render_live_dispatch_map_exists(self):
        from scripts.dashboard import _render_live_dispatch_map

        assert callable(_render_live_dispatch_map)

    def test_pydeck_is_imported(self):
        """pydeck must be a try-import with HAS_PYDECK flag (project convention)."""
        import inspect

        from scripts import dashboard

        source = inspect.getsource(dashboard)
        assert "HAS_PYDECK" in source
        assert "import pydeck" in source

    def test_build_dispatch_trips_basic(self):
        """Trip path follows the Mississippi centerline (multi-segment).
        Endpoints sit at the OSM-sourced river waypoints; intermediate
        points trace the channel between them."""
        from scripts.dashboard import ZONE_DOCK_COORDS, _build_dispatch_trips

        dispatches = [
            {
                "pickup_zone": "French Quarter",
                "dispatch_json": '[{"vessel_id":"VESSEL-13","new_zone":"French Quarter"}]',
            }
        ]
        vessel_home = {"VESSEL-13": "Central Business District (CBD)"}
        trips = _build_dispatch_trips(dispatches, vessel_home)
        assert len(trips) == 1
        trip = trips[0]
        # First and last points are the dock coords for origin/destination
        assert trip["path"][0] == ZONE_DOCK_COORDS["Central Business District (CBD)"]
        assert trip["path"][-1] == ZONE_DOCK_COORDS["French Quarter"]
        # Multi-segment river path — at least origin + dest
        assert len(trip["path"]) >= 2
        # Timestamps strictly monotonic
        assert all(
            trip["timestamps"][i] <= trip["timestamps"][i + 1]
            for i in range(len(trip["timestamps"]) - 1)
        )
        # One timestamp per waypoint
        assert len(trip["timestamps"]) == len(trip["path"])
        assert trip["vessel_id"] == "VESSEL-13"
        assert trip["destination"] == "French Quarter"

    def test_build_dispatch_trips_skips_same_zone(self):
        """A vessel already in the surge zone has nothing to animate."""
        from scripts.dashboard import _build_dispatch_trips

        dispatches = [
            {
                "pickup_zone": "French Quarter",
                "dispatch_json": '[{"vessel_id":"V1","new_zone":"French Quarter"}]',
            }
        ]
        # Vessel's home is the destination — origin == dest
        trips = _build_dispatch_trips(dispatches, {"V1": "French Quarter"})
        assert trips == []

    def test_build_dispatch_trips_handles_missing_vessel_home(self):
        from scripts.dashboard import _build_dispatch_trips

        dispatches = [
            {
                "pickup_zone": "French Quarter",
                "dispatch_json": '[{"vessel_id":"UNKNOWN","new_zone":"French Quarter"}]',
            }
        ]
        assert _build_dispatch_trips(dispatches, {}) == []

    def test_build_dispatch_trips_handles_invalid_json(self):
        """REQ-E-200 (2026-05-15): when dispatch_json is unparseable but
        pickup_zone + vessel_home are valid, _build_dispatch_trips falls
        back to synthesized trips. Previously returned [] — the new
        behavior is intentional (see spec)."""
        from scripts.dashboard import _build_dispatch_trips

        dispatches = [{"pickup_zone": "French Quarter", "dispatch_json": "not json"}]
        trips = _build_dispatch_trips(dispatches, {"V1": "Bywater"})
        assert len(trips) == 1
        assert trips[0]["vessel_id"] == "V1"
        assert trips[0]["destination"] == "French Quarter"

    def test_build_dispatch_trips_skips_unknown_zone(self):
        """Unknown zone names (e.g. typo from LLM) are dropped, not crashed on."""
        from scripts.dashboard import _build_dispatch_trips

        dispatches = [
            {
                "pickup_zone": "Atlantis",
                "dispatch_json": '[{"vessel_id":"V1","new_zone":"Atlantis"}]',
            }
        ]
        assert _build_dispatch_trips(dispatches, {"V1": "Bywater"}) == []

    def test_interpolate_boat_positions_midpoint(self):
        from scripts.dashboard import _interpolate_boat_positions

        trips = [
            {
                "path": [[0.0, 0.0], [10.0, 20.0]],
                "timestamps": [1000, 2000],
                "vessel_id": "V1",
                "destination": "X",
            }
        ]
        # halfway through
        icons = _interpolate_boat_positions(trips, 1500)
        assert len(icons) == 1
        assert icons[0]["position"] == [5.0, 10.0]
        assert icons[0]["vessel_id"] == "V1"

    def test_interpolate_boat_positions_outside_window(self):
        """Boat is hidden when current_time falls outside the trip window."""
        from scripts.dashboard import _interpolate_boat_positions

        trips = [
            {
                "path": [[0.0, 0.0], [1.0, 1.0]],
                "timestamps": [5000, 6000],
                "vessel_id": "V1",
                "destination": "X",
            }
        ]
        assert _interpolate_boat_positions(trips, 100) == []
        assert _interpolate_boat_positions(trips, 9000) == []

    def test_build_zone_markers_highlights_active(self):
        from scripts.dashboard import _build_zone_markers

        markers = _build_zone_markers({"French Quarter"})
        # MongoDB green for surge, dim grey otherwise
        for m in markers:
            if m["name"] == "French Quarter":
                assert m["color"] == [0, 237, 100]
            else:
                assert m["color"] == [200, 213, 222]

    def test_build_zone_markers_dedupes_cbd_alias(self):
        """The 'CBD' alias and its long form share a coord — markers must not be duplicated."""
        from scripts.dashboard import ZONE_COORDS, _build_zone_markers

        markers = _build_zone_markers(set())
        positions = [tuple(m["position"]) for m in markers]
        # Each unique coordinate appears exactly once
        assert len(positions) == len(set(positions))
        # And we got exactly the count of unique coords
        unique_coords = {tuple(c) for c in ZONE_COORDS.values()}
        assert len(markers) == len(unique_coords)

    def test_loop_constants_are_consistent(self):
        """Trip duration must fit inside loop window with trail headroom."""
        from scripts.dashboard import (
            TRIPS_DURATION_MS,
            TRIPS_LOOP_MS,
            TRIPS_TRAIL_MS,
        )

        assert TRIPS_DURATION_MS < TRIPS_LOOP_MS
        assert TRIPS_TRAIL_MS <= TRIPS_DURATION_MS

    def test_live_map_moved_to_mission_control(self):
        """2026-07-14: the live dispatch map moved OUT of the Streamlit page
        and into the Mission Control HUD (web/, served by live_server), where
        it animates continuously instead of re-mounting on every rerun. The
        Streamlit flow must NOT render the map anymore, must link presenters
        to Mission Control instead, and the map helpers must stay importable
        (web/map.js mirrors them — parity is asserted elsewhere)."""
        import inspect

        from scripts import dashboard

        source = inspect.getsource(dashboard._run_dashboard)
        assert "_render_live_dispatch_map" not in source
        assert "_live_sse_url" in source  # the Mission Control call-out
        # Helpers survive for tests + JS-port parity.
        assert callable(dashboard._render_live_dispatch_map)
        assert callable(dashboard._build_dispatch_trips)


class TestDashboardPortFallback:
    """Issue (2026-05-29): `uv run dashboard` hard-failed with
    "Port 8501 is not available / Streamlit exited with code 1" when the
    default port was occupied (e.g. a stale dashboard from a prior
    deploy). The deploy-path launcher already falls back to the next free
    port; the standalone launcher must too.
    """

    def test_find_free_port_helper_exists(self):
        from scripts import dashboard

        assert hasattr(
            dashboard, "_find_free_port"
        ), "dashboard must expose _find_free_port for port-conflict fallback"

    def test_find_free_port_returns_requested_when_free(self):
        """When the requested port is free, return it unchanged."""
        import socket

        from scripts import dashboard

        # Find a definitely-free port by binding then releasing.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        free_port = s.getsockname()[1]
        s.close()
        assert dashboard._find_free_port(free_port) == free_port

    def test_find_free_port_falls_back_when_occupied(self):
        """When the requested port is occupied, return a different free port."""
        import socket

        from scripts import dashboard

        # Occupy a port for the duration of the test.
        occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        occupied.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        taken_port = occupied.getsockname()[1]
        try:
            result = dashboard._find_free_port(taken_port)
            assert (
                result != taken_port
            ), "must fall back to a different port when the requested one is taken"
            assert isinstance(result, int) and result > 0
        finally:
            occupied.close()

    def test_main_uses_find_free_port(self):
        """main() must consult _find_free_port before launching streamlit,
        so a busy default port no longer hard-fails."""
        import inspect

        from scripts import dashboard

        src = inspect.getsource(dashboard.main)
        assert "_find_free_port" in src, (
            "main() must call _find_free_port so an occupied port falls "
            "back instead of exiting with code 1"
        )


class TestDispatchSummaryCleaning:
    """Dispatch Log rendering fixes (2026-05-29): the dispatch_summary field
    can contain the FULL raw agent transcript (tool_call XML, tool_response
    blocks, chain-of-thought, duplicated JSON) when the SQL REGEXP_EXTRACT
    in dispatch-insert.sql fails to match and COALESCE falls back to the
    raw response. The dashboard must render a CLEAN summary regardless.
    """

    RAW_TRANSCRIPT = (
        "I'll analyze this surge and coordinate the dispatch. Let me start "
        "by reviewing available vessels.\n\n"
        '<tool_call> {"name": "get_vessel_catalog", "arguments": {}} </tool_call>\n'
        '<tool_response> { "vessels": [ {"vessel_id": "RB-101"} ] } </tool_response>\n\n'
        "With a 2.6x surge ratio I'll dispatch 6 boats.\n\n"
        '<tool_call> {"name": "dispatch_boats", "arguments": {"zone": "French Quarter"}} </tool_call>\n'
        '<tool_response> { "status": "success", "total_capacity_added": 94 } </tool_response>\n\n'
        "Dispatch Summary: Due to the surge in demand in the French Quarter "
        "(2.6x normal, 29 requests in 5 minutes), we dispatched 6 additional "
        "boats. Total added capacity is 94 seats.\n\n"
        'Dispatch JSON:\n{\n  "zone": "French Quarter"\n}\n'
        'API Response:\n{\n  "status": "success"\n}'
    )

    def test_clean_helper_exists(self):
        from scripts import dashboard

        assert hasattr(dashboard, "_clean_dispatch_summary"), (
            "dashboard must expose _clean_dispatch_summary to strip raw "
            "agent transcript noise"
        )

    def test_strips_tool_call_and_response_blocks(self):
        from scripts.dashboard import _clean_dispatch_summary

        cleaned = _clean_dispatch_summary(self.RAW_TRANSCRIPT)
        assert (
            "<tool_call>" not in cleaned and "</tool_call>" not in cleaned
        ), "must strip <tool_call> blocks"
        assert (
            "<tool_response>" not in cleaned and "</tool_response>" not in cleaned
        ), "must strip <tool_response> blocks"
        assert "get_vessel_catalog" not in cleaned, "must strip tool-call payloads"

    def test_extracts_summary_paragraph(self):
        from scripts.dashboard import _clean_dispatch_summary

        cleaned = _clean_dispatch_summary(self.RAW_TRANSCRIPT)
        assert (
            "Due to the surge in demand in the French Quarter" in cleaned
        ), "must keep the human-readable Dispatch Summary paragraph"
        # And drop the duplicated JSON / API Response dumps
        assert "Dispatch JSON" not in cleaned and "API Response" not in cleaned, (
            "must not include the raw Dispatch JSON / API Response dumps "
            "(those render separately via st.json)"
        )

    def test_already_clean_summary_passthrough(self):
        from scripts.dashboard import _clean_dispatch_summary

        clean = "Dispatched 6 boats (94 seats) to French Quarter."
        assert _clean_dispatch_summary(clean).strip() == clean

    def test_only_tool_blocks_no_marker_returns_cleaned(self):
        """If there's no 'Dispatch Summary:' marker, still strip tool noise
        and return whatever prose remains (graceful, never the XML)."""
        from scripts.dashboard import _clean_dispatch_summary

        raw = (
            "Reviewing fleet.\n"
            '<tool_call> {"name": "x"} </tool_call>\n'
            '<tool_response> {"ok": true} </tool_response>\n'
            "Done."
        )
        cleaned = _clean_dispatch_summary(raw)
        assert "<tool_call>" not in cleaned
        assert "Reviewing fleet." in cleaned and "Done." in cleaned

    def test_empty_input_graceful(self):
        from scripts.dashboard import _clean_dispatch_summary

        assert _clean_dispatch_summary("") == ""
        assert _clean_dispatch_summary(None) == ""

    def test_strips_markdown_bold_around_markers(self):
        """The agent wraps section markers in markdown bold
        (**Dispatch Summary:** / **Dispatch JSON:**). The cleaner must not
        leave stray leading `** ` / trailing `**` around the summary."""
        from scripts.dashboard import _clean_dispatch_summary

        raw = (
            "**Dispatch Summary:** Due to the surge in French Quarter "
            "(2.6x), we dispatched 6 boats (94 seats).\n\n"
            '**Dispatch JSON:**\n{"zone": "French Quarter"}\n\n'
            '**API Response:**\n{"status": "success"}'
        )
        out = _clean_dispatch_summary(raw)
        assert not out.lstrip().startswith("*"), f"leading ** not stripped: {out!r}"
        assert not out.rstrip().endswith("*"), f"trailing ** not stripped: {out!r}"
        assert out.startswith("Due to the surge"), out
        assert "Dispatch JSON" not in out and "API Response" not in out
        assert "{" not in out, "structured JSON must not leak into the summary"

    def test_strips_bold_marker_no_inline_summary(self):
        """**Dispatch Summary:** on its own line, body on the next line."""
        from scripts.dashboard import _clean_dispatch_summary

        raw = (
            "**Dispatch Summary:**\n\nSent 3 boats to Marigny (35 seats).\n\n"
            "**Dispatch JSON:**\n{}"
        )
        out = _clean_dispatch_summary(raw)
        assert out.strip() == "Sent 3 boats to Marigny (35 seats)."


class TestDispatchEntryTimestampFallback:
    """Dispatch Log fix (2026-05-29): the header showed 'N/A' because
    dispatched_at is null (ASP $_stream_meta.source.ts resolves null for
    the Kafka-sourced processor). window_time IS populated and is the
    meaningful per-window time — use it as the fallback."""

    def test_falls_back_to_window_time_when_dispatched_at_null(self):
        from scripts.dashboard import _prepare_dispatch_entries

        wt = datetime(2026, 5, 29, 8, 0, tzinfo=timezone.utc)
        raw = [
            {
                "pickup_zone": "French Quarter",
                "dispatched_at": None,
                "window_time": wt,
                "dispatch_summary": "Dispatched 6 boats.",
                "dispatch_json": "{}",
            }
        ]
        entries = _prepare_dispatch_entries(raw)
        assert (
            entries[0]["dispatched_at"] == wt
        ), "must fall back to window_time when dispatched_at is null"

    def test_prefers_dispatched_at_when_present(self):
        from scripts.dashboard import _prepare_dispatch_entries

        da = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)
        wt = datetime(2026, 5, 29, 8, 0, tzinfo=timezone.utc)
        raw = [
            {
                "pickup_zone": "CBD",
                "dispatched_at": da,
                "window_time": wt,
                "dispatch_summary": "x",
                "dispatch_json": "{}",
            }
        ]
        entries = _prepare_dispatch_entries(raw)
        assert (
            entries[0]["dispatched_at"] == da
        ), "must use dispatched_at when present (not override with window_time)"

    def test_summary_is_cleaned_in_entries(self):
        """_prepare_dispatch_entries must run the summary through the cleaner."""
        from scripts.dashboard import _prepare_dispatch_entries

        raw = [
            {
                "pickup_zone": "French Quarter",
                "dispatched_at": datetime(2026, 5, 29, 8, 0, tzinfo=timezone.utc),
                "dispatch_summary": (
                    '<tool_call> {"name": "x"} </tool_call>\n'
                    "Dispatch Summary: Sent 6 boats.\n\nDispatch JSON:\n{}"
                ),
                "dispatch_json": "{}",
            }
        ]
        entries = _prepare_dispatch_entries(raw)
        assert (
            "<tool_call>" not in entries[0]["summary"]
        ), "_prepare_dispatch_entries must clean the summary"
        assert "Sent 6 boats." in entries[0]["summary"]


class TestDispatchRenderNoDoubleEscape:
    """Dispatch Log fix (2026-05-29): the summary was html.escape()'d before
    st.markdown, so `\"` rendered as the literal `&quot;` entity. Streamlit
    markdown (unsafe_allow_html=False) already sanitizes HTML/script and
    dangerous URLs, so the manual escape only corrupted display — and it
    never prevented the markdown-link attack it claimed to (html.escape
    doesn't touch [ ] ( ))."""

    def test_render_does_not_html_escape_summary(self):
        import inspect

        from scripts import dashboard

        src = inspect.getsource(dashboard._render_dispatches)
        # The summary render line must NOT wrap entry["summary"] in
        # html.escape (which produced the &quot; artifact).
        assert (
            'html.escape(str(entry["summary"]' not in src
            and "html.escape(str(entry['summary']" not in src
        ), (
            "summary must not be html.escape'd before st.markdown "
            "(causes &quot; display corruption)"
        )
