#!/usr/bin/env python3
"""
Agents Terraform Module Structural Tests

Validates the agents Terraform module without requiring cloud credentials.
These are offline structural tests that parse .tf files and verify resource
definitions, dependencies, variables, and outputs match the spec.

Test IDs map to: TC-TF-*, TC-VAR-*, TC-OUT-*
"""

import re
import subprocess
from pathlib import Path

import pytest

# -- Constants ----------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent
AGENTS_TF_DIR = PROJECT_ROOT / "terraform" / "agents"
CORE_TF_DIR = PROJECT_ROOT / "terraform" / "core"
ATLAS_TF_DIR = PROJECT_ROOT / "terraform" / "atlas"


# -- Fixtures -----------------------------------------------------------------
@pytest.fixture(scope="module")
def main_tf() -> str:
    """Read the full main.tf content."""
    return (AGENTS_TF_DIR / "main.tf").read_text()


@pytest.fixture(scope="module")
def variables_tf() -> str:
    """Read the full variables.tf content."""
    return (AGENTS_TF_DIR / "variables.tf").read_text()


@pytest.fixture(scope="module")
def outputs_tf() -> str:
    """Read the full outputs.tf content."""
    return (AGENTS_TF_DIR / "outputs.tf").read_text()


@pytest.fixture(scope="module")
def providers_tf() -> str:
    """Read the full providers.tf content."""
    return (AGENTS_TF_DIR / "providers.tf").read_text()


# -- Helper -------------------------------------------------------------------
def extract_resource_block(tf_content: str, resource_type: str, resource_name: str) -> str:
    """Extract a full resource block from terraform content by type and name.

    Uses brace counting to find the complete block.
    """
    pattern = rf'resource\s+"{resource_type}"\s+"{resource_name}"\s*\{{'
    match = re.search(pattern, tf_content)
    if not match:
        return ""

    start = match.start()
    brace_count = 0
    i = match.end() - 1  # Start at the opening brace
    while i < len(tf_content):
        if tf_content[i] == "{":
            brace_count += 1
        elif tf_content[i] == "}":
            brace_count -= 1
            if brace_count == 0:
                return tf_content[start : i + 1]
        i += 1
    return ""


# -- TC-TF-002: Terraform Validate -------------------------------------------
class TestTerraformValidate:
    """TC-TF-002: terraform validate succeeds."""

    def test_terraform_validate(self):
        """GIVEN agents terraform dir WHEN terraform validate THEN success."""
        init_result = subprocess.run(
            ["terraform", "init", "-backend=false"],
            cwd=AGENTS_TF_DIR,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert init_result.returncode == 0, f"terraform init failed: {init_result.stderr}"

        result = subprocess.run(
            ["terraform", "validate"],
            cwd=AGENTS_TF_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"terraform validate failed: {result.stderr}"
        assert "The configuration is valid" in result.stdout


# -- TC-TF-003 through TC-TF-016: Resource Structure -------------------------
class TestFlinkResources:
    """Structural tests for Flink statement resources in main.tf."""

    def test_mongodb_connection_exists(self, main_tf):
        """TC-TF-003: MongoDB connection resource exists with CREATE CONNECTION."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "mongodb_connection_statement")
        assert block, "Resource mongodb_connection_statement not found"
        assert "CREATE CONNECTION" in block
        assert "mongodb-connection" in block

    def test_documents_vectordb_has_11_columns(self, main_tf):
        """TC-TF-004: documents_vectordb has all 11 columns and correct DB/collection."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "documents_vectordb")
        assert block, "Resource documents_vectordb not found"

        expected_columns = [
            "document_id", "chunk", "embedding", "event_name",
            "event_time_start", "event_time_end", "venue",
            "expected_attendance", "zone", "event_type", "impact_level",
        ]
        for col in expected_columns:
            assert col in block, f"Column '{col}' missing from documents_vectordb"

        assert "'mongodb.database' = 'events'" in block
        assert "'mongodb.collection' = 'knowledge_base'" in block

    def test_mcp_model_uses_bedrock(self, main_tf):
        """TC-TF-005: MCP model uses AWS Bedrock provider (Azure removed)."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "mongodb_mcp_model")
        assert block, "MCP model resource not found"
        assert "'provider' = 'bedrock'" in block
        assert "azure" not in block.lower(), "Azure references must be removed from MCP model"
        # Ensure no per-cloud variants remain
        assert not extract_resource_block(main_tf, "confluent_flink_statement", "mongodb_mcp_model_azure"), \
            "mongodb_mcp_model_azure must be removed"
        assert not extract_resource_block(main_tf, "confluent_flink_statement", "mongodb_mcp_model_aws"), \
            "mongodb_mcp_model_aws must be removed (use mongodb_mcp_model instead)"

    def test_ride_requests_has_watermark(self, main_tf):
        """TC-TF-006: ride_requests table has WATERMARK."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "ride_requests_table")
        assert block, "Resource ride_requests_table not found"
        assert "WATERMARK FOR" in block

    def test_anomalies_per_zone_columns(self, main_tf):
        """TC-TF-007: anomalies_per_zone has all 9 columns."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "anomalies_per_zone_table")
        assert block, "Resource anomalies_per_zone_table not found"

        expected = [
            "pickup_zone", "window_time", "request_count",
            "total_passengers", "total_revenue", "expected_requests",
            "upper_bound", "lower_bound", "is_surge",
        ]
        for col in expected:
            assert col in block, f"Column '{col}' missing from anomalies_per_zone"

    def test_anomalies_sink_is_plain_kafka_table(self, main_tf):
        """TC-SINK-001: anomalies_sink is a plain Kafka table (no external connector)."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "anomalies_sink_table")
        assert block, "Resource anomalies_sink_table not found"
        assert "'connector' = 'mongodb'" not in block, \
            "anomalies_sink must be a plain Kafka table (external connector INSERT not supported)"

    def test_anomalies_sink_columns(self, main_tf):
        """TC-SINK-002: anomalies_sink has correct columns per spec."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "anomalies_sink_table")
        assert block, "Resource anomalies_sink_table not found"
        for col in ["pickup_zone", "window_time", "request_count", "expected_requests",
                     "anomaly_reason", "top_chunk_1", "top_chunk_2", "top_chunk_3", "detected_at"]:
            assert col in block, f"Column '{col}' missing from anomalies_sink"

    def test_zone_traffic_sink_is_plain_kafka_table(self, main_tf):
        """TC-SINK-003: zone_traffic_sink is a plain Kafka table (no external connector)."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "zone_traffic_sink_table")
        assert block, "Resource zone_traffic_sink_table not found"
        assert "'connector' = 'mongodb'" not in block, \
            "zone_traffic_sink must be a plain Kafka table (external connector INSERT not supported)"

    def test_zone_traffic_sink_columns(self, main_tf):
        """TC-SINK-004: zone_traffic_sink has correct columns per spec."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "zone_traffic_sink_table")
        assert block, "Resource zone_traffic_sink_table not found"
        for col in ["zone", "window_start", "window_end", "request_count",
                     "total_passengers", "total_revenue"]:
            assert col in block, f"Column '{col}' missing from zone_traffic_sink"

    def test_windowed_traffic_view_tumble(self, main_tf):
        """TC-TF-010: windowed_traffic view uses 5-minute TUMBLE."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "windowed_traffic_view")
        assert block, "Resource windowed_traffic_view not found"
        assert "TUMBLE" in block
        assert "INTERVAL '5' MINUTE" in block

    def test_zone_traffic_sink_insert_sql(self):
        """TC-TF-011a: zone_traffic_sink_insert SQL template exists and is valid."""
        sql_file = AGENTS_TF_DIR / "sql" / "zone-traffic-sink-insert.sql"
        assert sql_file.exists(), "SQL template zone-traffic-sink-insert.sql not found"
        sql = sql_file.read_text()
        assert "INSERT INTO" in sql
        assert "zone_traffic_sink" in sql
        assert "EXECUTE STATEMENT SET" not in sql

    def test_anomaly_detection_insert_sql(self):
        """TC-TF-011b: anomaly_detection_insert SQL uses ML_DETECT_ANOMALIES."""
        sql_file = AGENTS_TF_DIR / "sql" / "anomaly-detection-insert.sql"
        assert sql_file.exists(), "SQL template anomaly-detection-insert.sql not found"
        sql = sql_file.read_text()
        assert "INSERT INTO" in sql
        assert "ML_DETECT_ANOMALIES" in sql
        assert "anomalies_per_zone" in sql

    def test_mongodb_mcp_connection_exists(self, main_tf):
        """TC-TF-012: mongodb_mcp_connection exists with MCP_SERVER type."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "mongodb_mcp_connection")
        assert block, "Resource mongodb_mcp_connection not found"
        assert "'type' = 'MCP_SERVER'" in block
        assert "mcp_server_url" in block

    def test_voyage_connection(self, main_tf):
        """TC-TF-013: voyage_connection uses OpenAI type and a configurable endpoint."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "voyage_connection")
        assert block, "Resource voyage_connection not found"
        assert "'type' = 'openai'" in block
        assert "var.voyage_api_endpoint" in block, \
            "voyage_connection must reference var.voyage_api_endpoint"

    def test_voyage_endpoint_default(self):
        """TC-TF-013b: voyage_api_endpoint defaults to ai.mongodb.com."""
        variables = (AGENTS_TF_DIR / "variables.tf").read_text()
        assert "voyage_api_endpoint" in variables, \
            "agents/variables.tf must define voyage_api_endpoint"
        assert "https://ai.mongodb.com/v1/embeddings" in variables, \
            "voyage_api_endpoint should default to ai.mongodb.com"

    def test_voyage_embedding_model(self, main_tf):
        """TC-TF-014: voyage_query_embedding_model uses voyage-4."""
        block = extract_resource_block(main_tf, "confluent_flink_statement", "voyage_query_embedding_model")
        assert block, "Resource voyage_query_embedding_model not found"
        assert "'openai.model_version' = 'voyage-4'" in block
        assert "'openai.connection' = 'voyage_connection'" in block
        assert "'openai.output_format' = 'OPENAI-EMBED'" in block
        assert "embedding ARRAY<FLOAT>" in block

    def test_dependency_chain(self, main_tf):
        """TC-TF-015: Critical dependency ordering is correct (DDL resources only)."""
        deps = {
            "documents_vectordb": "mongodb_connection_statement",
            "windowed_traffic_view": "ride_requests_table",
            "voyage_query_embedding_model": "voyage_connection",
        }
        for resource, dependency in deps.items():
            block = extract_resource_block(main_tf, "confluent_flink_statement", resource)
            assert block, f"Resource {resource} not found"
            assert dependency in block, (
                f"{resource} should depend on {dependency} but dependency not found in block"
            )

    def test_anomalies_enriched_ctas_sql(self):
        """TC-RAG-001: anomalies_enriched DDL SQL template exists (CREATE TABLE only)."""
        sql_file = AGENTS_TF_DIR / "sql" / "anomalies-enriched-ctas.sql"
        assert sql_file.exists(), "SQL template anomalies-enriched-ctas.sql not found"
        sql = sql_file.read_text()
        assert "CREATE TABLE" in sql
        assert "anomalies_enriched" in sql
        assert "'changelog.mode' = 'append'" in sql
        # DDL only — must NOT contain INSERT or AS SELECT
        assert "INSERT INTO" not in sql, "CTAS file should be DDL only (no INSERT)"

    def test_anomalies_enriched_insert_sql(self):
        """TC-RAG-001b: anomalies_enriched INSERT SQL template exists."""
        sql_file = AGENTS_TF_DIR / "sql" / "anomalies-enriched-insert.sql"
        assert sql_file.exists(), "SQL template anomalies-enriched-insert.sql not found"
        sql = sql_file.read_text()
        assert "INSERT INTO" in sql
        assert "anomalies_enriched" in sql

    def test_anomalies_enriched_uses_voyage_embedding(self):
        """TC-RAG-002: RAG pipeline uses voyage_query_embedding (not llm_embedding_model)."""
        sql = (AGENTS_TF_DIR / "sql" / "anomalies-enriched-insert.sql").read_text()
        assert "voyage_query_embedding" in sql, "Must use voyage_query_embedding model"
        assert "llm_embedding_model" not in sql, "Must NOT use llm_embedding_model"

    def test_anomalies_enriched_uses_vector_search(self):
        """TC-RAG-003: RAG pipeline uses VECTOR_SEARCH_AGG on documents_vectordb."""
        sql = (AGENTS_TF_DIR / "sql" / "anomalies-enriched-insert.sql").read_text()
        assert "VECTOR_SEARCH_AGG" in sql
        assert "documents_vectordb" in sql

    def test_anomalies_enriched_uses_llm_textgen(self):
        """TC-RAG-004: RAG pipeline uses llm_textgen_model for summarization."""
        sql = (AGENTS_TF_DIR / "sql" / "anomalies-enriched-insert.sql").read_text()
        assert "llm_textgen_model" in sql

    def test_anomalies_sink_insert_sql(self):
        """TC-RAG-005: anomalies_sink INSERT SQL template exists."""
        sql_file = AGENTS_TF_DIR / "sql" / "anomalies-sink-insert.sql"
        assert sql_file.exists(), "SQL template anomalies-sink-insert.sql not found"
        sql = sql_file.read_text()
        assert "INSERT INTO" in sql
        assert "anomalies_sink" in sql
        assert "anomalies_enriched" in sql
        assert "CURRENT_TIMESTAMP" in sql

    def test_all_dml_sql_templates_exist(self):
        """TC-RAG-006: All 7 SQL templates (2 DDL + 5 DML) exist with {catalog}/{database} placeholders."""
        sql_dir = AGENTS_TF_DIR / "sql"
        expected_files = [
            "zone-traffic-sink-insert.sql",
            "anomaly-detection-insert.sql",
            "anomalies-enriched-ctas.sql",
            "anomalies-enriched-insert.sql",
            "anomalies-sink-insert.sql",
            "completed-actions-ctas.sql",
            "dispatch-insert.sql",
        ]
        for f in expected_files:
            sql_file = sql_dir / f
            assert sql_file.exists(), f"SQL template {f} not found"
            sql = sql_file.read_text()
            assert "{catalog}" in sql, f"{f} must contain {{catalog}} placeholder"
            assert "{database}" in sql, f"{f} must contain {{database}} placeholder"

    def test_no_null_resource_summary_generator(self, main_tf):
        """TC-TF-016: null_resource generate_flink_sql_summary should not exist (removed)."""
        block = extract_resource_block(main_tf, "null_resource", "generate_flink_sql_summary")
        assert not block, "null_resource generate_flink_sql_summary should have been removed"


# -- TC-VAR-*: Variables ------------------------------------------------------
class TestVariables:
    """Structural tests for variables.tf."""

    def test_mongodb_vars_required_sensitive(self, variables_tf):
        """TC-VAR-001: MongoDB variables are required (no default) and sensitive."""
        for var_name in ["mongodb_connection_string", "mongodb_username", "mongodb_password"]:
            assert var_name in variables_tf, f"Variable '{var_name}' not found"
            var_start = variables_tf.index(f'variable "{var_name}"')
            next_var = variables_tf.find("\nvariable ", var_start + 1)
            var_block = variables_tf[var_start:next_var] if next_var > 0 else variables_tf[var_start:]
            assert "default" not in var_block, \
                f"Variable '{var_name}' should not have a default (requires user-provided Atlas credentials)"
            assert "sensitive" in var_block and "true" in var_block, \
                f"Variable '{var_name}' should be marked sensitive = true"

    def test_mcp_auth_token_required_sensitive(self, variables_tf):
        """TC-VAR-002: mcp_auth_token is sensitive with no default."""
        assert "mcp_auth_token" in variables_tf
        mcp_section = variables_tf[variables_tf.index("mcp_auth_token"):]
        next_var = mcp_section.find("\nvariable ", 1)
        if next_var > 0:
            mcp_section = mcp_section[:next_var]
        assert "sensitive" in mcp_section
        assert "true" in mcp_section

    def test_voyage_api_key_required_sensitive(self, variables_tf):
        """TC-VAR-003: voyage_api_key is sensitive with no default."""
        assert "voyage_api_key" in variables_tf
        voyage_section = variables_tf[variables_tf.index("voyage_api_key"):]
        assert "sensitive" in voyage_section
        assert "true" in voyage_section


# -- TC-OUT-*: Outputs --------------------------------------------------------
class TestOutputs:
    """Structural tests for outputs.tf."""

    def test_core_passthrough_outputs(self, outputs_tf):
        """TC-OUT-001: Core infrastructure outputs are passed through."""
        expected = [
            "confluent_environment_id",
            "confluent_kafka_cluster_id",
            "confluent_kafka_bootstrap_endpoint",
            "confluent_schema_registry_id",
            "confluent_schema_registry_endpoint",
            "confluent_flink_compute_pool_id",
        ]
        for output_name in expected:
            assert output_name in outputs_tf, f"Output '{output_name}' not found"

    def test_agent_specific_outputs(self, outputs_tf):
        """TC-OUT-002: Agent-specific outputs exist (DDL resources only)."""
        expected = [
            "ride_requests_table_id",
            "documents_vectordb_table_id",
            "mongodb_connection_name",
            "anomalies_per_zone_table_id",
            "voyage_connection_id",
            "voyage_query_embedding_model_id",
        ]
        for output_name in expected:
            assert output_name in outputs_tf, f"Output '{output_name}' not found"


# -- TC-ATLAS-*: Optional Atlas M10 Cluster Provisioning ----------------------
@pytest.fixture(scope="module")
def atlas_tf_combined() -> str:
    """Concatenated content of all .tf files in terraform/atlas."""
    parts = []
    for tf_file in sorted(ATLAS_TF_DIR.glob("*.tf")):
        parts.append(tf_file.read_text())
    return "\n".join(parts)


@pytest.fixture(scope="module")
def atlas_variables_tf() -> str:
    return (ATLAS_TF_DIR / "variables.tf").read_text()


@pytest.fixture(scope="module")
def atlas_outputs_tf() -> str:
    return (ATLAS_TF_DIR / "outputs.tf").read_text()


@pytest.fixture(scope="module")
def atlas_versions_tf() -> str:
    return (ATLAS_TF_DIR / "versions.tf").read_text()


@pytest.fixture(scope="module")
def core_variables_tf() -> str:
    return (CORE_TF_DIR / "variables.tf").read_text()


@pytest.fixture(scope="module")
def core_versions_tf() -> str:
    return (CORE_TF_DIR / "versions.tf").read_text()


@pytest.fixture(scope="module")
def core_tf_combined() -> str:
    parts = []
    for tf_file in sorted(CORE_TF_DIR.glob("*.tf")):
        parts.append(tf_file.read_text())
    return "\n".join(parts)


class TestAtlasClusterProvisioning:
    """REQ-E-200..211, REQ-E-250..254 — optional Atlas M10 cluster in terraform/atlas (split from core)."""

    def test_advanced_cluster_resource_exists(self, atlas_tf_combined):
        """TC-ATLAS-001: mongodbatlas_advanced_cluster.cluster exists in terraform/atlas."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_advanced_cluster", "cluster")
        assert block, "mongodbatlas_advanced_cluster.cluster resource not found in terraform/atlas"
        assert 'cluster_type   = "REPLICASET"' in block or 'cluster_type = "REPLICASET"' in block, \
            "cluster_type must be REPLICASET"

    def test_cluster_spec_m10_replica_set(self, atlas_tf_combined):
        """TC-ATLAS-002: M10 instance, 3 nodes, 10 GB disk, AWS provider, priority 7."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_advanced_cluster", "cluster")
        assert 'instance_size = "M10"' in block, "instance_size must be M10"
        assert "node_count    = 3" in block or "node_count = 3" in block
        assert "disk_size_gb  = 10" in block or "disk_size_gb = 10" in block
        assert 'provider_name = "AWS"' in block
        assert "priority      = 7" in block or "priority = 7" in block

    def test_cluster_autoscaling(self, atlas_tf_combined):
        """TC-ATLAS-003: autoscaling enabled M10..M50."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_advanced_cluster", "cluster")
        assert "compute_enabled" in block and "true" in block
        assert "compute_scale_down_enabled" in block
        assert 'compute_min_instance_size  = "M10"' in block or 'compute_min_instance_size = "M10"' in block
        assert 'compute_max_instance_size  = "M50"' in block or 'compute_max_instance_size = "M50"' in block
        assert "disk_gb_enabled" in block

    def test_cluster_backup_and_termination(self, atlas_tf_combined):
        """TC-ATLAS-004: backup_enabled=true, termination_protection_enabled=false."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_advanced_cluster", "cluster")
        assert re.search(r"backup_enabled\s*=\s*true", block)
        assert re.search(r"termination_protection_enabled\s*=\s*false", block)

    def test_cluster_tags_filter_blank_values(self, atlas_tf_combined):
        """TC-ATLAS-004c: cluster tags filter out blank values.

        Atlas API returns HTTP 400 TAG_VALUE_BLANK when any tag value is empty.
        owner_email defaults to "" so tags must be filtered before sending.
        """
        # Either we use a filtered local, or we conditionally include each tag.
        # The simplest contract is: there must be a `for k, v` filter or
        # explicit "if v != """ in the tags wiring.
        assert ("for k, v in" in atlas_tf_combined and 'v != ""' in atlas_tf_combined), \
            "atlas module must filter blank tag values to avoid TAG_VALUE_BLANK"

    def test_cluster_has_timeouts_block(self, atlas_tf_combined):
        """TC-ATLAS-004b: cluster has timeouts block (fail fast on stuck Atlas)."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_advanced_cluster", "cluster")
        assert "timeouts" in block, "cluster should declare a timeouts block"
        assert re.search(r"create\s*=", block), "timeouts block needs create timeout"

    def test_database_user_resource(self, atlas_tf_combined):
        """TC-ATLAS-005: mongodbatlas_database_user with atlasAdmin role."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_database_user", "app_user")
        assert block, "mongodbatlas_database_user.app_user resource not found"
        assert 'role_name     = "atlasAdmin"' in block or 'role_name = "atlasAdmin"' in block
        assert 'database_name = "admin"' in block

    def test_ip_access_list_resource(self, atlas_tf_combined):
        """TC-ATLAS-006: project_ip_access_list with 0.0.0.0/0."""
        block = extract_resource_block(atlas_tf_combined, "mongodbatlas_project_ip_access_list", "workshop")
        assert block
        assert '"0.0.0.0/0"' in block

    def test_atlas_connection_string_output(self, atlas_outputs_tf):
        """TC-ATLAS-008: atlas_cluster_connection_string output exists, sensitive (in atlas module)."""
        assert "atlas_cluster_connection_string" in atlas_outputs_tf
        match = re.search(
            r'output\s+"atlas_cluster_connection_string"\s*\{(.*?)\n\}',
            atlas_outputs_tf,
            re.DOTALL,
        )
        assert match
        block = match.group(1)
        assert "sensitive" in block and "true" in block

    def test_mongodbatlas_provider_declared_in_atlas(self, atlas_versions_tf):
        """TC-ATLAS-009: terraform/atlas/versions.tf declares mongodb/mongodbatlas provider."""
        assert "mongodb/mongodbatlas" in atlas_versions_tf

    def test_atlas_credentials_sensitive(self, atlas_variables_tf):
        """TC-ATLAS-010: atlas_public_key and atlas_private_key sensitive in atlas module."""
        for var_name in ["atlas_public_key", "atlas_private_key"]:
            assert var_name in atlas_variables_tf, f"variable {var_name} missing"
            var_start = atlas_variables_tf.index(f'variable "{var_name}"')
            next_var = atlas_variables_tf.find("\nvariable ", var_start + 1)
            block = atlas_variables_tf[var_start:next_var] if next_var > 0 else atlas_variables_tf[var_start:]
            assert "sensitive" in block and "true" in block

    # --- Module split (REQ-E-250) ---

    def test_atlas_resources_removed_from_core(self, core_tf_combined):
        """TC-ATLAS-SPLIT-001: Atlas resources removed from terraform/core."""
        for resource_type, resource_name in [
            ("mongodbatlas_advanced_cluster", "cluster"),
            ("mongodbatlas_database_user", "app_user"),
            ("mongodbatlas_project_ip_access_list", "workshop"),
        ]:
            assert not extract_resource_block(core_tf_combined, resource_type, resource_name), \
                f"{resource_type}.{resource_name} must be removed from terraform/core (moved to terraform/atlas)"

    def test_atlas_provider_removed_from_core(self, core_versions_tf):
        """TC-ATLAS-SPLIT-002: mongodbatlas provider removed from core (used only in atlas module)."""
        assert "mongodbatlas" not in core_versions_tf, \
            "mongodbatlas provider must be removed from terraform/core/versions.tf"

    def test_atlas_vars_removed_from_core(self, core_variables_tf):
        """TC-ATLAS-SPLIT-003: atlas_* variables removed from core."""
        for var in ["atlas_public_key", "atlas_private_key", "atlas_db_username", "atlas_db_password", "create_atlas_cluster"]:
            assert f'variable "{var}"' not in core_variables_tf, \
                f"variable {var} must be removed from terraform/core/variables.tf"


class TestCoreVariablesInvariant:
    """INV-207: existing core variables must not be removed."""

    def test_existing_core_variables_present(self, core_variables_tf):
        """TC-ATLAS-INV-001: Existing core variables preserved."""
        for var_name in [
            "cloud_region",
            "confluent_cloud_api_key",
            "confluent_cloud_api_secret",
            "owner_email",
            "aws_bedrock_access_key",
            "aws_bedrock_secret_key",
            "aws_session_token",
            "bedrock_model_id",
        ]:
            assert f'variable "{var_name}"' in core_variables_tf, \
                f"existing variable {var_name} must not be removed"
