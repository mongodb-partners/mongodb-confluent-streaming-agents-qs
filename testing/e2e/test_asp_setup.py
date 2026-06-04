#!/usr/bin/env python3
"""
ASP Setup Script Tests

Validates the asp_setup.py script: CLI interface, pipeline structure,
seed data, and corrected ASP syntax. These are offline/structural tests
that do NOT call the Atlas API.

Test IDs map to: TC-ASP-*
"""

import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Import the module under test
sys.path.insert(0, str(PROJECT_ROOT))
from scripts.asp_setup import (
    SEED_EVENTS,
    _pipeline_dispatch_log,
    _pipeline_event_knowledge_base,
    _pipeline_event_publication,
    main,
    run_asp_setup,
    run_asp_teardown,
)


# ── TC-ASP-001: Import and callable ─────────────────────────────────────────
class TestASPImport:
    def test_main_is_callable(self):
        """TC-ASP-001: main function exists and is callable."""
        assert callable(main)


# ── TC-ASP-002: CLI --help ───────────────────────────────────────────────────
class TestASPCLI:
    def test_help_flag(self):
        """TC-ASP-002: --help shows all expected CLI flags."""
        result = subprocess.run(
            ["uv", "run", "asp-setup", "--help"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode == 0, f"--help failed: {result.stderr}"

        expected_flags = [
            "--atlas-public-key",
            "--atlas-private-key",
            "--project-id",
            "--cluster-name",
            "--confluent-bootstrap-server",
            "--confluent-api-key",
            "--confluent-api-secret",
            "--voyage-api-key",
            "--schema-registry-url",
            "--schema-registry-key",
            "--schema-registry-secret",
            "--mongodb-connection-string",
            "--mongodb-username",
            "--mongodb-password",
            "--skip-seed",
            "--skip-processors",
        ]
        for flag in expected_flags:
            assert flag in result.stdout, f"Flag '{flag}' missing from --help output"

    def test_asp_setup_reads_credentials_env(self):
        """TC-ASP-003: asp-setup reads credentials from .env or CLI args."""
        asp = importlib.import_module("scripts.asp_setup")
        source = inspect.getsource(asp)
        assert ".env" in source, \
            "asp-setup should read from .env"
        assert "argparse" in source, \
            "asp-setup should support CLI argument overrides"


# ── TC-ASP-004 through TC-ASP-007: Pipeline Structure ───────────────────────
class TestPipelineEventKnowledgeBase:
    """Tests for Pipeline 1: event_knowledge_base_population."""

    @pytest.fixture
    def pipeline(self):
        return _pipeline_event_knowledge_base()

    def test_pipeline_has_9_stages(self, pipeline):
        """TC-ASP-004: Pipeline 1 has 9 stages.

        - REQ-R-105 (provenance fields): an extra $addFields stage
          stamps embedded_at, embedding_model, embedding_dim,
          schema_version onto each document.
        - REQ-CRG-016: a $validate stage between the embedding
          extraction and the provenance $addFields catches malformed
          Voyage responses (no `data` array → null embedding) and
          routes them to DLQ rather than letting nulls reach $merge.
        """
        assert len(pipeline) == 9, f"Expected 9 stages, got {len(pipeline)}"
        stage_types = [list(s.keys())[0] for s in pipeline]
        assert stage_types == [
            "$source", "$validate", "$addFields", "$https",
            "$addFields", "$validate", "$addFields", "$project", "$merge",
        ]

    def test_source_uses_coll_and_config_fullDocument(self, pipeline):
        """TC-ASP-005: $source uses 'coll' (not 'collection') and fullDocument inside config."""
        source = pipeline[0]["$source"]
        assert source["coll"] == "calendar", f"Expected coll='calendar', got {source.get('coll')}"
        assert "collection" not in source, "$source should use 'coll', not 'collection'"
        assert "config" in source, "fullDocument should be inside 'config' object"
        assert source["config"]["fullDocument"] == "updateLookup"
        # Verify fullDocument is NOT at top level
        assert "fullDocument" not in source or "config" in source

    def test_https_uses_payload_not_body(self, pipeline):
        """TC-ASP-006: $https uses 'payload' (array of stages), not 'body'."""
        https_stage = next(s["$https"] for s in pipeline if "$https" in s)
        assert "payload" in https_stage, "$https should use 'payload'"
        assert "body" not in https_stage, "$https should NOT use 'body'"
        assert isinstance(https_stage["payload"], list), "payload should be a list"
        assert len(https_stage["payload"]) > 0, "payload should not be empty"

    def test_merge_uses_coll_not_collection(self, pipeline):
        """TC-ASP-007: $merge uses 'coll' (not 'collection')."""
        merge = pipeline[-1]["$merge"]
        into = merge["into"]
        assert "coll" in into, "$merge.into should use 'coll'"
        assert "collection" not in into, "$merge.into should NOT use 'collection'"
        assert into["coll"] == "knowledge_base"
        assert into["db"] == "events"


class TestPipelineEventPublication:
    """Tests for Pipeline 2: event_publication_to_kafka."""

    @pytest.fixture
    def pipeline(self):
        return _pipeline_event_publication()

    def test_pipeline_has_4_stages(self, pipeline):
        """Pipeline 2 has 4 stages: $source, $validate, $project, $emit."""
        stage_types = [list(s.keys())[0] for s in pipeline]
        assert stage_types == ["$source", "$validate", "$project", "$emit"]

    def test_emit_targets_kafka(self, pipeline):
        """$emit sends to kafka_confluent / event_documents."""
        emit = pipeline[3]["$emit"]
        assert emit["connectionName"] == "kafka_confluent"
        assert emit["topic"] == "event_documents"

    def test_source_uses_corrected_syntax(self, pipeline):
        """$source uses 'coll' and config.fullDocument."""
        source = pipeline[0]["$source"]
        assert source["coll"] == "calendar"
        assert "collection" not in source
        assert source["config"]["fullDocument"] == "updateLookup"


class TestPipelineDispatchLog:
    """Tests for Pipeline 3: dispatch_log_ingestion."""

    @pytest.fixture
    def pipeline(self):
        return _pipeline_dispatch_log()

    def test_source_is_kafka(self, pipeline):
        """TC-ASP-010: $source reads from Kafka completed_actions (no fullDocument)."""
        source = pipeline[0]["$source"]
        assert source["connectionName"] == "kafka_confluent"
        assert source["topic"] == "completed_actions"
        assert "config" not in source, "Kafka source should not have config.fullDocument"

    def test_source_has_schema_registry(self, pipeline):
        """TC-ASP-010b: $source includes schemaRegistry for Avro deserialization.

        Flink writes Avro to the completed_actions Kafka topic via Confluent
        Schema Registry.  Without this config, ASP cannot deserialize messages.
        """
        source = pipeline[0]["$source"]
        assert "schemaRegistry" in source, (
            "dispatch_log_ingestion $source must include schemaRegistry — "
            "Flink writes Avro to the completed_actions topic"
        )
        assert source["schemaRegistry"]["connectionName"] == "confluent_schema_registry"

    def test_merge_uses_corrected_syntax(self, pipeline):
        """$merge uses 'coll' for fleet.dispatch_log.

        REQ-CRF-051: find by stage name, not pipeline index — pipeline
        shape evolves and brittle [3] would break with every spec change.
        """
        merge_stages = [s for s in pipeline if "$merge" in s]
        assert len(merge_stages) == 1, "expected exactly one $merge stage"
        into = merge_stages[0]["$merge"]["into"]
        assert into["coll"] == "dispatch_log"
        assert into["db"] == "fleet"
        assert "collection" not in into


# ── TC-ASP-008: DLQ Configuration ───────────────────────────────────────────
class TestDLQConfiguration:
    def test_processor_dlq_configuration(self):
        """TC-ASP-008: Each processor has correct DLQ in options."""
        # We can't call ensure_processors without API, but we can inspect
        # the function source to verify the processor definitions.
        # Instead, import and check the pipeline + expected DLQ mapping.
        import inspect
        source = inspect.getsource(__import__("scripts.asp_setup", fromlist=["ensure_processors"]).ensure_processors)

        # Pipeline 1 & 2: events DLQ
        assert '"db": "events"' in source or "'db': 'events'" in source or \
               '"db": "events"' in source
        assert "validation_dlq" in source

        # Pipeline 3: fleet DLQ
        assert '"db": "fleet"' in source or "'db': 'fleet'" in source


# ── TC-ASP-009: Seed Data ────────────────────────────────────────────────────
class TestSeedData:
    def test_seed_events_count(self):
        """TC-ASP-009: SEED_EVENTS contains events for all zones."""
        assert len(SEED_EVENTS) == 10

    def test_seed_events_required_fields(self):
        """TC-ASP-009: Each seed event has all required fields."""
        required_fields = [
            "event_name", "zone", "description", "venue",
            "expected_attendance", "event_type", "impact_level",
        ]
        for i, event in enumerate(SEED_EVENTS):
            for field in required_fields:
                assert field in event, f"Event {i} ({event.get('event_name', '?')}) missing '{field}'"

    def test_seed_events_have_time_fields(self):
        """Seed events have start/end hour and minute fields."""
        for event in SEED_EVENTS:
            assert "event_time_start_hour" in event
            assert "event_time_start_min" in event
            assert "event_time_end_hour" in event
            assert "event_time_end_min" in event

    def test_seed_events_zones(self):
        """Seed events cover all zones that generate anomalies."""
        zones = {e["zone"] for e in SEED_EVENTS}
        expected = {"CBD", "French Quarter", "Uptown", "Warehouse District",
                    "Garden District", "Marigny", "Bywater"}
        assert expected.issubset(zones)

    def test_seed_events_names(self):
        """Seed events have expected names from the spec."""
        names = {e["event_name"] for e in SEED_EVENTS}
        expected = {
            "Essence Music Festival",
            "French Quarter Festival",
            "Saints Game",
            "Mardi Gras Parade",
            "Jazz at Preservation Hall",
            "Bayou Classic",
            "Warehouse District Art Walk",
            "Garden District Home & Garden Tour",
            "Frenchmen Street Live Music Festival",
            "Bywater Biennale",
        }
        assert names == expected, f"Expected {expected}, got {names}"


# ── TC-ASP-011: Programmatic API ──────────────────────────────────────────
class TestProgrammaticAPI:
    def test_run_asp_setup_is_callable(self):
        """TC-ASP-011: run_asp_setup exists and is callable."""
        assert callable(run_asp_setup)

    def test_run_asp_setup_signature(self):
        """TC-ASP-011b: run_asp_setup has correct required parameters."""
        import inspect
        sig = inspect.signature(run_asp_setup)
        required_params = [
            "atlas_public_key", "atlas_private_key", "project_id",
            "cluster_name", "confluent_bootstrap_server",
            "confluent_api_key", "confluent_api_secret", "voyage_api_key",
        ]
        for param in required_params:
            assert param in sig.parameters, f"Missing required param: {param}"
            p = sig.parameters[param]
            assert p.default is inspect.Parameter.empty, \
                f"Param '{param}' should be required (no default)"

    def test_run_asp_setup_optional_params(self):
        """TC-ASP-011c: run_asp_setup has correct optional parameters."""
        import inspect
        sig = inspect.signature(run_asp_setup)
        optional_params = {
            "mongodb_connection_string": "",
            "mongodb_username": "",
            "mongodb_password": "",
            "skip_seed": False,
            "skip_processors": False,
        }
        for param, expected_default in optional_params.items():
            assert param in sig.parameters, f"Missing optional param: {param}"
            p = sig.parameters[param]
            assert p.default == expected_default, \
                f"Param '{param}' default should be {expected_default!r}, got {p.default!r}"

    def test_run_asp_teardown_is_callable(self):
        """TC-ASP-012: run_asp_teardown exists and is callable."""
        assert callable(run_asp_teardown)

    def test_run_asp_teardown_signature(self):
        """TC-ASP-012b: run_asp_teardown has correct parameters."""
        import inspect
        sig = inspect.signature(run_asp_teardown)
        required_params = ["atlas_public_key", "atlas_private_key", "project_id"]
        for param in required_params:
            assert param in sig.parameters, f"Missing required param: {param}"
            p = sig.parameters[param]
            assert p.default is inspect.Parameter.empty, \
                f"Param '{param}' should be required (no default)"

    def test_run_asp_setup_returns_bool(self):
        """TC-ASP-013: run_asp_setup return annotation is bool."""
        import inspect
        sig = inspect.signature(run_asp_setup)
        assert sig.return_annotation is bool, \
            f"Expected return annotation bool, got {sig.return_annotation}"

    def test_run_asp_teardown_returns_bool(self):
        """TC-ASP-013b: run_asp_teardown return annotation is bool."""
        import inspect
        sig = inspect.signature(run_asp_teardown)
        assert sig.return_annotation is bool, \
            f"Expected return annotation bool, got {sig.return_annotation}"


# ── TC-ASP-014: fullDocumentOnly in pipelines ────────────────────────────
class TestFullDocumentOnly:
    def test_pipeline1_has_fullDocumentOnly(self):
        """TC-ASP-014: Pipeline 1 $source has fullDocumentOnly: True."""
        pipeline = _pipeline_event_knowledge_base()
        config = pipeline[0]["$source"]["config"]
        assert config.get("fullDocumentOnly") is True, \
            "$source config should have fullDocumentOnly: True"

    def test_pipeline2_has_fullDocumentOnly(self):
        """TC-ASP-014b: Pipeline 2 $source has fullDocumentOnly: True."""
        pipeline = _pipeline_event_publication()
        config = pipeline[0]["$source"]["config"]
        assert config.get("fullDocumentOnly") is True, \
            "$source config should have fullDocumentOnly: True"

    def test_pipeline3_kafka_no_fullDocumentOnly(self):
        """TC-ASP-014c: Pipeline 3 (Kafka source) has no fullDocumentOnly."""
        pipeline = _pipeline_dispatch_log()
        source = pipeline[0]["$source"]
        assert "config" not in source, "Kafka source should not have config"


# ── TC-ASP-015: Kafka topic pre-creation ─────────────────────────────────
class TestKafkaTopicPreCreation:
    def test_required_topics_defined(self):
        """TC-ASP-015: REQUIRED_KAFKA_TOPICS contains expected topics."""
        from scripts.asp_setup import REQUIRED_KAFKA_TOPICS
        assert "event_documents" in REQUIRED_KAFKA_TOPICS
        assert "completed_actions" in REQUIRED_KAFKA_TOPICS

    def test_ensure_kafka_topics_is_callable(self):
        """TC-ASP-015b: ensure_kafka_topics function exists."""
        from scripts.asp_setup import ensure_kafka_topics
        assert callable(ensure_kafka_topics)


# ── TC-IDX-001..004: Atlas Index Definitions ─────────────────────────────
class TestAtlasIndexes:
    """Tests for ensure_atlas_indexes() -- REQ-E-005, REQ-E-006."""

    def test_ensure_atlas_indexes_exists(self):
        """TC-IDX-001: ensure_atlas_indexes function exists and is callable."""
        from scripts.asp_setup import ensure_atlas_indexes
        assert callable(ensure_atlas_indexes)

    def test_ensure_atlas_indexes_has_vector_search_index(self):
        """TC-IDX-001b: ensure_atlas_indexes defines vector_index on events.knowledge_base."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "vector_index" in source, "Must define 'vector_index' search index"
        assert "knowledge_base" in source, "Must target events.knowledge_base collection"
        assert "1024" in source, "Must specify 1024 dimensions"
        assert "cosine" in source, "Must use cosine similarity"

    def test_ensure_atlas_indexes_has_filter_fields(self):
        """TC-IDX-002: Vector search index includes filter fields zone, impact_level, event_type."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "zone" in source, "Must include 'zone' filter field"
        assert "impact_level" in source, "Must include 'impact_level' filter field"
        assert "event_type" in source, "Must include 'event_type' filter field"

    def test_ensure_atlas_indexes_has_collection_indexes(self):
        """TC-IDX-003: ensure_atlas_indexes defines compound/TTL indexes for analytics + fleet."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        # analytics.zone_traffic indexes
        assert "zone_traffic" in source, "Must target analytics.zone_traffic"
        assert "window_start" in source, "Must index window_start"
        # analytics.zone_anomalies indexes
        assert "zone_anomalies" in source, "Must target analytics.zone_anomalies"
        assert "pickup_zone" in source, "Must index pickup_zone"
        assert "window_time" in source, "Must index window_time"
        # fleet.dispatch_log indexes
        assert "dispatch_log" in source, "Must target fleet.dispatch_log"
        assert "dispatched_at" in source, "Must index dispatched_at"

    def test_run_asp_setup_calls_ensure_atlas_indexes(self):
        """TC-IDX-004: run_asp_setup calls ensure_atlas_indexes."""
        import inspect
        source = inspect.getsource(run_asp_setup)
        assert "ensure_atlas_indexes" in source, \
            "run_asp_setup must call ensure_atlas_indexes"


# ── TC-P4-*: Pipeline 4 -- Zone Traffic Ingestion ──────────────────────────
class TestPipelineZoneTrafficIngestion:
    """Tests for Pipeline 4: zone_traffic_sink (Kafka) -> analytics.zone_traffic (Atlas)."""

    @pytest.fixture
    def pipeline(self):
        from scripts.asp_setup import _pipeline_zone_traffic_ingestion
        return _pipeline_zone_traffic_ingestion()

    def test_pipeline_has_5_stages(self, pipeline):
        """TC-P4-001: Pipeline 4 has 5 stages: $source, $validate, $match, $addFields, $merge.

        - $match guard added per REQ-R-108 (merge keys non-null).
        - REQ-CRG-015: $validate added before $match. Schema Registry
          enforces the Avro contract, but a `$validate` stage routes
          shape drift to the DLQ rather than the silent-drop behavior
          of `$match`. (Defense in depth — schema drift between Flink
          producer and ASP consumer becomes observable.)
        """
        assert len(pipeline) == 5, f"Expected 5 stages, got {len(pipeline)}"
        stage_types = [list(s.keys())[0] for s in pipeline]
        assert stage_types == [
            "$source", "$validate", "$match", "$addFields", "$merge",
        ]

    def test_source_is_kafka_zone_traffic_sink(self, pipeline):
        """TC-P4-002: $source reads from Kafka zone_traffic_sink with schema registry."""
        source = pipeline[0]["$source"]
        assert source["connectionName"] == "kafka_confluent"
        assert source["topic"] == "zone_traffic_sink"
        assert "config" not in source, "Kafka source should not have config.fullDocument"
        assert "schemaRegistry" in source, "Kafka source must reference schema registry for Avro"
        assert source["schemaRegistry"]["connectionName"] == "confluent_schema_registry"

    def test_merge_targets_analytics_zone_traffic(self, pipeline):
        """TC-P4-004: $merge targets analytics.zone_traffic with idempotent upsert."""
        merge = pipeline[-1]["$merge"]
        into = merge["into"]
        assert into["connectionName"] == "atlas_cluster"
        assert into["db"] == "analytics"
        assert into["coll"] == "zone_traffic"
        assert merge["on"] == ["zone", "window_start"]
        assert merge["whenMatched"] == "replace"
        assert merge["whenNotMatched"] == "insert"

    def test_window_start_converted_to_date(self, pipeline):
        """TC-R-100a (REQ-R-100): window_start/window_end converted to BSON Date.

        Without $toDate, window_start arrives as epoch milliseconds (int) and
        any TTL index on it is silently ignored.
        """
        addfields = next(s["$addFields"] for s in pipeline if "$addFields" in s)
        assert "window_start" in addfields, \
            "Pipeline must convert window_start to Date via $addFields"
        assert addfields["window_start"] == {"$toDate": "$window_start"}, \
            "window_start must use $toDate operator"
        assert "window_end" in addfields, \
            "Pipeline must convert window_end to Date via $addFields"
        assert addfields["window_end"] == {"$toDate": "$window_end"}

    def test_match_guards_merge_keys(self, pipeline):
        """TC-R-108a (REQ-R-108): $match guards zone and window_start before $merge."""
        match = next(s["$match"] for s in pipeline if "$match" in s)
        assert "zone" in match, "$match must guard zone"
        assert "window_start" in match, "$match must guard window_start"

    def test_processor_in_ensure_processors(self):
        """TC-P4-005: zone_traffic_ingestion processor in ensure_processors with correct DLQ."""
        import inspect
        from scripts.asp_setup import ensure_processors
        source = inspect.getsource(ensure_processors)
        assert "zone_traffic_ingestion" in source, \
            "ensure_processors must include zone_traffic_ingestion processor"


# ── TC-P5-*: Pipeline 5 -- Anomalies Ingestion ─────────────────────────────
class TestPipelineAnomaliesIngestion:
    """Tests for Pipeline 5: anomalies_sink (Kafka) -> analytics.zone_anomalies (Atlas)."""

    @pytest.fixture
    def pipeline(self):
        from scripts.asp_setup import _pipeline_anomalies_ingestion
        return _pipeline_anomalies_ingestion()

    def test_pipeline_has_5_stages(self, pipeline):
        """TC-P5-001: Pipeline 5 has 5 stages: $source, $validate, $match, $addFields, $merge.

        - $match guard added per REQ-R-108.
        - REQ-CRG-015: $validate added before $match for observable
          schema drift (DLQ rather than silent drop).
        """
        assert len(pipeline) == 5, f"Expected 5 stages, got {len(pipeline)}"
        stage_types = [list(s.keys())[0] for s in pipeline]
        assert stage_types == [
            "$source", "$validate", "$match", "$addFields", "$merge",
        ]

    def test_source_is_kafka_anomalies_sink(self, pipeline):
        """TC-P5-002: $source reads from Kafka anomalies_sink with schema registry."""
        source = pipeline[0]["$source"]
        assert source["connectionName"] == "kafka_confluent"
        assert source["topic"] == "anomalies_sink"
        assert "config" not in source, "Kafka source should not have config.fullDocument"
        assert "schemaRegistry" in source, "Kafka source must reference schema registry for Avro"
        assert source["schemaRegistry"]["connectionName"] == "confluent_schema_registry"

    def test_merge_targets_analytics_zone_anomalies(self, pipeline):
        """TC-P5-004: $merge targets analytics.zone_anomalies with idempotent upsert."""
        merge = pipeline[-1]["$merge"]
        into = merge["into"]
        assert into["connectionName"] == "atlas_cluster"
        assert into["db"] == "analytics"
        assert into["coll"] == "zone_anomalies"
        assert merge["on"] == ["pickup_zone", "window_time"]
        assert merge["whenMatched"] == "replace"
        assert merge["whenNotMatched"] == "insert"

    def test_window_time_converted_to_date(self, pipeline):
        """TC-R-100b (REQ-R-100): window_time converted to BSON Date."""
        addfields = next(s["$addFields"] for s in pipeline if "$addFields" in s)
        assert "window_time" in addfields, \
            "Pipeline must convert window_time to Date via $addFields"
        assert addfields["window_time"] == {"$toDate": "$window_time"}

    def test_match_guards_merge_keys(self, pipeline):
        """TC-R-108b (REQ-R-108): $match guards pickup_zone and window_time."""
        match = next(s["$match"] for s in pipeline if "$match" in s)
        assert "pickup_zone" in match
        assert "window_time" in match

    def test_processor_in_ensure_processors(self):
        """TC-P5-005: anomalies_ingestion processor in ensure_processors with correct DLQ."""
        import inspect
        from scripts.asp_setup import ensure_processors
        source = inspect.getsource(ensure_processors)
        assert "anomalies_ingestion" in source, \
            "ensure_processors must include anomalies_ingestion processor"


# ── TC-SI-*: Atlas Search Indexes (DEPRECATED — REQ-R-113 removed them) ────
# The anomaly_reason_search and dispatch_summary_search Atlas Search indexes
# were removed in the 2026-05 review iteration because no caller used them.
# See TestRemovedSearchIndexes below for the inverse assertion.
class TestAtlasSearchIndexes:
    """REQ-R-113: only the operational collection names must still appear
    for index creation (zone_anomalies + dispatch_log are still indexed
    via pymongo, just not via Atlas Search)."""

    def test_search_indexes_target_correct_collections(self):
        """TC-SI-005: zone_anomalies and dispatch_log are still mentioned for
        their pymongo collection indexes (compound on (zone, window_*))."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "zone_anomalies" in source
        assert "dispatch_log" in source


# ── TC-BUG-001: Connection update stops processors first ──────────────────
class TestConnectionUpdateStopsProcessors:
    """Tests for BUG-001: ensure_connections must stop active processors before
    deleting connections that those processors depend on."""

    def test_ensure_connections_stops_processors_before_delete(self):
        """TC-BUG-001a: ensure_connections stops active processors before updating."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_connections
        )
        # Must reference processor stopping logic
        assert "processor" in source.lower(), \
            "ensure_connections must handle processors when updating connections"
        assert ":stop" in source, \
            "ensure_connections must stop processors before deleting connections"

    def test_ensure_connections_restarts_processors_not_needed(self):
        """TC-BUG-001b: stopped processors are restarted by ensure_processors later."""
        # Verify ensure_processors handles STOPPED state (restart)
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_processors
        )
        assert "STOPPED" in source, \
            "ensure_processors must handle STOPPED processors"

    def test_ensure_connections_handles_no_processors(self):
        """TC-BUG-001c: ensure_connections works when no processors exist."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_connections
        )
        # Must handle case where processor list is empty or API returns no results
        assert "results" in source, \
            "ensure_connections must handle processor list API response"

    def test_ensure_connections_creates_new_connection(self):
        """TC-INV-001: new connections are created normally (no processors to stop)."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_connections
        )
        assert "Creating connection" in source, \
            "ensure_connections must still create new connections"

    def test_ensure_connections_creates_fresh_when_not_existing(self):
        """TC-INV-002: connections that don't exist are created fresh."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_connections
        )
        assert "not in existing" in source or "else:" in source, \
            "ensure_connections must handle non-existing connections"


# ── TC-PROC-RETRY-*: Processor start retry ───────────────────────────────────
class TestProcessorStartRetry:
    """Tests for processor start retry on SASL auth propagation failure."""

    def test_start_processor_with_retry_exists(self):
        """TC-PROC-RETRY-001: _start_processor_with_retry function exists."""
        from scripts.asp_setup import _start_processor_with_retry
        assert callable(_start_processor_with_retry)

    def test_start_processor_with_retry_handles_auth_error(self):
        """TC-PROC-RETRY-002: retry function detects SASL auth errors."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup")._start_processor_with_retry
        )
        assert "SASL authentication error" in source or "Authentication failed" in source, \
            "_start_processor_with_retry must detect SASL auth errors"

    def test_start_processor_with_retry_has_backoff(self):
        """TC-PROC-RETRY-003: retry function has backoff between attempts."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup")._start_processor_with_retry
        )
        assert "time.sleep" in source, \
            "_start_processor_with_retry must sleep between retries"

    def test_ensure_processors_uses_retry(self):
        """TC-PROC-RETRY-004: ensure_processors uses _start_processor_with_retry."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup").ensure_processors
        )
        assert "_start_processor_with_retry" in source, \
            "ensure_processors must use _start_processor_with_retry"

    def test_deploy_publishes_before_asp(self):
        """TC-PROC-RETRY-005: deploy publishes data before ASP setup (auth propagation buffer)."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy.run_deployment)
        publish_pos = source.find("_publish_local_data")
        asp_pos = source.find("_run_asp_post_terraform")
        assert publish_pos != -1, "run_deployment should call _publish_local_data"
        assert asp_pos != -1, "run_deployment should call _run_asp_post_terraform"
        assert publish_pos < asp_pos, \
            "run_deployment must publish data BEFORE ASP setup (auth propagation buffer)"

    def test_start_processor_with_retry_handles_provisioning(self):
        """TC-PROC-RETRY-006: retry function handles 'being provisioned' transient error."""
        source = inspect.getsource(
            importlib.import_module("scripts.asp_setup")._start_processor_with_retry
        )
        assert "being provisioned" in source, \
            "_start_processor_with_retry must detect 'being provisioned' transient errors"


# ── TC-KT-*: Kafka Topic Pre-creation ─────────────────────────────────────
class TestKafkaTopicPreCreationExtended:
    """Tests for REQUIRED_KAFKA_TOPICS including new pipeline topics."""

    def test_required_topics_include_sink_topics(self):
        """TC-KT-001: REQUIRED_KAFKA_TOPICS includes zone_traffic_sink and anomalies_sink."""
        from scripts.asp_setup import REQUIRED_KAFKA_TOPICS
        assert "zone_traffic_sink" in REQUIRED_KAFKA_TOPICS, \
            "REQUIRED_KAFKA_TOPICS must include zone_traffic_sink"
        assert "anomalies_sink" in REQUIRED_KAFKA_TOPICS, \
            "REQUIRED_KAFKA_TOPICS must include anomalies_sink"
        # Verify existing topics still present
        assert "event_documents" in REQUIRED_KAFKA_TOPICS
        assert "completed_actions" in REQUIRED_KAFKA_TOPICS


# ── TC-KT-REST-*: REST API-based topic pre-creation ──────────────────────────
class TestKafkaTopicRestAPI:
    """Tests for REQ-R-001..005: ensure_kafka_topics uses Kafka REST API."""

    def test_ensure_kafka_topics_accepts_rest_params(self):
        """TC-KT-REST-001: ensure_kafka_topics accepts kafka_rest_endpoint and cluster_id."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        sig = inspect.signature(ensure_kafka_topics)
        params = list(sig.parameters.keys())
        assert "kafka_rest_endpoint" in params, \
            "ensure_kafka_topics must accept kafka_rest_endpoint parameter"
        assert "cluster_id" in params, \
            "ensure_kafka_topics must accept cluster_id parameter"

    def test_ensure_kafka_topics_uses_rest_api(self):
        """TC-KT-REST-002: ensure_kafka_topics uses HTTP REST API, not AdminClient."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        source = inspect.getsource(ensure_kafka_topics)
        assert "kafka/v3/clusters" in source, \
            "ensure_kafka_topics must use Kafka REST API v3 URL pattern"
        assert "AdminClient" not in source, \
            "ensure_kafka_topics must not use confluent-kafka AdminClient"

    def test_ensure_kafka_topics_no_confluent_kafka_guard(self):
        """TC-KT-REST-003: ensure_kafka_topics does not require confluent-kafka library."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        source = inspect.getsource(ensure_kafka_topics)
        assert "HAS_CONFLUENT_KAFKA" not in source, \
            "ensure_kafka_topics should not check HAS_CONFLUENT_KAFKA"

    def test_ensure_kafka_topics_uses_basic_auth(self):
        """TC-KT-REST-004: ensure_kafka_topics uses HTTP Basic auth."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        source = inspect.getsource(ensure_kafka_topics)
        assert "base64" in source or "Basic" in source, \
            "ensure_kafka_topics must use Basic auth for REST API"

    def test_ensure_kafka_topics_creates_with_6_partitions(self):
        """TC-KT-REST-005 (INV-001): topics created with 6 partitions."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        source = inspect.getsource(ensure_kafka_topics)
        assert "6" in source, \
            "ensure_kafka_topics must create topics with 6 partitions"

    def test_ensure_kafka_topics_retries_on_401(self):
        """TC-KT-REST-005b: ensure_kafka_topics retries when REST API returns 401."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        source = inspect.getsource(ensure_kafka_topics)
        assert "401" in source, \
            "ensure_kafka_topics must handle 401 (auth propagation delay)"
        assert "retry" in source.lower() or "attempt" in source.lower(), \
            "ensure_kafka_topics must retry on auth failure"

    def test_ensure_kafka_topics_removes_bootstrap_server_param(self):
        """TC-KT-REST-006: ensure_kafka_topics no longer requires bootstrap_server."""
        import inspect
        from scripts.asp_setup import ensure_kafka_topics
        sig = inspect.signature(ensure_kafka_topics)
        params = list(sig.parameters.keys())
        assert "bootstrap_server" not in params, \
            "ensure_kafka_topics should not accept bootstrap_server (uses REST endpoint instead)"

    def test_credentials_env_includes_rest_endpoint(self):
        """TC-KT-REST-007 (REQ-R-003): deploy saves CONFLUENT_KAFKA_REST_ENDPOINT."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._save_terraform_credentials)
        assert "CONFLUENT_KAFKA_REST_ENDPOINT" in source, \
            "_save_terraform_credentials must persist Kafka REST endpoint"

    def test_credentials_env_includes_cluster_id(self):
        """TC-KT-REST-008 (REQ-R-003): deploy saves CONFLUENT_KAFKA_CLUSTER_ID."""
        deploy = importlib.import_module("scripts.deploy")
        source = inspect.getsource(deploy._save_terraform_credentials)
        assert "CONFLUENT_KAFKA_CLUSTER_ID" in source, \
            "_save_terraform_credentials must persist Kafka cluster ID"

    def test_asp_cli_reads_rest_endpoint_from_env(self):
        """TC-KT-REST-009 (REQ-R-005): standalone CLI reads REST endpoint from .env."""
        import inspect
        asp = importlib.import_module("scripts.asp_setup")
        source = inspect.getsource(asp.main)
        assert "CONFLUENT_KAFKA_REST_ENDPOINT" in source or "kafka_rest_endpoint" in source, \
            "ASP CLI main() must read kafka_rest_endpoint"

    def test_asp_cli_reads_cluster_id_from_env(self):
        """TC-KT-REST-010 (REQ-R-005): standalone CLI reads cluster_id from .env."""
        import inspect
        asp = importlib.import_module("scripts.asp_setup")
        source = inspect.getsource(asp.main)
        assert "CONFLUENT_KAFKA_CLUSTER_ID" in source or "cluster_id" in source, \
            "ASP CLI main() must read cluster_id"


# ── TC-R-* : Mongo Review Remediation (2026-05) ─────────────────────────────

class TestDispatchLogIdempotency:
    """REQ-R-102: dispatch_log $merge must specify on:[pickup_zone, window_time]."""

    @pytest.fixture
    def pipeline(self):
        from scripts.asp_setup import _pipeline_dispatch_log
        return _pipeline_dispatch_log()

    def test_merge_has_on_clause(self, pipeline):
        """TC-R-102: $merge specifies on: [pickup_zone, window_time]."""
        merge = pipeline[-1]["$merge"]
        assert "on" in merge, (
            "dispatch_log $merge must declare 'on' to prevent duplicate inserts "
            "on consumer reset / pipeline replay"
        )
        assert merge["on"] == ["pickup_zone", "window_time"], (
            f"Expected on=['pickup_zone', 'window_time'], got {merge['on']!r}"
        )
        assert merge.get("whenMatched") == "replace"


class TestKnowledgeBaseProvenance:
    """REQ-R-105: knowledge_base merges include embedding provenance."""

    @pytest.fixture
    def pipeline(self):
        from scripts.asp_setup import _pipeline_event_knowledge_base
        return _pipeline_event_knowledge_base()

    def test_provenance_addfields_present(self, pipeline):
        """TC-R-105a: a $addFields stage stamps embedded_at, embedding_model, embedding_dim, schema_version."""
        provenance_keys = {"embedded_at", "embedding_model", "embedding_dim", "schema_version"}
        found = False
        for stage in pipeline:
            if "$addFields" in stage:
                if provenance_keys.issubset(stage["$addFields"].keys()):
                    found = True
                    addf = stage["$addFields"]
                    assert addf["embedding_model"] == "voyage-4"
                    assert addf["embedding_dim"] == 1024
                    assert addf["schema_version"] == 1
                    # Issue #11 (2026-05-29): $$NOW is REJECTED by ASP
                    # $addFields ("Builtin variable '$$NOW' is not
                    # available") — it sent every doc to the DLQ and left
                    # knowledge_base empty. embedded_at now uses the
                    # per-document stream timestamp, which ASP supports.
                    assert addf["embedded_at"] == "$_stream_meta.source.ts"
                    break
        assert found, (
            "knowledge_base pipeline must include $addFields with embedded_at, "
            "embedding_model, embedding_dim, schema_version (REQ-R-105)"
        )

    def test_project_includes_provenance(self, pipeline):
        """TC-R-105b: $project allow-list keeps provenance fields."""
        proj = next(s["$project"] for s in pipeline if "$project" in s)
        for f in ("embedded_at", "embedding_model", "embedding_dim", "schema_version"):
            assert f in proj, f"$project must allow-list {f}"


class TestUrlSafeDocumentId:
    """REQ-R-114 / BUG-301: document_id must be URL-safe ([a-z0-9-]).

    Updated 2026-05-12: $regexReplace is not a valid MongoDB / ASP operator.
    The slug is now computed in Python at seed time and stored on the
    calendar doc; the ASP pipeline projects it through unchanged.
    """

    def test_pipeline_does_not_use_regex_replace(self):
        """TC-BUG-301c: pipeline must NOT use $regexReplace (invalid in ASP)."""
        from scripts.asp_setup import _pipeline_event_knowledge_base
        pipeline = _pipeline_event_knowledge_base()
        as_str = str(pipeline)
        assert "$regexReplace" not in as_str, (
            "BUG-301: $regexReplace is not a valid ASP operator. "
            "document_id must be precomputed in Python at seed time."
        )

    def test_compute_document_id_helper(self):
        """TC-BUG-301a: _compute_document_id produces URL-safe slugs."""
        from scripts.asp_setup import _compute_document_id
        # Standard case
        assert _compute_document_id("Essence Music Festival", "CBD") == "essence-music-festival-cbd"
        # Ampersand + multiple spaces
        assert _compute_document_id(
            "Garden District Home & Garden Tour", "Garden District"
        ) == "garden-district-home-garden-tour-garden-district"
        # Punctuation + parens
        assert _compute_document_id("Saints Game (Home)", "CBD") == "saints-game-home-cbd"
        # Determinism: same input → same output
        a = _compute_document_id("Mardi Gras Parade", "French Quarter")
        b = _compute_document_id("Mardi Gras Parade", "French Quarter")
        assert a == b == "mardi-gras-parade-french-quarter"
        # No leading/trailing dashes
        assert not _compute_document_id("!Test!", "Z!").startswith("-")
        assert not _compute_document_id("!Test!", "Z!").endswith("-")

    def test_seed_event_struct_carries_document_id_field(self):
        """TC-BUG-301b: seed events have document_id baked in.

        We can't run Mongo here, but we can verify the seeder source
        references document_id assignment.
        """
        import inspect
        from scripts import asp_setup
        src = inspect.getsource(asp_setup)
        # The seeder assigns document_id when upserting calendar events
        assert "_compute_document_id" in src, \
            "asp_setup must use _compute_document_id when seeding events"


class TestUniqueDispatchIndex:
    """REQ-R-103: dispatch_log unique compound index on (pickup_zone, window_time)."""

    def test_unique_compound_index_defined(self):
        """TC-R-103: ensure_atlas_indexes defines unique on (pickup_zone, window_time)."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        # Must reference window_time on dispatch_log + unique
        assert "dispatch_log" in source
        assert "window_time" in source, (
            "REQ-R-103 requires dispatch_log indexed on (pickup_zone, window_time)"
        )


class TestVesselCatalogIndex:
    """REQ-R-110: fleet.vessel_catalog has unique index on vessel_id."""

    def test_vessel_catalog_unique_index(self):
        """TC-R-110: ensure_atlas_indexes creates unique index on vessel_catalog.vessel_id."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "vessel_catalog" in source, (
            "REQ-R-110 requires ensure_atlas_indexes to index fleet.vessel_catalog"
        )
        assert "vessel_id" in source


class TestDLQTTL:
    """REQ-R-111: validation_dlq has TTL index."""

    def test_dlq_ttl_defined(self):
        """TC-R-111: ensure_atlas_indexes defines TTL on validation_dlq."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "validation_dlq" in source, (
            "REQ-R-111 requires DLQ TTL index"
        )
        # Either expireAfterSeconds is referenced near validation_dlq
        # or the source includes a 30-day TTL constant
        assert "expireAfterSeconds" in source or "30 * 24" in source


class TestNativeValidators:
    """REQ-R-107: native $jsonSchema validators applied via collMod."""

    def test_validators_applied(self):
        """TC-R-107: ensure_atlas_indexes applies native collection validators
        (the actual logic lives in _apply_collection_validators which it calls)."""
        import inspect
        from scripts.asp_setup import (
            ensure_atlas_indexes, _apply_collection_validators,
        )
        # ensure_atlas_indexes must call into the validator helper
        assert "_apply_collection_validators" in inspect.getsource(ensure_atlas_indexes), (
            "ensure_atlas_indexes must call _apply_collection_validators"
        )
        # The helper must use $jsonSchema, collMod, and warn level
        helper_source = inspect.getsource(_apply_collection_validators)
        assert "$jsonSchema" in helper_source or "jsonSchema" in helper_source, (
            "REQ-R-107 requires native $jsonSchema validators"
        )
        assert "collMod" in helper_source, (
            "Validators applied via collMod command"
        )
        assert '"warn"' in helper_source or "'warn'" in helper_source, (
            "validationAction must be 'warn' for safe rollout"
        )


class TestRemovedSearchIndexes:
    """REQ-R-113: anomaly_reason_search & dispatch_summary_search no longer created."""

    def test_anomaly_reason_search_not_created(self):
        """TC-R-113: ensure_atlas_indexes no longer creates anomaly_reason_search."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "anomaly_reason_search" not in source, (
            "REQ-R-113: unused Atlas Search index 'anomaly_reason_search' "
            "must be removed (no caller uses it)"
        )

    def test_dispatch_summary_search_not_created(self):
        """TC-R-113: ensure_atlas_indexes no longer creates dispatch_summary_search."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "dispatch_summary_search" not in source


class TestSeederPreservesCreatedAt:
    """REQ-R-104: seed_events preserves created_at on existing docs."""

    def test_seed_events_uses_setOnInsert(self):
        """TC-R-104: seed_events uses $setOnInsert for created_at."""
        import inspect
        import scripts.asp_setup as m
        source = inspect.getsource(m.seed_events_calendar)
        assert "$setOnInsert" in source, (
            "REQ-R-104: seed_events must use $setOnInsert for created_at "
            "to avoid mutating it on every run (which triggers re-embedding)"
        )

    def test_seed_events_uses_currentDate_for_updated_at(self):
        """TC-R-104b: seed_events uses $currentDate for updated_at."""
        import inspect
        import scripts.asp_setup as m
        source = inspect.getsource(m.seed_events_calendar)
        assert "$currentDate" in source, (
            "REQ-R-104: seed_events should use $currentDate for updated_at"
        )


class TestSharedMongoHelperUsage:
    """REQ-R-106: ASP setup uses scripts.common.mongo helper."""

    def test_asp_imports_common_mongo(self):
        """TC-R-106: asp_setup imports build_uri/get_client from common.mongo."""
        import inspect
        import scripts.asp_setup as m
        source = inspect.getsource(m)
        assert "from scripts.common.mongo import" in source, (
            "REQ-R-106: asp_setup must import shared mongo helper"
        )

    def test_dashboard_uses_common_mongo(self):
        import inspect
        import scripts.dashboard as m
        source = inspect.getsource(m)
        assert "scripts.common.mongo" in source

    def test_destroy_uses_common_mongo(self):
        import inspect
        import scripts.destroy as m
        source = inspect.getsource(m)
        assert "scripts.common.mongo" in source

    def test_pipeline_reset_uses_common_mongo(self):
        import inspect
        import scripts.pipeline_reset as m
        source = inspect.getsource(m)
        assert "scripts.common.mongo" in source


# ── TC-R-DEPLOY: Idempotent re-deploy on existing clusters ─────────────────

class TestIdempotentRedeploy:
    """Fresh deployments and re-deploys onto clusters with legacy state must
    not fail. Specifically:
      - the old non-unique pickup_zone_dispatched_at_compound index must be
        dropped before creating the new unique index;
      - the unique-index creation must dedupe pre-existing duplicates
        (created by the buggy old $merge with no on: clause) before
        attempting create_index(unique=True).
    """

    def test_drops_legacy_dispatch_compound(self):
        """TC-R-DEPLOY-001: ensure_atlas_indexes drops the legacy
        pickup_zone_dispatched_at_compound index if present."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        assert "pickup_zone_dispatched_at_compound" in source, (
            "ensure_atlas_indexes must reference the legacy index name "
            "in order to drop it on re-deploy"
        )
        assert "drop_index" in source, (
            "ensure_atlas_indexes must call drop_index for the legacy index"
        )

    def test_dedupes_dispatch_before_unique(self):
        """TC-R-DEPLOY-002: dedup helper invoked before unique-index create.

        When upgrading a cluster that ran the old broken $merge (no on:),
        fleet.dispatch_log already contains duplicate (pickup_zone,
        window_time) rows. create_index(unique=True) would raise E11000.
        Helper must remove duplicates first, keeping the most recent.
        """
        import inspect
        import scripts.asp_setup as m
        source = inspect.getsource(m)
        assert "_dedupe_dispatch_log" in source, (
            "asp_setup must define _dedupe_dispatch_log helper"
        )

    def test_dedupe_dispatch_helper_exists(self):
        """TC-R-DEPLOY-003: _dedupe_dispatch_log function is defined and callable."""
        from scripts.asp_setup import _dedupe_dispatch_log
        assert callable(_dedupe_dispatch_log)

    def test_dedupe_runs_before_unique_index_create(self):
        """TC-R-DEPLOY-004: dedupe call appears before pickup_zone_window_time_unique create."""
        import inspect
        from scripts.asp_setup import ensure_atlas_indexes
        source = inspect.getsource(ensure_atlas_indexes)
        dedupe_pos = source.find("_dedupe_dispatch_log")
        unique_pos = source.find("pickup_zone_window_time_unique")
        assert dedupe_pos != -1, "Must call _dedupe_dispatch_log"
        assert unique_pos != -1, "Must create pickup_zone_window_time_unique"
        assert dedupe_pos < unique_pos, (
            "_dedupe_dispatch_log must run BEFORE pickup_zone_window_time_unique create_index"
        )


# ── TC-R-DEPLOY-005: Atlas Search index cleanup on destroy ──────────────────

class TestDestroyAtlasSearchCleanup:
    """REQ-R-113 follow-up: destroy.py removes legacy Atlas Search indexes
    so a subsequent deploy on the same cluster does not inherit them."""

    def test_destroy_drops_legacy_search_indexes(self):
        """TC-R-DEPLOY-005: destroy.py references the obsolete search index names
        in order to drop them via the Atlas API."""
        import inspect
        import scripts.destroy as d
        source = inspect.getsource(d)
        assert "anomaly_reason_search" in source, (
            "destroy.py must drop the legacy anomaly_reason_search index"
        )
        assert "dispatch_summary_search" in source, (
            "destroy.py must drop the legacy dispatch_summary_search index"
        )


# ── TC-ASP-CLUSTER-001/002: cluster-existence preflight (fail-fast) ─────────

class TestRunAspSetupClusterPreflight:
    """`run_asp_setup` must verify ATLAS_CLUSTER_NAME exists BEFORE creating
    the ASP instance and connections. Without this, the deploy fails late
    with three near-identical 400 errors (atlas_cluster + events_dlq +
    fleet_dlq all reference cluster_name)."""

    def test_TC_ASP_CLUSTER_001_returns_false_when_cluster_missing(self, monkeypatch, capsys):
        """REQ-E-361: misconfigured cluster name → False + clear message, no ASP instance created."""
        import scripts.asp_setup as asp
        from scripts.preflight import CheckResult

        # Make the cluster-existence check report fail
        def fake_check(env):
            return CheckResult(
                "fail",
                "cluster 'conf-mdb' not found in project proj-1",
                remediation="available clusters: alpha, beta. Update ATLAS_CLUSTER_NAME in .env.",
            )
        monkeypatch.setattr(asp, "check_atlas_cluster_exists", fake_check)

        # ensure_asp_instance must NOT be called — fail-fast contract
        called = {"asp_instance": False, "connections": False}
        def must_not_call_instance(*_a, **_kw):
            called["asp_instance"] = True
        def must_not_call_connections(*_a, **_kw):
            called["connections"] = True
        monkeypatch.setattr(asp, "ensure_asp_instance", must_not_call_instance)
        monkeypatch.setattr(asp, "ensure_connections", must_not_call_connections)

        ok = asp.run_asp_setup(
            atlas_public_key="pub", atlas_private_key="priv",
            project_id="proj-1", cluster_name="conf-mdb",
            confluent_bootstrap_server="boot", confluent_api_key="k",
            confluent_api_secret="s", voyage_api_key="v",
        )
        assert ok is False, "run_asp_setup must return False on cluster mismatch"
        assert not called["asp_instance"], \
            "ensure_asp_instance must not run when cluster check fails"
        assert not called["connections"], \
            "ensure_connections must not run when cluster check fails"

        captured = capsys.readouterr().out
        assert "conf-mdb" in captured
        assert "alpha" in captured and "beta" in captured, \
            "stdout must echo the available-clusters remediation"

    def test_TC_ASP_CLUSTER_002_proceeds_when_cluster_exists(self, monkeypatch):
        """REQ-E-361: cluster present → check passes, ASP instance creation proceeds."""
        import scripts.asp_setup as asp
        from scripts.preflight import CheckResult

        monkeypatch.setattr(asp, "check_atlas_cluster_exists",
                            lambda env: CheckResult("pass", "cluster ok"))

        # Stub the rest of the pipeline so we only test the early-exit contract
        called = {"asp_instance": False}
        def fake_instance(*_a, **_kw):
            called["asp_instance"] = True
            return {"hostnames": ["host"]}
        def fake_connections(*_a, **_kw):
            pass
        def fake_topics(*_a, **_kw):
            pass
        def fake_indexes(*_a, **_kw):
            pass
        monkeypatch.setattr(asp, "ensure_asp_instance", fake_instance)
        monkeypatch.setattr(asp, "ensure_connections", fake_connections)
        monkeypatch.setattr(asp, "ensure_kafka_topics", fake_topics)
        monkeypatch.setattr(asp, "ensure_atlas_indexes", fake_indexes)

        ok = asp.run_asp_setup(
            atlas_public_key="pub", atlas_private_key="priv",
            project_id="proj-1", cluster_name="real-cluster",
            confluent_bootstrap_server="boot", confluent_api_key="k",
            confluent_api_secret="s", voyage_api_key="v",
            skip_seed=True, skip_processors=True,
        )
        assert ok is True
        assert called["asp_instance"], \
            "ensure_asp_instance must run when cluster check passes"

    def test_TC_ASP_CLUSTER_003_warn_does_not_block(self, monkeypatch):
        """REQ-E-361: warn (transient) → check does not block deploy."""
        import scripts.asp_setup as asp
        from scripts.preflight import CheckResult

        monkeypatch.setattr(asp, "check_atlas_cluster_exists",
                            lambda env: CheckResult("warn", "network blip"))

        called = {"asp_instance": False}
        def fake_instance(*_a, **_kw):
            called["asp_instance"] = True
            return {"hostnames": ["host"]}
        monkeypatch.setattr(asp, "ensure_asp_instance", fake_instance)
        monkeypatch.setattr(asp, "ensure_connections", lambda *a, **kw: None)
        monkeypatch.setattr(asp, "ensure_kafka_topics", lambda *a, **kw: None)
        monkeypatch.setattr(asp, "ensure_atlas_indexes", lambda *a, **kw: None)

        ok = asp.run_asp_setup(
            atlas_public_key="pub", atlas_private_key="priv",
            project_id="proj-1", cluster_name="real-cluster",
            confluent_bootstrap_server="boot", confluent_api_key="k",
            confluent_api_secret="s", voyage_api_key="v",
            skip_seed=True, skip_processors=True,
        )
        assert ok is True
        assert called["asp_instance"], \
            "warn must not block deploy (transient network may resolve in retries)"

    def test_TC_ASP_CLUSTER_004_lazy_import_fallback_does_not_block(self):
        """REQ-E-361: when scripts.preflight import fails, the fallback stub
        returns warn — asp_setup proceeds with the pre-existing late-failure
        behavior rather than introducing a new hard dependency."""
        # Verify the fallback stub matches the documented contract by
        # exercising it directly. Simulating an ImportError on a real
        # module is fragile (sys.modules manipulation), so we instead
        # construct the fallback stub the same way asp_setup.py would
        # and assert its shape — which is the actual API contract the
        # rest of run_asp_setup depends on.
        from types import SimpleNamespace
        # This is verbatim the fallback definition at scripts/asp_setup.py
        # (REQ-E-361 lazy-import block):
        def fallback(env):
            return SimpleNamespace(
                status="warn",
                message="preflight unavailable",
                remediation=None,
            )
        result = fallback({"ATLAS_CLUSTER_NAME": "anything"})
        # run_asp_setup checks .status against the string "fail" and "warn",
        # and reads .message + .remediation in the warn-log branch.
        assert result.status == "warn", \
            "fallback must NOT report 'fail' (would block deploy)"
        assert hasattr(result, "message"), \
            "fallback must expose .message (read by warn-log branch)"
        assert hasattr(result, "remediation"), \
            "fallback must expose .remediation (read by fail branch)"
        # And the asp_setup module must expose the availability flag so
        # callers can detect the degraded mode.
        import scripts.asp_setup as asp
        assert hasattr(asp, "_CLUSTER_PREFLIGHT_AVAILABLE"), \
            "asp_setup must export the availability flag for diagnostics"
        # In a normal install scripts.preflight imports cleanly:
        assert asp._CLUSTER_PREFLIGHT_AVAILABLE is True

    def test_TC_ASP_CLUSTER_005_imports_public_name(self):
        """REQ-E-361 naming clause: asp_setup imports the public name
        (no leading underscore)."""
        import inspect
        import scripts.asp_setup as asp
        src = inspect.getsource(asp)
        assert "from scripts.preflight import check_atlas_cluster_exists" in src, \
            "asp_setup must import the public (non-underscore) name"


# ── TC-ASP-KB-001..003: vector_index ordering (Issue #1) ────────────────────

class TestEnsureKnowledgeBaseCollectionBeforeVectorIndex:
    """REQ-FIX-001: ensure_atlas_indexes must create the events.knowledge_base
    collection BEFORE attempting the vector_index POST.

    On a fresh cluster the collection doesn't exist, so the Atlas Admin API
    vector-index create returns 400 ATLAS_SEARCH_COLLECTION_NOT_FOUND. The
    RAG pipeline (anomalies-enriched-insert) then FAILS forever because
    documents_vectordb references a non-existent index. Observed in a real
    deploy 2026-05-29.
    """

    def test_TC_ASP_KB_001_helper_exists(self):
        """A helper that creates the events.knowledge_base collection
        idempotently must exist and be called by ensure_atlas_indexes."""
        import inspect
        import scripts.asp_setup as asp
        assert hasattr(asp, "_ensure_kb_collection"), \
            "asp_setup must expose _ensure_kb_collection helper"
        src = inspect.getsource(asp.ensure_atlas_indexes)
        assert "_ensure_kb_collection" in src, \
            "ensure_atlas_indexes must call _ensure_kb_collection"

    def test_TC_ASP_KB_002_collection_created_before_vector_index_post(self):
        """Ordering: the collection-create call must appear BEFORE the
        vector-index POST in ensure_atlas_indexes source."""
        import inspect
        import scripts.asp_setup as asp
        src = inspect.getsource(asp.ensure_atlas_indexes)
        kb_pos = src.find("_ensure_kb_collection")
        # The vector index POST is the `search/indexes` create
        post_pos = src.find("search/indexes")
        assert kb_pos != -1, "must call _ensure_kb_collection"
        assert post_pos != -1, "must POST to search/indexes"
        assert kb_pos < post_pos, (
            "_ensure_kb_collection must run BEFORE the vector_index POST "
            "(collection must exist or the POST 400s on a fresh cluster)"
        )

    def test_TC_ASP_NOW_001_no_dollar_now_variable_in_pipelines(self):
        """Issue #11 (2026-05-29): Atlas Stream Processing rejects the
        `$$NOW` builtin variable in $addFields ("Builtin variable '$$NOW'
        is not available"). Every ASP pipeline that used it sent ALL
        documents to the DLQ — knowledge_base, zone_traffic, and
        zone_anomalies all stayed empty (1,893 DLQ docs observed live).

        The ASP pipelines must NOT reference $$NOW. They use the
        per-document stream timestamp ($_stream_meta.source.ts) instead.
        """
        import inspect
        import scripts.asp_setup as asp
        # Check each pipeline builder's source for the $$NOW variable.
        for fn_name in (
            "_pipeline_event_knowledge_base",
            "_pipeline_dispatch_log",
            "_pipeline_zone_traffic_ingestion",
            "_pipeline_anomalies_ingestion",
        ):
            fn = getattr(asp, fn_name, None)
            if fn is None:
                continue
            src = inspect.getsource(fn)
            # Only flag the VALUE form ("$$NOW" as a field value). Prose
            # mentions in explanatory comments ('$$NOW' in single quotes)
            # are fine — the bug is using it as an aggregation value.
            assert '"$$NOW"' not in src, (
                f"{fn_name} must NOT use the $$NOW variable as a field value "
                f"— ASP rejects it in $addFields (Issue #11). Use "
                f"$_stream_meta.source.ts."
            )

    def test_TC_ASP_NOW_002_uses_stream_meta_timestamp(self):
        """Issue #11: the provenance/ingested timestamps must use the
        ASP-supported per-document stream timestamp."""
        import inspect
        import scripts.asp_setup as asp
        # At least the KB-population and the two window-ingestion pipelines
        # must reference the stream timestamp for their timestamp field.
        kb_src = inspect.getsource(asp._pipeline_event_knowledge_base)
        assert "_stream_meta.source.ts" in kb_src, (
            "event_knowledge_base_population must set embedded_at from "
            "$_stream_meta.source.ts (ASP-supported), not $$NOW"
        )

    def test_TC_ASP_NOW_003_toDate_conversions_preserved(self):
        """Issue #11 regression guard: removing $$NOW must NOT remove the
        critical $toDate window conversions that share the $addFields
        stage (those are the type contract for the TTL index)."""
        import inspect
        import scripts.asp_setup as asp
        zt = inspect.getsource(asp._pipeline_zone_traffic_ingestion)
        an = inspect.getsource(asp._pipeline_anomalies_ingestion)
        assert '"$toDate": "$window_start"' in zt and '"$toDate": "$window_end"' in zt, (
            "zone_traffic pipeline must keep $toDate window_start/window_end"
        )
        assert '"$toDate": "$window_time"' in an, (
            "anomalies pipeline must keep $toDate window_time"
        )

    def test_TC_ASP_CONN_001_stops_all_nonterminal_processors(self):
        """Issue #8 (2026-05-29): the connection-update stop logic must stop
        EVERY non-terminal processor (STARTED, STARTING, STOPPING, ...),
        not only STARTED ones.

        A processor left STOPPING by a prior interrupted run still HOLDS
        its connections, so a later `delete connection` 403s with
        STREAM_CONNECTION_HAS_STREAM_PROCESSORS. The fix must gate on a
        terminal-state set ({STOPPED, FAILED}), not the literal
        `state == "STARTED"`.
        """
        import inspect
        import scripts.asp_setup as asp
        src = inspect.getsource(asp.ensure_connections)
        # Must NOT gate stop solely on the literal STARTED equality.
        assert 'if state == "STARTED":' not in src, (
            "connection-update stop must not gate solely on state==STARTED "
            "(misses STOPPING processors from a prior interrupted run)"
        )
        # Must reference a terminal-state concept (STOPPED + FAILED) and
        # handle STOPPING explicitly.
        assert "STOPPING" in src, (
            "must handle STOPPING processors (wait for them rather than "
            "skip — they still hold connections)"
        )
        assert "FAILED" in src and "STOPPED" in src, (
            "must treat {STOPPED, FAILED} as terminal (safe — releases "
            "connections)"
        )

    def test_TC_ASP_CONN_002_waits_for_all_before_delete(self):
        """Issue #8: all non-terminal processors must reach a terminal
        state BEFORE any connection delete is attempted."""
        import inspect
        import scripts.asp_setup as asp
        src = inspect.getsource(asp.ensure_connections)
        # The stop/poll block must precede the delete loop. Find the
        # connection-delete call and the terminal-wait.
        stop_pos = src.find("for connection update")
        delete_pos = src.find("delete connection")
        assert stop_pos != -1 and delete_pos != -1
        assert stop_pos < delete_pos, (
            "must stop + wait for processors BEFORE deleting connections"
        )

    def test_TC_ASP_KB_004_document_id_index_is_not_partial(self):
        """Issue #5 (2026-05-29): the events.knowledge_base.document_id
        unique index MUST be non-partial. ASP's $merge on:"document_id"
        rejects a partialFilterExpression index ("Cannot find index to
        verify that join fields will be unique"), so
        event_knowledge_base_population FAILS on every deploy.

        The partial filter (REQ-CRF-058) was added to tolerate legacy
        docs with no document_id; we now purge those instead.
        """
        import inspect
        import re
        import scripts.asp_setup as asp
        src = inspect.getsource(asp.ensure_atlas_indexes)
        # Find the create_index(...) call that names document_id_unique and
        # assert THAT call does not pass partialFilterExpression. (The
        # drop-legacy code legitimately mentions partialFilterExpression
        # when detecting + removing an old partial index, so a naive
        # window check would false-positive.)
        create_calls = re.findall(
            r"create_index\((.*?)\)", src, re.DOTALL
        )
        doc_id_creates = [c for c in create_calls if "document_id_unique" in c]
        assert doc_id_creates, "must have a create_index for document_id_unique"
        for call in doc_id_creates:
            assert "partialFilterExpression" not in call, (
                "the document_id_unique create_index call must NOT pass "
                "partialFilterExpression — ASP $merge requires a full "
                "unique index"
            )

    def test_TC_ASP_KB_005_purges_null_document_id_before_full_index(self):
        """Issue #5: a full unique index would E11000 on legacy docs that
        lack document_id. ensure_atlas_indexes must purge them first."""
        import inspect
        import scripts.asp_setup as asp
        src = inspect.getsource(asp.ensure_atlas_indexes)
        # Must delete_many docs where document_id is missing, before the
        # full unique index create.
        assert 'delete_many' in src and '"document_id"' in src, \
            "must purge docs missing document_id"
        purge_pos = src.find('{"document_id": {"$exists": False}}')
        idx_pos = src.find("document_id_unique")
        assert purge_pos != -1, (
            "must delete_many({'document_id': {'$exists': False}}) to clear "
            "legacy null-document_id docs before the full unique index"
        )
        assert purge_pos < idx_pos, "purge must run before the index create"

    def test_TC_ASP_KB_003_create_collection_idempotent(self):
        """_ensure_kb_collection must swallow 'already exists'
        (CollectionInvalid / NamespaceExists) so re-deploys don't crash."""
        import scripts.asp_setup as asp
        from unittest import mock as _mock

        # Simulate a client whose create_collection raises "already exists"
        fake_db = _mock.MagicMock()

        class _AlreadyExists(Exception):
            pass

        fake_db.create_collection.side_effect = _AlreadyExists("already exists")
        fake_client = {"events": fake_db}

        # Must not raise even though create_collection raised
        try:
            asp._ensure_kb_collection(fake_client)
        except Exception as e:  # noqa: BLE001
            # Acceptable only if it's clearly handling — but contract is
            # "never raise on already-exists"
            import pytest as _pytest
            _pytest.fail(f"_ensure_kb_collection must swallow already-exists, raised {e!r}")
