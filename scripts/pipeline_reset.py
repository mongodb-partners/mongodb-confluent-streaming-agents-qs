#!/usr/bin/env python3
"""Pipeline reset for data generation restarts.

When ShadowTraffic is restarted (e.g. via the dashboard "Start Data Generation"
button), stale data in ride_requests poisons the Flink watermark and prevents
anomaly detection from working. This module performs a full pipeline reset:

1. Clear MongoDB sink collections (stale pipeline output)
2. Stop & delete Flink DML streaming statements
3. Delete & recreate pipeline Kafka topics
4. Recreate Flink DDL + DML statements

This ensures the streaming pipeline starts from a clean state with no stale
watermarks or leftover data from a previous ShadowTraffic run.
"""

import json
import logging
import subprocess
import time
import urllib.request
import urllib.error
import base64
from pathlib import Path

try:
    # HAS_PYMONGO availability sentinel (Mongo access goes through
    # scripts.common.mongo.get_client). Kept (noqa) so the guard works.
    from pymongo import MongoClient  # noqa: F401
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

try:
    from dotenv import dotenv_values
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

logger = logging.getLogger(__name__)

# statement names imported from canonical source.
from scripts.common.flink_statements import (
    DDL_STATEMENTS as _CANONICAL_DDL,
    DML_STATEMENTS as _CANONICAL_DML,
    AGENT_BOOTSTRAP_STATEMENTS as _CANONICAL_AGENT_BOOTSTRAP,
)

# Flink DML statements (streaming INSERT INTO — must be stopped/restarted)
DML_STATEMENTS = list(_CANONICAL_DML)

# Flink DDL statement (CREATE TABLE — must be recreated after topic deletion)
DDL_STATEMENTS = list(_CANONICAL_DDL)

# the dispatch agent + tool. deploy.py creates
# these before dispatch-insert; pipeline_reset previously did NOT, so
# every `uv run datagen` left dispatch-insert FAILED ("agent does not
# exist"). Bootstrapped in restart_flink_dml now.
AGENT_BOOTSTRAP_STATEMENTS = list(_CANONICAL_AGENT_BOOTSTRAP)

# dispatch-insert is the only DML that requires the agent chain + a
# healthy MCP server. It's submitted separately (gated) rather than in
# the generic DML loop.
_DISPATCH_STMT = "dispatch-insert"

# All pipeline Kafka topics to delete and recreate.
#
# anomalies_enriched and completed_actions are deliberately
# excluded — they are CREATED BY their CTAS DDL. Pre-creating their Kafka
# topics causes Confluent to auto-register a phantom raw-byte catalog table
# that blocks the CTAS DDL. (See deploy.py:_ensure_flink_topics )
PIPELINE_TOPICS = [
    "ride_requests",
    "windowed_traffic",
    "anomalies_per_zone",
    "zone_traffic_sink",
    "anomalies_sink",
]

# Flink catalog tables/views to DROP before topic recreation.
# When Kafka topics are recreated empty, Confluent Cloud auto-registers them in
# the Flink catalog as raw-byte tables (key VARBINARY, val VARBINARY). These
# phantom tables block Terraform's CREATE TABLE/VIEW IF NOT EXISTS from creating
# the properly typed tables. Dropping them first ensures clean DDL recreation.
#
# anomalies_enriched and completed_actions are excluded for
# the same reason as PIPELINE_TOPICS above — the CTAS owns their lifecycle.
FLINK_CATALOG_TABLES = [
    "windowed_traffic",      # view (depends on ride_requests, drop first)
    "ride_requests",
    "anomalies_per_zone",
    "zone_traffic_sink",
    "anomalies_sink",
]

# CTAS-managed output tables. These are created by
# `CREATE TABLE IF NOT EXISTS ... ('changelog.mode'='append')` DDL
# (anomalies-enriched-ctas / completed-actions-ctas). They are NOT in
# FLINK_CATALOG_TABLES because their topics must not be pre-created. BUT
# they MUST be dropped before the CTAS DDL is recreated: if they already
# exist as auto-registered raw-byte phantoms ([val: BYTES]), the
# CREATE TABLE IF NOT EXISTS no-ops against the phantom, and the
# downstream INSERT (anomalies-enriched-insert) FAILS with
# "Sink schema: [val: BYTES]" / "Different number of columns". deploy.py
# already drops these; pipeline_reset must too.
CTAS_CATALOG_TABLES = [
    "anomalies_enriched",
    "completed_actions",
]

# Terraform resource addresses for Flink DDL statements that must be
# force-recreated after topic deletion. Maps to confluent_flink_statement
# resources in terraform/agents/main.tf.
TERRAFORM_DDL_RESOURCES = [
    "confluent_flink_statement.ride_requests_table",
    "confluent_flink_statement.windowed_traffic_view",
    "confluent_flink_statement.anomalies_per_zone_table",
    "confluent_flink_statement.anomalies_sink_table",
    "confluent_flink_statement.zone_traffic_sink_table",
]

# Schema Registry subjects to delete (key + value for every pipeline topic).
#
# generated from PIPELINE_TOPICS × ("-value", "-key") so a
# future Flink CTAS that produces a keyed topic gets its -key subject
# cleaned up automatically. Previously this was a hand-maintained list
# that only cleaned -key for ride_requests; if any other topic ever
# became keyed, the stale -key subject would survive a reset and Flink
# would reconstruct the table with a phantom `key VARBINARY` column.
SCHEMA_SUBJECTS = [
    f"{topic}{suffix}"
    for topic in PIPELINE_TOPICS
    for suffix in ("-value", "-key")
] + ["anomalies_enriched-value"]  # CTAS-managed output

# MongoDB sink collections populated by ASP processors (db, collection)
MONGODB_SINK_COLLECTIONS = [
    ("analytics", "zone_traffic"),
    ("analytics", "zone_anomalies"),
    ("fleet", "dispatch_log"),
]

# ShadowTraffic Docker image used by datagen
SHADOWTRAFFIC_IMAGE = "shadowtraffic/shadowtraffic:1.14.1"


def _get_terraform_outputs(root: Path) -> dict | None:
    """Read terraform outputs from the core module.

    delegates to the cached helper.
    """
    from scripts.common.terraform_outputs import get_core_outputs
    outputs = get_core_outputs(root)
    if not outputs:
        logger.warning("No core terraform state / outputs — cannot reset pipeline")
        return None
    return outputs


def _get_flink_credentials(outputs: dict) -> dict | None:
    """Extract Flink REST API credentials from terraform outputs."""
    flink_key = outputs.get("app_manager_flink_api_key", {}).get("value", "")
    flink_secret = outputs.get("app_manager_flink_api_secret", {}).get("value", "")
    org_id = outputs.get("confluent_organization_id", {}).get("value", "")
    env_id = outputs.get("confluent_environment_id", {}).get("value", "")
    compute_pool_id = outputs.get("confluent_flink_compute_pool_id", {}).get("value", "")
    principal_id = outputs.get("app_manager_service_account_id", {}).get("value", "")
    flink_endpoint = outputs.get("confluent_flink_rest_endpoint", {}).get("value", "")
    catalog = outputs.get("confluent_environment_display_name", {}).get("value", "")
    database = outputs.get("confluent_kafka_cluster_display_name", {}).get("value", "")

    if not all([flink_key, flink_secret, org_id, env_id, flink_endpoint]):
        return None

    cred_bytes = base64.b64encode(f"{flink_key}:{flink_secret}".encode()).decode()
    return {
        "headers": {
            "Content-Type": "application/json",
            "Authorization": f"Basic {cred_bytes}",
        },
        "base_url": f"{flink_endpoint}/sql/v1/organizations/{org_id}/environments/{env_id}/statements",
        "compute_pool_id": compute_pool_id,
        "principal_id": principal_id,
        "catalog": catalog,
        "database": database,
        # Raw fields for FlinkRestClient construction
        "rest_endpoint": flink_endpoint,
        "api_key":       flink_key,
        "api_secret":    flink_secret,
        "org_id":        org_id,
        "env_id":        env_id,
    }


def _get_kafka_credentials(outputs: dict) -> dict | None:
    """Extract Kafka REST API credentials from terraform outputs."""
    rest_endpoint = outputs.get("confluent_kafka_cluster_rest_endpoint", {}).get("value", "")
    cluster_id = outputs.get("confluent_kafka_cluster_id", {}).get("value", "")
    kafka_api_key = outputs.get("app_manager_kafka_api_key", {}).get("value", "")
    kafka_api_secret = outputs.get("app_manager_kafka_api_secret", {}).get("value", "")

    if not all([rest_endpoint, cluster_id, kafka_api_key, kafka_api_secret]):
        return None

    cred = base64.b64encode(f"{kafka_api_key}:{kafka_api_secret}".encode()).decode()
    return {
        "rest_endpoint": rest_endpoint,
        "cluster_id": cluster_id,
        "headers": {
            "Content-Type": "application/json",
            "Authorization": f"Basic {cred}",
        },
    }


def _get_schema_registry_credentials(root: Path) -> dict | None:
    """Get Schema Registry credentials from .env."""
    if not HAS_DOTENV:
        return None

    env_file = root / ".env"
    if not env_file.exists():
        return None

    env = {k: v for k, v in dotenv_values(env_file).items() if v}
    sr_url = env.get("CONFLUENT_SCHEMA_REGISTRY_URL", "")
    sr_key = env.get("CONFLUENT_SCHEMA_REGISTRY_API_KEY", "")
    sr_secret = env.get("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "")

    if not all([sr_url, sr_key, sr_secret]):
        return None

    cred = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    return {
        "url": sr_url,
        "headers": {
            "Authorization": f"Basic {cred}",
        },
    }


def _delete_schema_subjects(root: Path) -> bool:
    """Delete Schema Registry subjects for pipeline topics.

    Stale schemas cause column mismatch errors when Flink DDL tables are
    recreated on fresh topics. Deleting subjects forces clean re-registration.

    success is now logged only when the DELETE actually
    succeeded (returned without raising, or raised HTTPError(404)).
    Previously the log line ran unconditionally, showing green
    checkmarks even when SR returned 500/403.
    """
    sr = _get_schema_registry_credentials(root)
    if sr is None:
        logger.info("No Schema Registry credentials — skipping subject cleanup")
        return False

    for subject in SCHEMA_SUBJECTS:
        url = f"{sr['url']}/subjects/{subject}"
        # Soft delete first, then hard delete.
        deleted_ok = False
        unexpected_error: Exception | None = None
        for params in ["", "?permanent=true"]:
            try:
                req = urllib.request.Request(
                    f"{url}{params}", method="DELETE", headers=sr["headers"],
                )
                urllib.request.urlopen(req, timeout=15)
                deleted_ok = True
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    deleted_ok = True  # already gone is success
                else:
                    unexpected_error = e
            except Exception as e:
                unexpected_error = e
        if deleted_ok:
            logger.info(f"Deleted schema subject '{subject}'")
        else:
            logger.warning(
                f"Failed to delete schema subject '{subject}': "
                f"{unexpected_error or 'unknown'}"
            )

    return True


def _drop_flink_catalog_tables(flink: dict) -> None:
    """Drop Flink catalog tables/views that will be recreated by Terraform.

    When Kafka topics are deleted and recreated, Confluent Cloud auto-registers
    raw-byte tables in the Flink catalog. These must be explicitly DROPped so
    that Terraform's CREATE TABLE/VIEW IF NOT EXISTS can create the properly
    typed versions.

    SQL "DROP TABLE IF EXISTS" delegates to FlinkRestClient.drop_table.
    """
    # Build a FlinkRestClient from the existing flink dict (constructed by
    # _get_flink_credentials).
    from scripts.common.flink_rest import FlinkRestClient
    client = FlinkRestClient(
        rest_endpoint=flink.get("rest_endpoint", ""),
        api_key=flink.get("api_key", ""),
        api_secret=flink.get("api_secret", ""),
        org_id=flink.get("org_id", ""),
        env_id=flink.get("env_id", ""),
        compute_pool_id=flink.get("compute_pool_id", ""),
        service_account_id=flink.get("principal_id", ""),
        catalog=flink.get("catalog", ""),
        database=flink.get("database", ""),
    )
    for table in FLINK_CATALOG_TABLES:
        try:
            client.drop_table(table, if_exists=True)
            logger.info(f"Dropped Flink catalog entry '{table}'")
        except Exception as e:
            logger.debug(f"drop_table({table}) raised: {e}")
        time.sleep(1)


def _drop_ctas_catalog_tables(flink: dict) -> None:
    """Drop the CTAS-managed catalog tables (anomalies_enriched,
    completed_actions) so the CTAS DDL recreates them typed.

    `CREATE TABLE IF NOT EXISTS` no-ops against an
    existing raw-byte phantom ([val: BYTES]). Dropping the table first
    forces the CTAS to create the proper 8-column / typed table, so
    anomalies-enriched-insert binds to the correct sink schema instead of
    failing with "Sink schema: [val: BYTES]". Mirrors deploy.py
    """
    from scripts.common.flink_rest import FlinkRestClient
    client = FlinkRestClient(
        rest_endpoint=flink.get("rest_endpoint", ""),
        api_key=flink.get("api_key", ""),
        api_secret=flink.get("api_secret", ""),
        org_id=flink.get("org_id", ""),
        env_id=flink.get("env_id", ""),
        compute_pool_id=flink.get("compute_pool_id", ""),
        service_account_id=flink.get("principal_id", ""),
        catalog=flink.get("catalog", ""),
        database=flink.get("database", ""),
    )
    for table in CTAS_CATALOG_TABLES:
        try:
            client.drop_table(table, if_exists=True)
            logger.info(f"Dropped CTAS catalog entry '{table}'")
        except Exception as e:
            logger.debug(f"drop_table({table}) raised: {e}")
        time.sleep(1)


def _run_terraform_ddl_replace(root: Path) -> bool:
    """Force-recreate Terraform-managed Flink DDL statements.

    Uses `terraform apply -replace=...` to destroy and recreate the DDL
    statements (CREATE TABLE, CREATE VIEW) so they pick up the new Kafka
    topic schemas instead of stale auto-created raw-byte tables.
    """
    agents_dir = root / "terraform" / "agents"
    if not (agents_dir / "main.tf").exists():
        logger.warning("No agents terraform module — skipping DDL recreation")
        return False

    cmd = ["terraform", "apply", "-auto-approve"]
    for resource in TERRAFORM_DDL_RESOURCES:
        cmd.extend(["-replace", resource])

    try:
        result = subprocess.run(
            cmd, cwd=agents_dir, capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            logger.info("Terraform DDL statements recreated successfully")
            return True
        else:
            logger.warning(f"Terraform apply failed: {result.stderr[:300]}")
            return False
    except Exception as e:
        logger.warning(f"Terraform apply failed: {e}")
        return False


def _stop_flink_statement(stmt_name: str, flink: dict) -> None:
    """Stop a Flink statement by setting stopped=true via PATCH."""
    url = f"{flink['base_url']}/{stmt_name}"
    try:
        stop_body = json.dumps([
            {"op": "replace", "path": "/spec/stopped", "value": True}
        ]).encode()
        req = urllib.request.Request(
            url, data=stop_body, method="PATCH",
            headers={**flink["headers"], "Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=15)
        logger.info(f"Stopped {stmt_name}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            pass  # already gone
        else:
            logger.debug(f"Could not stop {stmt_name}: HTTP {e.code}")
    except Exception:
        pass


def _wait_for_statement_gone(stmt_name: str, flink: dict, max_wait: int = 30) -> bool:
    """Poll until a Flink statement returns 404 (fully deleted)."""
    url = f"{flink['base_url']}/{stmt_name}"
    for _ in range(max_wait // 3):
        time.sleep(3)
        try:
            req = urllib.request.Request(url, method="GET", headers=flink["headers"])
            urllib.request.urlopen(req, timeout=10)
            # Still exists — keep waiting
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return True
        except Exception:
            pass
    return False


def _delete_flink_statement(stmt_name: str, flink: dict) -> bool:
    """Delete a Flink statement and wait for deletion confirmation."""
    url = f"{flink['base_url']}/{stmt_name}"
    try:
        req = urllib.request.Request(url, method="DELETE", headers=flink["headers"])
        urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return True  # already gone
    except Exception:
        pass

    return _wait_for_statement_gone(stmt_name, flink)


def _delete_kafka_topic(topic: str, kafka: dict) -> bool:
    """Delete a single Kafka topic via REST API v3."""
    url = f"{kafka['rest_endpoint']}/kafka/v3/clusters/{kafka['cluster_id']}/topics/{topic}"
    try:
        req = urllib.request.Request(url, method="DELETE", headers=kafka["headers"])
        urllib.request.urlopen(req, timeout=15)
        logger.info(f"Deleted topic '{topic}'")
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return True  # already gone
        logger.debug(f"Could not delete topic '{topic}': HTTP {e.code}")
        return False
    except Exception:
        return False


def _wait_for_kafka_topic_gone(topic: str, kafka: dict, timeout: int = 30) -> bool:
    """Poll for Kafka topic deletion (404 from GET) up to `timeout` seconds.

    Kafka DELETE is async — broker GC takes time. Without
    polling, the immediate recreate may NO-OP (TopicExists swallowed)
    and the freshly "recreated" topic still holds the old partition data.
    Returns True when topic is gone, False on timeout.
    """
    url = f"{kafka['rest_endpoint']}/kafka/v3/clusters/{kafka['cluster_id']}/topics/{topic}"
    import time as _time
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, method="GET", headers=kafka["headers"])
            urllib.request.urlopen(req, timeout=10)
            # 200 OK — still exists
            _time.sleep(2)
            continue
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return True
        except Exception:
            pass
        _time.sleep(2)
    return False


def _create_kafka_topic(topic: str, kafka: dict, num_partitions: int = 6) -> bool:
    """Create a Kafka topic via REST API v3 (POST)."""
    url = f"{kafka['rest_endpoint']}/kafka/v3/clusters/{kafka['cluster_id']}/topics"
    body = json.dumps({
        "topic_name": topic,
        "partitions_count": num_partitions,
    }).encode()
    try:
        req = urllib.request.Request(url, data=body, method="POST", headers=kafka["headers"])
        urllib.request.urlopen(req, timeout=30)
        logger.info(f"Created topic '{topic}'")
        return True
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ""
        if "TopicExistsException" in resp_body or e.code == 409:
            return True
        logger.warning(f"Failed to create topic '{topic}': HTTP {e.code}")
        return False
    except Exception as e:
        logger.warning(f"Failed to create topic '{topic}': {e}")
        return False


def _submit_flink_statement(stmt_name: str, flink: dict, sql_dir: Path, is_ddl: bool = False) -> bool:
    """Submit a Flink SQL statement from an SQL template file."""
    sql_file = sql_dir / f"{stmt_name}.sql"
    if not sql_file.exists():
        logger.warning(f"SQL template not found: {sql_file}")
        return False

    sql = sql_file.read_text().strip().format(
        catalog=flink["catalog"],
        database=flink["database"],
    )

    payload = {
        "name": stmt_name,
        "spec": {
            "statement": sql,
            "properties": {
                "sql.current-catalog": flink["catalog"],
                "sql.current-database": flink["database"],
            },
            "compute_pool_id": flink["compute_pool_id"],
            "principal": flink["principal_id"],
        },
    }
    body = json.dumps(payload).encode()

    max_attempts = 3
    retry_backoff = [3, 6, 12]
    for attempt in range(max_attempts):
        req = urllib.request.Request(
            flink["base_url"], data=body, method="POST", headers=flink["headers"],
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                phase = data.get("status", {}).get("phase", "unknown")
                logger.info(f"Created {stmt_name} (phase: {phase})")
                return True
        except urllib.error.HTTPError as e:
            if e.code in (429,) or e.code >= 500:
                if attempt < max_attempts - 1:
                    wait = retry_backoff[attempt]
                    logger.debug(f"Retrying {stmt_name}: HTTP {e.code}, wait {wait}s")
                    time.sleep(wait)
                    continue
            logger.warning(f"Could not create {stmt_name}: HTTP {e.code}")
            return False
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(retry_backoff[attempt])
                continue
            logger.warning(f"Could not create {stmt_name}: {e}")
            return False
    return False


def _wait_for_dml_running(flink: dict, max_wait: int = 120,
                          statements: list[str] | None = None) -> bool:
    """Poll DML statements until all reach RUNNING or timeout.

    now returns a bool — True iff every expected
    statement reached RUNNING, False if any reached FAILED/STOPPED or
    timed out. Previously returned None, so restart_flink_dml reported
    success even on a fully-FAILED pipeline.

    Args:
        flink: credentials dict.
        max_wait: seconds to poll.
        statements: which statements to wait for. Defaults to the full
            DML_STATEMENTS list. Callers that gated dispatch-insert pass
            only the statements they actually submitted.
    """
    pending = set(statements if statements is not None else DML_STATEMENTS)
    failed = set()
    elapsed = 0
    poll_interval = 5

    while pending and elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        for stmt_name in list(pending):
            url = f"{flink['base_url']}/{stmt_name}"
            try:
                req = urllib.request.Request(url, method="GET", headers=flink["headers"])
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read())
                    phase = data.get("status", {}).get("phase", "")
                    if phase == "RUNNING":
                        pending.discard(stmt_name)
                    elif phase in ("FAILED", "STOPPED"):
                        logger.warning(f"{stmt_name} reached {phase}")
                        failed.add(stmt_name)
                        pending.discard(stmt_name)
            except Exception:
                pass

    if not pending and not failed:
        logger.info("All DML statements are RUNNING")
        return True
    if failed:
        logger.warning(f"{len(failed)} statement(s) failed: {', '.join(sorted(failed))}")
    if pending:
        logger.warning(f"Timed out waiting for: {', '.join(sorted(pending))}")
    return False


def _resolve_mongodb_uri(root: Path) -> str | None:
    """Resolve MongoDB connection URI from .env.

    Follows the same credential chain as the dashboard:
    .env -> TF_VAR_mongodb_connection_string + user/pass.
    """
    if not HAS_DOTENV:
        return None

    env_file = root / ".env"
    if not env_file.exists():
        return None

    env = {k: v for k, v in dotenv_values(env_file).items() if v}
    conn = env.get("TF_VAR_mongodb_connection_string")
    if not conn:
        return None

    user = env.get("TF_VAR_mongodb_username", "")
    pwd = env.get("TF_VAR_mongodb_password", "")
    from scripts.common.mongo import build_uri
    return build_uri(conn, user, pwd)


def _clear_mongodb_collections(root: Path) -> bool:
    """Clear pipeline sink collections in MongoDB Atlas.

    Removes stale output data from previous ShadowTraffic runs so the
    dashboard shows only fresh data from the current run.
    """
    if not HAS_PYMONGO:
        logger.info("pymongo not available — skipping MongoDB cleanup")
        return False

    uri = _resolve_mongodb_uri(root)
    if not uri:
        logger.info("No MongoDB URI found — skipping MongoDB cleanup")
        return False

    try:
        from scripts.common.mongo import get_client
        client = get_client(uri, app_name="streaming-agents-pipeline-reset")
        client.admin.command("ping")
    except Exception as e:
        logger.warning(f"Could not connect to MongoDB: {e}")
        return False

    try:
        for db_name, coll_name in MONGODB_SINK_COLLECTIONS:
            result = client[db_name][coll_name].delete_many({})
            logger.info(f"Cleared {db_name}.{coll_name} ({result.deleted_count} documents)")
        return True
    except Exception as e:
        logger.warning(f"Error clearing MongoDB collections: {e}")
        return False


def reset_pipeline(root: Path) -> bool:
    """Perform a full pipeline reset for clean data generation restart.

    Clears MongoDB sink collections, stops Flink DML statements, deletes and
    recreates pipeline Kafka topics, then recreates Flink DDL + DML statements.
    This ensures the streaming pipeline starts from a clean state with no stale
    watermarks.

    Args:
        root: Project root directory.

    Returns:
        True if reset succeeded, False otherwise.
    """
    print("\n=== Pipeline Reset (cleaning stale data) ===")

    outputs = _get_terraform_outputs(root)
    if outputs is None:
        print("  [warn] No terraform outputs — skipping pipeline reset")
        return False

    flink = _get_flink_credentials(outputs)
    if flink is None:
        print("  [warn] Missing Flink credentials — skipping pipeline reset")
        return False

    kafka = _get_kafka_credentials(outputs)
    if kafka is None:
        print("  [warn] Missing Kafka credentials — skipping pipeline reset")
        return False

    # Step 0: Clear MongoDB sink collections (stale pipeline output)
    print("  -> Clearing MongoDB sink collections...")
    _clear_mongodb_collections(root)

    # Step 1: Stop all DML statements
    print("  -> Stopping Flink DML statements...")
    for stmt_name in DML_STATEMENTS:
        _stop_flink_statement(stmt_name, flink)

    # Step 2: Delete all DML + DDL statements
    print("  -> Deleting Flink statements...")
    for stmt_name in DML_STATEMENTS + DDL_STATEMENTS:
        _delete_flink_statement(stmt_name, flink)

    # Step 3: Delete all pipeline Kafka topics
    print("  -> Deleting Kafka topics...")
    for topic in PIPELINE_TOPICS:
        _delete_kafka_topic(topic, kafka)

    # Step 4: Delete Schema Registry subjects (stale schemas cause column mismatches)
    print("  -> Deleting Schema Registry subjects...")
    _delete_schema_subjects(root)

    # Step 5: Drop Flink catalog tables/views to prevent auto-created raw-byte
    #         tables from blocking Terraform DDL recreation
    print("  -> Dropping Flink catalog tables...")
    _drop_flink_catalog_tables(flink)

    # Step 6: Wait for each topic deletion to propagate.
    # The fixed 5s sleep is insufficient for 6-partition topics under
    # GC pressure; without per-topic polling the recreate would NO-OP
    # (TopicExists swallowed) and stale partition data survives.
    print("  -> Waiting for topic deletions to propagate...")
    for topic in PIPELINE_TOPICS:
        if not _wait_for_kafka_topic_gone(topic, kafka, timeout=30):
            print(f"     [warn] Topic '{topic}' still exists after 30s; "
                  "recreate may keep stale data")

    # Step 7: Recreate all pipeline Kafka topics
    print("  -> Recreating Kafka topics...")
    for topic in PIPELINE_TOPICS:
        _create_kafka_topic(topic, kafka)

    # Step 8: Wait for topic metadata to propagate
    time.sleep(5)

    # Step 8b: — restart ASP processors that consume the
    # recreated topics. Without this they remain `STARTED` but stop
    # consuming because their committed offsets point at the old
    # topic generation.
    try:
        from scripts.common.asp_restart import restart_processors_for_topics
        from scripts.common.pipeline_logger import PipelineLogger
        from requests.auth import HTTPDigestAuth
        from dotenv import dotenv_values
        env = dotenv_values(root / ".env")
        atlas_pub = (env.get("ATLAS_PUBLIC_KEY") or "").strip()
        atlas_priv = (env.get("ATLAS_PRIVATE_KEY") or "").strip()
        atlas_proj = (env.get("ATLAS_PROJECT_ID") or "").strip()
        plog = PipelineLogger(name="pipeline-reset", root=root)
        if atlas_pub and atlas_priv and atlas_proj:
            print("  -> Restarting ASP processors (post-topic-recreate)...")
            with plog.step("reset", "asp_restart_after_topic_recreate",
                           topics=list(PIPELINE_TOPICS)):
                restart_processors_for_topics(
                    project_id=atlas_proj,
                    instance="asp-instance",
                    topics=PIPELINE_TOPICS,
                    auth=HTTPDigestAuth(atlas_pub, atlas_priv),
                    timeout_per_processor=60,
                )
        else:
            print("  -> Skipping ASP restart (no Atlas Admin keys in env)")
            plog.event("reset", "asp_restart_after_topic_recreate", "warn",
                       reason="no_atlas_keys")
        plog.close()
    except Exception as exc:
        # graceful degradation — never abort reset on ASP failure
        print(f"  [warn] ASP restart raised: {exc} (continuing)")

    # Step 9: Reset the dashboard's "Seed Next Batch" counter. The pipeline
    # is now empty, so the next click in the dashboard must publish from
    # batch 1 (gentle 3x surge), not whatever multiplier the previous run
    # left behind.
    from scripts.common.datagen_helpers import reset_batch_counter
    if reset_batch_counter(root):
        print("  -> Reset dashboard batch counter (.batch_counter)")

    # Note: Terraform DDL -replace is intentionally NOT run here. Once
    # ShadowTraffic starts producing data, Confluent Cloud will auto-register
    # raw-byte catalog tables that would overwrite any DDL created now.
    # Instead, restart_flink_dml() handles DROP + terraform -replace AFTER
    # schemas have been registered by ShadowTraffic data production.

    print("  [ok] Pipeline cleanup complete (topics + schemas + MongoDB cleared)")
    print("  DML statements will be recreated after data starts flowing.")
    return True


def _bootstrap_agent_statement(stmt_name: str, sql: str, flink: dict, max_wait: int = 60) -> bool:
    """DELETE + CREATE + poll-to-COMPLETED for an agent/tool statement.

    mirrors deploy.py:_bootstrap_agent_statement so
    `uv run datagen` produces the same dispatch agent chain as a deploy.
    The agent SQL is not in an SQL file — it comes from the dashboard
    single-source-of-truth (AGENT_SQL_CREATE_TOOL / AGENT_SQL_CREATE_AGENT),
    so this can't reuse _submit_flink_statement (which reads SQL files).

    Returns True on COMPLETED, False on FAILED / timeout / transport error.

    NOTE: this duplicates deploy.py's proven closure-based helper. The
    deploy version is on the critical path and battle-tested; consolidating
    both into scripts/common/flink_pipeline is tracked as a follow-up so we
    don't risk the deploy path here.
    """
    base_url = flink["base_url"]
    headers = flink["headers"]
    check_url = f"{base_url}/{stmt_name}"

    # 1. DELETE existing copy, poll for 404 (max 30s).
    try:
        del_req = urllib.request.Request(check_url, method="DELETE", headers=headers)
        urllib.request.urlopen(del_req, timeout=10)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            logger.warning(f"{stmt_name}: HTTP {e.code} (auth) on DELETE")
            return False
    except Exception:
        pass
    delete_deadline = time.time() + 30
    while time.time() < delete_deadline:
        try:
            probe = urllib.request.Request(check_url, method="GET", headers=headers)
            urllib.request.urlopen(probe, timeout=10)
            time.sleep(1)
            continue  # still exists
        except urllib.error.HTTPError as e:
            if e.code == 404:
                break
            if e.code in (401, 403):
                logger.warning(f"{stmt_name}: HTTP {e.code} (auth) on GET")
                return False
        except Exception:
            pass
        time.sleep(1)

    # 2. CREATE
    payload = json.dumps({
        "name": stmt_name,
        "spec": {
            "statement": sql,
            "properties": {
                "sql.current-catalog": flink["catalog"],
                "sql.current-database": flink["database"],
            },
            "compute_pool_id": flink["compute_pool_id"],
            "principal": flink["principal_id"],
        },
    }).encode()
    try:
        req = urllib.request.Request(base_url, data=payload, method="POST", headers=headers)
        urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()[:300] if e.fp else ""
        logger.warning(f"{stmt_name}: HTTP {e.code} {body_text}")
        return False
    except Exception as e:
        logger.warning(f"{stmt_name}: {e}")
        return False

    # 3. Poll phase to COMPLETED (terminal for DDL-like agent statements).
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            probe = urllib.request.Request(check_url, method="GET", headers=headers)
            with urllib.request.urlopen(probe, timeout=10) as resp:
                data = json.loads(resp.read())
                phase = data.get("status", {}).get("phase", "")
                if phase == "COMPLETED":
                    logger.info(f"  {stmt_name} reached COMPLETED")
                    return True
                if phase in ("FAILED", "DEGRADED"):
                    detail = data.get("status", {}).get("detail", "")
                    logger.warning(f"  {stmt_name} {phase}: {detail[:200]}")
                    return False
        except Exception:
            pass
        time.sleep(3)
    logger.warning(f"  {stmt_name} did not reach COMPLETED within {max_wait}s")
    return False


def _bootstrap_agents(flink: dict) -> bool:
    """Create create-tool-mongodb-fleet + create-agent-boat-dispatch.

    Returns True only if BOTH reached COMPLETED. dispatch-insert must NOT
    be submitted when this returns False.
    """
    try:
        from scripts.dashboard import AGENT_SQL_CREATE_TOOL, AGENT_SQL_CREATE_AGENT
    except ImportError as e:
        logger.warning(f"Could not import agent SQL from dashboard: {e}")
        return False

    steps = [
        ("create-tool-mongodb-fleet", AGENT_SQL_CREATE_TOOL),
        ("create-agent-boat-dispatch", AGENT_SQL_CREATE_AGENT),
    ]
    all_ok = True
    for stmt_name, sql in steps:
        if not _bootstrap_agent_statement(stmt_name, sql, flink):
            all_ok = False
    return all_ok


def _mcp_healthy_from_env(root: Path) -> bool:
    """Probe MCP health using credentials from .env (mirrors deploy gating)."""
    if not HAS_DOTENV:
        return False
    env_file = root / ".env"
    if not env_file.exists():
        return False
    env = {k: v for k, v in dotenv_values(env_file).items() if v}
    url = env.get("TF_VAR_mcp_server_url", "")
    token = env.get("TF_VAR_mcp_auth_token", "")
    if not (url and token):
        return False
    try:
        from scripts.common.flink_pipeline import check_mcp_health
        return check_mcp_health(url, token)
    except Exception:
        return False


def restart_flink_dml(root: Path) -> bool:
    """Recreate Flink DDL + DML statements after data has started flowing.

    Call this AFTER ShadowTraffic has been running for a few seconds so that
    ride_requests has data and its Avro schema is registered in Schema Registry.
    Without registered schemas, DML statements fail with column-not-found errors.

    This function also re-drops Flink catalog tables and re-runs terraform apply
    -replace for DDL, because ShadowTraffic data production may have triggered
    Confluent Cloud to auto-register raw-byte catalog entries that overwrite
    the DDL tables created during reset_pipeline().

    Args:
        root: Project root directory.

    Returns:
        True if statements were created successfully, False otherwise.
    """
    print("\n=== Recreating Flink Statements ===")

    outputs = _get_terraform_outputs(root)
    if outputs is None:
        print("  [warn] No terraform outputs — cannot recreate statements")
        return False

    flink = _get_flink_credentials(outputs)
    if flink is None:
        print("  [warn] Missing Flink credentials — cannot recreate statements")
        return False

    sql_dir = root / "terraform" / "agents" / "sql"

    # Delete any existing FAILED/stale DML + agent + DDL statements before
    # recreation. include AGENT_BOOTSTRAP_STATEMENTS so a stale
    # FAILED agent/tool from a prior run doesn't 409 the recreate.
    print("  -> Deleting existing Flink statements...")
    for stmt_name in DML_STATEMENTS + AGENT_BOOTSTRAP_STATEMENTS + DDL_STATEMENTS:
        _delete_flink_statement(stmt_name, flink)

    # Drop any auto-registered catalog entries created by data production
    print("  -> Dropping stale Flink catalog entries...")
    _drop_flink_catalog_tables(flink)

    # Force-recreate Terraform DDL (tables + views with proper schemas)
    print("  -> Recreating Terraform DDL statements...")
    _run_terraform_ddl_replace(root)

    # drop the CTAS catalog tables BEFORE
    # recreating the CTAS DDL. The CTAS uses CREATE TABLE IF NOT EXISTS,
    # which no-ops against an existing raw-byte phantom ([val: BYTES]) and
    # leaves anomalies-enriched-insert failing with a sink-schema
    # mismatch. Dropping first forces the CTAS to create the typed table.
    print("  -> Dropping CTAS catalog tables (anomalies_enriched, completed_actions)...")
    _drop_ctas_catalog_tables(flink)

    # Recreate REST API-managed DDL statements (anomalies-enriched-ctas)
    print("  -> Recreating Flink DDL statements...")
    for stmt_name in DDL_STATEMENTS:
        _submit_flink_statement(stmt_name, flink, sql_dir, is_ddl=True)

    # bootstrap the dispatch agent + tool BEFORE dispatch-insert.
    # Without this the dispatch-insert DML FAILS ("agent does not exist").
    print("  -> Bootstrapping dispatch agent + tool...")
    agent_ok = _bootstrap_agents(flink)
    if not agent_ok:
        print("  [warn] Agent/tool bootstrap did not complete — "
              "dispatch-insert will be skipped (other DML still created).")

    # Recreate DML statements EXCEPT dispatch-insert (gated below).
    print("  -> Recreating Flink DML statements...")
    non_dispatch_dml = [s for s in DML_STATEMENTS if s != _DISPATCH_STMT]
    for stmt_name in non_dispatch_dml:
        _submit_flink_statement(stmt_name, flink, sql_dir, is_ddl=False)

    # dispatch-insert only when the agent chain bootstrapped AND
    # MCP is healthy — mirrors deploy.py. Submitting it otherwise
    # guarantees a FAILED statement.
    dispatch_submitted = False
    if agent_ok and _mcp_healthy_from_env(root):
        print("  -> Submitting dispatch-insert (agent ready, MCP healthy)...")
        _submit_flink_statement(_DISPATCH_STMT, flink, sql_dir, is_ddl=False)
        dispatch_submitted = True
    else:
        reason = "agent bootstrap incomplete" if not agent_ok else "MCP unhealthy"
        print(f"  [SKIP] dispatch-insert not submitted — {reason}. "
              "The other DML statements were created. Fix the cause and "
              "re-run, or click 'Run Agent Dispatch' in the dashboard.")

    # Wait for DML to reach RUNNING. Only the statements we actually
    # submitted are expected to run.
    print("  -> Waiting for DML statements to reach RUNNING...")
    expected = list(non_dispatch_dml)
    if dispatch_submitted:
        expected.append(_DISPATCH_STMT)
    dml_ok = _wait_for_dml_running(flink, max_wait=120, statements=expected)

    # report honestly. Overall success requires DML running AND
    # the agent chain bootstrapped (so dispatch works).
    overall_ok = dml_ok and agent_ok
    if overall_ok:
        print("  [ok] Flink statements recreated — all RUNNING")
    else:
        print("  [warn] Flink statements recreated with FAILURES — "
              "run 'uv run health' for details")
    return overall_ok


def check_shadowtraffic_running() -> bool:
    """Check if a ShadowTraffic Docker container is currently running.

    Uses ``docker ps`` to detect running containers with the ShadowTraffic image.

    Returns:
        True if at least one ShadowTraffic container is running, False otherwise.
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"ancestor={SHADOWTRAFFIC_IMAGE}",
             "--format", "{{.ID}}"],
            capture_output=True, text=True, timeout=10,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False


def stop_shadowtraffic() -> bool:
    """Stop all running ShadowTraffic Docker containers.

    Returns:
        True if containers were stopped (or none were running), False on error.
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"ancestor={SHADOWTRAFFIC_IMAGE}",
             "--format", "{{.ID}}"],
            capture_output=True, text=True, timeout=10,
        )
        container_ids = result.stdout.strip().split()
        if not container_ids or container_ids == ['']:
            return True  # nothing to stop

        logger.info(f"Stopping {len(container_ids)} ShadowTraffic container(s)...")
        for cid in container_ids:
            subprocess.run(
                ["docker", "stop", cid],
                capture_output=True, timeout=30,
            )
        return True
    except Exception as e:
        logger.warning(f"Failed to stop ShadowTraffic: {e}")
        return False
