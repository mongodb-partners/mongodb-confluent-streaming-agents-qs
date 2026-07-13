#!/usr/bin/env python3
"""Destroy script — reads credentials from .env.

Visually and tonally distinct from deploy. Requires typing "destroy" to confirm.

By default, the Terraform-managed Atlas cluster (in terraform/atlas) is
PRESERVED across destroys so the next deploy avoids the ~7-15 min provision
wait. Pass --include-cluster to also tear down the cluster.
"""

import argparse
from scripts.common.http_auth import basic_auth_token
import os
import shutil
import sys
from pathlib import Path

from .common.terraform import get_project_root
from .common.terraform_runner import run_terraform_destroy


def _load_credentials(root: Path) -> dict:
    """Load all values from .env."""
    env_file = root / ".env"
    if not env_file.exists():
        return {}
    from dotenv import dotenv_values
    return {k: v for k, v in dotenv_values(env_file).items() if v}


def _cleanup(env_path: Path) -> None:
    try:
        for f in env_path.glob("*.tfstate*"):
            f.unlink()
        for f in env_path.glob("*.tfvars*"):
            f.unlink()
        td = env_path / ".terraform"
        if td.exists():
            shutil.rmtree(td)
        for name in (".terraform.lock.hcl", "FLINK_SQL_COMMANDS.md", "mcp_commands.txt"):
            p = env_path / name
            if p.exists():
                p.unlink()
    except Exception:
        pass


def _delete_flink_dml_statements(root: Path) -> None:
    """Delete the 5 Flink statements (1 DDL + 4 DML) managed outside Terraform.

    REST mechanics delegated to FlinkRestClient.
    statement names imported from canonical source.
    """
    import json
    import urllib.request
    import urllib.error

    from scripts.common.flink_statements import ALL_DELETABLE_STATEMENTS

    # Reverse-DDL order: delete leaves first, then bases (CTAS tables).
    DML_STATEMENTS = list(ALL_DELETABLE_STATEMENTS)

    # cached terraform output helper.
    from scripts.common.terraform_outputs import get_core_outputs
    outputs = get_core_outputs(root)
    if not outputs:
        return  # no core state / terraform error — nothing to delete

    flink_key = outputs.get("app_manager_flink_api_key", {}).get("value", "")
    flink_secret = outputs.get("app_manager_flink_api_secret", {}).get("value", "")
    org_id = outputs.get("confluent_organization_id", {}).get("value", "")
    env_id = outputs.get("confluent_environment_id", {}).get("value", "")
    flink_endpoint = outputs.get("confluent_flink_rest_endpoint", {}).get("value", "")

    if not all([flink_key, flink_secret, org_id, env_id, flink_endpoint]):
        return

    # build a FlinkRestClient for delete_and_wait. The PATCH-stop
    # step still uses raw urllib because the client doesn't yet expose stop
    # (intentional — most statements respond to plain DELETE; stop-then-delete
    # is only useful when a stuck DELETING phase needs nudging).
    from scripts.common.flink_rest import FlinkRestClient
    flink_client = FlinkRestClient(
        rest_endpoint=flink_endpoint,
        api_key=flink_key, api_secret=flink_secret,
        org_id=org_id, env_id=env_id,
        compute_pool_id=outputs.get("confluent_flink_compute_pool_id", {}).get("value", ""),
        service_account_id=outputs.get("app_manager_service_account_id", {}).get("value", ""),
        catalog="",   # not used by delete_and_wait
        database="",  # not used by delete_and_wait
    )

    cred_bytes = basic_auth_token(flink_key, flink_secret)
    headers = {
        "Authorization": f"Basic {cred_bytes}",
    }

    print("\n  -> Deleting Flink DML statements...")
    for stmt_name in DML_STATEMENTS:
        url = f"{flink_endpoint}/sql/v1/organizations/{org_id}/environments/{env_id}/statements/{stmt_name}"
        # First stop the statement, then delete (PATCH endpoint not yet on client)
        try:
            stop_body = json.dumps([{"op": "replace", "path": "/spec/stopped", "value": True}]).encode()
            stop_req = urllib.request.Request(
                url, data=stop_body, method="PATCH",
                headers={**headers, "Content-Type": "application/json"},
            )
            urllib.request.urlopen(stop_req, timeout=15)
        except Exception:
            pass  # may already be stopped or not exist

        # delete_and_wait via FlinkRestClient
        try:
            flink_client.delete_and_wait(stmt_name, timeout=15)
            print(f"     Deleted {stmt_name}")
        except Exception as e:
            print(f"     Warning: Could not delete {stmt_name}: {e}")


def _delete_kafka_topics(root: Path) -> None:
    """Delete all pipeline Kafka topics via the Kafka REST API v3.

    Stale topics with old schema data cause FAILED Flink statements on re-deploy.
    This function removes them so the next deployment starts clean.
    """
    import urllib.request
    import urllib.error

    # canonical source: every pipeline topic across all groups (full teardown).
    from scripts.common.pipeline_topics import ALL_PIPELINE_TOPICS
    TOPICS = list(ALL_PIPELINE_TOPICS)

    # cached terraform output helper.
    from scripts.common.terraform_outputs import get_core_outputs
    outputs = get_core_outputs(root)
    if not outputs:
        return

    rest_endpoint = outputs.get("confluent_kafka_cluster_rest_endpoint", {}).get("value", "")
    cluster_id = outputs.get("confluent_kafka_cluster_id", {}).get("value", "")
    kafka_api_key = outputs.get("app_manager_kafka_api_key", {}).get("value", "")
    kafka_api_secret = outputs.get("app_manager_kafka_api_secret", {}).get("value", "")

    if not all([rest_endpoint, cluster_id, kafka_api_key, kafka_api_secret]):
        print("  [warn] Missing Kafka REST credentials — skipping topic cleanup")
        return

    cred = basic_auth_token(kafka_api_key, kafka_api_secret)
    headers = {
        "Authorization": f"Basic {cred}",
    }

    print("\n  -> Deleting Kafka topics...")
    for topic in TOPICS:
        url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}"
        try:
            req = urllib.request.Request(url, method="DELETE", headers=headers)
            urllib.request.urlopen(req, timeout=15)
            print(f"     Deleted topic '{topic}'")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                pass  # already gone
            else:
                print(f"     Warning: Could not delete topic '{topic}': HTTP {e.code}")
        except Exception:
            pass


def _drop_legacy_atlas_search_indexes(creds: dict) -> None:
    """Drop Atlas Search indexes that were retired.

    On clusters that previously ran asp-setup, the indexes
    `anomaly_reason_search` (analytics.zone_anomalies) and
    `dispatch_summary_search` (fleet.dispatch_log) survive even though
    no caller uses them. They cost storage and write throughput. This
    helper removes them via the Atlas Admin API.
    """
    atlas_pub = creds.get("ATLAS_PUBLIC_KEY", "")
    atlas_priv = creds.get("ATLAS_PRIVATE_KEY", "")
    atlas_proj = creds.get("ATLAS_PROJECT_ID", "")
    cluster = creds.get("ATLAS_CLUSTER_NAME", "")
    if not all([atlas_pub, atlas_priv, atlas_proj, cluster]):
        return

    try:
        from scripts.asp_setup import AtlasAPI
    except ImportError:
        return

    api = AtlasAPI(atlas_pub, atlas_priv, atlas_proj)
    api_v = "application/vnd.atlas.2024-05-30+json"
    legacy = [
        ("analytics", "zone_anomalies", "anomaly_reason_search"),
        ("fleet", "dispatch_log", "dispatch_summary_search"),
    ]
    for db_name, coll_name, idx_name in legacy:
        try:
            list_resp = api.get(
                f"/clusters/{cluster}/search/indexes/{db_name}/{coll_name}",
                api_version=api_v,
            )
            if not list_resp.ok:
                continue
            for idx in list_resp.json():
                if idx.get("name") == idx_name:
                    idx_id = idx.get("indexID") or idx.get("_id")
                    if idx_id:
                        api.delete(
                            f"/clusters/{cluster}/search/indexes/{idx_id}",
                            api_version=api_v,
                        )
                        print(f"    ✓ Dropped legacy search index {db_name}.{coll_name}.{idx_name}")
        except Exception as e:
            print(f"    ⚠ Could not drop {idx_name}: {e}")


def _clear_mongodb_collections(creds: dict) -> None:
    """Clear all MongoDB sink collections so re-deploy starts with a clean slate."""
    try:
        # Availability probe for pymongo; the connection itself is built via
        # scripts.common.mongo.get_client below. Kept (noqa) as the guard.
        from pymongo import MongoClient  # noqa: F401
    except ImportError:
        print("  Warning: pymongo not available — skipping MongoDB cleanup")
        return

    uri = creds.get("TF_VAR_mongodb_connection_string", "")
    user = creds.get("TF_VAR_mongodb_username", "")
    pw = creds.get("TF_VAR_mongodb_password", "")
    if not uri:
        print("  Warning: No MongoDB URI found — skipping MongoDB cleanup")
        return

    from scripts.common.mongo import build_uri, get_client
    uri = build_uri(uri, user, pw)

    print("\n  -> Clearing MongoDB sink collections...")
    try:
        client = get_client(uri, app_name="streaming-agents-destroy")
        client.admin.command("ping")
    except Exception as e:
        print(f"  Warning: Could not connect to MongoDB: {e}")
        return

    # canonical source: all pipeline collections (full teardown).
    from scripts.common.pipeline_topics import ALL_MONGODB_COLLECTIONS
    collections = [tuple(c) for c in ALL_MONGODB_COLLECTIONS]
    try:
        for db_name, coll_name in collections:
            result = client[db_name][coll_name].delete_many({})
            print(f"    Cleared {db_name}.{coll_name} ({result.deleted_count} documents)")
    except Exception as e:
        print(f"  Warning: Error clearing MongoDB collections: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Agents teardown script")
    parser.add_argument(
        "--include-cluster", action="store_true",
        help="Also destroy the Terraform-managed Atlas cluster "
             "(default: preserve cluster across redeploys)",
    )
    parser.add_argument(
        "--no-log", action="store_true",
        help="Disable logging CLI output to logs/destroy-<timestamp>.log",
    )
    args = parser.parse_args()

    # bootstrap session logging AFTER arg parse, gated
    # on --no-log. Previously bootstrap ran first and ignored args.
    if not args.no_log:
        from scripts.common.cli_logging import bootstrap_logging
        bootstrap_logging("destroy")

    w = 54
    title = "  Streaming Agents  ·  Teardown Script"
    print("+" + "=" * w + "+")
    print(f"|{title:<{w}}|")
    print("+" + "=" * w + "+")
    print()
    print("  This will remove all provisioned resources.")
    print("  This action cannot be undone.")
    if args.include_cluster:
        print()
        print("  --include-cluster: Atlas cluster WILL be destroyed.")
    else:
        print()
        print("  (Atlas cluster will be PRESERVED. Use --include-cluster to destroy it.)")
    print()

    root = get_project_root()
    creds = _load_credentials(root)

    region = creds.get("TF_VAR_cloud_region", "us-east-1")

    # Load all TF_VAR_* from .env into environment
    for k, v in creds.items():
        if k.startswith("TF_VAR_") and v:
            os.environ[k] = v

    os.environ["TF_VAR_cloud_region"] = region

    # Confirmation — must type the word "destroy"
    print()
    print("  " + "-" * 51)
    print("  WARNING: This will tear down all provisioned resources.")
    print()
    try:
        print('  Type "destroy" to confirm, or CTRL+C to cancel: ', end="", flush=True)
        confirm = input().strip()
    except KeyboardInterrupt:
        print("\n\n  Destroy cancelled.")
        sys.exit(0)

    if confirm != "destroy":
        print("\n  Destroy cancelled.")
        sys.exit(0)

    # Pre-destroy: Delete MCP Server (ECS Express Mode)
    print("\n  -> Deleting MCP Server (ECS Express)...")
    try:
        from scripts.mcp_deploy import destroy_mcp_server
        destroy_mcp_server(region)
    except ImportError:
        print("  Warning: Could not import mcp_deploy module")
    except Exception as e:
        print(f"  Warning: MCP teardown error: {e}")

    # Pre-destroy: Delete DML Flink statements (not managed by Terraform)
    _delete_flink_dml_statements(root)

    # Pre-destroy: Delete Kafka topics (stale data causes failures on re-deploy)
    _delete_kafka_topics(root)

    # Delete Schema Registry subjects too. Otherwise stale
    # -value/-key subjects from this deploy survive destroy and the next
    # deploy (with a different schema) inherits them — Flink then
    # reconstructs tables with a phantom `key VARBINARY` column.
    try:
        from scripts.pipeline_reset import _delete_schema_subjects
        print("\n  -> Deleting Schema Registry subjects...")
        _delete_schema_subjects(root)
    except Exception as e:
        print(f"  [warn] Could not clean Schema Registry subjects: {e}")

    # Pre-destroy: ASP Teardown
    agents_path = root / "terraform" / "agents"
    if agents_path.exists() and (agents_path / "terraform.tfstate").exists():
        atlas_pub = creds.get("ATLAS_PUBLIC_KEY", "")
        atlas_priv = creds.get("ATLAS_PRIVATE_KEY", "")
        atlas_proj = creds.get("ATLAS_PROJECT_ID", "")
        if atlas_pub and atlas_priv and atlas_proj:
            print("\n  -> Tearing down ASP resources...")
            try:
                from scripts.asp_setup import run_asp_teardown
                run_asp_teardown(atlas_pub, atlas_priv, atlas_proj)
            except ImportError:
                print("  Warning: Could not import asp_setup -- ASP resources may remain")
            except Exception as e:
                print(f"  Warning: ASP teardown error: {e} -- ASP resources may remain")
        else:
            print("\n  Warning: Atlas Admin API keys not in .env -- skipping ASP teardown")
            print("    To clean up manually: delete ASP instance in Atlas UI")

    # Pre-destroy: Drop legacy Atlas Search indexes ( cleanup)
    print("\n  -> Dropping retired Atlas Search indexes (if any)...")
    _drop_legacy_atlas_search_indexes(creds)

    # Pre-destroy: Clear MongoDB sink collections
    _clear_mongodb_collections(creds)

    # Destroy environments in reverse order (agents first, then core).
    # Atlas module is preserved by default; --include-cluster opts in.
    envs = ["agents", "core"]
    if args.include_cluster:
        envs.append("atlas")

    print("\n=== Starting Destroy ===")
    destroy_failed = False # track per-env success
    for env in envs:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"  Skipping {env}: directory does not exist")
            continue
        if not (env_path / "terraform.tfstate").exists():
            print(f"  Skipping {env}: no terraform state found (never deployed)")
            continue
        print(f"\n  -> Destroying {env}...")
        if run_terraform_destroy(env_path):
            _cleanup(env_path)
        else:
            print(f"  Failed at {env}. Continuing with remaining environments...")
            destroy_failed = True

    # only clear .env credentials when ALL environments
    # destroyed cleanly. Otherwise the user is left with half-destroyed cloud
    # infra AND wiped credentials, blocking re-destroy / triage.
    if not destroy_failed:
        # Remove infra-generated values from .env (stale after destroy).
        # When --include-cluster, the deploy-managed mongo creds are also stale.
        _remove_stale_credentials(root, include_cluster=args.include_cluster)
    else:
        print()
        print("  [warn] One or more environments failed to destroy.")
        print("         Preserving .env credentials so you can re-run destroy.")
        print("         Fix the underlying error (check terraform output above)")
        print("         then run `uv run destroy` again.")

    # Reset the dashboard's "Seed Next Batch" counter so a fresh deploy
    # starts at batch 1 instead of resuming wherever the previous run left
    # off (which would skip the gentle warm-up surges).
    from scripts.common.datagen_helpers import reset_batch_counter
    if reset_batch_counter(root):
        print("  Reset dashboard batch counter (.batch_counter)")

    print("\n  Destroy process completed!")


def _remove_stale_credentials(root: Path, include_cluster: bool = False) -> None:
    """Remove infrastructure-generated values from .env.

    These are written by deploy after terraform and point to resources
    that no longer exist after destroy.

    When include_cluster=True AND TF_VAR_create_atlas_cluster=true was set,
    the deploy-generated MongoDB connection string + username + password
    are also cleared (the cluster's gone). When include_cluster=False, the
    mongo creds are PRESERVED — the cluster still exists in
    terraform/atlas state and the next deploy will reuse it. User-provided
    BYO credentials (when create_atlas_cluster=false) are always preserved.
    """
    env_file = root / ".env"
    if not env_file.exists():
        return

    from dotenv import dotenv_values
    current = dotenv_values(env_file)
    cluster_was_terraform_managed = (
        (current.get("TF_VAR_create_atlas_cluster") or "").lower() == "true"
    )

    STALE_KEYS = {
        "CONFLUENT_BOOTSTRAP_SERVER",
        "CONFLUENT_KAFKA_API_KEY",
        "CONFLUENT_KAFKA_API_SECRET",
        "CONFLUENT_KAFKA_REST_ENDPOINT",
        "CONFLUENT_KAFKA_CLUSTER_ID",
        "CONFLUENT_SCHEMA_REGISTRY_URL",
        "CONFLUENT_SCHEMA_REGISTRY_API_KEY",
        "CONFLUENT_SCHEMA_REGISTRY_API_SECRET",
        "TF_VAR_mcp_server_url",
        "TF_VAR_mcp_auth_token",
        # Deploy state breadcrumbs: clear all of these so a
        # subsequent `uv run deploy` starts fresh after `uv run destroy`.
        "DEPLOY_PHASE",
        "DEPLOY_LAST_INTERRUPTED_PHASE",
        "DEPLOY_LAST_FAILURE",
        "DEPLOY_LAST_FAILED_PHASE",
    }

    if include_cluster and cluster_was_terraform_managed:
        STALE_KEYS |= {
            "TF_VAR_mongodb_connection_string",
            "TF_VAR_mongodb_username",
            "TF_VAR_mongodb_password",
            "TF_VAR_atlas_db_username",
            "TF_VAR_atlas_db_password",
            "TF_VAR_create_atlas_cluster",
        }

    lines = env_file.read_text().splitlines()
    kept = []
    removed = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            kept.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in STALE_KEYS:
            removed.append(key)
        else:
            kept.append(line)

    if removed:
        env_file.write_text("\n".join(kept) + "\n")
        print(f"\n  -> Cleaned {len(removed)} stale keys from .env")


if __name__ == "__main__":
    main()
