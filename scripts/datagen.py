#!/usr/bin/env python3
"""
Anomaly Detection Data Generation Script

Generates streaming data using ShadowTraffic and Docker.
Creates ride_requests topic on Confluent Cloud.

Usage:
    uv run datagen                      # Run ShadowTraffic in Docker
    uv run datagen --local              # Use pre-generated data (no Docker)
    uv run datagen --dry-run            # Validate setup without running
    uv run datagen --duration 300       # Run for 5 minutes
"""

import argparse
import logging
import subprocess
import sys
import threading
from pathlib import Path

from .common.terraform import extract_kafka_credentials, validate_terraform_state, get_project_root
from .common.datagen_helpers import (
    check_dependencies,
    validate_dependencies,
    generate_all_connections,
    check_shadowtraffic_config,
    run_shadowtraffic_docker
)
from .common.logging_utils import setup_logging
from .pipeline_reset import reset_pipeline, restart_flink_dml, stop_shadowtraffic


# Configuration
CONNECTION_NAMES = ["ride-requests"]
REQUIRED_GENERATORS = ["base-rides.json", "steady-state-rides.json", "surge-rides.json"]
AGENTS_DIR_NAME = "agents"


def _schedule_flink_restart(project_root: Path, delay_seconds: int = 30) -> None:
    """Schedule Flink DML statement recreation in a background daemon thread.

    After pipeline reset deletes Flink statements + Kafka topics, ShadowTraffic
    needs to produce data for a few seconds so that Avro schemas are registered
    in Schema Registry. Only then can Flink DDL/DML statements be recreated
    without column-not-found errors.

    The restart_flink_dml() function also re-drops catalog tables and re-runs
    terraform apply -replace for DDL, since ShadowTraffic data may trigger
    auto-registration of raw-byte catalog entries.

    Args:
        project_root: Project root directory.
        delay_seconds: Seconds to wait for ShadowTraffic to produce data.
    """
    logger = logging.getLogger(__name__)

    def _delayed_restart():
        import time
        logger.info(f"Flink DML restart scheduled in {delay_seconds}s (waiting for schemas)...")
        time.sleep(delay_seconds)
        logger.info("Recreating Flink DDL + DML statements...")
        # This runs on a background daemon thread while ShadowTraffic blocks
        # the foreground, so we cannot propagate the result to the exit code.
        # Surface it via the logger so a failed restart is not silent (the
        # synchronous --local path checks the return value directly instead).
        try:
            ok = restart_flink_dml(project_root)
        except Exception as exc:
            logger.error(f"Flink DML restart raised an exception: {exc}")
            return
        if ok:
            logger.info("Flink DML restart completed successfully.")
        else:
            logger.error(
                "Flink DML restart FAILED — one or more statements did not "
                "reach RUNNING or the dispatch agent did not bootstrap. The "
                "pipeline may be incomplete. Check Flink statement status and "
                "re-run `uv run datagen`, or `uv run deploy --from-phase flink_dml`."
            )

    thread = threading.Thread(target=_delayed_restart, daemon=True)
    thread.start()


def run_datagen(
    duration: int = None,
    messages_per_minute: int = None,
    dry_run: bool = False,
    verbose: bool = False
) -> int:
    """
    Run data generation workflow.

    Args:
        duration: Duration to run in seconds
        messages_per_minute: Ride requests per minute to generate
        dry_run: If True, validate setup but don't run
        verbose: If True, show detailed output

    Returns:
        Exit code (0 for success)
    """
    logger = logging.getLogger(__name__)

    try:
        # Get project root and build paths
        project_root = get_project_root()
        agents_dir = project_root / "terraform" / AGENTS_DIR_NAME
        datagen_dir = agents_dir / "data-gen"

        if not datagen_dir.exists():
            logger.error(f"Data-gen directory not found: {datagen_dir}")
            return 1

        connections_dir = datagen_dir / "connections"
        generators_dir = datagen_dir / "generators"
        zones_dir = datagen_dir / "zones"
        functions_dir = datagen_dir / "functions"
        root_config = datagen_dir / "root.json"

        # Check dependencies
        logger.info("Checking dependencies...")
        deps = check_dependencies()
        if not validate_dependencies(deps):
            return 1

        # Extract credentials from terraform
        logger.info("Extracting AWS credentials...")
        credentials = extract_kafka_credentials(project_root)

        # Generate connection files
        generate_all_connections(credentials, connections_dir, ["ride-requests", "vessel-telemetry"])

        # Check ShadowTraffic configuration
        if not check_shadowtraffic_config(generators_dir, REQUIRED_GENERATORS):
            return 1

        # Pipeline reset: stop any existing ShadowTraffic and reset Flink/Kafka
        # so the new run starts with clean watermarks and no stale data.
        if not dry_run:
            logger.info("Stopping any running ShadowTraffic containers...")
            stop_shadowtraffic()
            logger.info("Resetting streaming pipeline (Flink statements + Kafka topics)...")
            reset_pipeline(project_root)

        # Start ShadowTraffic (blocks until Docker exits).
        # When not dry_run, schedule Flink DML restart in a background thread
        # so that DML statements are recreated ~30s after ShadowTraffic begins
        # producing data (schemas must be registered before DML can start).
        # restart_flink_dml() also re-drops catalog tables and re-runs terraform
        # apply -replace, since ShadowTraffic data triggers auto-registration.
        if not dry_run:
            _schedule_flink_restart(project_root, delay_seconds=30)

        # Run ShadowTraffic with volumes (includes zones and functions)
        return run_shadowtraffic_docker(
            datagen_dir=datagen_dir,
            connections_dir=connections_dir,
            generators_dir=generators_dir,
            root_config=root_config,
            zones_dir=zones_dir,
            functions_dir=functions_dir,
            duration=duration,
            messages_per_minute=messages_per_minute,
            dry_run=dry_run
        )

    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        if verbose:
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
        return 1


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        prog="datagen",
        description=(
            "Reset the streaming pipeline and publish ride data. "
            "Default: uses pre-generated assets/data/ride_requests.jsonl "
            "(no Docker). Pass --shadowtraffic for the legacy live-streaming "
            "Docker path."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run datagen                      # Default: pipeline reset + publish 23k pre-gen records (no Docker)
  uv run datagen --dry-run            # Validate setup only (no real ops)
  uv run datagen --shadowtraffic      # Legacy: run ShadowTraffic in Docker (continuous live streaming)
  uv run datagen --shadowtraffic --duration 300   # Run ShadowTraffic for 5 minutes
  uv run datagen --shadowtraffic -m 20            # Generate 20 ride requests per minute

Dependencies (default, no Docker):
  - Confluent CLI: https://docs.confluent.io/confluent-cli/current/install.html
  - Terraform (for pipeline_reset's terraform apply -replace step)

Dependencies (--shadowtraffic mode, legacy):
  - Docker: https://docs.docker.com/get-docker/
  - Terraform: https://developer.hashicorp.com/terraform/install
  - Confluent CLI: https://docs.confluent.io/confluent-cli/current/install.html

Migration note:
  Previously, `uv run datagen` defaulted to ShadowTraffic Docker
  and `--local` was the opt-in flag. The default is now inverted because
  (a) the workshop only needs the pre-generated 24h dataset to demo the
  pipeline, (b) Docker is unavailable on many corporate-managed laptops,
  and (c) --local mode now runs the full 3-phase pipeline reset. --local is retained as a no-op alias for backwards compat.
        """.strip()
    )

    parser.add_argument(
        "--duration",
        type=int,
        help="(--shadowtraffic only) Duration to run ShadowTraffic in seconds"
    )

    parser.add_argument(
        "--messages-per-minute", "-m",
        type=int,
        help="(--shadowtraffic only) Ride requests per minute to generate"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate setup without running real reset/publish/restart operations"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output and debug information"
    )

    parser.add_argument(
        "--shadowtraffic",
        action="store_true",
        help=(
            "(LEGACY) Use ShadowTraffic Docker container for continuous live "
            "data generation. Requires Docker — not available on many "
            "corporate-managed laptops. The default (no flag) is to publish "
            "the pre-generated assets/data/ride_requests.jsonl and is the "
            "recommended path for workshops."
        ),
    )

    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "DEPRECATED: --local is now the default behavior. Flag kept as a "
            "no-op for backwards compatibility — will be removed in a future "
            "release."
        ),
    )

    return parser


def main() -> None:
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    logger.info("Anomaly Detection - Data Generation")

    # Surface the deprecation: --local is now a no-op (it's the default).
    if args.local:
        logger.warning(
            "--local is deprecated and now a no-op (it's the default). "
            "Just run `uv run datagen` without the flag."
        )

    # Default path (no --shadowtraffic): pre-generated data + full pipeline reset.
    # Pass --shadowtraffic to opt into the legacy Docker path below.
    use_local = not args.shadowtraffic
    #
    # --local runs the SAME 3-phase sequence as the ShadowTraffic path, just
    # with publish_data substituting for the ShadowTraffic container:
    #   Phase 1: reset_pipeline (drops FAILED Flink statements, deletes
    #            topics, drops auto-registered raw-byte Flink catalog
    #            tables, recreates topics, restarts ASP processors)
    #   Phase 2: publish_data (the synchronous ~30s of publishing 23,289
    #            pre-generated records registers Avro schemas and triggers
    #            auto-registration that Phase 3 will clean up)
    #   Phase 3: restart_flink_dml (drops auto-registered tables again,
    #            runs `terraform apply -replace` on agents to force
    #            proper-schema DDL, recreates the 5 DML statements)
    #
    # Without Phases 1 and 3, --local is just a data publisher — useless
    # for any recovery scenario where the Flink catalog / DML statements
    # are in a broken state.
    if use_local:
        logger.info("Using local pre-generated data (no ShadowTraffic/Docker required)")
        try:
            project_root = get_project_root()
            data_file = project_root / "assets" / "data" / "ride_requests.jsonl"

            if not data_file.exists():
                logger.error(f"Data file not found: {data_file}")
                sys.exit(1)

            if not args.dry_run:
                # Phase 1: stop any stray ShadowTraffic containers (idempotent),
                # then reset the pipeline (Flink statements, Kafka topics, etc.)
                logger.info("Stopping any running ShadowTraffic containers...")
                stop_shadowtraffic()
                logger.info("Phase 1: Resetting streaming pipeline (Flink statements + Kafka topics)...")
                if not reset_pipeline(project_root):
                    logger.error("Pipeline reset failed — aborting --local run")
                    sys.exit(1)

            # Phase 2: publish pre-generated data via publish_data.
            # --force bypasses the idempotency guard since reset_pipeline
            # just emptied the topic (the guard's offset-fetch path may be
            # flaky for newly-recreated topics).
            logger.info("Phase 2: Publishing pre-generated ride data...")
            cmd = ["uv", "run", "publish_data", "--data-file", str(data_file), "--force"]
            if args.verbose:
                cmd.append("--verbose")
            if args.dry_run:
                cmd.append("--dry-run")

            result = subprocess.run(cmd, cwd=project_root)
            if result.returncode != 0:
                logger.error(f"publish_data failed with exit code {result.returncode}")
                sys.exit(result.returncode)

            if not args.dry_run:
                # Phase 3: recreate Flink DDL + DML. publish_data publishes
                # synchronously over ~30s, so by the time we get here Schema
                # Registry has the Avro schema registered — restart_flink_dml
                # can safely drop the auto-registered raw-byte tables and
                # force-recreate via terraform apply -replace.
                logger.info("Phase 3: Recreating Flink DDL + DML statements...")
                if not restart_flink_dml(project_root):
                    logger.error("Flink DML restart failed")
                    sys.exit(1)

            logger.info("--local: pipeline reset + data publication complete.")
            sys.exit(0)

        except Exception as e:
            logger.error(f"Failed to publish local data: {e}")
            sys.exit(1)

    try:
        # Get project root
        project_root = get_project_root()
        logger.debug(f"Project root: {project_root}")

        # Validate terraform state
        if not validate_terraform_state(project_root):
            logger.error("Terraform state validation failed")
            logger.error("Please run 'terraform apply' in terraform/core/ and terraform/agents/")
            sys.exit(1)

        # Run data generation
        exit_code = run_datagen(
            duration=args.duration,
            messages_per_minute=args.messages_per_minute,
            dry_run=args.dry_run,
            verbose=args.verbose
        )

        if args.dry_run:
            logger.info("Dry run completed")
        else:
            logger.info(f"Data generation completed with exit code {exit_code}")

        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        if args.verbose:
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
