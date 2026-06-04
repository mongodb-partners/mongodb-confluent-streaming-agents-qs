"""
Terraform execution wrapper utilities.

Provides functions for:
- Running terraform init and apply
- Running terraform destroy
- Handling terraform errors and output
"""

import json
import subprocess
import sys
import time
from pathlib import Path

from .generate_deployment_summary import generate_credentials_markdown


# Confluent control-plane propagation lag patterns. When a service-account
# role binding (EnvironmentAdmin) is created, Schema Registry / Kafka ACL
# permissions can take 30-120s to propagate. The first agents apply hits
# the SR cluster from Flink runtime (or the Kafka broker from Flink's
# Kafka client) and gets a permission-denied error. Retrying after a
# short sleep almost always succeeds.
#
# this pattern set is intentionally NARROW. Earlier
# revisions included `"is not authorized"` and `"Authorization failed"`,
# which also match permanent RBAC misconfigurations — retrying for
# 4 min 15 s before surfacing those wastes the operator's time and
# masks the real error. The SR-specific phrasing below is unique to
# the propagation-lag race.
#
# added Kafka ACL propagation patterns. Same root
# cause (newly-created service-account role binding + ACL propagation
# lag); same fix (retry after sleep). The patterns below are unique to
# Kafka ACL lag and DO NOT match permanent RBAC misconfig (which
# surfaces as `403 Forbidden` from the Confluent control plane, not a
# `TopicAuthorizationException` from the broker).
_PROPAGATION_ERROR_PATTERNS = (
    # Schema Registry
    "Permission denied to access the Schema Registry",
    "permission denied to access the schema registry",
    # Kafka ACL lag: Kafka brokers raise these
    # exception names verbatim. They appear as
    # `org.apache.kafka.common.errors.TopicAuthorizationException`
    # and friends in Flink / terraform plan output.
    "TopicAuthorizationException",
    "ClusterAuthorizationException",
    "GroupAuthorizationException",
)
_PROPAGATION_MAX_ATTEMPTS = 3
_PROPAGATION_BACKOFF_S = (45, 90, 120)


def _looks_like_propagation_lag(stderr: str, stdout: str = "") -> bool:
    """Detect transient permission errors caused by control-plane propagation."""
    blob = (stderr + "\n" + stdout).lower()
    return any(p.lower() in blob for p in _PROPAGATION_ERROR_PATTERNS)


def _run_terraform_apply(
    env_path: Path,
    auto_approve: bool,
    replace_resources: list | None,
) -> tuple[int, str, str]:
    """Execute terraform apply with live streaming + reliable error capture.

    two daemon threads tee stdout / stderr to both the
    chunk lists (for error-pattern matching after the process exits) AND
    sys.stdout / sys.stderr (so the operator sees progress in real time).
    The previous implementation used `proc.communicate()` which buffered
    everything until the process exited; on 7–15 minute Atlas applies the
    deploy appeared hung. Two daemon threads avoid the
    fill-the-OS-pipe-buffer deadlock the original comment cited.

    Returns (returncode, stdout, stderr).
    """
    import threading

    apply_cmd = ["terraform", "apply"]
    if auto_approve:
        apply_cmd.append("-auto-approve")
    if replace_resources:
        for res in replace_resources:
            apply_cmd.extend(["-replace", res])

    proc = subprocess.Popen(
        apply_cmd,
        cwd=env_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def _tee(src, sink_stream, chunks):
        try:
            for line in iter(src.readline, ""):
                if not line:
                    break
                try:
                    sink_stream.write(line)
                    sink_stream.flush()
                except Exception:
                    pass
                chunks.append(line)
        finally:
            try:
                src.close()
            except Exception:
                pass

    t_out = threading.Thread(
        target=_tee, args=(proc.stdout, sys.stdout, stdout_chunks),
        daemon=True,
    )
    t_err = threading.Thread(
        target=_tee, args=(proc.stderr, sys.stderr, stderr_chunks),
        daemon=True,
    )
    t_out.start()
    t_err.start()
    proc.wait()
    # Allow tee threads a brief window to drain any final lines.
    t_out.join(timeout=5)
    t_err.join(timeout=5)
    return proc.returncode, "".join(stdout_chunks), "".join(stderr_chunks)


def run_terraform(
    env_path: Path,
    auto_approve: bool = True,
    replace_resources: list | None = None,
    pre_retry_hook=None,
) -> bool:
    """
    Run terraform init and apply in the specified environment.

    Args:
        env_path: Path to terraform directory
        auto_approve: Whether to auto-approve terraform apply (default: True)
        replace_resources: Optional list of resource addresses to force-replace
        pre_retry_hook: Optional callable invoked between propagation-lag
            retries. Receives the attempt number (1-indexed). Useful for
            sweeping server-side orphans that a partial apply left behind.

    Returns:
        True if successful, False otherwise

    Raises:
        SystemExit: If terraform binary is not found

    Retry behavior:
        - Auto-retries up to 3 attempts (45s/90s/120s backoff) when stderr
          matches Confluent control-plane propagation patterns. This handles
          the "Permission denied to access the Schema Registry" failure
          that hits agents apply when Flink tries to register an Avro schema
          before the EnvironmentAdmin role-binding has propagated.
        - If `replace_resources` is set and the apply fails for any reason,
          falls back to a non-replace apply.
    """
    print(f"\nInitializing Terraform in {env_path}...")

    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)
    except subprocess.CalledProcessError:
        print(f"✗ Terraform init failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)

    print(f"Running terraform apply in {env_path}...")
    for attempt in range(1, _PROPAGATION_MAX_ATTEMPTS + 1):
        rc, out, err = _run_terraform_apply(env_path, auto_approve, replace_resources)
        if rc == 0:
            print(f"✓ Deployment successful: {env_path.name}")
            if env_path.name == "core":
                _generate_deployment_summary(env_path)
            return True

        if not _looks_like_propagation_lag(err, out):
            break  # not a propagation issue — fall through to fallback / fail

        if attempt < _PROPAGATION_MAX_ATTEMPTS:
            wait = _PROPAGATION_BACKOFF_S[attempt - 1]
            print(
                "\n  [retry] Detected Confluent control-plane propagation lag "
                "(SR/ACL permissions not yet propagated)."
            )
            print(f"  [retry] Sleeping {wait}s, then re-applying (attempt {attempt + 1}/{_PROPAGATION_MAX_ATTEMPTS})...")
            time.sleep(wait)
            if pre_retry_hook is not None:
                try:
                    pre_retry_hook(attempt + 1)
                except Exception as exc:
                    print(f"  [warn] pre_retry_hook raised: {exc}")
        else:
            print(f"\n  [warn] Propagation lag persisted across {_PROPAGATION_MAX_ATTEMPTS} attempts.")

    # Fallback: if replace_resources was set, retry once without -replace.
    # Some replace targets refer to resources that don't exist on a fresh apply.
    if replace_resources:
        print("  Retrying without -replace (resources may not exist yet)...")
        rc, _out, _err = _run_terraform_apply(env_path, auto_approve, replace_resources=None)
        if rc == 0:
            print(f"✓ Deployment successful: {env_path.name}")
            if env_path.name == "core":
                _generate_deployment_summary(env_path)
            return True

    print(f"✗ Terraform failed in {env_path.name}")
    return False


def run_terraform_destroy(env_path: Path, auto_approve: bool = True) -> bool:
    """
    Run terraform destroy in the specified environment.

    Args:
        env_path: Path to terraform directory
        auto_approve: Whether to auto-approve terraform destroy (default: True)

    Returns:
        True if successful, False otherwise

    Raises:
        SystemExit: If terraform binary is not found
    """
    print(f"\nInitializing Terraform in {env_path}...")

    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        destroy_cmd = ["terraform", "destroy"]
        if auto_approve:
            destroy_cmd.append("-auto-approve")

        print(f"Running terraform destroy in {env_path}...")
        subprocess.run(destroy_cmd, cwd=env_path, check=True)

        print(f"✓ Destroy successful: {env_path.name}")

        # Clean up deployment summary for Core deployments
        if env_path.name == "core":
            _cleanup_deployment_summary(env_path)

        return True

    except subprocess.CalledProcessError:
        print(f"✗ Terraform destroy failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)


def _generate_deployment_summary(env_path: Path) -> None:
    """
    Generate DEPLOYED_RESOURCES.md file after successful Core deployment.

    Args:
        env_path: Path to the terraform core directory (e.g., terraform/core)
    """
    try:
        # Get terraform outputs as JSON
        print("\nGenerating deployment summary...")
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=env_path,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse terraform outputs
        tf_outputs = json.loads(result.stdout)

        # Generate markdown file
        output_file = env_path / "DEPLOYED_RESOURCES.md"
        generate_credentials_markdown(tf_outputs, output_file)

    except Exception as e:
        print(f"Warning: Failed to generate deployment summary: {e}")
        # Don't fail the deployment if summary generation fails


def _cleanup_deployment_summary(env_path: Path) -> None:
    """
    Delete DEPLOYED_RESOURCES.md file after successful Core destroy.

    Args:
        env_path: Path to the terraform core directory (e.g., terraform/core)
    """
    try:
        output_file = env_path / "DEPLOYED_RESOURCES.md"
        if output_file.exists():
            output_file.unlink()
            print(f"Removed {output_file}")
    except Exception as e:
        print(f"Warning: Failed to remove deployment summary: {e}")
