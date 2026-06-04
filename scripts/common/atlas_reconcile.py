"""Atlas reconciliation helpers — extracted from scripts/deploy.py.

 / Phase C-3 (extraction). These four helpers manage two
orthogonal reconciliation scenarios:

1. **Orphan project-scoped Atlas DB user.** Atlas DB users are
   project-scoped, not cluster-scoped. If a previous deploy created
   the user but its terraform state is gone (fresh state, manual
   cleanup, partial deploy), the next `terraform apply` fails with
   HTTP 409 USER_ALREADY_EXISTS. The reconcile function GETs the
   user; if found and not in terraform state, it imports — falling
   back to delete on import failure.

2. **Stale `terraform/agents/terraform.tfstate`.** The agents module
   reads core's environment_id / compute_pool_id via
   `data.terraform_remote_state`, but its OWN tfstate caches
   `confluent_flink_statement` resource IDs that embed the old
   environment_id (e.g. `env-g3q85r/lfcp-...`). If core was destroyed
   and re-created, the new env-id won't match those cached IDs, and
   terraform refresh hits HTTP 401. The quarantine function moves the
   stale state aside so the next apply starts fresh.

Originally lived in deploy.py as `_atlas_db_user_exists`,
`_delete_atlas_db_user`, `_reconcile_orphan_atlas_db_user`, and
`_quarantine_stale_agents_state`. deploy.py keeps thin shim aliases
for backward compatibility.
"""
from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


ATLAS_API_BASE = "https://cloud.mongodb.com/api/atlas/v2"
ATLAS_API_VERSION = "application/vnd.atlas.2023-02-01+json"


def db_user_exists(
    public_key: str,
    private_key: str,
    project_id: str,
    username: str,
) -> Optional[bool]:
    """Check whether a project-scoped Atlas DB user already exists.

    Returns True/False, or None if the lookup itself failed (network
    error, bad creds, anything other than 200/404) — caller should
    treat None as "unknown, skip reconcile" and let terraform
    surface the real error.
    """
    try:
        import requests
        from requests.auth import HTTPDigestAuth
    except ImportError:
        return None

    url = f"{ATLAS_API_BASE}/groups/{project_id}/databaseUsers/admin/{username}"
    headers = {"Accept": ATLAS_API_VERSION}
    try:
        resp = requests.get(
            url, auth=HTTPDigestAuth(public_key, private_key),
            headers=headers, timeout=15,
        )
    except Exception as e:
        print(f"  [warn] Could not check Atlas DB user existence: {e}")
        return None

    if resp.status_code == 200:
        return True
    if resp.status_code == 404:
        return False
    # 401/403 → bad creds; anything else → API hiccup. Treat as unknown.
    print(
        f"  [warn] Atlas DB user GET returned HTTP {resp.status_code}; "
        "skipping orphan reconcile."
    )
    return None


def delete_db_user(
    public_key: str,
    private_key: str,
    project_id: str,
    username: str,
) -> bool:
    """Delete the project-scoped Atlas DB user. Returns True on
    success or 404 (already gone)."""
    try:
        import requests
        from requests.auth import HTTPDigestAuth
    except ImportError:
        return False

    url = f"{ATLAS_API_BASE}/groups/{project_id}/databaseUsers/admin/{username}"
    headers = {"Accept": ATLAS_API_VERSION}
    try:
        resp = requests.delete(
            url, auth=HTTPDigestAuth(public_key, private_key),
            headers=headers, timeout=15,
        )
    except Exception as e:
        print(f"  [warn] Atlas DB user delete failed: {e}")
        return False

    if resp.status_code in (200, 204, 404):
        return True
    print(f"  [warn] Atlas DB user delete returned HTTP {resp.status_code}.")
    return False


def reconcile_orphan_db_user(
    project_root: Path,
    public_key: str,
    private_key: str,
    project_id: str,
    username: str = "streaming_agents_app",
) -> None:
    """Bring an orphaned project-scoped Atlas DB user under terraform
    control.

    Strategy:
      1. GET the user via Atlas Admin API. If absent (404), nothing to do.
      2. If present and the resource is not in terraform state, run
         ``terraform import``. Subsequent ``terraform apply`` will then
         reset the password to whatever is in tfvars, so .env and Atlas
         stay aligned.
      3. If import fails (e.g., terraform not yet initialized, transient
         API error), fall back to deleting the orphan so terraform can
         recreate it from scratch.

    All failures are best-effort/non-fatal — terraform will surface the
    real error if this turns out to matter.

    explicit empty-string check on username. The default
    "streaming_agents_app" only applies when the argument is omitted;
    a caller passing username="" gets a ValueError rather than silently
    operating on the wrong account.
    """
    if not (public_key and private_key and project_id):
        return
    if not username:
        raise ValueError(
            "reconcile_orphan_db_user: username must be a non-empty string"
        )

    exists = db_user_exists(public_key, private_key, project_id, username)
    if exists is False:
        return  # nothing to reconcile
    if exists is None:
        return  # lookup failed; let terraform surface the real error

    atlas_dir = project_root / "terraform" / "atlas"
    resource_addr = "mongodbatlas_database_user.app_user"
    # Atlas DB user import id format: {project_id}-{username}-{auth_db}
    import_id = f"{project_id}-{username}-admin"

    # Skip import if the resource is already tracked (state list is cheap).
    try:
        state_list = subprocess.run(
            ["terraform", "state", "list", resource_addr],
            cwd=atlas_dir, capture_output=True, text=True, timeout=30,
        )
        if state_list.returncode == 0 and resource_addr in state_list.stdout:
            print(
                f"  [ok] Atlas DB user '{username}' already in terraform "
                "state; skipping reconcile."
            )
            return
    except Exception:
        pass  # fall through to import attempt

    print(
        f"  [info] Atlas DB user '{username}' exists but is not in "
        "terraform state; importing..."
    )

    # terraform import requires init to have run; do it idempotently
    # here so this function works even when called before run_terraform().
    try:
        subprocess.run(
            ["terraform", "init", "-input=false"],
            cwd=atlas_dir, capture_output=True, text=True, timeout=120,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"  [warn] terraform init failed before import: {e.stderr or e.stdout}")
        if delete_db_user(public_key, private_key, project_id, username):
            print(
                f"  [ok] Deleted orphaned Atlas DB user '{username}' as fallback."
            )
        return

    try:
        result = subprocess.run(
            ["terraform", "import", resource_addr, import_id],
            cwd=atlas_dir, capture_output=True, text=True, timeout=120,
        )
    except Exception as e:
        print(f"  [warn] terraform import raised: {e}")
        if delete_db_user(public_key, private_key, project_id, username):
            print(
                f"  [ok] Deleted orphaned Atlas DB user '{username}' as fallback."
            )
        return

    if result.returncode == 0:
        print(
            f"  [ok] Imported existing Atlas DB user '{username}' into "
            "terraform state. Apply will refresh its password to match tfvars."
        )
        return

    # Import failed — fall back to delete + recreate.
    err = (result.stderr or result.stdout or "").strip()
    last_line = err.splitlines()[-1] if err else "unknown"
    print(
        f"  [warn] terraform import failed ({last_line}); falling back to delete."
    )
    if delete_db_user(public_key, private_key, project_id, username):
        print(f"  [ok] Deleted orphaned Atlas DB user '{username}'.")


def quarantine_stale_agents_state(project_root: Path) -> bool:
    """Move terraform/agents/terraform.tfstate aside when it references
    a Confluent environment that no longer matches the current core
    output.

    The agents module reads core's environment_id / compute_pool_id via
    ``data.terraform_remote_state``, but its OWN tfstate also caches
    ``confluent_flink_statement`` resource IDs that embed the old
    environment_id (e.g. ``env-g3q85r/lfcp-...``). If core was destroyed
    and re-created, the new env-id won't match those cached IDs, and
    terraform refresh hits HTTP 401 trying to read statements in an
    environment that no longer exists.

    Returns True if state was moved aside, False if state is already
    consistent (or no state exists yet).
    """
    agents_state = project_root / "terraform" / "agents" / "terraform.tfstate"
    agents_backup = agents_state.with_suffix(".tfstate.backup")
    core_state = project_root / "terraform" / "core" / "terraform.tfstate"

    if not core_state.exists():
        return False  # First-time deploy — nothing to quarantine
    # also check the .backup file. If a user manually rm'd
    # terraform.tfstate but left the backup, the next `terraform init`
    # may pick up the backup with stale env-id refs. Quarantine the
    # backup too so init starts clean.
    if not agents_state.exists() and not agents_backup.exists():
        return False

    # Use the cached `get_core_outputs` helper for consistency with the
    # rest of the deploy.
    from .terraform_outputs import get_core_outputs
    outputs = get_core_outputs(project_root)
    current_env_id = (
        (outputs.get("confluent_environment_id") or {})
        .get("value", "")
        .strip()
    )
    if not current_env_id or not current_env_id.startswith("env-"):
        return False

    # Scan agents state for env-id refs (any resource ID like "env-XXXX/...")
    # Read from whichever state file exists; the backup is good enough
    # for drift detection.
    state_file = agents_state if agents_state.exists() else agents_backup
    try:
        text = state_file.read_text()
    except Exception:
        return False

    env_ids_in_state = set(re.findall(r"env-[A-Za-z0-9]+", text))
    stale_envs = env_ids_in_state - {current_env_id}
    if not stale_envs:
        return False

    # Drift detected — quarantine the state files (don't delete; user
    # can restore if needed).
    # UTC timestamp (matches ).
    suffix = datetime.now(timezone.utc).strftime("stale-%Y%m%dT%H%M%SZ")
    quarantined = []
    for f in (agents_state, agents_backup):
        if f.exists():
            target = f.with_name(f.name + "." + suffix)
            f.rename(target)
            quarantined.append(target.name)

    print(
        f"\n  [info] Agents state references stale environment(s) "
        f"{', '.join(sorted(stale_envs))}, current is {current_env_id}."
    )
    print(f"  [info] Quarantined: {', '.join(quarantined)}")
    print(f"  [info] Agents will be re-created against {current_env_id}.")
    return True
