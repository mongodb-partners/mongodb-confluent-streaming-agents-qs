"""
Terraform variables file (terraform.tfvars) management utilities.

Provides functions for:
- Writing terraform.tfvars files with automatic backup
- Generating formatted tfvars content for core and agents modules
- Orchestrating tfvars file creation across environments
"""

import os
import shutil
from pathlib import Path
from typing import Dict, Optional


def _detect_egress_cidrs() -> Optional[list]:
    """Best-effort: return the deployer's public IP as a /32 CIDR list.

    scopes Atlas IP access list for non-workshop deploys.
    Returns None on any failure (caller falls back to 0.0.0.0/0).
    """
    try:
        import urllib.request
        with urllib.request.urlopen(
            "https://checkip.amazonaws.com",
            timeout=5,
        ) as resp:
            ip = resp.read().decode().strip()
        # Sanity check: must look like an IPv4 dotted-quad.
        parts = ip.split(".")
        if len(parts) != 4 or not all(0 <= int(p) <= 255 for p in parts):
            return None
        return [f"{ip}/32"]
    except Exception:
        return None


def get_credential_value(creds: Dict[str, str], key: str) -> Optional[str]:
    """
    Get credential value, checking both TF_VAR_ prefixed and non-prefixed keys.

    Args:
        creds: Dictionary of credentials
        key: Key to look up (without TF_VAR_ prefix)

    Returns:
        Value if found, None otherwise
    """
    return creds.get(key) or creds.get(f"TF_VAR_{key}")


def write_tfvars_file(tfvars_path: Path, content: str) -> bool:
    """
    Write terraform.tfvars file with backup of existing file.

    Args:
        tfvars_path: Path to terraform.tfvars file
        content: Content to write

    Returns:
        True if successful, False otherwise
    """
    try:
        # Backup existing file
        if tfvars_path.exists():
            backup_path = tfvars_path.with_suffix(".tfvars.backup")
            shutil.copy2(tfvars_path, backup_path)

        # Ensure parent directory exists
        tfvars_path.parent.mkdir(parents=True, exist_ok=True)

        # write with mode 0o600 so the secrets in tfvars
        # (cloud API keys, AWS keys, Atlas password, MongoDB password,
        # MCP auth token, Voyage AI key) are not world-readable.
        fd = os.open(tfvars_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
        except Exception:
            raise
        # Tighten in case the file pre-existed with broader perms.
        try:
            os.chmod(tfvars_path, 0o600)
        except OSError:
            pass

        # Also tighten the backup file we just made.
        try:
            backup_path = tfvars_path.with_suffix(".tfvars.backup")
            if backup_path.exists():
                os.chmod(backup_path, 0o600)
        except OSError:
            pass

        return True
    except Exception as e:
        print(f"Error writing {tfvars_path}: {e}")
        return False


def generate_core_tfvars_content(
    region: str,
    api_key: str,
    api_secret: str,
    owner_email: Optional[str] = None,
    aws_bedrock_access_key: Optional[str] = None,
    aws_bedrock_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    bedrock_model_id: Optional[str] = None,
    # Backward-compat: callers may still pass these from older code paths;
    # they are no longer emitted in core tfvars (atlas lives in its own module).
    create_atlas_cluster: bool = False,
    atlas_public_key: Optional[str] = None,
    atlas_private_key: Optional[str] = None,
    atlas_project_id: Optional[str] = None,
    atlas_cluster_name: Optional[str] = None,
    atlas_db_username: Optional[str] = None,
    atlas_db_password: Optional[str] = None,
) -> str:
    """
    Generate terraform.tfvars content for Core module.

    Atlas resources live in terraform/atlas (separate state). Core no longer
    emits atlas_* tfvars; the legacy parameters are accepted for backward
    compatibility but ignored.
    """
    _ = (create_atlas_cluster, atlas_public_key, atlas_private_key,
         atlas_project_id, atlas_cluster_name, atlas_db_username,
         atlas_db_password)  # accepted, intentionally unused

    content = f"""# Core Infrastructure Configuration
cloud_region = "{region}"
confluent_cloud_api_key = "{api_key}"
confluent_cloud_api_secret = "{api_secret}"
"""

    if owner_email:
        content += f'owner_email = "{owner_email}"\n'

    # AWS Bedrock credentials
    if aws_bedrock_access_key and aws_bedrock_secret_key:
        content += f'aws_bedrock_access_key = "{aws_bedrock_access_key}"\n'
        content += f'aws_bedrock_secret_key = "{aws_bedrock_secret_key}"\n'
        if aws_session_token:
            content += f'aws_session_token = "{aws_session_token}"\n'
        if bedrock_model_id:
            content += f'bedrock_model_id = "{bedrock_model_id}"\n'

    return content


def generate_atlas_tfvars_content(
    atlas_public_key: str,
    atlas_private_key: str,
    atlas_project_id: str,
    atlas_cluster_name: str,
    atlas_db_username: str,
    atlas_db_password: str,
    cloud_region: str = "us-east-1",
    owner_email: Optional[str] = None,
    atlas_access_cidrs: Optional[list] = None,
) -> str:
    """Generate terraform.tfvars content for the standalone atlas module.

    ``atlas_access_cidrs`` defaults to ``["0.0.0.0/0"]``
    (workshop). Non-workshop deploys pass the deployer's egress IP as
    a /32.
    """
    content = f"""# Atlas Cluster Configuration (independent module, persists across redeploys)
cloud_region = "{cloud_region}"
atlas_public_key = "{atlas_public_key}"
atlas_private_key = "{atlas_private_key}"
atlas_project_id = "{atlas_project_id}"
atlas_cluster_name = "{atlas_cluster_name}"
atlas_db_username = "{atlas_db_username}"
atlas_db_password = "{atlas_db_password}"
"""
    if owner_email:
        content += f'owner_email = "{owner_email}"\n'
    # emit the access-list CIDRs.
    cidrs = atlas_access_cidrs or ["0.0.0.0/0"]
    cidr_hcl = ", ".join(f'"{c}"' for c in cidrs)
    content += f"atlas_access_cidrs = [{cidr_hcl}]\n"
    return content


def generate_agents_tfvars_content(
    mcp_server_url: str,
    mcp_auth_token: str,
    voyage_api_key: str,
    mongo_conn: str,
    mongo_user: str,
    mongo_pass: str,
    voyage_api_endpoint: Optional[str] = None,
) -> str:
    """
    Generate terraform.tfvars content for Agents module.

    Args:
        mcp_server_url: MongoDB MCP server URL (ECS Express Mode deployment)
        mcp_auth_token: Bearer token for MCP server authentication
        voyage_api_key: Voyage AI API key
        mongo_conn: MongoDB Atlas connection string (required)
        mongo_user: MongoDB Atlas username (required)
        mongo_pass: MongoDB Atlas password (required)
        voyage_api_endpoint: Optional Voyage embeddings endpoint URL.
            When None the terraform default (ai.mongodb.com) is used.

    Returns:
        Formatted terraform.tfvars content
    """
    content = f"""# Agents Configuration
mcp_server_url = "{mcp_server_url}"
mcp_auth_token = "{mcp_auth_token}"
voyage_api_key = "{voyage_api_key}"
mongodb_connection_string = "{mongo_conn}"
mongodb_username = "{mongo_user}"
mongodb_password = "{mongo_pass}"
"""
    if voyage_api_endpoint:
        content += f'voyage_api_endpoint = "{voyage_api_endpoint}"\n'

    return content


def write_tfvars_for_deployment(
    root: Path,
    region: str,
    creds: Dict[str, str],
    envs_to_deploy: list
) -> None:
    """
    Write terraform.tfvars files for all environments being deployed.

    Args:
        root: Project root directory
        region: AWS region
        creds: Credentials dictionary (supports both TF_VAR_ prefixed and non-prefixed keys)
        envs_to_deploy: List of environments to deploy (core, agents)
    """
    # Atlas terraform.tfvars (independent module, persists across redeploys)
    if "atlas" in envs_to_deploy:
        atlas_public_key = creds.get("ATLAS_PUBLIC_KEY") or get_credential_value(creds, "atlas_public_key")
        atlas_private_key = creds.get("ATLAS_PRIVATE_KEY") or get_credential_value(creds, "atlas_private_key")
        atlas_project_id = creds.get("ATLAS_PROJECT_ID") or get_credential_value(creds, "atlas_project_id")
        atlas_cluster_name = creds.get("ATLAS_CLUSTER_NAME") or get_credential_value(creds, "atlas_cluster_name") or "streaming-agents-cluster"
        atlas_db_username = get_credential_value(creds, "atlas_db_username") or "streaming_agents_app"
        atlas_db_password = get_credential_value(creds, "atlas_db_password")
        owner_email = get_credential_value(creds, "owner_email")

        if atlas_public_key and atlas_private_key and atlas_project_id and atlas_db_password:
            atlas_tfvars_path = root / "terraform" / "atlas" / "terraform.tfvars"
            # pick CIDRs based on workshop-mode (passed
            # through creds["workshop_mode"] = "true"/"false" by deploy).
            workshop = (creds.get("workshop_mode") or "").lower() == "true"
            if workshop:
                cidrs = ["0.0.0.0/0"]
            else:
                # warn loudly when egress
                # detection fails and the deploy falls back to
                # 0.0.0.0/0. Without the warning, a corporate proxy /
                # rate limit / IPv6-only egress silently weakens the
                # security gate the user explicitly opted into.
                cidrs = _detect_egress_cidrs()
                if cidrs is None:
                    print(
                        "  [warn] Could not detect egress IP via "
                        "checkip.amazonaws.com. Falling back to "
                        "0.0.0.0/0 (open access). Re-run with "
                        "--workshop-mode to accept this default "
                        "explicitly, OR set "
                        "TF_VAR_atlas_access_cidrs='[\"<your-ip>/32\"]' "
                        "in .env to scope manually."
                    )
                    cidrs = ["0.0.0.0/0"]
            content = generate_atlas_tfvars_content(
                atlas_public_key=atlas_public_key,
                atlas_private_key=atlas_private_key,
                atlas_project_id=atlas_project_id,
                atlas_cluster_name=atlas_cluster_name,
                atlas_db_username=atlas_db_username,
                atlas_db_password=atlas_db_password,
                cloud_region=region,
                owner_email=owner_email,
                atlas_access_cidrs=cidrs,
            )
            if write_tfvars_file(atlas_tfvars_path, content):
                print(f"  Wrote {atlas_tfvars_path} (Atlas CIDRs: {cidrs})")
        else:
            print("  [warn] Atlas creds incomplete — skipping atlas tfvars write")

    # Core terraform.tfvars
    if "core" in envs_to_deploy:
        api_key = get_credential_value(creds, "confluent_cloud_api_key")
        api_secret = get_credential_value(creds, "confluent_cloud_api_secret")
        owner_email = get_credential_value(creds, "owner_email")

        aws_bedrock_access_key = get_credential_value(creds, "aws_bedrock_access_key")
        aws_bedrock_secret_key = get_credential_value(creds, "aws_bedrock_secret_key")
        aws_session_token = get_credential_value(creds, "aws_session_token")
        bedrock_model_id = get_credential_value(creds, "bedrock_model_id")

        if api_key and api_secret:
            core_tfvars_path = root / "terraform" / "core" / "terraform.tfvars"
            content = generate_core_tfvars_content(
                region, api_key, api_secret,
                owner_email=owner_email,
                aws_bedrock_access_key=aws_bedrock_access_key,
                aws_bedrock_secret_key=aws_bedrock_secret_key,
                aws_session_token=aws_session_token,
                bedrock_model_id=bedrock_model_id,
            )
            if write_tfvars_file(core_tfvars_path, content):
                print(f"  Wrote {core_tfvars_path}")

    # Agents terraform.tfvars
    if "agents" in envs_to_deploy:
        mcp_server_url = get_credential_value(creds, "mcp_server_url")
        mcp_auth_token = get_credential_value(creds, "mcp_auth_token")
        voyage_api_key = get_credential_value(creds, "voyage_api_key")
        voyage_api_endpoint = get_credential_value(creds, "voyage_api_endpoint")

        # MongoDB Atlas credentials (required for agents)
        mongo_conn = get_credential_value(creds, "mongodb_connection_string")
        mongo_user = get_credential_value(creds, "mongodb_username")
        mongo_pass = get_credential_value(creds, "mongodb_password")

        if mcp_server_url and mcp_auth_token and voyage_api_key and mongo_conn and mongo_user and mongo_pass:
            agents_tfvars_path = root / "terraform" / "agents" / "terraform.tfvars"
            content = generate_agents_tfvars_content(
                mcp_server_url,
                mcp_auth_token,
                voyage_api_key,
                mongo_conn,
                mongo_user,
                mongo_pass,
                voyage_api_endpoint=voyage_api_endpoint,
            )
            if write_tfvars_file(agents_tfvars_path, content):
                print(f"  Wrote {agents_tfvars_path}")
        elif not (mongo_conn and mongo_user and mongo_pass):
            print("  MongoDB Atlas credentials are required (connection string, username, password)")
        elif not (mcp_server_url and mcp_auth_token):
            print("  MCP Server URL/token not available — run MCP deploy first")
