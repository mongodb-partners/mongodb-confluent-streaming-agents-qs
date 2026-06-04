"""
Credential loading and management utilities.

Provides functions for:
- Generating Confluent Cloud API keys via CLI

removed dead `load_credentials_json` (referenced
a `tests/credentials.template.json` file that doesn't exist in this
repo).

also removed `load_or_create_credentials_file` —
dead code (no callers; deploy.py has its own `_load_env` /
`_env_path` helpers that handle the `.env` lifecycle properly without
the destructive `.env.example.unlink()` side-effect this function had).
"""

import json
import subprocess
import time
from typing import Optional, Tuple


def generate_confluent_api_keys(prefix: str = "streaming-agents") -> Tuple[Optional[str], Optional[str]]:
    """
    Generate Confluent API keys using CLI.

    Creates a service account and generates API keys with OrganizationAdmin role.

    every `confluent` invocation now uses `-o json`
    and parses structured output instead of splitting the human-readable
    table format. The Confluent CLI has changed its default table layout
    twice in the past 18 months; the JSON output has been stable since
    v2.x. Without this, a CLI upgrade silently broke every extraction
    (api_key=None, api_secret=None) and the deploy proceeded with empty
    credentials → confusing downstream auth errors.

    Args:
        prefix: Prefix for service account name (default: "streaming-agents")

    Returns:
        Tuple of (api_key, api_secret) or (None, None) if generation fails
    """
    try:
        timestamp = str(int(time.time()))[-6:]
        sa_name = f"{prefix}-setup-sa-{timestamp}"

        print(f"Creating service account: {sa_name}...")
        sa_result = subprocess.run(
            ["confluent", "iam", "service-account", "create", sa_name,
             "--description", f"Service account for {prefix} streaming agents setup",
             "-o", "json"],
            capture_output=True, text=True, check=True
        )

        sa_id: Optional[str] = None
        try:
            sa_data = json.loads(sa_result.stdout)
            # Confluent CLI JSON output: {"id": "sa-xxxxx", ...}
            sa_id = sa_data.get("id") or sa_data.get("ID")
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Error: Could not parse `confluent` JSON output: {e}")
            return None, None

        if not sa_id:
            print("Error: Failed to extract service account ID from JSON output.")
            return None, None

        print("Creating API key with Cloud Resource Management scope...")
        key_result = subprocess.run(
            ["confluent", "api-key", "create",
             "--service-account", sa_id,
             "--resource", "cloud",
             "--description", f"{prefix} setup key",
             "-o", "json"],
            capture_output=True, text=True, check=True
        )

        api_key: Optional[str] = None
        api_secret: Optional[str] = None
        try:
            key_data = json.loads(key_result.stdout)
            # Confluent CLI: {"api_key": "...", "api_secret": "..."} or
            # the older {"key": "...", "secret": "..."} form.
            api_key = (
                key_data.get("api_key") or key_data.get("key")
                or key_data.get("API Key") or key_data.get("ID")
            )
            api_secret = (
                key_data.get("api_secret") or key_data.get("secret")
                or key_data.get("API Secret")
            )
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Error: Could not parse api-key JSON output: {e}")
            return None, None

        if api_key and api_secret:
            print("Assigning OrganizationAdmin role...")
            try:
                subprocess.run(
                    ["confluent", "iam", "rbac", "role-binding", "create",
                     "--principal", f"User:{sa_id}",
                     "--role", "OrganizationAdmin"],
                    capture_output=True, text=True, check=True
                )
                print("✓ API keys generated successfully!")
                return api_key, api_secret
            except subprocess.CalledProcessError:
                print("Warning: Role assignment failed, but API keys were created.")
                return api_key, api_secret

    except subprocess.CalledProcessError as e:
        print(f"Error generating API keys: {e}")

    return None, None
