"""
Generate a comprehensive DEPLOYED_RESOURCES.md file from Terraform outputs.

This module creates a markdown file containing all deployed resources, credentials,
and configuration details for easy reference after Core deployment.

Usage:
    # From terraform_runner (automatic)
    generate_credentials_markdown(tf_outputs, output_path)

    # Standalone (manual)
    uv run deployment-summary core
    uv run deployment-summary agents
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any


def generate_credentials_markdown(tf_outputs: Dict[str, Any], output_path: Path) -> None:
    """
    Generate DEPLOYED_RESOURCES.md file from Terraform outputs.

    Args:
        tf_outputs: Dictionary of terraform outputs (from terraform output -json)
        output_path: Path where the markdown file should be saved
    """
    try:
        # Extract values from terraform outputs (handle sensitive values)
        def get_output(key: str, default: str = "") -> str:
            """Extract value from terraform output, handling sensitive values."""
            if key not in tf_outputs:
                return default
            output = tf_outputs[key]
            # If it's a dict with 'value' key (terraform output format)
            if isinstance(output, dict) and 'value' in output:
                return str(output['value']) if output['value'] is not None else default
            return str(output) if output is not None else default

        # Build markdown sections
        sections = [
            _build_header(),
            _build_account_section(tf_outputs, get_output),
            _build_cloud_details_section(get_output),
            _build_cloud_resources_section(get_output),
            _build_credentials_section(tf_outputs, get_output),
            _build_resource_inventory_section(tf_outputs, get_output),
            _build_llm_configuration_section(get_output),
        ]

        # Combine all sections
        markdown_content = "\n\n".join(sections)

        # Write to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown_content)

        print(f"Resource summary saved to: {output_path}")

    except Exception as e:
        print(f"Warning: Failed to generate DEPLOYED_RESOURCES.md: {e}")
        # Don't fail the deployment if markdown generation fails


def _build_header() -> str:
    """Build the warning header."""
    return """# Confluent Cloud Resources

**WARNING: This file contains API keys, secrets, and other sensitive credentials. Do not commit to version control or share publicly.**

---"""


def _build_account_section(tf_outputs: Dict[str, Any], get_output: callable) -> str:
    """Build the Account Information section."""
    owner_email = get_output("owner_email", "Not provided")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    region = get_output("cloud_region")
    env_name = get_output("confluent_environment_display_name")
    env_id = get_output("confluent_environment_id")

    return f"""## Account Information

**Owner Email**: `{owner_email}`
**Deployed**: {timestamp}
**Region**: {region}
**Environment**: {env_name}
**Environment ID**: `{env_id}`

---"""


def _build_cloud_details_section(get_output: callable) -> str:
    """Build the Cloud Details section."""
    region = get_output("cloud_region")

    return f"""## Cloud Details

- **Provider**: AWS
- **Region**: `{region}`

---"""


def _build_cloud_resources_section(get_output: callable) -> str:
    """Build the cloud-specific resources section.

    Skip this section entirely when no AWS IAM resources
    were provisioned by this stack. The outputs `random_id` /
    `aws_access_key_id` are not produced by this stack. Without
    this guard the markdown emits rows like
    `IAM User: bedrock-user-` with empty values.
    """
    random_id = get_output("random_id")
    access_key_id = get_output("aws_access_key_id")
    # If neither output is populated, this stack doesn't create AWS IAM
    # resources — return an empty section so the markdown stays clean.
    if not random_id and not access_key_id:
        return ""
    iam_user = f"bedrock-user-{random_id}" if random_id else "(not created)"
    iam_policy = f"bedrock-policy-{random_id}" if random_id else "(not created)"

    return f"""## AWS Resources Created

The following AWS resources were created in this deployment:

| Resource Type | Name/ID | Purpose |
|---------------|---------|---------|
| **IAM User** | `{iam_user}` | Bedrock API access |
| **IAM Policy** | `{iam_policy}` | Bedrock permissions |
| **IAM Access Key** | `{access_key_id or '(not created)'}` | Bedrock credentials |

---"""


def _build_credentials_section(tf_outputs: Dict[str, Any], get_output: callable) -> str:
    """Build the Service Credentials section."""
    # Primary credentials
    org_id = get_output("confluent_organization_id")
    env_id = get_output("confluent_environment_id")
    cloud_key = get_output("confluent_cloud_api_key")
    cloud_secret = get_output("confluent_cloud_api_secret")

    # Additional credentials
    kafka_bootstrap = get_output("confluent_kafka_cluster_bootstrap_endpoint")
    kafka_key = get_output("app_manager_kafka_api_key")
    kafka_secret = get_output("app_manager_kafka_api_secret")

    sr_endpoint = get_output("confluent_schema_registry_rest_endpoint")
    sr_key = get_output("app_manager_schema_registry_api_key")
    sr_secret = get_output("app_manager_schema_registry_api_secret")

    flink_endpoint = get_output("confluent_flink_rest_endpoint")
    flink_pool = get_output("confluent_flink_compute_pool_id")
    flink_key = get_output("app_manager_flink_api_key")
    flink_secret = get_output("app_manager_flink_api_secret")

    # mask secrets in the generated markdown.
    # DEPLOYED_RESOURCES.md is gitignored but lives on disk in plaintext
    # and is easy to share by accident (e.g. attaching to a support
    # ticket). Mask to {first4}…{last2} so the value is recognizable
    # but not usable. Single source of truth: redaction.mask_secret.
    from scripts.common.redaction import mask_secret as _mask

    return f"""## Service Credentials

> Secrets below are MASKED for safety. Retrieve the full values from
> `.env` or the terraform outputs when you need to use them.

### Primary Credentials (Organization Admin)

| Service | Endpoint/Resource | API Key | API Secret |
|---------|-------------------|---------|------------|
| **Confluent Cloud** | Org: `{org_id}`<br>Env: `{env_id}` | `{_mask(cloud_key)}` | `{_mask(cloud_secret)}` |

**Note**: These are your Organization Admin credentials — retrieve full
values from your `.env` file for CLI access and account management.

### Additional Service Credentials

| Service | Endpoint/Resource | API Key | API Secret |
|---------|-------------------|---------|------------|
| **Kafka Cluster** | `{kafka_bootstrap}` | `{_mask(kafka_key)}` | `{_mask(kafka_secret)}` |
| **Schema Registry** | `{sr_endpoint}` | `{_mask(sr_key)}` | `{_mask(sr_secret)}` |
| **Flink** | `{flink_endpoint}`<br>Pool: `{flink_pool}` | `{_mask(flink_key)}` | `{_mask(flink_secret)}` |

---"""


def _build_resource_inventory_section(tf_outputs: Dict[str, Any], get_output: callable) -> str:
    """Build the Resource Inventory section."""
    env_id = get_output("confluent_environment_id")
    env_name = get_output("confluent_environment_display_name")

    cluster_id = get_output("confluent_kafka_cluster_id")
    cluster_name = get_output("confluent_kafka_cluster_display_name")
    cluster_rest = get_output("confluent_kafka_cluster_rest_endpoint")

    sr_id = get_output("confluent_schema_registry_id")
    sr_endpoint = get_output("confluent_schema_registry_rest_endpoint")

    flink_pool_id = get_output("confluent_flink_compute_pool_id")

    sa_id = get_output("app_manager_service_account_id")

    return f"""## Resource Inventory

| Resource Type | ID | Display Name / Details |
|---------------|----|-----------------------|
| Environment | `{env_id}` | {env_name} |
| Kafka Cluster | `{cluster_id}` | {cluster_name}<br>REST: `{cluster_rest}` |
| Schema Registry | `{sr_id}` | `{sr_endpoint}` |
| Flink Pool | `{flink_pool_id}` | - |
| Service Account | `{sa_id}` | Role: EnvironmentAdmin |

---"""


def _build_llm_configuration_section(get_output: callable) -> str:
    """Build the LLM Configuration section."""
    textgen_connection = get_output("llm_connection_name")
    env_name = get_output("confluent_environment_display_name")
    cluster_name = get_output("confluent_kafka_cluster_display_name")

    return f"""## LLM Configuration

### Flink Connections

The following Flink AI connections were created via Terraform:

- **Text Generation Connection**: `{textgen_connection}` (AWS Bedrock — Claude)
- **Embedding Connection**: `voyage_connection` (Voyage AI via `https://ai.mongodb.com/v1/embeddings`, created in the agents module)

### Flink Models

#### Text Generation Model

**Model Name**: `llm_textgen_model`

```sql
CREATE MODEL `{env_name}`.`{cluster_name}`.`llm_textgen_model`
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH(
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = '{textgen_connection}',
  'bedrock.params.max_tokens' = '50000'
);
```

#### Embedding Model

**Model Name**: `voyage_query_embedding` (defined in the agents module)

```sql
CREATE MODEL `{env_name}`.`{cluster_name}`.`voyage_query_embedding`
INPUT (text STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH(
  'provider' = 'openai',
  'openai.connection' = 'voyage_connection',
  'task' = 'embedding',
  'openai.model_version' = 'voyage-4',
  'openai.output_format' = 'OPENAI-EMBED'
);
```

### Usage Example

```sql
-- Generate text with the LLM
SELECT response
FROM my_table,
LATERAL TABLE(ML_PREDICT('llm_textgen_model', prompt_column));

-- Generate embeddings
SELECT embedding
FROM my_table,
LATERAL TABLE(ML_PREDICT('voyage_query_embedding', text_column));
```"""


def main():
    """
    Main entry point for standalone script execution.

    Usage:
        uv run deployment-summary <env-name>
    """
    if len(sys.argv) != 2:
        print("Usage: uv run deployment-summary <env-name>")
        print("Example: uv run deployment-summary core")
        print("         uv run deployment-summary agents")
        sys.exit(1)

    # Parse arguments - prepend terraform/ to the env name
    env_name = sys.argv[1]
    terraform_dir = Path("terraform") / env_name

    # Validate path
    if not terraform_dir.exists():
        print(f"Error: Directory not found: {terraform_dir}")
        sys.exit(1)

    if not (terraform_dir / "main.tf").exists():
        print(f"Error: Not a valid terraform directory (no main.tf found): {terraform_dir}")
        sys.exit(1)

    # Run terraform output -json
    print(f"Reading Terraform outputs from {terraform_dir}...")
    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=terraform_dir,
            capture_output=True,
            text=True,
            check=True
        )
        tf_outputs = json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run terraform output: {e}")
        print("Make sure terraform has been initialized and applied in this directory.")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: terraform command not found. Please install Terraform.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse terraform output JSON: {e}")
        sys.exit(1)

    # Generate markdown
    output_file = terraform_dir / "DEPLOYED_RESOURCES.md"
    generate_credentials_markdown(tf_outputs, output_file)
    print(f"\nSuccess! Deployment summary generated at: {output_file}")


if __name__ == "__main__":
    main()
