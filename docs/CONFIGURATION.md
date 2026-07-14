# Configuration Reference

All configuration is managed through `.env` in the project root. This file is created by `uv run deploy` and read by all CLI tools.

## Non-interactive deploys

`uv run deploy --non-interactive` (alias `-y`) runs the deploy wizard fully unattended for CI/CD pipelines, EC2 jump-hosts, and other contexts where no human is available to answer prompts.

### Behavior

When `--non-interactive` is set, the deploy script:

1. **Implies `--plain`:** disables the rich/questionary UI.
2. **Hydrates `.env` from process environment variables:** any of the keys listed below that are set in `os.environ` but missing from `.env` are written to `.env` before validation. Existing `.env` values are preserved (file wins over env), so individual fields can be overridden via the file without an env-var shadow surprise.
3. **Validates required credentials up front:** if any required key is missing from both `.env` and the environment, the deploy exits with code **2** and prints the list of missing keys. (Exit code 1 is reserved for preflight failures.)
4. **Auto-confirms every interactive prompt:** the deployment summary review, the Bedrock advisory check, and the resume-from-existing-deploy prompt all proceed with their default choice without asking. A complete deploy that is re-invoked unattended without `--force` prints the summary and exits (no silent re-deploy).
5. **Refuses to prompt mid-flight:** if any code path reaches `_text()` without a saved default, the deploy raises a clear `RuntimeError` instead of hanging on stdin.

`--non-interactive` is compatible with `--from-phase`, `--force`, `--skip-preflight`, and `--workshop-mode`.

### Required credential keys

A `--non-interactive` run hard-fails (exit 2) if any of these are missing from both `.env` and `os.environ`:

| Key | Purpose |
|-----|---------|
| `TF_VAR_confluent_cloud_api_key` | Confluent Cloud API key (OrganizationAdmin) |
| `TF_VAR_confluent_cloud_api_secret` | Confluent Cloud API secret |
| `TF_VAR_aws_bedrock_access_key` | AWS Bedrock access key |
| `TF_VAR_aws_bedrock_secret_key` | AWS Bedrock secret key |
| `TF_VAR_mongodb_connection_string` | Atlas connection string (`mongodb+srv://...`) |
| `TF_VAR_mongodb_username` | Atlas database username |
| `TF_VAR_mongodb_password` | Atlas database password |
| `TF_VAR_voyage_api_key` | Voyage AI API key |
| `ATLAS_PUBLIC_KEY` | Atlas Admin API public key (for ASP) |
| `ATLAS_PRIVATE_KEY` | Atlas Admin API private key |
| `ATLAS_PROJECT_ID` | Atlas project ID |

These optional keys are also hydrated from `os.environ` when present (no error if missing): `TF_VAR_aws_session_token`, `ATLAS_CLUSTER_NAME`, `TF_VAR_owner_email`, `TF_VAR_cloud_region`, `TF_VAR_bedrock_model_id`, `TF_VAR_voyage_api_endpoint`, `TF_VAR_mcp_server_url`, `TF_VAR_mcp_auth_token`, `TF_VAR_create_atlas_cluster`, `TF_VAR_atlas_db_username`, `TF_VAR_atlas_db_password`.

### Pattern 1: pre-populated `.env`

Write a complete `.env` once, then run unattended. This is the lowest-friction path because everything else reads `.env` already.

```bash
# .env (chmod 600; do not commit)
TF_VAR_confluent_cloud_api_key=CCK...
TF_VAR_confluent_cloud_api_secret=...
TF_VAR_aws_bedrock_access_key=AKIA...
TF_VAR_aws_bedrock_secret_key=...
TF_VAR_mongodb_connection_string=mongodb+srv://...
TF_VAR_mongodb_username=admin
TF_VAR_mongodb_password=...
TF_VAR_voyage_api_key=pa-...
ATLAS_PUBLIC_KEY=...
ATLAS_PRIVATE_KEY=...
ATLAS_PROJECT_ID=...
# Optional: provision a fresh Atlas M10 instead of BYO
# TF_VAR_create_atlas_cluster=true
# ATLAS_CLUSTER_NAME=streaming-agents-cluster

# Run
confluent login --organization $ORG_ID --save   # one-time, interactive
uv run deploy --non-interactive
```

### Pattern 2: environment variables only (CI/CD)

If your CI/CD platform injects secrets as environment variables (GitHub Actions, GitLab CI, Drone), export them in the job step and let `--non-interactive` hydrate them into `.env`:

```bash
export TF_VAR_confluent_cloud_api_key="$CONFLUENT_CLOUD_API_KEY"
export TF_VAR_confluent_cloud_api_secret="$CONFLUENT_CLOUD_API_SECRET"
export TF_VAR_aws_bedrock_access_key="$AWS_ACCESS_KEY_ID"
export TF_VAR_aws_bedrock_secret_key="$AWS_SECRET_ACCESS_KEY"
export TF_VAR_mongodb_connection_string="$MONGODB_URI"
export TF_VAR_mongodb_username="$MONGODB_USERNAME"
export TF_VAR_mongodb_password="$MONGODB_PASSWORD"
export TF_VAR_voyage_api_key="$VOYAGE_API_KEY"
export ATLAS_PUBLIC_KEY="$ATLAS_PUBLIC_KEY"
export ATLAS_PRIVATE_KEY="$ATLAS_PRIVATE_KEY"
export ATLAS_PROJECT_ID="$ATLAS_PROJECT_ID"

uv run deploy --non-interactive
```

After hydration the deploy writes these values to `.env` so subsequent CLI tools (`asp-setup`, `datagen`, `live`) can read them without re-export.

### Pattern 3: resume from a failed phase

When a `--non-interactive` deploy fails mid-flight, `DEPLOY_PHASE` records progress (atomic via temp-file + `os.replace`). Resume the same way you would interactively:

```bash
uv run deploy --non-interactive --from-phase asp_setup
# or to restart from scratch:
uv run deploy --non-interactive --force
```

Without either flag, an in-progress deploy is silently resumed from the next phase, and a complete deploy short-circuits to the summary (no silent re-deploy).

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Deploy completed (or `--summary` printed cleanly, or `--list-phases` listed) |
| 1 | Preflight failure, or an existing deploy was `cancel`led |
| 2 | `--non-interactive` could not start because required credentials were missing |
| 130 | SIGINT / SIGTERM during a deploy phase (records `DEPLOY_LAST_INTERRUPTED_PHASE`) |
| other | Uncaught exception inside `run_deployment` (records `DEPLOY_LAST_FAILURE` and `DEPLOY_LAST_FAILED_PHASE`) |

### Caveats

- **One-time interactive setup is still required.** `confluent login` opens a browser SSO flow and cannot be scripted from inside `deploy.py`. Run it once on the target machine before scheduling automated deploys. Preflight will tell you the exact command.
- **`.env` is written to disk.** This project's model is single-user / single-host / local state, not multi-tenant. If your environment requires zero-disk-secrets, mount `.env` from a tmpfs / secret store and revoke afterwards; the file is read once at startup and once per phase (for `DEPLOY_PHASE` updates).
- **AWS Bedrock advisory check is downgraded to a warning under `--non-interactive`.** If Claude Sonnet access fails the live check, the deploy proceeds (the operator opted into unattended). Re-run without the flag to abort and fix credentials interactively.

## .env Variables

### Confluent Cloud API

| Variable | Description | Set By |
|----------|-------------|--------|
| `TF_VAR_confluent_cloud_api_key` | Confluent Cloud API key | User (deploy wizard) |
| `TF_VAR_confluent_cloud_api_secret` | Confluent Cloud API secret | User (deploy wizard) |

> **IMPORTANT:** The `confluent_cloud_api_key` MUST be issued to
> a principal with the **OrganizationAdmin** Confluent Cloud role. The
> deploy's Terraform creates a brand-new Confluent **environment** (in
> `terraform/core/main.tf`) and grants `EnvironmentAdmin` to a service
> account inside that env. Both operations require OrganizationAdmin
> on the calling principal.
>
> To deploy with a scoped `EnvironmentAdmin` key instead (production
> hardening): pre-create the Confluent environment manually, change the
> `resource "confluent_environment" "staging"` to `data "confluent_environment"`,
> and pre-grant `EnvironmentAdmin` to the app-manager service account.
> Restructuring is out of scope for the workshop default.

### LLM Configuration (AWS Bedrock)

| Variable | Description | Set By |
|----------|-------------|--------|
| `TF_VAR_aws_bedrock_access_key` | AWS access key for Bedrock | User (deploy wizard) |
| `TF_VAR_aws_bedrock_secret_key` | AWS secret key for Bedrock | User (deploy wizard) |
| `TF_VAR_bedrock_model_id` | Bedrock model ID for the LLM connection | User (optional) |

**`TF_VAR_bedrock_model_id`** controls which LLM is used for the RAG enrichment pipeline. Default: `global.anthropic.claude-sonnet-4-6` (Sonnet 4.6 via the cross-region "global" inference profile). To use a different model:

```bash
# In .env:
TF_VAR_bedrock_model_id='anthropic.claude-haiku-4-5-20251001-v1:0'
```

For region-specific inference profiles include the region prefix (e.g., `us.` for US regions, `eu.` for EU). The `global.` prefix selects the cross-region profile and is the default.

### MCP Server

| Variable | Description | Set By |
|----------|-------------|--------|
| `TF_VAR_mcp_server_url` | MCP server endpoint (ECS Express URL) | Auto (mcp_deploy.py) |
| `TF_VAR_mcp_auth_token` | Bearer token for MCP authentication | Auto (mcp_deploy.py) |

### MongoDB Atlas

| Variable | Description | Set By |
|----------|-------------|--------|
| `TF_VAR_mongodb_connection_string` | Atlas connection string (mongodb+srv://...) | User (deploy wizard) OR Terraform output (when `TF_VAR_create_atlas_cluster=true`) |
| `TF_VAR_mongodb_username` | Atlas database username | User (deploy wizard) OR auto-generated |
| `TF_VAR_mongodb_password` | Atlas database password | User (deploy wizard) OR auto-generated |
| `TF_VAR_voyage_api_key` | Voyage AI API key (from Atlas project settings) | User (deploy wizard) |
| `TF_VAR_voyage_api_endpoint` | Voyage embeddings endpoint URL (default: `https://ai.mongodb.com/v1/embeddings`) | User (optional) |

#### Optional Terraform-managed M10 cluster

When you do not have a pre-existing cluster, the deploy wizard can provision
a Terraform-managed M10 replica set inside your Atlas project. Choose
"Create a new M10 cluster" at the prompt, or set the variables manually:

| Variable | Description | Default |
|----------|-------------|---------|
| `TF_VAR_create_atlas_cluster` | Gate: when `true`, terraform/core provisions the cluster | `false` |
| `TF_VAR_atlas_db_username` | Database user created with the cluster | `streaming_agents_app` |
| `TF_VAR_atlas_db_password` | Database password for the new user (generated by deploy) | (random) |
| `ATLAS_PUBLIC_KEY` / `ATLAS_PRIVATE_KEY` / `ATLAS_PROJECT_ID` / `ATLAS_CLUSTER_NAME` | Provider auth + target project + cluster name | (required) |

Cluster topology (modeled after the official `m10-replicaset` example):

- 3-node REPLICASET, AWS, region matched to `cloud_region`
- Instance size M10, 10 GB disk
- Autoscaling enabled M10 → M50, scale-down enabled, disk autoscale enabled
- `backup_enabled = true`
- `termination_protection_enabled = false` so `uv run destroy` can clean up
- IP access list permits `0.0.0.0/0` (workshop-style; tighten for production)

### Atlas Admin API (for ASP)

| Variable | Description | Set By |
|----------|-------------|--------|
| `ATLAS_PUBLIC_KEY` | Atlas Admin API public key | User (deploy wizard) |
| `ATLAS_PRIVATE_KEY` | Atlas Admin API private key | User (deploy wizard) |
| `ATLAS_PROJECT_ID` | Atlas project ID | User (deploy wizard) |
| `ATLAS_CLUSTER_NAME` | Atlas cluster name | User (deploy wizard) |

### Kafka Credentials (Auto-Generated)

These are written by `deploy.py` after Terraform completes. Do not edit manually.

| Variable | Description | Set By |
|----------|-------------|--------|
| `CONFLUENT_BOOTSTRAP_SERVER` | Kafka bootstrap server URL | Auto (terraform output) |
| `CONFLUENT_KAFKA_API_KEY` | Kafka API key | Auto (terraform output) |
| `CONFLUENT_KAFKA_API_SECRET` | Kafka API secret | Auto (terraform output) |
| `CONFLUENT_KAFKA_REST_ENDPOINT` | Kafka REST API endpoint | Auto (terraform output) |
| `CONFLUENT_KAFKA_CLUSTER_ID` | Kafka cluster ID | Auto (terraform output) |
| `CONFLUENT_SCHEMA_REGISTRY_URL` | Schema Registry endpoint | Auto (terraform output) |
| `CONFLUENT_SCHEMA_REGISTRY_API_KEY` | Schema Registry API key | Auto (terraform output) |
| `CONFLUENT_SCHEMA_REGISTRY_API_SECRET` | Schema Registry API secret | Auto (terraform output) |

### Deployment State

| Variable | Description | Values |
|----------|-------------|--------|
| `DEPLOY_PHASE` | Tracks deployment progress for resume-on-failure | `atlas_terraform`, `mcp_server`, `terraform`, `credentials`, `publish_data`, `asp_setup`, `flink_dml`, `complete` |

### Mission Control (Live UI)

| Variable | Description | Set By |
|----------|-------------|--------|
| `LIVE_SSE_URL` | URL of the running Mission Control / SSE sidecar (e.g. `http://localhost:8502`) | Auto (deploy, when it launches the live server) |
| `LIVE_SSE_ALLOW_ORIGINS` | Extra CORS origins for the SSE endpoints (comma-separated) | User (optional) |

## Terraform Variables

### Core Module (`terraform/core/variables.tf`)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `cloud_region` | string | `us-east-1` | Region for deployment |
| `confluent_cloud_api_key` | string | (required) | Confluent Cloud API key |
| `confluent_cloud_api_secret` | string | (required) | Confluent Cloud API secret |
| `owner_email` | string | `""` | Resource owner email for tagging |
| `aws_bedrock_access_key` | string | `""` | AWS Bedrock access key |
| `aws_bedrock_secret_key` | string | `""` | AWS Bedrock secret key |
| `aws_session_token` | string | `""` | AWS session token (temporary creds) |
| `bedrock_model_id` | string | `global.anthropic.claude-sonnet-4-6` | Bedrock model ID for LLM connection |

### Agents Module (`terraform/agents/variables.tf`)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `mcp_server_url` | string | (required) | MCP server endpoint URL |
| `mcp_auth_token` | string | (required) | MCP server bearer token |
| `voyage_api_key` | string | (required) | Voyage AI API key |
| `voyage_api_endpoint` | string | `https://ai.mongodb.com/v1/embeddings` | Voyage embeddings endpoint URL (Flink + ASP both use this) |
| `mongodb_connection_string` | string | (required) | Atlas connection string |
| `mongodb_username` | string | (required) | Atlas database username |
| `mongodb_password` | string | (required) | Atlas database password |

## Kafka Topics

| Topic | Partitions | Producer | Consumer |
|-------|-----------|----------|----------|
| `ride_requests` | 6 | ShadowTraffic / publish_data | Flink (windowed_traffic view) |
| `windowed_traffic` | 6 | Flink (view materialization) | Flink (anomaly detection) |
| `anomalies_per_zone` | 6 | Flink (anomaly-detection-insert) | Flink (enrichment + dispatch) |
| `anomalies_enriched` | 6 | Flink (anomalies-enriched-insert) | ASP `anomalies_enriched_ingestion`: merges the LLM reason + `top_chunk_*` evidence onto `analytics.zone_anomalies` docs |
| `zone_traffic_sink` | 6 | Flink (zone-traffic-sink-insert) | ASP (zone_traffic_ingestion) |
| `anomalies_sink` | 6 | Flink (anomalies-sink-insert) | ASP (anomalies_ingestion) |
| `event_documents` | 6 | ASP (event_publication_to_kafka) | Flink (documents_vectordb) |
| `completed_actions` | 6 | Flink (dispatch-insert) | ASP (dispatch_log_ingestion) |

## Flink Compute Pool

| Setting | Value | Notes |
|---------|-------|-------|
| Max CFU | 50 | Supports concurrent execution of 7 DML statements + ad-hoc queries |
| Cloud | Matches deployment region | AWS |

## Anomaly Detection Parameters

| Parameter | Value | Effect |
|-----------|-------|--------|
| `minTrainingSize` | 15 | Minimum 15 one-minute windows (~15 min) per zone before anomaly output |
| `maxTrainingSize` | 7000 | Maximum historical windows for the model |
| `confidencePercentage` | 99.999 | Only flag very high-confidence anomalies |
| `enableStl` | false | STL decomposition disabled for simpler model |

## MCP Tool Configuration

| Setting | Value | Notes |
|---------|-------|-------|
| `request_timeout` | 15s | Per-tool-call timeout (reduced from 30s) |
| `max_iterations` | 10 | Maximum agent reasoning iterations |
| `allowed_tools` | `get_vessel_catalog, dispatch_boats` | Whitelisted MCP tools |

## Mission Control (UI)

Mission Control is the live HUD served by the SSE sidecar (`scripts/live_server.py`). `uv run deploy` launches it automatically; relaunch manually with `uv run live` (flags: `--host`, `--port`).

| Setting | Value |
|---------|-------|
| Port | 8502 (deploy picks the first free port in 8502-8510 and records it in `LIVE_SSE_URL`) |
| Host | `127.0.0.1` (override with `--host`) |
| Static UI | Serves `web/` same-origin on the same port |
| Endpoints | `GET /api/bootstrap` (warm-start payload), `GET /api/stream` (SSE change-stream events), `GET /api/health` (watcher status) |
| Collections watched (change streams) | `analytics.zone_anomalies`, `fleet.dispatch_log`, `analytics.zone_traffic`, `events.knowledge_base` |
| Collections read at bootstrap | The four watched collections plus `fleet.vessel_catalog` |
| CORS | Local origins on ports 8501/8502 allowed by default; extend via `LIVE_SSE_ALLOW_ORIGINS` (comma-separated) |
| RAG fallback | Backfills Atlas `$vectorSearch` evidence chunks onto anomaly docs the best-effort Flink enrichment missed; needs `TF_VAR_voyage_api_key`; head-start delay via `RAG_FALLBACK_DELAY_S` (default 40) |
| Logs | `logs/live-<port>.log` when launched by deploy |

The Streamlit dashboard (`uv run dashboard`, port 8501) is decommissioned as a product surface; it is kept only for manual legacy use and is no longer launched by deploy.
