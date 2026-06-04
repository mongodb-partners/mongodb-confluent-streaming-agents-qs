# Troubleshooting

## Pipeline Issues

### No anomalies detected

**Cause:** The anomaly detection model requires at least 50 five-minute tumbling windows per zone (~4 hours 10 minutes of continuous data) before it starts producing output.

**Fix:**
1. Verify data is flowing: `SELECT COUNT(*) FROM ride_requests`
2. Ensure the pre-generated dataset covers enough time. Each batch file covers 24 hours (288 windows per zone), which satisfies the requirement.
3. If you just deployed, wait 5-10 minutes for Flink to process the windowed aggregation.
4. If publishing additional batches doesn't produce new anomalies, the batch data uses escalating surge multipliers (3x through 12x) specifically designed to always exceed the ML model's learned upper bound regardless of training history. Verify the batch files haven't been replaced with uniform data.

### No dispatches in fleet.dispatch_log

**Possible causes (check in order):**

1. **dispatch-insert statement not running.** Check Flink statement status:
   - Navigate to Confluent Cloud > Flink > SQL workspace
   - Run: `SHOW STATEMENTS` or check the Flink UI
   - If missing, click "Run Agent Dispatch" in the dashboard sidebar

2. **ASP dispatch_log_ingestion processor failed.** Check ASP processor status in Atlas UI under Stream Processing. If FAILED, restart it.

3. **No data in anomalies_per_zone.** The dispatch reads directly from `anomalies_per_zone`. If no anomalies exist yet, no dispatches will be triggered. See "No anomalies detected" above.

4. **MCP server unhealthy.** Test connectivity:
   ```bash
   curl -s https://<your-mcp-url>/mcp -H "Authorization: Bearer <token>"
   ```
   A 405 response is normal (MCP requires POST). A timeout or DNS error indicates the server is down.

5. **Timing issue.** If the dispatch statement was created AFTER anomalies were produced, it starts from the latest Kafka offset and won't see historical anomalies. Publish more data: `uv run publish_data --data-file assets/data/ride_requests.jsonl --force`

### anomalies-enriched-insert FAILED with "/ by zero"

**Cause:** The anomaly detection model occasionally outputs a row where `expected_requests` is 0. The percentage calculation `(request_count - expected_requests) / expected_requests * 100` crashes.

**Fix:** This is fixed in the current codebase (uses `NULLIF(expected_requests, 0)` with `COALESCE` fallback). If you see this on an older deployment:
1. Delete the failed statement in the Flink UI
2. Pull the latest code: `git pull`
3. Redeploy: `uv run deploy` (it will skip already-completed steps)

### Flink statement shows FAILED with "SourceInvalidValue (1200)"

**Cause:** Kafka topics contain data with stale Avro schema IDs from a previous deployment (different Confluent account or environment). Flink cannot deserialize messages with unrecognized schema IDs.

**Fix:** Delete and recreate topics:
```bash
uv run destroy   # Deletes topics and schemas
uv run deploy    # Recreates fresh topics
```

### Flink statement FAILED with "SubjectNotFoundException"

**Cause:** A DML statement reads from a topic that has no registered schema (topic is empty or was just created).

**Fix:** Publish data first to register schemas:
```bash
uv run publish_data --data-file assets/data/ride_requests.jsonl --force
```

### Flink statement FAILED with "Column 'pickup_zone' not found"

**Cause:** After topics are deleted and recreated, Confluent Cloud auto-registers them as raw-byte tables (`key VARBINARY, val VARBINARY`). These phantom catalog entries block Terraform's DDL from applying typed schemas.

**Fix:** Drop the Flink catalog tables, then re-run Terraform:
```sql
-- In Flink SQL workspace:
DROP TABLE IF EXISTS ride_requests;
DROP TABLE IF EXISTS windowed_traffic;
DROP TABLE IF EXISTS anomalies_per_zone;
DROP TABLE IF EXISTS anomalies_enriched;
DROP TABLE IF EXISTS zone_traffic_sink;
DROP TABLE IF EXISTS anomalies_sink;
DROP TABLE IF EXISTS completed_actions;
```
Then: `uv run deploy` (Terraform will recreate the DDL).

## MCP Server Issues

### MCP server returns 503 or "exec format error" in ECS logs

**Cause:** Docker image was built on Apple Silicon (ARM) but cached layers were reused, producing a mixed-architecture image that crashes on AMD64 ECS hosts.

**Fix:** The deploy script now uses `--no-cache --pull` for all builds. If you built manually:
```bash
docker buildx build --platform linux/amd64 --no-cache --pull -t <image> --push mcp-server/
```

### MCP server deploy fails with "service already exists" or "still draining"

**Cause:** ECS Express Mode reserves service names during DRAINING state (can last 30-60s after deletion).

**Fix:** The deploy script handles this automatically by retrying with a timestamp suffix. If running manually, wait 60 seconds or use a different service name.

### Flink AI_RUN_AGENT fails with "UnknownHostException"

**Cause:** The MCP server URL changed between deploys, but `CREATE CONNECTION IF NOT EXISTS` in Flink's catalog still points to the old URL. The stale connection propagates through the entire chain: connection -> model -> tool -> agent -> INSERT.

**Fix:**
```sql
-- Drop the entire cascade in Flink SQL workspace:
DROP AGENT IF EXISTS boat_dispatch_agent;
DROP TOOL IF EXISTS mongodb_fleet;
DROP MODEL IF EXISTS mongodb_mcp_model;
DROP CONNECTION IF EXISTS `mongodb-mcp-connection`;
```
Then run `uv run deploy` to recreate with the new URL.

### ALB health check fails (target unhealthy)

**Cause:** The MCP server returns 405 for GET requests. The ALB health check path must be configured to accept wide status codes.

**Fix:** The deploy script sets the health check to `GET /mcp` with matcher `200-499`. If manually created, update via AWS Console or:
```bash
aws elbv2 modify-target-group --target-group-arn <arn> \
  --health-check-path /mcp \
  --matcher HttpCode=200-499
```

### MCP URL returns 503 even though container is up

**Symptom:** ECS logs show "MCP proxy listening on :8080 -> :8000" and "Streamable HTTP Transport started", but `curl https://mo-...ecs.us-east-1.on.aws/mcp` returns HTTP 503.

**Cause:** ECS Express provisions blue/green target group pairs per service. The listener rule weighted-forwards across both. If only one TG was patched to `path=/mcp, matcher=200-499` and the other stayed on default `path=/, matcher=200`, the proxy returns 403 on `/`, the un-fixed TG never goes healthy, ECS Express never flips weights, and the URL serves 503.

**Fix:** As of 2026-05, `_fix_alb_health_check()` patches ALL target groups bound to port 8080 and re-runs every 60s during the health-wait loop. It also calls `_flip_listener_weights_to_registered_tg()` to manually flip listener weights to whichever TG has registered targets, in case ECS Express's own blue/green never succeeded. To fix manually:

```bash
# Find all ECS-gateway target groups with default health check
aws elbv2 describe-target-groups --region us-east-1 \
  --query 'TargetGroups[?starts_with(TargetGroupName, `ecs-gateway-tg-`) && HealthCheckPort==`8080` && HealthCheckPath==`/`].TargetGroupArn' \
  --output text | tr '\t' '\n' | while read arn; do
    aws elbv2 modify-target-group --target-group-arn "$arn" \
      --health-check-path /mcp --matcher HttpCode=200-499 --region us-east-1
done
```

Then check the listener rule and flip weights to a TG with registered targets if needed (`aws elbv2 describe-rules` / `aws elbv2 modify-rule`).

### `dispatch-insert` Flink statement fails with "Failed to initialize MCP client: 503"

**Cause:** MCP server was unhealthy (often the 503 issue above) when `dispatch-insert` was submitted.

**Fix:** As of 2026-05, deploy.py SKIPS submitting `dispatch-insert` when MCP probe fails (rather than submitting and guaranteeing FAILED). Once MCP is healthy:

```bash
uv run deploy   # Resumes from DEPLOY_PHASE=flink_dml; _submit_statement deletes the FAILED statement and recreates it.
```

Or click "Run Agent Dispatch" in the Streamlit dashboard.

### `anomalies-enriched-insert` fails with `SourceInvalidValue (1200)` after deploy

**Symptom:**
```
SourceInvalidValue (1200): Error while deserializing value from topic
'anomalies_per_zone', partition X, offset N. Please ensure that the
declared value schema matches your records.
```

**Cause:** Stale Avro records left over from a previous deploy carry a schema ID that doesn't match the schema registered by the new deploy. Flink can't skip the offset without silently losing data, so the DML goes FAILED.

**Fix:** `_ensure_flink_topics()` deletes + recreates the four streaming-output topics (`windowed_traffic`, `anomalies_per_zone`, `zone_traffic_sink`, `anomalies_sink`) and their `-value`/`-key` Schema Registry subjects on every deploy, before submitting DDL/DML. If you hit this:

```bash
uv run python -c "
from pathlib import Path
from scripts.pipeline_reset import reset_pipeline, restart_flink_dml
reset_pipeline(Path('.'))
"
uv run publish_data --data-file assets/data/ride_requests.jsonl --force
uv run python -c "
from pathlib import Path
from scripts.pipeline_reset import restart_flink_dml
restart_flink_dml(Path('.'))
"
```

### `dispatch-insert` keeps failing with `Cannot find table 'completed_actions'`

**Cause:** A previous agent-dispatch retry path dropped the `completed_actions` table but failed to recreate it (e.g. MCP was unhealthy at recreate time). The table never came back.

**Fix:** The dashboard's "Run Agent Dispatch" button does just-in-time drops (only immediately before CREATEing the corresponding object) and stops the chain on the first error rather than half-applying. To recover, click "Run Agent Dispatch" — it will now correctly DROP + CREATE `completed_actions` in one transactional step.

### `terraform apply` fails on agents with "Permission denied to access the Schema Registry cluster"

**Symptom:**
```
Error: error waiting for Flink Statement "ride-requests-create-table" to provision:
Flink Statement "ride-requests-create-table" provisioning status is "FAILED":
Permission denied to access the Schema Registry cluster 'lsrc-...'
```

**Cause:** Confluent control-plane permission propagation lag. Service-account `EnvironmentAdmin` role-bindings can take 30–120s to propagate to Flink runtime; the first Flink statement that talks to Schema Registry hits this on a fresh deploy.

**Fix:** As of 2026-05, `scripts/common/terraform_runner.run_terraform()` auto-detects the propagation error pattern and retries up to 3 times with 45s/90s/120s backoff. Between retries, deploy.py sweeps server-side orphan FAILED Flink statements (otherwise the next CREATE 409s on the leftover). If you hit this on an older codebase or after 3 retries:

```bash
# Wait 2 minutes, then re-run:
uv run deploy
```

## Atlas Stream Processing Issues

### ASP setup fails with "No cluster named X in group Y"

**Symptom:** During `uv run deploy` you see three near-identical errors in the ASP setup phase:

```
✗ create connection 'atlas_cluster': 400 ... "No cluster named conf-mdb in group 66c5bd..."
✗ create connection 'events_dlq':   400 ... (same)
✗ create connection 'fleet_dlq':    400 ... (same)
```

**Cause:** `ATLAS_CLUSTER_NAME` in `.env` points at a cluster that does not exist in the configured Atlas project. All three connections (`atlas_cluster`, `events_dlq`, `fleet_dlq`) reference the same cluster name, so a typo or stale name produces the same error three times.

**Diagnose:** Run the preflight directly:

```bash
uv run preflight --phase asp_setup
```

You'll get an actionable result listing the project's actual clusters:

```
[FAIL] atlas_cluster_exists : cluster 'conf-mdb' not found in project 66c5bd...
      → available clusters: langchain-agent-log, solutions-library, mongodb-non-prod.
        Update ATLAS_CLUSTER_NAME in .env (and TF_VAR_mongodb_connection_string to
        match), OR set TF_VAR_create_atlas_cluster=true to provision a fresh M10
        via Terraform.
```

**Fix (choose one):**

- **Use an existing cluster** (must be M10 or higher, with IP allowlist + DB user configured):

  ```bash
  # Edit .env:
  ATLAS_CLUSTER_NAME='<one of the available clusters>'
  TF_VAR_mongodb_connection_string='mongodb+srv://<name>.<suffix>.mongodb.net'
  uv run deploy   # resumes from asp_setup
  ```

- **Provision a fresh M10 via Terraform** (takes 7–15 min):

  ```bash
  # Edit .env:
  TF_VAR_create_atlas_cluster=true
  uv run deploy   # provisions cluster, then resumes asp_setup
  ```

This preflight was added so the failure surfaces in <2 seconds with a single actionable message instead of after creating the ASP instance.

### ASP processor in FAILED state

**Common causes:**
- Kafka credentials haven't propagated yet (takes ~30s after API key creation)
- Kafka topic doesn't exist
- Schema Registry connection failed

**Fix:** Restart the processor:
```bash
uv run asp-setup  # Re-runs setup, starts failed processors
```

### events.knowledge_base is empty

**Check:**
1. Verify `events.calendar` has the 10 seed events
2. Verify the `event_knowledge_base_population` processor is STARTED in Atlas UI
3. Check `events.validation_dlq` for failed documents
4. Verify Voyage AI is enabled on your Atlas project

### dispatch_log_ingestion processor keeps failing

**Cause:** The processor subscribes to `completed_actions` topic. If no messages exist on the topic when the processor starts, and the Kafka connection has auth issues, it may fail immediately.

**Fix:**
1. Verify the `completed_actions` topic exists in Confluent Cloud
2. Verify the Kafka credentials in the ASP connection are valid
3. Restart: run `uv run asp-setup` (it's idempotent)

## Deployment Issues

### deploy hangs at "Waiting for DML statements to reach RUNNING"

**Cause:** DML statements may be stuck in PENDING due to insufficient compute resources or dependency on a DDL that hasn't completed.

**Fix:**
1. Check statement status in the Flink UI
2. If DDL shows FAILED, fix the DDL first
3. If all statements are PENDING, the compute pool may be at capacity. Wait or increase `max_cfu` in `terraform/core/main.tf`

### deploy fails with "Missing terraform outputs"

**Cause:** Core Terraform apply failed or was partially applied.

**Fix:**
```bash
cd terraform/core
terraform init
terraform apply -auto-approve
```
Then re-run `uv run deploy`.

### "Terraform not found" error

**Fix:** Install Terraform:
```bash
brew tap hashicorp/tap && brew install hashicorp/tap/terraform
```

### Dashboard shows "Cannot load Flink credentials"

**Cause:** The `terraform/core/terraform.tfstate` file doesn't exist or doesn't contain the expected outputs.

**Fix:** Run `uv run deploy` at least through the Terraform step, or verify the tfstate file exists.

### Deploy says "Dashboard running at http://localhost:8501" but the URL refuses to connect

**Cause (pre-2026-05):** The launcher used `subprocess.Popen` with `stderr=DEVNULL`, then printed "running" after a 2s sleep without verifying the port bound. If Streamlit crashed during startup, the user got a misleading success message.

**Fix:** As of 2026-05, `_launch_dashboard()` polls `127.0.0.1:<port>` for up to 20s and only prints "[ok] running" once the port actually accepts connections. Streamlit logs go to `logs/dashboard-<port>.log`. The child runs with `start_new_session=True` so it survives the parent shell. If the dashboard fails:

```bash
# Inspect the log:
tail -50 logs/dashboard-8501.log

# Or run Streamlit in the foreground to see startup errors:
uv run dashboard
```

## Data Issues

### Dashboard shows empty charts

**Cause:** Time filter is set to "Last 24 hours" but data timestamps are older.

**Fix:** Change the time filter to "All time" in the dashboard sidebar.

### publish_data returns exit code 1

**Cause:** Topic already has messages. This is a safety check to prevent duplicate data.

**Fix:** Use `--force` flag:
```bash
uv run publish_data --data-file assets/data/ride_requests.jsonl --force
```

### Schema Registry key subjects cause extra columns

**Cause:** ShadowTraffic registers both `-value` and `-key` Avro subjects. If the `-key` subject survives a pipeline reset, Flink reconstructs the table with an extra `key` column.

**Fix:** Delete both subject suffixes via the Schema Registry API, or run a full `uv run destroy` + `uv run deploy`.

## Common Error Messages

| Error | Meaning | Fix |
|-------|---------|-----|
| `Unsupported configuration options found` | Tried to set `scan.startup.mode` (not supported in Confluent Cloud Flink) | Remove the unsupported property |
| `Unknown media type returned: text/plain` | MCP server responding without the proxy | Verify proxy.mjs is running on port 8080 |
| `only scalar functions can be used in projection` | Used `SELECT AI_RUN_AGENT(...)` | Must use `SELECT * FROM TABLE(AI_RUN_AGENT(...))` |
| `The window function requires the timecol is a time attribute type` | Table missing WATERMARK definition | `ALTER TABLE ride_requests MODIFY (WATERMARK FOR request_ts AS request_ts - INTERVAL '5' SECOND)` |
| `Runtime received bad response code 403` | LLM access denied | Activate the configured model in AWS Bedrock |
