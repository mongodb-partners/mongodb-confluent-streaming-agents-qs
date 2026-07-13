# MongoDB + Confluent Streaming Agents Quickstart

Real-time anomaly detection and autonomous fleet dispatch powered by **MongoDB Atlas** and **Confluent Cloud**. A fully agentic system that detects demand surges, understands their cause via Retrieval-Augmented Generation (RAG), and autonomously dispatches boats. Everything runs as native Flink SQL streaming operators.

<p align="center">
  <img src="./assets/MongoDB_ConfluentJointSolution.png" width="768" alt="Architecture">
</p>

## What Makes This Agentic

When Flink detects a demand surge, the system decides what to do on its own:

1. The LLM **queries the vessel catalog** via MCP tool calls to see what boats are available
2. It **reasons about** proximity, capacity, and surge magnitude
3. It **decides** which boats to dispatch and how many (up to 8 for large surges)
4. It **executes** the dispatch via MCP, updating the MongoDB fleet database

No human approves these decisions. No hardcoded rules determine allocation. The agent evaluates each situation independently using real-time fleet state.



## Pipeline Architecture

```
ride_requests → [1-min window aggregation] → [ML_DETECT_ANOMALIES]
                                                      |
                                              anomalies_per_zone
                                                 /           \
                              PATH A: Explain                 PATH B: Act
                              (RAG enrichment)                (Agent dispatch)
                                     |                              |
                         [Voyage AI embedding]               [AI_RUN_AGENT]
                         [Vector Search]                      /           \
                         [LLM explanation]           [get_vessel_catalog] [dispatch_boats]
                                     |                              |
                          anomalies_enriched                completed_actions
                                     |                              |
                          [ASP → Atlas]                     [ASP → Atlas]
                      analytics.zone_anomalies          fleet.dispatch_log
```

**Path A** (display) enriches anomalies with context from the knowledge base for dashboards.
**Path B** (dispatch) acts immediately on raw anomaly data. It does not wait for RAG to complete.

## Prerequisites

**macOS:**
```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli docker-desktop
```

**Windows:**
```powershell
winget install astral-sh.uv Git.Git Docker.DockerDesktop Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

**Accounts and credentials needed:**
- **Confluent Cloud** account with API key/secret
- **LLM Access:** AWS Bedrock API keys
- **MongoDB Atlas** M10+ cluster with ASP and Voyage AI enabled
- **Voyage AI API Key** from your Atlas project settings
- **Atlas Admin API Key** (public/private key pair with Project Owner permissions)
- **AWS credentials** for MCP server deployment (Docker + AWS CLI required)

## Quick Start

```bash
git clone https://github.com/mongodb-partners/mongodb-confluent-streaming-agents-qs.git
cd mongodb-confluent-streaming-agents-qs
confluent login
uv run deploy
```

Or, for fully unattended runs (CI/CD, jump-hosts) when `.env` is already populated or all required credentials are exported as environment variables:

```bash
uv run deploy --non-interactive    # alias: -y
```

See [docs/CONFIGURATION.md § Non-interactive deploys](docs/CONFIGURATION.md#non-interactive-deploys) for the full required-keys list, exit codes, and CI/CD patterns.

The deploy wizard prompts for all credentials and handles the complete setup:
- Deploys MongoDB MCP Server to AWS ECS Express Mode
- Provisions Confluent Cloud infrastructure (Kafka, Flink 50-CFU pool, Schema Registry)
- Deploys 14+ Flink SQL DDL resources via Terraform
- Creates 7 Flink streaming statements via REST API (2 DDL + 5 DML)
- Sets up Atlas Stream Processing with 5 processors
- Seeds the event knowledge base with 10 events across all zones (embeddings computed at seed time via Voyage AI)
- Publishes initial ride data to bootstrap the pipeline
- Launches Mission Control (the UI) on port 8502

### Mission Control (the UI)

`uv run live` serves **Mission Control** at http://localhost:8502 — a real-time
HUD driven entirely by MongoDB Atlas change streams: an animated dispatch map
(boats follow the real Mississippi centerline), a sense→reason→act pipeline rail
that pulses as events land, the surge queue with per-zone traffic charts and the
event knowledge base, the agent's reasoning with its Atlas Vector Search
context, and stage banners when a surge is detected and the agent dispatches.
A built-in guided tour (the `? Tour` button) walks first-time viewers through
every panel. Trigger the whole loop on cue with `uv run surge`.

### Verify your deployment

Two commands prove the whole pipeline is up:

```bash
uv run health    # one-shot report: Flink statements, ASP processors, Kafka topics, Atlas collections
uv run surge     # trigger a demand surge and watch Mission Control react end to end
```

A healthy deployment shows `Overall: HEALTHY` from `uv run health`, and within
about two minutes of `uv run surge` Mission Control (http://localhost:8502)
displays the full loop: SURGE DETECTED banner → agent reasoning with Vector
Search evidence → AGENT DISPATCHING → boats moving on the map. A screenshot of
that screen, or your `uv run health` output, is your proof of a working
deployment. If anything reports unhealthy, see
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

### Generate continuous data (optional)

```bash
# Live streaming via ShadowTraffic (requires Docker)
uv run datagen

# Lightweight mode (no Docker required)
uv run datagen --local
```

## Commands

| Command | Description |
|---------|-------------|
| `uv run deploy` | Interactive deployment wizard (one-command setup) |
| `uv run deploy -y` | Non-interactive deploy (alias for `--non-interactive`); reads credentials from `.env` or env vars, exits 2 on missing |
| `uv run destroy` | Teardown all resources (Flink, ASP, MCP, Terraform) |
| `uv run datagen` | Start ShadowTraffic data generation |
| `uv run datagen --local` | Publish pre-generated data (no Docker) |
| `uv run asp-setup` | Provision ASP resources (standalone) |
| `uv run mcp-deploy` | Deploy MCP server to ECS Express |
| `uv run mcp-deploy --destroy` | Tear down MCP server |
| `uv run live` | Launch Mission Control — the UI (HUD + SSE, port 8502) |
| `uv run surge` | Trigger a deterministic, window-aligned demand surge |
| `uv run publish_data --data-file <path> --force` | Publish ride data to Kafka |
| `uv run health` | Single-command pipeline health report (Flink + ASP + Kafka + Atlas) |
| `uv run preflight` | Phase-aware connectivity probes (`--phase X`, `--json`) |

## Configuration

The LLM model is configurable via `.env`:

```bash
# Default: Sonnet 4.6 via the cross-region 'global' inference profile
TF_VAR_bedrock_model_id='global.anthropic.claude-sonnet-4-6'

# Alternative: Haiku 4.5 (fast, cheaper)
TF_VAR_bedrock_model_id='anthropic.claude-haiku-4-5-20251001-v1:0'
```

The Voyage AI embedding endpoint is also configurable (default points at
MongoDB Atlas's hosted proxy):

```bash
# Default: MongoDB Atlas-hosted Voyage proxy (recommended)
TF_VAR_voyage_api_endpoint='https://ai.mongodb.com/v1/embeddings'

# Alternative: direct Voyage AI endpoint
# TF_VAR_voyage_api_endpoint='https://api.voyageai.com/v1/embeddings'
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full environment variable reference.

## Project Structure

```
scripts/
  deploy.py              Interactive deployment wizard
  destroy.py             Resource teardown
  mcp_deploy.py          MCP server ECS Express deployment
  asp_setup.py           Atlas Stream Processing provisioning
  live_server.py         Mission Control server (SSE + static HUD + bootstrap)
  dashboard.py           Legacy Streamlit dashboard (decommissioned; manual use only)
  datagen.py             ShadowTraffic data generation
  pipeline_reset.py      Pipeline reset (cleanup + restart)
  publish_data.py        Kafka data publisher
  common/                Shared utilities (terraform, tfvars, credentials)
terraform/
  core/                  Confluent Cloud infrastructure (Kafka, Flink, connections)
  agents/                Flink SQL DDL resources (tables, models, views)
    sql/                 SQL templates for REST API-managed statements
mcp-server/
  Dockerfile             MCP server image (Node.js + proxy)
  proxy.mjs              Flink compatibility proxy
assets/data/             Pre-generated ride data (10 batches with escalating surges)
testing/e2e/             890+ structural/offline tests across 35 files
docs/
  ARCHITECTURE.md        System design and data flow explanation
  CONFIGURATION.md       Environment variable reference
  TROUBLESHOOTING.md     Common issues and fixes
```

## Documentation

| Document | Content |
|----------|---------|
| [WALKTHROUGH.md](WALKTHROUGH.md) | Step-by-step guide through the pipeline |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data flow, design decisions |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | All environment variables and Terraform settings |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues, error messages, and fixes |

## Cleanup

```bash
uv run destroy
```

Destroys all resources in reverse order: Flink statements, Kafka topics, ASP processors, MCP server, Terraform infrastructure.

## License

Apache-2.0
