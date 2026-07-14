# Architecture

This document explains the system design, why specific technologies were chosen, and how data flows through the streaming agents pipeline.

## System Overview

The streaming agents quickstart implements a real-time, closed-loop autonomous system: it detects demand surges, understands their cause, and takes corrective action, all without human intervention. The system operates on three planes:

1. **Data Plane:** Kafka topics carry ride requests, traffic aggregates, anomalies, and dispatch actions as continuous streams
2. **Compute Plane:** Flink SQL processes streams with windowed aggregation, ML anomaly detection, vector search, LLM inference, and agent tool calling
3. **Storage Plane:** MongoDB Atlas persists zone traffic, anomalies, dispatch logs, and the vector knowledge base

## Why This Is Agentic

The term "agentic" here is precise, not marketing. The dispatch system meets the three criteria that distinguish an agent from a pipeline:

### 1. Autonomous Decision-Making

The LLM inside `AI_RUN_AGENT` decides **which** boats to dispatch and **how many**. It evaluates:
- Surge magnitude (how many extra requests beyond expected)
- Vessel proximity to the target zone
- Boat capacity relative to demand
- Current availability status

No human approves these decisions. No hardcoded rule says "if surge > 2x, send 4 boats." The LLM reasons about the situation and makes a judgment call each time.

### 2. Tool Use (MCP)

The agent does more than generate text. It takes real actions via the Model Context Protocol:
- **`get_vessel_catalog`:** Queries the MongoDB fleet database for available vessels, their positions, capacities, and status
- **`dispatch_boats`:** Writes dispatch commands to MongoDB, updating vessel assignments

These are real database operations, not simulated. The MCP server runs on AWS ECS Express Mode and proxies requests to the MongoDB MCP Server.

### 3. Closed-Loop Execution

The agent's actions have observable consequences:
- Dispatched boats appear in `fleet.dispatch_log` via ASP
- The vessel catalog is updated, so subsequent dispatches see the new state
- Mission Control (the live HUD) shows the dispatch in real time, as a pure projection of Atlas change streams

This creates a feedback loop: the agent's past decisions change the world state that future decisions operate on.

## Parallel Pipeline Architecture

The system runs two parallel paths from anomaly detection, optimizing for both **insight** (explaining anomalies) and **action** (dispatching boats):

```
                                ride_requests (Kafka)
                                        |
                            [TUMBLE 1-min window aggregation]
                                        |
                                windowed_traffic (View)
                                        |
                            [ML_DETECT_ANOMALIES where is_surge=true]
                                        |
                                anomalies_per_zone (Kafka)
                                   /              \
                                  /                \
                 PATH A: Insight                    PATH B: Dispatch
                 (RAG Enrichment)                   (Agent Action)
                        |                                  |
           [Voyage AI embedding]                  [AI_RUN_AGENT]
                        |                           /         \
           [VECTOR_SEARCH_AGG]            [get_vessel_catalog] [dispatch_boats]
                        |                           \         /
           [LLM explanation]                  completed_actions (Kafka)
                        |                                  |
              anomalies_enriched (Kafka)             [ASP processor]
                        |                                  |
                [ASP merge processor]              fleet.dispatch_log (Atlas)
                        |
      analytics.zone_anomalies (reason + chunks
      merged onto the doc the sink path wrote)

    In parallel, anomalies_per_zone feeds the sink path directly
    (so anomalies reach Atlas even if RAG enrichment fails):

    anomalies_per_zone --> [anomalies_sink] --> [ASP processor]
        --> analytics.zone_anomalies (Atlas) --> [Mission Control]
```

### Why Two Paths?

**Latency.** The RAG enrichment path (Path A) involves three sequential ML model calls:
1. Voyage AI embedding generation (~1-2s)
2. Vector search against the knowledge base (~1-2s)
3. LLM text generation for the explanation (~5-15s)

If dispatch waited for enrichment, every anomaly would take 20-50 seconds before a boat was dispatched. By reading directly from `anomalies_per_zone`, the dispatch agent fires within seconds of anomaly detection.

**Independence.** The dispatch decision doesn't need a prose explanation. It needs the raw data: which zone, how many requests, how far above expected. The agent's LLM handles its own reasoning.

## Component Architecture

### Confluent Cloud (Compute + Transport)

| Component | Purpose |
|-----------|---------|
| Kafka Cluster | 8 topics carrying all pipeline data |
| Flink Compute Pool | 50 CFU max, runs all SQL statements |
| Schema Registry | Avro schemas for all topics |
| Flink Connections | Bedrock LLM, MongoDB (vector search + MCP), Voyage AI |

### MongoDB Atlas (Storage + Intelligence)

| Component | Purpose |
|-----------|---------|
| `events.calendar` | Source events with zone, time, attendance data |
| `events.knowledge_base` | Voyage AI-embedded event documents for vector search |
| `analytics.zone_traffic` | Windowed traffic aggregates (via ASP) |
| `analytics.zone_anomalies` | Detected anomalies (synthesized reason first, then RAG reason + evidence chunks merged in when enrichment completes) |
| `fleet.dispatch_log` | Agent dispatch actions with summaries (via ASP) |
| Atlas Vector Search | `vector_index` on `events.knowledge_base` (queried from Flink via the `documents_vectordb` table) for RAG |
| Atlas Stream Processing | 5 processors for bidirectional data flow |
| Voyage AI | Embedding generation (1024-dim, `voyage-4` model) |

### MCP Server (Tool Execution)

The MongoDB MCP Server runs on AWS ECS Express Mode with a Node.js reverse proxy:

```
Flink AI_RUN_AGENT
      |
      v
[ALB: port 443, HTTPS]
      |
      v
[proxy.mjs: port 8080]    <-- Adds Accept header, fixes Content-Type
      |
      v
[mongodb-mcp-server: port 8000]  <-- Actual MCP tool execution
      |
      v
[MongoDB Atlas]            <-- get_vessel_catalog, dispatch_boats
```

The proxy exists because Flink's Spring AI MCP Client (v0.3.1) has content-type validation bugs: it rejects `text/plain` responses even on HTTP 202 acknowledgments. The proxy rewrites these to `application/json`.

### Atlas Stream Processing (5 Processors)

| Processor | Source | Destination | Purpose |
|-----------|--------|-------------|---------|
| `event_publication_to_kafka` | `events.calendar` | Kafka `event_documents` | Publish events for Flink |
| `zone_traffic_ingestion` | Kafka `zone_traffic_sink` | `analytics.zone_traffic` | Sink traffic aggregates |
| `anomalies_ingestion` | Kafka `anomalies_sink` | `analytics.zone_anomalies` | Sink anomalies |
| `dispatch_log_ingestion` | Kafka `completed_actions` | `fleet.dispatch_log` | Sink dispatch actions |
| `anomalies_enriched_ingestion` | Kafka `anomalies_enriched` | `analytics.zone_anomalies` | Merge RAG reason + evidence chunks onto anomaly docs |

> **Note:** Knowledge-base embedding is no longer an ASP processor. ASP `$https`
> calls to the Voyage endpoint fail with HTTP 400, so `events.knowledge_base` is
> embedded and seeded in Python at deploy time by `populate_knowledge_base()`
> (`scripts/asp_setup.py`).

### Mission Control (UI)

Mission Control is the single UI, launched by `uv run deploy` (or manually via `uv run live`). It is a FastAPI sidecar (`scripts/live_server.py`) that serves the static SPA in `web/` same-origin on port 8502, so there is no CORS to misconfigure:

```
MongoDB Atlas ──(cluster-level change stream, watcher thread)──▶ ChangeStreamHub
                                                                      |
                                        GET /api/stream (SSE) ──▶ browser (web/)
```

- **`GET /api/bootstrap`** warm-starts the page: vessels, recent anomalies, dispatches, knowledge-base cards, traffic, and collection counts in one payload.
- **`GET /api/stream`** is a Server-Sent Events stream; each Atlas change-stream event on a watched collection (`analytics.zone_anomalies`, `fleet.dispatch_log`, `analytics.zone_traffic`, `events.knowledge_base`) is fanned out to every connected browser.
- **`GET /api/health`** reports the change-stream watcher's state.
- The watcher holds a cluster-level change stream and reconnects with exponential backoff (1s → 30s cap) if Atlas drops the connection; the browser's EventSource auto-reconnects and drives the LIVE / RECONNECTING / OFFLINE badge.

The UI is a pure projection of database writes: nothing is staged client-side, so what appears on screen is evidence that the pipeline actually wrote to Atlas.

The server also runs a **RAG fallback worker**: the Flink enrichment statement (`anomalies-enriched-insert`) is best-effort. Its per-anomaly federated vector search can time out and kill the statement. When an anomaly document still has no `top_chunk_*` evidence ~40 s after landing, the worker performs the same retrieval directly (Voyage query embedding + Atlas `$vectorSearch` on `events.knowledge_base`) and `$set`s the chunks onto the document (`enriched_by: rag-fallback`), which the change stream then pushes to the browser. The worker writes real vector-search results to the database; the UI still only renders database state. It requires `TF_VAR_voyage_api_key` and is skipped (with a log line) otherwise.

## Flink Statement Management

Statements are managed at two levels:

### Terraform-Managed (DDL)

11 `confluent_flink_statement` resources in the `agents` module:
- Table definitions (ride_requests, anomalies_per_zone, zone_traffic_sink, anomalies_sink)
- Connection definitions (MongoDB vector search, MongoDB MCP, Voyage AI)
- Model definitions (mongodb_mcp_model, voyage_query_embedding). The `llm_textgen_model` definition lives in the `core` module
- View definition (windowed_traffic)

These are idempotent (`CREATE IF NOT EXISTS`) and managed by `terraform apply`.

### REST API-Managed (DML)

7 statements created by `deploy.py` via the Flink REST API:

| Statement | Type | Expected State | Purpose |
|-----------|------|---------------|---------|
| `anomalies-enriched-ctas` | DDL | COMPLETED | Create anomalies_enriched table |
| `completed-actions-ctas` | DDL | COMPLETED | Create completed_actions table |
| `zone-traffic-sink-insert` | DML | RUNNING | Sink traffic to Kafka |
| `anomaly-detection-insert` | DML | RUNNING | ML anomaly detection |
| `anomalies-enriched-insert` | DML | RUNNING | RAG enrichment pipeline |
| `anomalies-sink-insert` | DML | RUNNING | Sink anomalies to Kafka |
| `dispatch-insert` | DML | RUNNING | Agent dispatch (reads anomalies_per_zone) |

These are long-running streaming jobs that don't fit Terraform's plan/apply lifecycle. They are deleted on `uv run destroy` and recreated on `uv run deploy`.

> **Note:** `anomalies-enriched-insert` is best-effort. It passes `MAP['client_timeout', 120, 'retry_count', 6]` to `VECTOR_SEARCH_AGG`, but the statement can still go FAILED on a vector-search timeout. Because `anomalies-sink-insert` reads directly from `anomalies_per_zone`, anomalies keep reaching Atlas (and Mission Control) even while enrichment is down. Recover with `uv run datagen` (restarts the DML statements) or by resuming deploy's `flink_dml` phase.

## Deployment Order

The deployment sequence is ordered to handle credential propagation and dependency chains:

```
1. MCP Server Deploy (ECS Express)     → .env gets URL + token
2. Terraform Apply (core)              → Creates Kafka, Flink pool, connections, models
3. Terraform Apply (agents)            → Creates DDL tables/views in Flink catalog
4. Save Terraform Credentials          → Kafka/SR creds written to .env
5. Publish Initial Data                → Registers schemas, propagates Kafka auth (~30s)
6. ASP Setup                           → Also seeds events + embeds knowledge base (Python)
7. Create Flink DML Statements         → Pre-creates topics, DDL first, then DML
8. Launch Mission Control              → Port 8502
```

## Data Model

### ride_requests (Source)

```
pickup_zone: STRING
dropoff_zone: STRING
number_of_passengers: INT
price: DOUBLE
request_ts: TIMESTAMP(3) WITH WATERMARK
```

### anomalies_per_zone (Anomaly Detection Output)

```
pickup_zone: STRING
window_time: TIMESTAMP(3)
request_count: BIGINT
total_passengers: BIGINT
total_revenue: DECIMAL(10,2)
expected_requests: BIGINT
upper_bound: DOUBLE
lower_bound: DOUBLE
is_surge: BOOLEAN
```

### completed_actions (Agent Dispatch Output)

```
pickup_zone: STRING
window_time: TIMESTAMP(3)
request_count: BIGINT
anomaly_reason: STRING
dispatch_summary: STRING
dispatch_json: STRING
api_response: STRING
```

## Design Decisions

### Why Flink SQL (not a Python orchestrator)?

The entire pipeline (windowed aggregation, anomaly detection, RAG, and agent dispatch) runs as native Flink SQL. This means:
- No external orchestrator to maintain
- Processing scales with Flink's parallelism (50 CFU pool)
- Exactly-once guarantees from Flink's checkpointing
- The agent is a streaming operator, not a batch job

### Why MongoDB MCP (not Zapier)?

This project uses a direct MongoDB MCP server (rather than a third-party proxy such as Zapier) because:
- No external account dependency (self-hosted on ECS)
- Purpose-built tools (`get_vessel_catalog`, `dispatch_boats`) vs generic webhooks
- Lower latency (direct MongoDB access vs Zapier → Lambda → MongoDB)
- Simpler for workshop participants (no Zapier setup)

### Model Configuration

Both the RAG explanation model (`llm_textgen_model`, defined in the `core` module) and the dispatch agent model (`mongodb_mcp_model`, defined in the `agents` module) use the **same** Bedrock connection and therefore the same LLM. The default is `global.anthropic.claude-sonnet-4-6` (Sonnet 4.6 via the cross-region inference profile); override it by setting `TF_VAR_bedrock_model_id` in `.env` before deploying (see [CONFIGURATION.md](CONFIGURATION.md)).

### Why Split DDL and DML?

`CREATE TABLE IF NOT EXISTS ... AS SELECT` (CTAS) in Confluent Cloud Flink returns COMPLETED immediately if the table already exists, without restarting the streaming INSERT. Splitting into separate DDL (CREATE TABLE) and DML (INSERT INTO) ensures re-deploys properly restart the streaming queries.

## Known Limitations / Future Work

- **Knowledge base uses single-chunk-per-document.** The `events.knowledge_base.chunk` field is currently a verbatim copy of `description`. Workshop events are short (well under Voyage AI's per-input token limit), so a single embedding per document is sufficient and the field name reflects intent. Production deployments with long-form event descriptions should implement real chunking (sentence splitter, 256-token windows with 32-token overlap) before the Voyage embedding call in `populate_knowledge_base()` (`scripts/asp_setup.py`), and store each chunk as a separate `knowledge_base` document keyed by `(document_id, chunk_index)`.
- **`ML_DETECT_ANOMALIES` cold start.** The Flink ML model requires `minTrainingSize=15` one-minute windows per zone (~15 minutes of data) before producing anomalies. The pre-generated 24-hour `ride_requests.jsonl` published during deploy satisfies this immediately; live ShadowTraffic from a cold start produces anomalies only after ~15 minutes.
