# Atlas-Enhanced Agentic Fleet Management: Walkthrough

This walkthrough guides you through the full streaming agents pipeline: real-time anomaly detection, RAG enrichment with Vector Search, autonomous agent dispatch, and bidirectional data flow between Confluent Cloud and MongoDB Atlas.

### What This Demo Showcases

1. **Flink MongoDB Sinks:** Zone traffic aggregates and enriched anomalies flow from Flink into Atlas collections (`analytics.zone_traffic`, `analytics.zone_anomalies`), enabling real-time dashboards and historical analysis.
2. **Atlas Stream Processing (ASP):** Five ASP processors handle event ingestion, embedding generation, dispatch log capture, zone traffic ingestion, and anomaly ingestion, all running natively on Atlas.
3. **Voyage AI Embeddings:** Events are embedded using MongoDB's Atlas-hosted Voyage AI endpoint (`ai.mongodb.com`) via both ASP (document embedding) and Flink (query embedding), producing aligned 1024-dimension vectors.
4. **Enhanced Vector Search:** The knowledge base uses an enriched schema with structured metadata (zone, event type, impact level, attendance) for pre-filtered vector search.

## Prerequisites

**Installation instructions:**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli docker-desktop
```

**Windows:**
```powershell
winget install astral-sh.uv Git.Git Docker.DockerDesktop Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

Once software is installed, you'll need:
- **LLM Access:** AWS Bedrock API keys
- **MongoDB Atlas:** M10+ cluster with ASP and Voyage AI enabled on your Atlas project
- **Voyage AI API Key:** Available from your MongoDB Atlas project settings
- **Atlas Admin API Key:** Public/private key pair with Project Owner permissions
- **AWS credentials:** For MCP server auto-deployment (Docker + AWS CLI must be installed)

> [!WARNING]
>
> **AWS Bedrock Users:** You must enable model access for the configured LLM. The default is Claude Sonnet 4.6 (via the cross-region `global.` inference profile). Visit the [Model Catalog](https://console.aws.amazon.com/bedrock/home#/model-catalog), find your configured model, and request access if needed. To change the model, set `TF_VAR_bedrock_model_id` in `.env` (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md)).

## Deploy

First, clone the repo:

```bash
git clone https://github.com/mongodb-partners/mongodb-confluent-streaming-agents-qs.git
cd mongodb-confluent-streaming-agents-qs
```

Run the deployment script:

```bash
uv run deploy
```

The deployment script will prompt you for:
- AWS Bedrock API keys
- MongoDB Atlas connection string, username, and password
- **Atlas Admin API keys** (public key, private key, project ID, cluster name)
- Voyage AI API key

The deploy script handles the complete setup in one pass:

1. **MCP Server:** MongoDB MCP Server is auto-deployed to AWS ECS Express Mode with a compatibility proxy
2. **Terraform** deploys 14+ Flink SQL DDL resources (connections, tables, models, views)
3. **Credentials:** Kafka and Schema Registry credentials are saved to `.env` for CLI tools
4. **Atlas Stream Processing** is provisioned automatically after Terraform completes:
   - Creates an ASP stream processing instance (SP10)
   - Registers 5 connection entries (Kafka, Atlas cluster, Voyage AI, two DLQ connections)
   - Pre-creates required Kafka topics (`event_documents`, `completed_actions`, `zone_traffic_sink`, `anomalies_sink`)
   - Starts 5 stream processors: `event_knowledge_base_population`,
     `event_publication_to_kafka`, `dispatch_log_ingestion`,
     `zone_traffic_ingestion`, `anomalies_ingestion`
   - Seeds 10 events into `events.calendar`
5. **Flink streaming statements:** 7 statements are created via the Flink REST API:
   - `anomalies-enriched-ctas` (DDL): Creates the `anomalies_enriched` table
   - `completed-actions-ctas` (DDL): Creates the `completed_actions` table
   - `zone-traffic-sink-insert`: Sinks windowed traffic to MongoDB
   - `anomaly-detection-insert`: Runs `ML_DETECT_ANOMALIES` anomaly detection
   - `anomalies-enriched-insert`: RAG enrichment pipeline (embedding → vector search → LLM)
   - `anomalies-sink-insert`: Sinks enriched anomalies to MongoDB
   - `dispatch-insert`: Agent dispatch (reads directly from anomalies_per_zone)
6. **Initial data:** Pre-generated ride data is published to bootstrap the pipeline
7. **Dashboard:** The Streamlit dashboard launches automatically

> **Manual fallback:** If ASP setup fails or you need to run it separately:
> ```bash
> uv run asp-setup \
>     --atlas-public-key <your-public-key> \
>     --atlas-private-key <your-private-key> \
>     --project-id <your-atlas-project-id> \
>     --cluster-name <your-cluster-name> \
>     --confluent-bootstrap-server <bootstrap-server> \
>     --confluent-api-key <confluent-api-key> \
>     --confluent-api-secret <confluent-api-secret> \
>     --voyage-api-key <your-voyage-api-key>
> ```

## Usecase Walkthrough

### Data Generation

The deploy script automatically publishes an initial batch of pre-generated ride data to bootstrap the pipeline. For continuous streaming, make sure **Docker Desktop** is running, then:

```bash
# live streaming via ShadowTraffic (requires Docker)
uv run datagen

# or, lightweight mode (no Docker required)
uv run datagen --local
```

The data generator produces a `ride_requests` stream: incoming boat ride requests with pickup zones and drop-off zones.

### 1. Verify zone traffic is flowing to Atlas

The deployment automatically writes windowed traffic aggregates into MongoDB. After data generation has been running for a few minutes, check the `analytics.zone_traffic` collection in Atlas:

```javascript
// In MongoDB Atlas Data Explorer or mongosh
use analytics
db.zone_traffic.find().sort({ window_start: -1 }).limit(5)
```

You should see 5-minute windowed aggregates with `zone`, `request_count`, `total_passengers`, and `total_revenue` fields.

### 2. Visualize anomaly detection

In the [Flink UI](https://confluen.cloud/go/flink), select your environment and open a SQL workspace. Verify that the anomaly detection pipeline is running:

```sql
SELECT * FROM anomalies_per_zone;
```

The deployment continuously detects anomalies using `ML_DETECT_ANOMALIES` across 5-minute tumbling windows and writes results to `anomalies_per_zone`. You should see anomalies detected in the `French Quarter` zone after about 5 minutes of data generation.

### 3. Verify ASP pipelines are processing events

The ASP setup seeded 10 events into `events.calendar`. The five ASP processors handle them automatically (`event_knowledge_base_population`, `event_publication_to_kafka`, `dispatch_log_ingestion`, `zone_traffic_ingestion`, `anomalies_ingestion`):

**`event_knowledge_base_population` (Event Knowledge Base Population):**
```javascript
// Check that events have been embedded and stored with Voyage AI vectors
use events
db.knowledge_base.find({}, { event_name: 1, zone: 1, embedding: { $slice: 3 } })
```

Each document should have a 1024-dimension `embedding` array generated by Voyage AI, plus structured metadata fields (`event_name`, `zone`, `venue`, `expected_attendance`, `event_type`, `impact_level`).

**`event_publication_to_kafka` (Event Publication to Kafka):**

In the Flink UI, verify the `event_documents` topic received the published events:

```sql
SELECT * FROM event_documents;
```

**`dispatch_log_ingestion` (Dispatch Log Ingestion)** will activate after the agent dispatches boats (Step 6).

### 4. Test Voyage AI query embedding model

Verify the Flink Voyage AI integration:

```sql
SELECT * FROM TABLE(ML_PREDICT('voyage_query_embedding', 'test embedding'));
```

This should return a 1024-dimension float array from the `voyage-4` model via `ai.mongodb.com`. The dimensions match the ASP-embedded documents in `events.knowledge_base`, ensuring vector search alignment.

### 5. Enrich anomalies with context using enhanced vector search

Once anomalies are detected and the knowledge base is populated, the RAG enrichment pipeline runs automatically. It uses `voyage_query_embedding` for query-time embeddings and searches the `documents_vectordb` table. The enriched results appear in the `anomalies_enriched` table:

```sql
SELECT * FROM anomalies_enriched;
```

The enrichment pipeline:
- Embeds each anomaly's context using `voyage_query_embedding`
- Performs `VECTOR_SEARCH_AGG` against the knowledge base
- Uses `llm_textgen_model` to generate human-readable explanations

### 6. Define and run the streaming agent

> **Note:** The deploy script now automatically creates the `dispatch-insert` statement, which reads directly from `anomalies_per_zone` and dispatches boats without waiting for RAG enrichment. This is the **parallel dispatch path**; it fires within seconds of anomaly detection.
>
> The dashboard button is available for manual triggering or re-creation if needed.

The agent tools and agent definition are created by Terraform (in the `agents` module). To inspect them in the Flink SQL workspace:

```sql
SHOW TOOLS;
SHOW AGENTS;
```

The tool definition:
```sql
CREATE TOOL mongodb_fleet
USING CONNECTION `mongodb-mcp-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'get_vessel_catalog, dispatch_boats',
  'request_timeout' = '15'
);
```

```sql
CREATE AGENT `boat_dispatch_agent`
USING MODEL `mongodb_mcp_model`
USING PROMPT 'You are an intelligent boat dispatch coordinator for a riverboat ride-sharing service.

Your workflow:
1. ANALYZE the surge information provided (zone, time, request count, anomaly reason)
2. REVIEW the available vessels list using the get_vessel_catalog tool
3. SELECT appropriate boats to dispatch based on:
   - Proximity to the target zone
   - Boat capacity
   - Current availability
   - Surge magnitude (dispatch up to 8 boats for large surges)
4. USE the dispatch_boats tool to dispatch selected boats to the target zone.
   Pass the zone and an array of boats with vessel_id, new_zone, and new_availability.

5. FORMAT your final response with these THREE sections:

Dispatch Summary:
Due to the surge in demand in [zone] as a result of [event], we dispatched [n] additional boats from [list of zones].

Dispatch JSON:
{the dispatch_boats parameters you sent}

API Response:
{the response from the dispatch_boats tool}

CRITICAL INSTRUCTIONS:
- Dispatch boats from nearby zones first
- Dispatch more boats with larger capacities for big surges (up to 8 boats)
- Your response MUST contain the three labeled sections
- The dispatch JSON must be valid
- Always execute the dispatch and include the tool response
- Do NOT include any other explanatory text outside these three sections'
USING TOOLS `mongodb_fleet`
WITH (
  'max_iterations' = '10'
);
```

### 7. Verify the agent dispatch is running

The `dispatch-insert` statement is created automatically by `uv run deploy`. It reads directly from `anomalies_per_zone` (the parallel dispatch path):

```sql
-- View the running dispatch statement
SELECT * FROM completed_actions;
```

The dispatch INSERT reads anomaly data and passes it to `AI_RUN_AGENT`, which:
1. Queries `get_vessel_catalog` to see available boats
2. Reasons about which boats to dispatch based on zone, capacity, and surge magnitude
3. Calls `dispatch_boats` to execute the allocation
4. Returns a structured response with dispatch summary, JSON payload, and API response

> **Architecture note:** The dispatch reads from `anomalies_per_zone` directly (not `anomalies_enriched`). This is the parallel dispatch path: the agent acts immediately on raw anomaly data without waiting for the RAG enrichment step to complete.

View the agent's dispatch actions:

```sql
SELECT * FROM completed_actions;
```

### 8. Verify dispatch log in Atlas

After the agent dispatches boats, the `dispatch_log_ingestion` ASP processor automatically captures the results in Atlas. It reads from the `completed_actions` Kafka topic using the Confluent Schema Registry for Avro deserialization.

```javascript
// In MongoDB Atlas Data Explorer or mongosh
use fleet
db.dispatch_log.find().sort({ dispatched_at: -1 }).limit(5)
```

You should see dispatch records with `pickup_zone`, `dispatch_summary`, `dispatch_json`, `api_response`, and `dispatched_at` timestamps.

> **Tip:** If `dispatch_log` remains empty while `completed_actions` has data in the Flink SQL shell, verify that the ASP `dispatch_log_ingestion` processor includes `schemaRegistry` in its `$source` stage.

### 9. Check enriched anomalies in Atlas

The Flink anomaly sink continuously writes enriched anomalies to Atlas:

```javascript
use analytics
db.zone_anomalies.find().sort({ window_time: -1 }).limit(5)
```

Each document includes the `anomaly_reason` (LLM-generated explanation), top matching event chunks from vector search, and the original anomaly metrics.

## Troubleshooting

<details>
<summary>Click to expand</summary>

- **No anomalies detected?** Check that data generation is running (`uv run datagen`). The first anomaly should appear after both data generation and the anomaly detection pipeline have been running for about 5 minutes (one full tumbling window).

- **Empty `events.knowledge_base`?** Verify the `event_knowledge_base_population` ASP processor is running:
  1. Check the ASP instance status in Atlas UI under Stream Processing
  2. Verify the `events.calendar` collection has the 10 seed events
  3. Check `events.validation_dlq` for any failed documents

- **Voyage AI embedding errors?** Verify your API key. Ensure Voyage AI is enabled on your Atlas project and the API key is valid.

- **Vector search returns no results?** The Atlas Vector Search index on `events.knowledge_base` may still be building. Check the index status in Atlas UI; it should show "READY". Also verify that embedding dimensions match (both ASP and Flink should produce 1024-dimension vectors from `voyage-4`).

- **`analytics.zone_traffic` not populating?** The pipeline needs the `ride_requests` table to have data flowing. Verify data generation is running and check the Flink statement status in the SQL workspace.

- **`dispatch_log_ingestion` not capturing dispatch logs?** The `completed_actions` Kafka topic must have data. This only happens after the agent successfully dispatches boats (Step 7). Check the topic in Confluent Cloud UI.

- **Error when running the RAG enrichment query?** `The window function requires the timecol is a time attribute type...`
  - Run this and retry:
  ```sql
  ALTER TABLE ride_requests
  MODIFY (WATERMARK FOR request_ts AS request_ts - INTERVAL '5' SECOND);
  ```

- `Runtime received bad response code 403` error?
  - Ensure you've activated the configured model in your AWS account. Default is Claude Sonnet 4.6 (via the `global.` cross-region inference profile). Check `TF_VAR_bedrock_model_id` in `.env`.

- **`/ by zero` error in anomalies-enriched-insert?** The anomaly detection model occasionally outputs `expected_requests = 0`. Pull the latest code; this is fixed with `NULLIF` protection.

- **Dispatch log empty but completed_actions has data?** The ASP `dispatch_log_ingestion` processor may have failed. Check its status in Atlas UI under Stream Processing and restart if needed.

For more detailed troubleshooting, see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

</details>

## Cleanup

```bash
uv run destroy
```

Choose your cloud provider when prompted. The destroy script will:

1. **Delete 7 Flink streaming statements** managed outside Terraform (via REST API)
2. **Delete Kafka topics** to prevent stale schema data on re-deploy
3. **Stop and delete ASP processors** and the ASP instance (if Atlas Admin API keys are in `.env`)
4. **Tear down MCP server** (ECS Express service)
5. **Destroy all Terraform resources** (Flink DDL resources, connections, tables)

> **Note:** Atlas collections (`events.knowledge_base`, `analytics.zone_traffic`, `fleet.dispatch_log`) are not managed by Terraform or ASP teardown. To remove them, use the Atlas UI or `mongosh`.
