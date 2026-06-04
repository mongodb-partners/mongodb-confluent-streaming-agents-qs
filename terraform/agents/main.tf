# Reference to core infrastructure
data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

# Local values
locals {
  cloud_region = data.terraform_remote_state.core.outputs.cloud_region

  # Requires a user-provided MongoDB Atlas M10+ cluster (no workshop defaults)
  # Strip scheme (mongodb+srv://) and any embedded credentials (user:pass@) from the
  # connection string, since Flink's MONGODB connection type expects just the host in
  # 'endpoint' with username/password provided separately.
  _raw_conn              = var.mongodb_connection_string
  _after_scheme          = length(regexall("://", local._raw_conn)) > 0 ? element(split("://", local._raw_conn), 1) : local._raw_conn
  _after_creds           = length(regexall("@", local._after_scheme)) > 0 ? join("@", slice(split("@", local._after_scheme), 1, length(split("@", local._after_scheme)))) : local._after_scheme
  effective_mongodb_conn = "mongodb+srv://${local._after_creds}"
  effective_mongodb_user = var.mongodb_username
  effective_mongodb_pass = var.mongodb_password
}

# Get organization data
data "confluent_organization" "main" {}

# Get Flink region data
data "confluent_flink_region" "flink_region" {
  cloud  = "AWS"
  region = local.cloud_region
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. MongoDB Connection
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "mongodb_connection_statement" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "mongodb-connection-create"

  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`mongodb-connection`
    WITH (
      'type' = 'MONGODB',
      'endpoint' = '${local.effective_mongodb_conn}',
      'username' = '${local.effective_mongodb_user}',
      'password' = '${local.effective_mongodb_pass}'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. Enhanced Vector DB Table (events.knowledge_base, 1024-dim)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "documents_vectordb" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "documents-vectordb-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS documents_vectordb (
      document_id STRING,
      chunk STRING,
      embedding ARRAY<FLOAT>,
      event_name STRING,
      event_time_start TIMESTAMP(3),
      event_time_end TIMESTAMP(3),
      venue STRING,
      expected_attendance INT,
      zone STRING,
      event_type STRING,
      impact_level STRING
    ) WITH (
      'connector' = 'mongodb',
      'mongodb.connection' = 'mongodb-connection',
      'mongodb.database' = 'events',
      'mongodb.collection' = 'knowledge_base',
      'mongodb.index' = 'vector_index',
      'mongodb.embedding_column' = 'embedding',
      'mongodb.numCandidates' = '500'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.mongodb_connection_statement
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. MongoDB MCP Connection
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "mongodb_mcp_connection" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "mongodb-mcp-connection-create"

  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`mongodb-mcp-connection`
    WITH (
      'type' = 'MCP_SERVER',
      'endpoint' = '${var.mcp_server_url}/mcp',
      'token' = '${var.mcp_auth_token}',
      'transport-type' = 'STREAMABLE_HTTP'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 4. MongoDB MCP Model (AWS/Bedrock)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "mongodb_mcp_model" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "mongodb-mcp-model-create"

  statement = <<-EOT
    CREATE MODEL IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`mongodb_mcp_model`
    INPUT (prompt STRING)
    OUTPUT (response STRING)
    WITH (
      'provider' = 'bedrock',
      'task' = 'text_generation',
      'bedrock.connection' = '${data.terraform_remote_state.core.outputs.llm_connection_name}',
      'bedrock.params.max_tokens' = '50000',
      'mcp.connection' = 'mongodb-mcp-connection'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = "default"
  }

  depends_on = [
    confluent_flink_statement.mongodb_mcp_connection
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 6. ride_requests Table (with WATERMARK)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "ride_requests_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "ride-requests-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`ride_requests` (
      `request_id` STRING NOT NULL,
      `customer_email` STRING NOT NULL,
      `pickup_zone` STRING NOT NULL,
      `drop_off_zone` STRING NOT NULL,
      `price` DOUBLE NOT NULL,
      `number_of_passengers` INT NOT NULL,
      `request_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      WATERMARK FOR `request_ts` AS `request_ts` - INTERVAL '5' SECOND
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 7. anomalies_per_zone Table (explicit CREATE TABLE, replaces CTAS)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "anomalies_per_zone_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "anomalies-per-zone-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`anomalies_per_zone` (
      `pickup_zone` STRING,
      `window_time` TIMESTAMP(3),
      `request_count` BIGINT,
      `total_passengers` BIGINT,
      `total_revenue` DECIMAL(10, 2),
      `expected_requests` BIGINT,
      `upper_bound` DOUBLE,
      `lower_bound` DOUBLE,
      `is_surge` BOOLEAN
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.ride_requests_table
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 8. anomalies_sink Table (plain Kafka table — MongoDB sinking via Confluent Sink Connector)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "anomalies_sink_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "anomalies-sink-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`anomalies_sink` (
      `pickup_zone` STRING,
      `window_time` TIMESTAMP(3),
      `request_count` BIGINT,
      `expected_requests` BIGINT,
      `anomaly_reason` STRING,
      `top_chunk_1` STRING,
      `top_chunk_2` STRING,
      `top_chunk_3` STRING,
      `detected_at` TIMESTAMP(3)
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.ride_requests_table
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 9. zone_traffic_sink Table (plain Kafka table — MongoDB sinking via Confluent Sink Connector)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "zone_traffic_sink_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "zone-traffic-sink-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`zone_traffic_sink` (
      `zone` STRING,
      `window_start` TIMESTAMP(3),
      `window_end` TIMESTAMP(3),
      `request_count` BIGINT,
      `total_passengers` BIGINT,
      `total_revenue` DECIMAL(10, 2)
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.ride_requests_table
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 10. windowed_traffic View (shared TUMBLE window)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "windowed_traffic_view" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "windowed-traffic-create-view"

  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`windowed_traffic` AS
    SELECT
        window_start,
        window_end,
        window_time,
        pickup_zone,
        COUNT(*) AS request_count,
        SUM(number_of_passengers) AS total_passengers,
        SUM(CAST(price AS DECIMAL(10, 2))) AS total_revenue
    FROM TABLE(
        -- Fully-qualified table ref. The TUMBLE arg was previously
        -- unqualified (`TABLE ride_requests`) which relied on the surrounding
        -- session catalog/database. Brittle if anything ever changes the
        -- session defaults.
        TUMBLE(
            TABLE `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`ride_requests`,
            DESCRIPTOR(request_ts),
            INTERVAL '5' MINUTE
        )
    )
    GROUP BY window_start, window_end, window_time, pickup_zone;
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.ride_requests_table
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 11–13. DML Statements (INSERT/CTAS) — managed by deploy.py via Flink REST API
#
# The following 4 streaming statements are NOT managed by Terraform because
# the Confluent provider (v2.66.0) cannot handle statements that go to
# STOPPING state when no upstream data is flowing.  Instead, deploy.py
# creates them via the Flink REST API after ASP setup completes:
#
#   - zone-traffic-sink-insert
#   - anomaly-detection-insert
#   - anomalies-enriched-ctas
#   - anomalies-sink-insert
#
# SQL templates live in terraform/agents/sql/*.sql
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# 14. Voyage AI Connection (OpenAI-compatible → ai.mongodb.com)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "voyage_connection" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "voyage-connection-create"

  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`voyage_connection`
    WITH (
      'type' = 'openai',
      'endpoint' = '${var.voyage_api_endpoint}',
      'api-key' = '${var.voyage_api_key}'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 15. Voyage Query Embedding Model (voyage-4, 1024-dim)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "voyage_query_embedding_model" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "voyage-query-embedding-model-create"

  statement = <<-EOT
    CREATE MODEL IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`voyage_query_embedding`
    INPUT (text STRING)
    OUTPUT (embedding ARRAY<FLOAT>)
    WITH (
      'provider' = 'openai',
      'openai.connection' = 'voyage_connection',
      'task' = 'embedding',
      'openai.model_version' = 'voyage-4',
      'openai.output_format' = 'OPENAI-EMBED'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.voyage_connection
  ]
}

