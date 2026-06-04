# Agents Outputs
output "lab_name" {
  value = "agents"
}

# Core infrastructure outputs (pass-through from remote state)
output "confluent_environment_id" {
  value = data.terraform_remote_state.core.outputs.confluent_environment_id
}

output "confluent_kafka_cluster_id" {
  value = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
}

output "confluent_kafka_bootstrap_endpoint" {
  value = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_bootstrap_endpoint
}

output "confluent_schema_registry_id" {
  value = data.terraform_remote_state.core.outputs.confluent_schema_registry_id
}

output "confluent_schema_registry_endpoint" {
  value = data.terraform_remote_state.core.outputs.confluent_schema_registry_rest_endpoint
}

output "confluent_flink_compute_pool_id" {
  value = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
}

# Agent-specific outputs
output "ride_requests_table_id" {
  value       = confluent_flink_statement.ride_requests_table.id
  description = "Flink statement ID for ride_requests table"
}

output "documents_vectordb_table_id" {
  value       = confluent_flink_statement.documents_vectordb.id
  description = "Flink statement ID for documents_vectordb table"
}

output "mongodb_connection_name" {
  value       = "mongodb-connection"
  description = "MongoDB connection name"
}

output "anomalies_per_zone_table_id" {
  value       = confluent_flink_statement.anomalies_per_zone_table.id
  description = "Flink statement ID for anomalies_per_zone table"
}

output "voyage_connection_id" {
  value       = confluent_flink_statement.voyage_connection.id
  description = "Flink statement ID for Voyage AI connection"
}

output "voyage_query_embedding_model_id" {
  value       = confluent_flink_statement.voyage_query_embedding_model.id
  description = "Flink statement ID for Voyage query embedding model"
}

# DML statement outputs removed — these are now managed by deploy.py via Flink REST API
