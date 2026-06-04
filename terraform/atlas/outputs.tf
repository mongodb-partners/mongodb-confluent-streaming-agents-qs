output "atlas_cluster_connection_string" {
  description = "mongodb+srv:// connection string for the Atlas cluster"
  value       = mongodbatlas_advanced_cluster.cluster.connection_strings.standard_srv
  sensitive   = true
}

output "atlas_cluster_name" {
  description = "Name of the provisioned cluster"
  value       = mongodbatlas_advanced_cluster.cluster.name
}

output "atlas_db_username" {
  description = "Database user created for the cluster"
  value       = mongodbatlas_database_user.app_user.username
}

output "atlas_db_password" {
  description = "Database user password (sensitive)"
  value       = var.atlas_db_password
  sensitive   = true
}
