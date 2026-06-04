variable "mongodb_connection_string" {
  description = "MongoDB Atlas connection string (e.g. mongodb+srv://cluster0.example.mongodb.net/)"
  type        = string
  sensitive   = true
}

variable "mongodb_username" {
  description = "MongoDB Atlas username"
  type        = string
  sensitive   = true
}

variable "mongodb_password" {
  description = "MongoDB Atlas password"
  type        = string
  sensitive   = true
}

variable "mcp_server_url" {
  description = "MongoDB MCP server URL (ECS Express Mode, e.g. https://mo-XXXXX.ecs.us-east-1.on.aws)"
  type        = string
}

variable "mcp_auth_token" {
  description = "Bearer token for MongoDB MCP server authentication"
  type        = string
  sensitive   = true
}

variable "voyage_api_key" {
  description = "Voyage AI API key for embeddings (via ai.mongodb.com)"
  type        = string
  sensitive   = true
}

variable "voyage_api_endpoint" {
  description = "Voyage AI embeddings endpoint URL. Defaults to MongoDB Atlas's hosted Voyage proxy at ai.mongodb.com; override to point at api.voyageai.com or another OpenAI-compatible Voyage endpoint."
  type        = string
  default     = "https://ai.mongodb.com/v1/embeddings"
}
