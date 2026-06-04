variable "cloud_region" {
  description = "Region for deployment (must support MongoDB Atlas M0 free tier)"
  type        = string
  default     = "us-east-1"
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "owner_email" {
  description = "Email address of the resource owner for tagging purposes"
  type        = string
  default     = ""
}

variable "aws_bedrock_access_key" {
  description = "AWS Access Key ID for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_bedrock_secret_key" {
  description = "AWS Secret Access Key for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_session_token" {
  description = "AWS Session Token for temporary credentials (required when access key starts with ASIA)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "bedrock_model_id" {
  description = "AWS Bedrock model ID for the LLM text generation connection"
  type        = string
  default     = "global.anthropic.claude-sonnet-4-6"
}

# MongoDB Atlas variables live in terraform/atlas/variables.tf — that module
# has its own state so the cluster can persist across core+agents redeploys.
