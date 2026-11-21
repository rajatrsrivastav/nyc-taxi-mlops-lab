# S0: expose the outputs listed in contracts/runtime.yaml under terraform_outputs.
# scripts/register-runner.sh, scripts/catch-up.sh and the grader read them by those names.

output "app_public_ip" {
  description = "Elastic IP of the app host"
  value       = module.app.public_ip
}

output "app_instance_id" {
  description = "Instance ID of the app host"
  value       = module.app.instance_id
}

output "platform_instance_id" {
  description = "Instance ID of the platform host"
  value       = module.platform.instance_id
}

output "llm_instance_id" {
  description = "Instance ID of the LLM host"
  value       = module.llm.instance_id
}

output "rds_endpoint" {
  description = "RDS Postgres endpoint (hostname)"
  value       = module.rds.endpoint
}

output "artifact_bucket" {
  description = "Name of the S3 artifact bucket"
  value       = module.artifact_bucket.name
}

# Used by later stages (platform.yaml CD, catch-up.sh) to set MLFLOW_TRACKING_URI.
output "platform_private_ip" {
  description = "Private IP of the platform host (for MLFLOW_TRACKING_URI)"
  value       = module.platform.private_ip
}

# Used by later stages (llm.yaml CD, catch-up.sh) to set LLM_HOST.
output "llm_private_ip" {
  description = "Private IP of the LLM host"
  value       = module.llm.private_ip
}
