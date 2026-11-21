module "redshift" {
  source     = "../modules/redshift"
  subnet_ids = data.aws_subnets.default.ids
}

output "redshift_cluster_id" {
  description = "Redshift cluster identifier for the Data API"
  value       = module.redshift.cluster_id
}

output "redshift_database" {
  description = "Redshift database for the S1 pipeline"
  value       = module.redshift.database
}
