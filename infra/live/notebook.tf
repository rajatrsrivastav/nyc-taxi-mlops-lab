module "notebook" {
  count     = var.github_repo == null ? 0 : 1
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.selected.id
  repo_url  = "https://github.com/${var.github_repo}.git"

  env = {
    MLFLOW_TRACKING_URI = "http://${module.platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    REDSHIFT_DB_USER    = "labadmin"
    AWS_DEFAULT_REGION  = "us-east-1"
    LAB_ARTIFACT_BUCKET = module.artifact_bucket.name
  }
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  count                        = var.github_repo == null ? 0 : 1
  security_group_id            = aws_security_group.platform.id
  referenced_security_group_id = module.notebook[0].security_group_id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

output "notebook_name" {
  value = one(module.notebook[*].name)
}
