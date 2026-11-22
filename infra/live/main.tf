# S0: wire the root module.
#
# Given modules in ../modules:
#   host    one EC2 host per role in contracts/runtime.yaml (platform, app, llm)
#   rds     Postgres for Dagster, MLflow and pgvector
#   bucket  your artifact bucket (never aws_s3_bucket: see AGENTS.md)
#   secret  the lab/wiki secret container
#
# variables.tf has the inputs; outputs.tf must expose what runtime.yaml lists under
# terraform_outputs. Ask your agent to propose the wiring, then check it against those files.

# ---------------------------------------------------------------------------
# Default VPC and subnets (Learner Lab provides one in us-east-1)
# ---------------------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Pick the subnet in the chosen AZ for the hosts.
data "aws_subnet" "selected" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.availability_zone
  default_for_az    = true
}

# ---------------------------------------------------------------------------
# Hosts (contracts/runtime.yaml: platform, app, llm)
# ---------------------------------------------------------------------------
module "app" {
  source             = "../modules/host"
  role               = "app"
  instance_type      = var.app_instance_type
  subnet_id          = data.aws_subnet.selected.id
  security_group_ids = [aws_security_group.app.id]
  elastic_ip         = true
}

module "platform" {
  source             = "../modules/host"
  role               = "platform"
  instance_type      = var.platform_instance_type
  subnet_id          = data.aws_subnet.selected.id
  security_group_ids = [aws_security_group.platform.id]
  elastic_ip         = true
}

module "llm" {
  source             = "../modules/host"
  role               = "llm"
  instance_type      = var.llm_instance_type
  subnet_id          = data.aws_subnet.selected.id
  security_group_ids = [aws_security_group.llm.id]
  elastic_ip         = true
}

# ---------------------------------------------------------------------------
# RDS — Postgres for Dagster, MLflow and pgvector
# ---------------------------------------------------------------------------
module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.default.ids
  security_group_ids = [aws_security_group.rds.id]
}

# ---------------------------------------------------------------------------
# Artifact bucket (MLflow artifacts, training sets)
# ---------------------------------------------------------------------------
module "artifact_bucket" {
  source = "../modules/bucket"
  name   = "lab-artifacts"
}

# ---------------------------------------------------------------------------
# Secrets Manager — lab/wiki secret container
# ---------------------------------------------------------------------------
module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki"
  description = "Wiki URL and token for the assistant (S7)"
}

# Grafana credentials are written out of band. Terraform manages only the container.
module "grafana_secret" {
  source      = "../modules/secret"
  name        = "lab/grafana"
  description = "Grafana Cloud metrics and rules credentials (S5)"
}
