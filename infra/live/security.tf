# S0: security groups and rules.
#
# Decision point: who may reach the app (ingress scope)? Record it in decisions/.
# Traffic between hosts (ports in contracts/runtime.yaml):
#   app -> platform  MLflow
#   app, platform -> llm  chat and embeddings
#   app, platform -> rds  Postgres
# There is no SSH in this lab: use SSM Session Manager for a shell and port forwarding for UIs.

# ---------------------------------------------------------------------------
# App host — Traefik on port 80, reachable from ingress_cidrs
# ---------------------------------------------------------------------------
resource "aws_security_group" "app" {
  name        = "lab-app"
  description = "App host: Traefik port 80 from allowed CIDRs"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-app" }
}

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = each.value
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "app_all" {
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# ---------------------------------------------------------------------------
# Platform host — MLflow on port 5000, Dagster on 3000 (from app only)
# ---------------------------------------------------------------------------
resource "aws_security_group" "platform" {
  name        = "lab-platform"
  description = "Platform host: MLflow (5000) and Dagster (3000) from app"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-platform" }
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow" {
  security_group_id            = aws_security_group.platform.id
  referenced_security_group_id = aws_security_group.app.id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "platform_all" {
  security_group_id = aws_security_group.platform.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# ---------------------------------------------------------------------------
# LLM host — chat (8080) and embed (8081) from app and platform
# ---------------------------------------------------------------------------
resource "aws_security_group" "llm" {
  name        = "lab-llm"
  description = "LLM host: chat and embed from app and platform"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-llm" }
}

resource "aws_vpc_security_group_ingress_rule" "llm_from_app" {
  security_group_id            = aws_security_group.llm.id
  referenced_security_group_id = aws_security_group.app.id
  from_port                    = 8080
  to_port                      = 8081
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "llm_from_platform" {
  security_group_id            = aws_security_group.llm.id
  referenced_security_group_id = aws_security_group.platform.id
  from_port                    = 8080
  to_port                      = 8081
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "llm_all" {
  security_group_id = aws_security_group.llm.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# ---------------------------------------------------------------------------
# RDS — Postgres 5432 from app and platform
# ---------------------------------------------------------------------------
resource "aws_security_group" "rds" {
  name        = "lab-rds"
  description = "RDS Postgres from app and platform"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-rds" }
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_app" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.app.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_platform" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.platform.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "rds_all" {
  security_group_id = aws_security_group.rds.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
