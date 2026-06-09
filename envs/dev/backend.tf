# Remote state — shared between local runs and CI so resources are never
# re-created from an empty state. The bucket and lock table are created once
# by `envs/bootstrap` (see envs/bootstrap/README or the runbook).
#
# Bucket naming matches the deploy role's S3 wildcard (lakehouse-dev-*),
# so no extra IAM grant is needed for state access.

terraform {
  backend "s3" {
    bucket         = "lakehouse-dev-884596874091-tfstate"
    key            = "lakehouse/dev/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "lakehouse-dev-tflock"
  }
}
