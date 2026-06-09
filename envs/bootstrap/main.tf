data "aws_caller_identity" "current" {}

locals {
  # Must match envs/dev/backend.tf exactly.
  state_bucket = "${var.project}-${var.environment}-${data.aws_caller_identity.current.account_id}-tfstate"
  lock_table   = "${var.project}-${var.environment}-tflock"
}

# --- Remote state backend for the dev stack -------------------------------

resource "aws_s3_bucket" "tfstate" {
  bucket = local.state_bucket
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tflock" {
  name         = local.lock_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

# --- Pipeline identity (OIDC provider + deploy role) ----------------------
# Lives here, not in the dev stack, so the pipeline never manages the trust
# anchor it authenticates with.

module "github_actions" {
  source      = "../../terraform/modules/github_actions"
  project     = var.project
  environment = var.environment
  github_repo = var.github_repo
}
