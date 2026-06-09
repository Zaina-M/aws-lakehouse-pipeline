terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Bootstrap uses LOCAL state on purpose: it creates the very S3 bucket and
  # lock table that the dev stack uses as its remote backend, so it cannot
  # depend on that backend existing. Commit the resulting terraform.tfstate
  # in this folder, or keep it with the admin who runs the bootstrap.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
