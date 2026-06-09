variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "project" {
  type    = string
  default = "lakehouse"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "github_repo" {
  type        = string
  description = "GitHub repository in owner/repo format (e.g. Zaina-M/aws-lakehouse-pipeline)"
}
