variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "github_repo" {
  type        = string
  description = "GitHub repository in owner/repo format (e.g. Zaina-M/aws-lakehouse-pipeline)"
}
