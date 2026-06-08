variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "glue_role_arn" {
  type = string
}

variable "scripts_bucket" {
  type = string
}

variable "raw_bucket" {
  type = string
}

variable "dwh_bucket" {
  type = string
}

variable "archive_bucket" {
  type = string
}

variable "rejects_bucket" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "timeout_minutes" {
  type = number
}

variable "max_retries" {
  type = number
}

variable "max_concurrent_runs" {
  type = number
}

variable "worker_type" {
  type = string
}

variable "num_workers" {
  type = number
}
