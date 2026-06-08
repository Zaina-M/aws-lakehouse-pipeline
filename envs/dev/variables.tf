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

variable "glue_job_timeout_minutes" {
  type    = number
  default = 60
}

variable "glue_job_max_retries" {
  type    = number
  default = 0
}

variable "glue_job_max_concurrent_runs" {
  type    = number
  default = 3
}

variable "glue_job_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_job_num_workers" {
  type    = number
  default = 2
}

variable "alert_email" {
  type        = string
  description = "Email address to receive pipeline failure notifications"
}
