module "s3" {
  source      = "../../terraform/modules/s3"
  project     = var.project
  environment = var.environment
}

module "iam" {
  source      = "../../terraform/modules/iam"
  project     = var.project
  environment = var.environment

  raw_bucket_arn     = module.s3.raw_bucket_arn
  scripts_bucket_arn = module.s3.scripts_bucket_arn
  dwh_bucket_arn     = module.s3.dwh_bucket_arn
  archive_bucket_arn = module.s3.archive_bucket_arn
  rejects_bucket_arn = module.s3.rejects_bucket_arn
  alert_topic_arn    = module.step_functions.alert_topic_arn
}

module "glue" {
  source      = "../../terraform/modules/glue"
  project     = var.project
  environment = var.environment

  glue_role_arn       = module.iam.glue_role_arn
  scripts_bucket      = module.s3.scripts_bucket_name
  raw_bucket          = module.s3.raw_bucket_name
  dwh_bucket          = module.s3.dwh_bucket_name
  archive_bucket      = module.s3.archive_bucket_name
  rejects_bucket      = module.s3.rejects_bucket_name
  glue_database_name  = "${var.project}_db"
  timeout_minutes     = var.glue_job_timeout_minutes
  max_retries         = var.glue_job_max_retries
  max_concurrent_runs = var.glue_job_max_concurrent_runs
  worker_type         = var.glue_job_worker_type
  num_workers         = var.glue_job_num_workers
}

module "github_actions" {
  source      = "../../terraform/modules/github_actions"
  project     = var.project
  environment = var.environment
  github_repo = var.github_repo
}

module "step_functions" {
  source      = "../../terraform/modules/step_functions"
  project     = var.project
  environment = var.environment

  sfn_role_arn  = module.iam.sfn_role_arn
  glue_job_name = module.glue.job_name
  crawler_name  = module.glue.crawler_name
  alert_email   = var.alert_email
}
