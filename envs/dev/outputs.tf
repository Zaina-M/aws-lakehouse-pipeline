output "raw_bucket_name" {
  value = module.s3.raw_bucket_name
}

output "scripts_bucket_name" {
  value = module.s3.scripts_bucket_name
}

output "dwh_bucket_name" {
  value = module.s3.dwh_bucket_name
}

output "archive_bucket_name" {
  value = module.s3.archive_bucket_name
}

output "rejects_bucket_name" {
  value = module.s3.rejects_bucket_name
}

output "glue_job_name" {
  value = module.glue.job_name
}

output "glue_database_name" {
  value = module.glue.database_name
}

output "crawler_name" {
  value = module.glue.crawler_name
}

output "state_machine_arn" {
  value = module.step_functions.state_machine_arn
}

output "alert_topic_arn" {
  value = module.step_functions.alert_topic_arn
}
