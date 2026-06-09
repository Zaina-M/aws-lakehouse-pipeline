aws_region  = "eu-west-1"
project     = "lakehouse"
environment = "dev"
alert_email = "abdullaizainab38@gmail.com"
github_repo = "Zaina-M/aws-lakehouse-pipeline"

glue_job_worker_type         = "G.1X"
glue_job_num_workers         = 2
glue_job_timeout_minutes     = 30
glue_job_max_retries         = 3
glue_job_max_concurrent_runs = 5
