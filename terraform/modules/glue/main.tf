resource "aws_glue_catalog_database" "this" {
  name = var.glue_database_name
}

resource "aws_glue_job" "etl" {
  name         = "${var.project}-${var.environment}-etl"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  worker_type      = var.worker_type
  number_of_workers = var.num_workers
  timeout          = var.timeout_minutes
  max_retries      = var.max_retries

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_jobs/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"               = "python"
    "--enable-glue-datacatalog"    = "true"
    "--datalake-formats"           = "delta"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"             = "true"
    "--extra-py-files"             = "s3://${var.scripts_bucket}/glue_jobs/etl_libs.zip"
    "--additional-python-modules"  = "openpyxl==3.1.2"
    "--RAW_BUCKET"                 = var.raw_bucket
    "--DWH_BUCKET"                 = var.dwh_bucket
    "--ARCHIVE_BUCKET"             = var.archive_bucket
    "--REJECT_BUCKET"              = var.rejects_bucket
    "--DATABASE_NAME"              = var.glue_database_name
    "--DATASET_TYPE"               = ""
    "--RUN_DATE"                   = ""
  }

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }
}

resource "aws_glue_crawler" "this" {
  name          = "${var.project}-${var.environment}-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.this.name

  delta_target {
    delta_tables           = [
      "s3://${var.dwh_bucket}/products/",
      "s3://${var.dwh_bucket}/orders/",
      "s3://${var.dwh_bucket}/order_items/",
    ]
    write_manifest         = false
    create_native_delta_table = true
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}
