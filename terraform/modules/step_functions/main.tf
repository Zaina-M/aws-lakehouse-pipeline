resource "aws_sns_topic" "alerts" {
  name = "${var.project}-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-${var.environment}-pipeline"
  role_arn = var.sfn_role_arn

  definition = templatefile("${path.module}/pipeline.json.tftpl", {
    glue_job_name = var.glue_job_name
    crawler_name  = var.crawler_name
    alert_topic   = aws_sns_topic.alerts.arn
  })

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${var.project}-${var.environment}-pipeline"
  retention_in_days = 30
}
