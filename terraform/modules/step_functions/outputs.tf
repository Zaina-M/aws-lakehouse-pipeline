output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
