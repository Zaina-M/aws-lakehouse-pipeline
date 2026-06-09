output "state_bucket_name" {
  value       = aws_s3_bucket.tfstate.id
  description = "S3 bucket holding the dev stack's Terraform state — matches envs/dev/backend.tf"
}

output "lock_table_name" {
  value       = aws_dynamodb_table.tflock.name
  description = "DynamoDB table used for state locking"
}

output "github_actions_role_arn" {
  value       = module.github_actions.role_arn
  description = "Paste this ARN as AWS_DEPLOY_ROLE_ARN secret / role-to-assume in deploy.yml"
}
