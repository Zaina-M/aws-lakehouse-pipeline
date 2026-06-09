output "role_arn" {
  value       = aws_iam_role.github_actions.arn
  description = "ARN of the IAM role GitHub Actions assumes via OIDC — use as role-to-assume in the deploy workflow"
}
