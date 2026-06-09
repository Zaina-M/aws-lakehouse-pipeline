# Bootstrap (run once, with admin credentials)

This stack owns the things the CI pipeline **cannot** create for itself:

- the S3 bucket + DynamoDB table that hold the dev stack's remote state
- the GitHub OIDC provider and the IAM role the pipeline assumes

It uses **local state** on purpose — it creates the backend that everything
else uses, so it can't depend on that backend existing. Run it manually; CI
never touches it.

## First-time setup

The OIDC provider and deploy role already exist (created by an earlier local
apply of the dev stack), so import them instead of recreating:

```bash
cd envs/bootstrap
terraform init

terraform import module.github_actions.aws_iam_openid_connect_provider.github \
  arn:aws:iam::884596874091:oidc-provider/token.actions.githubusercontent.com
terraform import module.github_actions.aws_iam_role.github_actions \
  lakehouse-dev-github-actions
terraform import module.github_actions.aws_iam_role_policy.github_actions_deploy \
  lakehouse-dev-github-actions:deploy

terraform apply
```

`terraform.tfvars` is auto-loaded — no `-var-file` flag needed.
`apply` creates the state bucket + lock table and updates the deploy role with
the permissions CI needs. Confirm the bucket name in the output matches
`envs/dev/backend.tf`.
