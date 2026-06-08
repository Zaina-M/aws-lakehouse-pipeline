# Remote state — uncomment and replace placeholders when ready.
#
# Pre-requisites:
#   aws s3 mb s3://YOUR-TF-STATE-BUCKET --region eu-west-1
#   aws dynamodb create-table --table-name terraform-state-lock \
#     --attribute-definitions AttributeName=LockID,AttributeType=S \
#     --key-schema AttributeName=LockID,KeyType=HASH \
#     --billing-mode PAY_PER_REQUEST
#
# Then run: terraform init -reconfigure

# terraform {
#   backend "s3" {
#     bucket         = "YOUR-TF-STATE-BUCKET"
#     key            = "lakehouse/dev/terraform.tfstate"
#     region         = "eu-west-1"
#     encrypt        = true
#     dynamodb_table = "terraform-state-lock"
#   }
# }
